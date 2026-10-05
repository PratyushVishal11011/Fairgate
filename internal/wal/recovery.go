package wal

import (
	"FairGate/internal/wire"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

func Recover(path string) ([]wire.Event, error) {
	//Open the wal file
	base, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		return nil, err
	}
	if err := base.Close(); err != nil {
		return nil, err
	}

	segments, err := listSegments(path)
	if err != nil {
		return nil, err
	}

	//close the file on return
	//defer file.Close()

	var events []wire.Event
	for i, seg := range segments {
		isLast := i == len(segments)-1
		file, err := os.OpenFile(seg.path, os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		recovered, err := recoverSegment(file, isLast)
		closeErr := file.Close()

		if err != nil {
			return nil, err
		}

		if closeErr != nil {
			return nil, closeErr
		}
		events = append(events, recovered...)
	}

	return events, nil
}

func recoverSegment(file *os.File, isLast bool) ([]wire.Event, error) {
	var offset int64
	var events []wire.Event
	//continues reading records until it reaches the end of the file, encounters an incomplete record, or encounters an error.
	for {
		//every record begins with an 8 bit header
		var header [8]byte

		_, err := io.ReadFull(file, header[:])

		//End loop when there are no more events to read
		if err == io.EOF {
			break
		}

		if err == io.ErrUnexpectedEOF {
			//Incomplete header at the end of WAL
			if err := file.Truncate(offset); err != nil {
				return nil, err
			}
			if err := file.Sync(); err != nil {
				return nil, err
			}
			break
		}

		if err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint32(header[0:4])
		checksum := binary.BigEndian.Uint32(header[4:8])

		//validation also prevents the function from allocating an excessively large payload buffer due to a corrupted length field.
		if length == 0 || length > maxRecordSize {
			return nil, fmt.Errorf("invalid record length at offset: %d", offset)
		}

		//creates a byte slice large enough to hold the JSON payload.
		//Reads exactly length bytes from the file into the payload slice.
		//If the server crashes while writing the payload, fewer bytes may be available than expected.
		payload := make([]byte, int(length))
		_, err = io.ReadFull(file, payload)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Checks if the payload is incomplete
			if !isLast {
				return nil, fmt.Errorf(
					"incomplete record in non-final WAL segment at offset: %d",
					offset,
				)
			}

			if err := file.Truncate(offset); err != nil {
				return nil, err
			}
			//Truncates the WAL to the start of the incomplete record
			//This essentially removes the header and partial payload
			if err := file.Sync(); err != nil {
				return nil, err
			}
			break
		}
		if err != nil {
			return nil, err
		}

		//calculate crc32 checksum
		//calculated checksum is compared to the stored checksum
		//mismatch = data corruption
		if crc32.ChecksumIEEE(payload) != checksum {
			return nil, fmt.Errorf("invalid checksum at offset: %d", offset)
		}

		//converts the JSON bytes into event object
		var event wire.Event
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, fmt.Errorf("invalid WAL event at offset: %d", offset)
		}

		//Add the successfully decoded event to the events slice.
		events = append(events, event)
		//update the offset
		offset += int64(len(header)) + int64(length)
	}
	return events, nil

}

func truncateTail(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return err
	}
	return file.Sync()
}
