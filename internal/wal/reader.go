package wal

import (
	"FairGate/internal/wire"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type Position struct {
	//to identify a location in a WAL
	SegmentId uint64
	Offset    int64
}

type Record struct {
	//contains an event and its WAL boundaries
	Event wire.Event
	Start Position
	End   Position
}

type Reader struct {
	wal       *WAL
	position  Position
	file      *os.File
	segmentId uint64
}

func NewReader(wal *WAL, position Position) *Reader {
	return &Reader{
		wal:      wal,
		position: position,
	}
}

func (reader *Reader) closeFile() error {
	if reader.file == nil {
		return nil
	}
	err := reader.file.Close()
	reader.file = nil

	return err
}

func (reader *Reader) openSegment() error {
	if reader.file != nil && reader.segmentId == reader.position.SegmentId {
		return nil
	}
	if err := reader.closeFile(); err != nil {
		return err
	}

	path := reader.wal.path

	if reader.position.SegmentId > 0 {
		path = segmentPath(reader.wal.path, reader.position.SegmentId)
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	reader.file = file
	reader.segmentId = reader.position.SegmentId

	return nil
}

func (reader *Reader) Next() (Record, error) {
	//modifying this function to make sure that when the current segment has no more durable data and there is a later durable segment,
	//close the current file, advance SegmentId, reset Offset to 0, and continue reading.
	for {
		if err := reader.openSegment(); err != nil {
			return Record{}, err
		}
		start := reader.position

		//debug ONLY
		//TODO: Come up with a better solution for this
		fileInfo, err := reader.file.Stat()

		if reader.position.Offset+8 > fileInfo.Size() {
			durable := reader.wal.DurableEnd()
			if reader.position.SegmentId < durable.SegmentId {
				segments, err := listSegments(reader.wal.path)
				if err != nil {
					return Record{}, err
				}
				next := uint64(0)
				found := false
				for _, seg := range segments {
					if seg.id > reader.position.SegmentId {
						next, found = seg.id, true
						break
					}
				}
				if !found {
					return Record{}, io.EOF
				}
				if err := reader.closeFile(); err != nil {
					return Record{}, err
				}

				reader.position = Position{
					SegmentId: next,
					Offset:    0,
				}
				continue
			}
			return Record{}, io.EOF
		}

		header := make([]byte, 8)
		_, err = reader.file.ReadAt(header, reader.position.Offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return Record{}, io.EOF
			}
			return Record{}, err
		}

		payloadLength := binary.BigEndian.Uint32(header[0:4])

		if payloadLength > maxRecordSize {
			return Record{}, fmt.Errorf(
				"WAL record too large at segment %d Offset %d",
				reader.position.SegmentId,
				reader.position.Offset,
			)
		}

		recordSize := int64(8 + payloadLength)

		if !reader.canRead(recordSize) {
			return Record{}, io.EOF
		}

		expectedCRC := binary.BigEndian.Uint32(header[4:8])

		payload := make([]byte, payloadLength)
		payloadOffset := reader.position.Offset + 8

		_, err = reader.file.ReadAt(payload, payloadOffset)

		if err != nil {
			if errors.Is(err, io.EOF) {
				return Record{}, fmt.Errorf(
					"incomplete WAL record at segment %d Offset %d",
					reader.position.SegmentId,
					reader.position.Offset,
				)
			}
			return Record{}, err
		}

		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != expectedCRC {
			return Record{}, fmt.Errorf(
				"WAL checksum mismatch at segment %d Offset %d",
				reader.position.SegmentId,
				reader.position.Offset,
			)
		}

		var event wire.Event
		if err := json.Unmarshal(payload, &event); err != nil {
			return Record{}, fmt.Errorf(
				"invalid WAL event at segment %d Offset %d: %w",
				reader.position.SegmentId,
				reader.position.Offset,
				err,
			)
		}

		end := Position{
			SegmentId: reader.position.SegmentId,
			Offset:    reader.position.Offset + 8 + int64(payloadLength),
		}

		reader.position = end

		return Record{
			Event: event,
			Start: start,
			End:   end,
		}, nil
	}
}

func (reader *Reader) canRead(size int64) bool {
	durable := reader.wal.DurableEnd()

	if reader.position.SegmentId < durable.SegmentId {
		return true
	}

	if reader.position.SegmentId > durable.SegmentId {
		return false
	}

	return reader.position.Offset+size <= durable.Offset
}
