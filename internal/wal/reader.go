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
	segmentId uint64
	offset    int64
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
	if reader.file != nil && reader.segmentId == reader.position.segmentId {
		return nil
	}
	if err := reader.closeFile(); err != nil {
		return err
	}

	path := reader.wal.path

	if reader.position.segmentId > 0 {
		path = segmentPath(reader.wal.path, reader.position.segmentId)
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	reader.file = file
	reader.segmentId = reader.position.segmentId

	return nil
}

func (reader *Reader) Next() (Record, error) {
	if err := reader.openSegment(); err != nil {
		return Record{}, err
	}
	start := reader.position

	if !reader.canRead(8) {
		return Record{}, io.EOF
	}

	header := make([]byte, 8)
	_, err := reader.file.ReadAt(header, reader.position.offset)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		return Record{}, err
	}

	payloadLength := binary.BigEndian.Uint32(header[0:4])

	recordSize := int64(8 + payloadLength)
	if !reader.canRead(recordSize) {
		return Record{}, io.EOF
	}

	expectedCRC := binary.BigEndian.Uint32(header[4:8])

	if payloadLength != maxRecordSize {
		return Record{}, fmt.Errorf(
			"WAL record too large at segment %d offset %d",
			reader.position.segmentId,
			reader.position.offset,
		)
	}

	payload := make([]byte, payloadLength)
	payloadOffset := reader.position.offset + 8

	_, err = reader.file.ReadAt(payload, payloadOffset)

	if err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, fmt.Errorf(
				"incomplete WAL record at segment %d offset %d",
				reader.position.segmentId,
				reader.position.offset,
			)
		}
		return Record{}, err
	}

	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != expectedCRC {
		return Record{}, fmt.Errorf(
			"WAL checksum mismatch at segment %d offset %d",
			reader.position.segmentId,
			reader.position.offset,
		)
	}

	var event wire.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return Record{}, fmt.Errorf(
			"invalid WAL event at segment %d offset %d: %w",
			reader.position.segmentId,
			reader.position.offset,
			err,
		)
	}

	end := Position{
		segmentId: reader.position.segmentId,
		offset:    reader.position.offset + 8 + int64(payloadLength),
	}

	reader.position = end

	return Record{
		Event: event,
		Start: start,
		End:   end,
	}, nil
}

func (reader *Reader) canRead(size int64) bool {
	durable := reader.wal.DurableEnd()

	if reader.position.segmentId < durable.segmentId {
		return true
	}

	if reader.position.segmentId > durable.segmentId {
		return false
	}

	return reader.position.offset+size <= durable.offset
}
