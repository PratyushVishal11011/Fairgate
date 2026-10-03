package wal

import (
	"FairGate/internal/wire"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Set max size of a record to 1MiB
const maxRecordSize = 1 << 20

// set default segment size to 64MiB
const defaultSegmentSize = 64 << 20

type WAL struct {
	mu             sync.Mutex
	file           *os.File
	path           string
	segmentId      uint64
	size           int64
	maxSegmentSize int64
}

func Open(path string) (*WAL, error) {
	return openWithSegmentSize(path, defaultSegmentSize)
}

func openWithSegmentSize(path string, segmentSize int64) (*WAL, error) {
	if segmentSize <= 0 {
		return nil, errors.New("segment size must be greater than zero")
	}

	segments, err := listSegments(path)
	if err != nil {
		return nil, err
	}
	var segmentId uint64
	activePath := path

	if len(segments) > 0 {
		last := segments[len(segments)-1]
		segmentId = last.id
		activePath = last.path
	}

	//O_CREATE - Create the file if it does not exist
	//O_APPEND - Write new data at the end of the file
	//O_WRONLY - Open the file for writing only
	//0600 - Give the file owner read / write permissions
	file, err := os.OpenFile(activePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &WAL{
		file:           file,
		path:           path,
		segmentId:      segmentId,
		size:           info.Size(),
		maxSegmentSize: segmentSize,
	}, nil
}

func (w *WAL) Append(event wire.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	//convert event into JSON
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	if len(payload) > maxRecordSize {
		return errors.New("payload too large")
	}

	var header [8]byte
	//This stores the payload length in the first four bytes of the header.
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	//This calculates a CRC32 checksum of the payload and stores it in the last four bytes of the header.
	//CRC Checksum is needed for data recovery in the event of a WAL failure / corruption
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	recordSize := int64(len(payload) + len(header))

	if w.size > 0 && w.size+recordSize > w.maxSegmentSize {
		// Rotate only if the active segment already contains data.
		// This allows a single record to exceed a very small test limit.
		if err := w.rotate(); err != nil {
			return err
		}
	}

	//header[:] converts the fixed-size array into a byte slice, which can be passed to writeFull().
	//The writeFull() function ensures that all bytes are written, even if a single call to file.Write() writes only part of the data.
	if err := writeFull(w.file, header[:]); err != nil {
		return err
	}

	if err := writeFull(w.file, payload); err != nil {
		return err
	}
	//Sync() asks the operating system to flush the file's buffered changes to persistent storage.
	if err := w.file.Sync(); err != nil {
		return err
	}

	w.size += recordSize

	return nil
}

func (w *WAL) rotate() error {
	if w.file == nil {
		return errors.New("file is nil")
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	nextId := w.segmentId + 1
	nextPath := segmentPath(w.path, nextId)

	file, err := os.OpenFile(nextPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	w.file = file
	w.segmentId = nextId
	w.size = 0

	return nil
}

func segmentPath(path string, id uint64) string {
	return fmt.Sprintf("%s.seg-%020d", path, id)
}

type segment struct {
	id   uint64
	path string
}

func listSegments(path string) ([]segment, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	prefix := base + ".seg-"

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	segments := []segment{{id: 0, path: path}}

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		idText := strings.TrimPrefix(name, prefix)
		id, err := strconv.ParseUint(idText, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid segment filename: %s", name)
		}

		segments = append(segments, segment{id: id, path: filepath.Join(dir, name)})
	}

	// Keep the original WAL first, followed by numbered segments.
	for i := 1; i < len(segments); i++ {
		for j := i; j > 1 && segments[j].id < segments[j-1].id; j-- {
			segments[j], segments[j-1] = segments[j-1], segments[j]
		}
	}

	// If the original file does not exist, it should not be treated
	// as a segment unless it is created by Open or Recover.
	if len(segments) == 1 {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil, nil
		}
	}
	return segments, nil
}

func (w *WAL) Close() error {
	//this function safely closes the WAL file.

	w.mu.Lock()
	defer w.mu.Unlock()

	//If w.file is already nil, it returns without doing anything.
	if w.file == nil {
		return nil
	}
	//Otherwise, it closes the file, sets the pointer to nil, and returns any error.
	err := w.file.Close()
	w.file = nil
	return err
}

func writeFull(file *os.File, data []byte) error {
	//loop continues as long as there are bytes left to write.
	for len(data) > 0 {
		//file.Write() attempts to write the bytes to the file.
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		//when zero bytes were written and no error occurred, return an error.
		if n == 0 {
			return errors.New("short write to WAL")
		}

		//Removing the bytes already written
		data = data[n:]
	}
	return nil
}
