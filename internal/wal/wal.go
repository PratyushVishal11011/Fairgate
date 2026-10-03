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
	"time"
)

// Set max size of a record to 1MiB
// set default segment size to 64MiB

const (
	maxRecordSize      = 1 << 20
	defaultSegmentSize = int64(64 << 20)
	maxBatchSize       = 64
	batchDelay         = 2 * time.Millisecond
)

type appendRequest struct {
	event wire.Event
	done  chan error
}

type WAL struct {
	//modified the WAL struct to support the dedicated writer goroutine and concurrent append requests.
	//to protect submission and shutdown
	//used a read-write mutex
	//allows multiple goroutines to read shared data simultaneously, but only one goroutine to write at a time.
	submitMu sync.RWMutex
	closed   bool

	//group commit coordination
	requests chan appendRequest
	done     chan struct{}
	closeErr error

	//owned exclusively by the writer goroutine
	file           *os.File
	path           string
	segmentId      uint64
	size           int64
	maxSegmentSize int64
	fatalError     error
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

	w := &WAL{
		file:           file,
		path:           path,
		segmentId:      segmentId,
		size:           info.Size(),
		maxSegmentSize: segmentSize,
		requests:       make(chan appendRequest, maxRecordSize),
		done:           make(chan struct{}),
	}

	go w.writerLoop()
	return w, nil
}

func (w *WAL) Append(event wire.Event) error {
	req := appendRequest{event: event, done: make(chan error, 1)}

	//prevent close from closing requests while an append is being submitted
	w.submitMu.RLock()
	if w.closed {
		w.submitMu.RUnlock()
		return errors.New("WAL closed")
	}

	w.requests <- req
	w.submitMu.RUnlock()

	return <-req.done
}

// writerLoop is the dedicated goroutine responsible for writing events
// to the WAL. Unlike the previous implementation, where each Append()
// call directly wrote to the file and synced it, this version collects
// multiple append requests into batches and performs a single Sync()
// for the entire batch (group commit).
func (w *WAL) writerLoop() {
	// Signal that the writer goroutine has exited. Close() waits on
	// this channel to ensure all pending writes have been processed.
	defer close(w.done)

	// Tracks whether the request channel has been closed. The writer
	// continues processing any requests already collected before exiting.
	closing := false

	for !closing {
		// Wait for the first append request. The previous WAL handled
		// individual appends; now requests are received by this
		// dedicated goroutine for batching.
		first, ok := <-w.requests
		if !ok {
			break
		}

		// Start a new batch with the first request.
		batch := []appendRequest{first}

		// Start a short timer to avoid waiting indefinitely for more
		// requests. This allows low-traffic workloads to commit without
		// needing to fill an entire batch.
		timer := time.NewTimer(batchDelay)

	collect:
		// Collect additional requests until the batch reaches its
		// maximum size, the timer expires, or the channel is closed.
		// This replaces one Sync() per event with one Sync() per batch.
		for len(batch) < maxBatchSize {
			select {
			case req, ok := <-w.requests:
				if !ok {
					// Shutdown has started. Stop collecting new
					// requests, but still write the requests already
					// collected in this batch before exiting.
					closing = true
					break collect
				}

				batch = append(batch, req)

			case <-timer.C:
				// The batching window has expired. Commit the current
				// batch rather than waiting for more requests.
				break collect
			}
		}

		// Stop the timer if it hasn't already expired. Drain it if
		// necessary to prevent an unread timer event.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		// Write all requests in the batch and perform a single Sync().
		// Each Append() caller is notified only after the batch's
		// durability operation has completed.
		w.writeBatch(batch)
	}

	// The request channel is closed and all queued requests have
	// been processed. Sync and close the active segment before
	// the writer goroutine exits.
	if w.file != nil {
		if err := w.file.Sync(); err != nil && w.closeErr == nil {
			w.closeErr = err
		}

		if err := w.file.Close(); err != nil && w.closeErr == nil {
			w.closeErr = err
		}

		w.file = nil
	}
}

// writeBatch writes a group of append requests to the WAL and performs
// a single Sync() for the entire batch.
//
// Previously, each Append() call wrote its event directly to the WAL
// and called Sync() individually. With group commit, the writer goroutine
// collects multiple requests and passes them here, reducing the number
// of disk synchronization operations.
//
// The writer goroutine is the sole owner of the WAL file, so this
// function does not need a mutex to protect file operations.
func (w *WAL) writeBatch(batch []appendRequest) {

	// If a previous write or Sync() failed, the WAL is considered
	// unusable for further appends. Notify every request in this batch
	// of the existing error without attempting additional writes.
	if w.fatalError != nil {
		for _, req := range batch {
			req.done <- w.fatalError
		}
		return
	}

	// Stores a request after its event has been encoded into the
	// format used by the WAL.
	type encodeRequest struct {
		req     appendRequest
		header  [8]byte
		payload []byte
		size    int64
	}

	// Prepare all valid requests before writing them to disk.
	// This separates serialization and validation from file I/O.
	encoded := make([]encodeRequest, 0, len(batch))

	for _, req := range batch {

		// Convert the event struct into JSON so it can be stored
		// persistently and reconstructed during WAL recovery.
		payload, err := json.Marshal(req.event)
		if err != nil {
			// Serialization failed for this event. Notify its caller
			// and continue processing the remaining requests.
			req.done <- err
			continue
		}

		// Reject records that exceed the maximum allowed payload size.
		// This prevents oversized records from being written to the WAL.
		if len(payload) > maxRecordSize {
			req.done <- errors.New("payload too large")
			continue
		}

		var header [8]byte

		// Store the payload length in the first four bytes of the header
		// using big-endian byte order. Recovery uses this value to
		// determine how many bytes belong to the record.
		binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))

		// Calculate a CRC32 checksum of the payload and store it in
		// the final four bytes of the header. Recovery recalculates
		// this checksum to detect corrupted records.
		binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

		// Add the encoded record to the batch. The total record size
		// includes both the 8-byte header and the JSON payload.
		encoded = append(encoded, encodeRequest{
			req:     req,
			header:  header,
			payload: payload,
			size:    int64(len(payload) + len(header)),
		})
	}

	// If all requests failed serialization or validation, there is
	// nothing left to write to the WAL.
	if len(encoded) == 0 {
		return
	}

	// Write each encoded record to the active WAL segment.
	// Segment rotation is handled here when the next record would
	// exceed the configured segment size.
	for _, item := range encoded {

		// Rotate only if the current segment already contains data
		// and adding this record would exceed the segment size.
		// This allows a single record to exceed a small segment limit
		// used in tests without creating an empty segment repeatedly.
		if w.size > 0 && w.size+item.size > w.maxSegmentSize {
			if err := w.rotate(); err != nil {
				// A rotation failure prevents the batch from being
				// safely completed. Store the error so future batches
				// are rejected as well.
				w.fatalError = err
				break
			}
		}

		// Write the record header first. Converting header[:] to a
		// byte slice allows it to be passed to writeFull().
		// writeFull() ensures all bytes are written, even if an
		// individual file.Write() call writes only part of the data.
		if err := writeFull(w.file, item.header[:]); err != nil {
			w.fatalError = err
			break
		}

		// Write the JSON payload immediately after its header.
		// If this write fails, the record may be incomplete.
		// Recovery handles incomplete trailing records by truncating
		// the final segment to the last valid record boundary.
		if err := writeFull(w.file, item.payload); err != nil {
			w.fatalError = err
			break
		}

		// Update the active segment's size only after the complete
		// header and payload have been written successfully.
		w.size += item.size
	}

	// Only synchronize the file if all records were written without
	// an error. One Sync() makes the entire successfully written batch
	// durable before the callers receive successful acknowledgements.
	if w.fatalError == nil {
		if err := w.file.Sync(); err != nil {
			// A Sync() failure means the durability of the batch
			// cannot be confirmed. Treat the WAL as failed.
			w.fatalError = err
		}
	}

	// Notify every request that was successfully encoded.
	// A nil error indicates that the batch was written and synced.
	// If any write, rotation, or Sync() operation failed, return
	// the WAL's fatal error rather than confirming success.
	for _, item := range encoded {
		item.req.done <- w.fatalError
	}
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

	w.submitMu.Lock()
	defer w.submitMu.Unlock()

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
