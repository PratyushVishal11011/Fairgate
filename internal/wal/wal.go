package wal

import (
	"FairGate/internal/wire"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Set max size of a record to 1MiB
// set default segment size to 64MiB

const (
	maxRecordSize      = 1 << 20
	defaultSegmentSize = int64(64 << 20)
	batchDelay         = 2 * time.Millisecond
	//how often a degraded WAL tries to move to a fresh segment.
	defaultResumeInterval = time.Second
)

// ErrRecordTooLarge reports an event whose encoded record exceeds the WAL's record limit.
// It is a property of the event, not a WAL failure: the WAL stays usable.
var ErrRecordTooLarge = errors.New("wal: record exceeds maximum size")

// ErrUnavailable reports a WAL that cannot make appends durable: a write or sync failed and moving
// to a fresh segment failed too. Appends are refused until a retry succeeds; nothing acknowledged
// earlier is affected.
var ErrUnavailable = errors.New("wal: unavailable")

// File is the part of *os.File the WAL writes segments through.
type File interface {
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
}

// Options configure OpenWithOptions.
type Options struct {
	//segment size in bytes before the writer rotates; 0 uses 64 MiB.
	SegmentSize int64
	//opens segment files for writing; nil uses os.OpenFile. Tests use it to inject write and sync failures.
	OpenFile func(name string, flag int, perm os.FileMode) (File, error)
}

type appendRequest struct {
	header  [v2HeaderSize]byte
	payload []byte
	done    chan error
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

	//protects durableEnd and health
	//separate mutex because the write goroutine updates durableEnd while future WAL will need to read it
	stateMu    sync.RWMutex
	durableEnd Position
	//non-nil while the WAL is degraded; wraps ErrUnavailable.
	health    error
	segmentMu sync.Mutex

	//owned exclusively by the writer goroutine
	file           File
	path           string
	segmentId      uint64
	size           int64
	maxSegmentSize int64
	openFile       func(name string, flag int, perm os.FileMode) (File, error)
	resumeInterval time.Duration
	resumeAt       time.Time

	failovers atomic.Int64

	//optional instrumentation hooks; read by the writer goroutine.
	observer atomic.Pointer[observerBox]
}

// Observer receives WAL events for instrumentation. Its methods run on the writer goroutine,
// so they must be cheap and must not call back into the WAL.
type Observer interface {
	//BatchCommitted reports a group commit: how many records and bytes it made durable and how long Sync took.
	BatchCommitted(records int, bytes int64, sync time.Duration)
	//SegmentRotated reports that the writer moved to a new segment.
	SegmentRotated()
}

type observerBox struct{ Observer }

// SetObserver installs an observer; nil removes it.
func (w *WAL) SetObserver(observer Observer) {
	if observer == nil {
		w.observer.Store(nil)
		return
	}
	w.observer.Store(&observerBox{observer})
}

func Open(path string) (*WAL, error) {
	return OpenWithOptions(path, Options{})
}

// OpenWithSegmentSize opens the WAL and rotates segments once they would exceed segmentSize bytes.
func OpenWithSegmentSize(path string, segmentSize int64) (*WAL, error) {
	if segmentSize <= 0 {
		return nil, errors.New("segment size must be greater than zero")
	}
	return OpenWithOptions(path, Options{SegmentSize: segmentSize})
}

// OpenWithOptions opens the WAL for appending. If the newest segment holds legacy records,
// appends go to a new segment so each segment has one record format.
func OpenWithOptions(path string, opts Options) (*WAL, error) {
	if opts.SegmentSize < 0 {
		return nil, errors.New("segment size must be greater than zero")
	}
	if opts.SegmentSize == 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	if opts.OpenFile == nil {
		opts.OpenFile = func(name string, flag int, perm os.FileMode) (File, error) {
			return os.OpenFile(name, flag, perm)
		}
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

	var size int64
	legacy := false
	info, err := os.Stat(activePath)
	switch {
	case err == nil:
		size = info.Size()
		f, err := os.Open(activePath)
		if err != nil {
			return nil, err
		}
		segmentFormat, err := detectFormat(f, size)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		legacy = segmentFormat == formatLegacy
	case !os.IsNotExist(err):
		return nil, err
	}

	w := &WAL{
		path:           path,
		segmentId:      segmentId,
		size:           size,
		maxSegmentSize: opts.SegmentSize,
		openFile:       opts.OpenFile,
		resumeInterval: defaultResumeInterval,
		requests:       make(chan appendRequest, maxRecordSize),
		done:           make(chan struct{}),

		//update for new fields added
		durableEnd: Position{
			SegmentId: segmentId,
			Offset:    size,
		},
	}

	if legacy {
		//keep the legacy segment as written and start the next one in the current format.
		if err := w.openSegment(segmentId + 1); err != nil {
			return nil, err
		}
	} else {
		//O_CREATE - Create the file if it does not exist
		//O_APPEND - Write new data at the end of the file
		//O_WRONLY - Open the file for writing only
		//0600 - Give the file owner read / write permissions
		file, err := w.openFile(activePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		w.file = file
		//a newly created segment is not durable until its directory entry is.
		if err := syncDir(filepath.Dir(path)); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	go w.writerLoop()
	return w, nil
}

func (w *WAL) Append(event wire.Event) error {
	//encode and validate on the caller's goroutine, so a bad event never reaches the
	//writer and the single writer goroutine spends its time on I/O, not JSON.
	req, err := encodeRecord(event)
	if err != nil {
		return err
	}

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

// Health returns nil while the WAL accepts appends, and an error wrapping ErrUnavailable while it is degraded.
func (w *WAL) Health() error {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	return w.health
}

// Failovers returns how many times the writer moved to a fresh segment after a write or sync failure.
func (w *WAL) Failovers() int64 {
	return w.failovers.Load()
}

func (w *WAL) setHealth(err error) {
	w.stateMu.Lock()
	w.health = err
	w.stateMu.Unlock()
}

// encodeRecord converts an event into a v2 WAL record: a 12-byte header followed by the JSON payload.
func encodeRecord(event wire.Event) (appendRequest, error) {
	// Convert the event struct into JSON so it can be stored
	// persistently and reconstructed during WAL recovery.
	payload, err := json.Marshal(event)
	if err != nil {
		return appendRequest{}, err
	}

	// Reject records that exceed the maximum allowed payload size.
	// Recovery refuses records above this limit.
	if len(payload) > maxRecordSize {
		return appendRequest{}, fmt.Errorf("%w: %d bytes, limit %d", ErrRecordTooLarge, len(payload), maxRecordSize)
	}

	req := appendRequest{payload: payload, done: make(chan error, 1)}

	// The magic marks the start of a record, so recovery can find the next
	// record after a damaged one.
	binary.BigEndian.PutUint32(req.header[0:4], recordMagic)

	// Store the payload length using big-endian byte order. Recovery uses
	// this value to determine how many bytes belong to the record.
	binary.BigEndian.PutUint32(req.header[4:8], uint32(len(payload)))

	// The checksum covers the length and the payload, so recovery detects a
	// corrupted length as well as a corrupted payload.
	binary.BigEndian.PutUint32(req.header[8:12], recordChecksum(formatV2, req.header[:], payload))

	return req, nil
}

func (r appendRequest) size() int64 {
	return int64(len(r.header) + len(r.payload))
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
		// While degraded, wake up periodically to retry a fresh segment
		// even when no appends arrive.
		var resume <-chan time.Time
		var resumeTimer *time.Timer
		if w.Health() != nil {
			resumeTimer = time.NewTimer(time.Until(w.resumeAt))
			resume = resumeTimer.C
		}

		// Wait for the first append request. The previous WAL handled
		// individual appends; now requests are received by this
		// dedicated goroutine for batching.
		var first appendRequest
		var ok bool
		select {
		case first, ok = <-w.requests:
		case <-resume:
			w.tryResume()
			continue
		}
		if resumeTimer != nil {
			resumeTimer.Stop()
		}
		if !ok {
			break
		}

		// Start a new batch with the first request.
		batch := []appendRequest{first}

		// Take every request that is already queued. These arrived while
		// the previous batch was being written and synced, so they share
		// the next Sync() without any extra wait. The batch has no size
		// cap: one Sync() covers every waiting caller, so latency under
		// load stays near one Sync() instead of growing with the number
		// of 64-record batches queued ahead of a caller.
	drain:
		for {
			select {
			case req, ok := <-w.requests:
				if !ok {
					// Shutdown has started. Stop collecting new
					// requests, but still write the requests already
					// collected in this batch before exiting.
					closing = true
					break drain
				}
				batch = append(batch, req)
			default:
				break drain
			}
		}

		// Only a lone request waits for company. A short timer gives
		// low-traffic workloads a chance to share the Sync() without
		// waiting indefinitely.
		if len(batch) == 1 && !closing {
			timer := time.NewTimer(batchDelay)

		collect:
			for {
				select {
				case req, ok := <-w.requests:
					if !ok {
						closing = true
						break collect
					}

					batch = append(batch, req)

				case <-timer.C:
					// The batching window has expired. Commit the
					// current batch rather than waiting for more.
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
// If a write, rotation, or Sync() fails, the batch is written again to a
// fresh segment (see failover). Callers are told about a failure only if
// that fails too, and then with an error wrapping ErrUnavailable.
//
// The writer goroutine is the sole owner of the WAL file, so this
// function does not need a mutex to protect file operations.
func (w *WAL) writeBatch(batch []appendRequest) {
	// A degraded WAL refuses appends at once; the resume timer in
	// writerLoop retries a fresh segment.
	err := w.Health()
	if err == nil {
		if err = w.writeAndSync(batch); err != nil {
			err = w.failover(batch, err)
		}
	}

	// Notify every request in the batch.
	// A nil error indicates that the batch was written and synced.
	for _, item := range batch {
		item.done <- err
	}
}

// writeAndSync writes the batch to the active segment, rotating as needed,
// and makes it durable with one Sync().
func (w *WAL) writeAndSync(batch []appendRequest) error {
	if w.file == nil {
		return errors.New("no active WAL segment")
	}

	// Records arrive already encoded and validated by Append().
	// Write each one to the active WAL segment.
	// Segment rotation is handled here when the next record would
	// exceed the configured segment size.
	var bytes int64
	for _, item := range batch {

		// Rotate only if the current segment already contains data
		// and adding this record would exceed the segment size.
		// This allows a single record to exceed a small segment limit
		// used in tests without creating an empty segment repeatedly.
		if w.size > 0 && w.size+item.size() > w.maxSegmentSize {
			if err := w.rotate(); err != nil {
				return err
			}
		}

		// Write the record header first. Converting header[:] to a
		// byte slice allows it to be passed to writeFull().
		// writeFull() ensures all bytes are written, even if an
		// individual file.Write() call writes only part of the data.
		if err := writeFull(w.file, item.header[:]); err != nil {
			return err
		}

		// Write the JSON payload immediately after its header.
		// If this write fails, the record may be incomplete; failover
		// cuts the segment back to its durable end.
		if err := writeFull(w.file, item.payload); err != nil {
			return err
		}

		// Update the active segment's size only after the complete
		// header and payload have been written successfully.
		w.size += item.size()
		bytes += item.size()
	}

	// One Sync() makes the entire batch durable before the callers
	// receive successful acknowledgements.
	syncStart := time.Now()
	if err := w.file.Sync(); err != nil {
		return err
	}

	w.stateMu.Lock()
	w.durableEnd = Position{
		SegmentId: w.segmentId,
		Offset:    w.size,
	}
	w.stateMu.Unlock()

	if box := w.observer.Load(); box != nil {
		box.BatchCommitted(len(batch), bytes, time.Since(syncStart))
	}
	return nil
}

// failover recovers from a failed write or Sync() without trusting the failed file again.
//
// After a failed fsync, Linux may drop the unwritten pages and report a later fsync of the same
// file as successful, so retrying the Sync() could acknowledge data that is not on disk. Instead,
// the failed segment is cut back to its durable end and the whole batch, which no caller has seen
// acknowledged, is written to a fresh segment. If any step fails, the WAL becomes degraded.
func (w *WAL) failover(batch []appendRequest, cause error) error {
	if err := w.cutBack(); err != nil {
		return w.degrade(cause, err)
	}
	if err := w.openSegment(w.segmentId + 1); err != nil {
		return w.degrade(cause, err)
	}
	if err := w.writeAndSync(batch); err != nil {
		return w.degrade(cause, err)
	}
	w.failovers.Add(1)
	return nil
}

// cutBack truncates the active segment to its durable end and closes it. Records past the
// durable end were never acknowledged. Copies of them that a rotation already synced are
// harmless: replay and the store treat an identical record as a duplicate.
func (w *WAL) cutBack() error {
	if w.file == nil {
		return nil
	}
	durable := w.DurableEnd()
	end := int64(0)
	if durable.SegmentId == w.segmentId {
		end = durable.Offset
	}
	if err := w.file.Truncate(end); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	w.size = end
	return nil
}

// degrade marks the WAL unavailable. Appends are refused until tryResume succeeds.
func (w *WAL) degrade(cause, err error) error {
	unavailable := fmt.Errorf("%w: %v; moving to a fresh segment failed: %v", ErrUnavailable, cause, err)
	w.resumeAt = time.Now().Add(w.resumeInterval)
	w.setHealth(unavailable)
	return unavailable
}

// tryResume moves a degraded WAL to a fresh segment, so appends can resume.
func (w *WAL) tryResume() {
	err := w.cutBack()
	if err == nil {
		err = w.openSegment(w.segmentId + 1)
	}
	if err != nil {
		w.resumeAt = time.Now().Add(w.resumeInterval)
		return
	}
	w.failovers.Add(1)
	w.setHealth(nil)
}

func (w *WAL) rotate() error {
	w.segmentMu.Lock()
	defer w.segmentMu.Unlock()
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

	return w.openSegmentLocked(w.segmentId + 1)
}

// openSegment creates segment id and makes it the active segment.
func (w *WAL) openSegment(id uint64) error {
	w.segmentMu.Lock()
	defer w.segmentMu.Unlock()
	return w.openSegmentLocked(id)
}

func (w *WAL) openSegmentLocked(id uint64) error {
	nextPath := segmentPath(w.path, id)

	// O_TRUNC: a segment past the active one was never acknowledged into, so anything an
	// earlier failed attempt left in it can be discarded.
	file, err := w.openFile(nextPath, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	// Without a directory sync, a crash can lose the new segment's directory entry
	// even after its records were synced and acknowledged.
	if err := syncDir(filepath.Dir(w.path)); err != nil {
		_ = file.Close()
		return err
	}

	w.file = file
	w.segmentId = id
	w.size = 0

	if box := w.observer.Load(); box != nil {
		box.SegmentRotated()
	}

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

	segments := make([]segment, 0)
	if _, err := os.Stat(path); err == nil {
		segments = append(segments, segment{id: 0, path: path})
	} else if !os.IsNotExist(err) {
		return nil, err
	}

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
	sort.Slice(segments, func(i, j int) bool { return segments[i].id < segments[j].id })
	return segments, nil
}

// ReclaimBefore removes completed WAL segments strictly older than the checkpoint segment.
// The current checkpoint segment is retained because its offset may still point to records that have not shipped.
var ErrCheckpointAhead = errors.New("wal: checkpoint is beyond durable end")

func (w *WAL) ReclaimBefore(checkpoint Position) error {
	w.segmentMu.Lock()
	defer w.segmentMu.Unlock()

	if checkpoint.SegmentId == 0 {
		return nil
	}

	// Reject checkpoints that refer to data the WAL has not made durable.
	if durable := w.DurableEnd(); checkpoint.SegmentId > durable.SegmentId {
		return ErrCheckpointAhead
	}

	// Never reclaim the checkpoint segment or the active segment.
	cutoff := checkpoint.SegmentId
	if w.segmentId < cutoff {
		cutoff = w.segmentId
	}

	segments, err := listSegments(w.path) // sorted ascending by id
	if err != nil {
		return err
	}

	removed := false
	for _, seg := range segments {
		if seg.id >= cutoff {
			break
		}
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		removed = true
	}
	if !removed {
		return nil
	}

	dir, err := os.Open(filepath.Dir(w.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func (w *WAL) Close() error {
	//this function safely closes the WAL file.
	w.submitMu.Lock()

	//the first time Close() is called, w.closed is false, so execution skips this block and proceeds to close the request channel.
	if w.closed {
		//release the mutex Close() acquired
		w.submitMu.Unlock()
		//wait until the writer goroutine has finished
		//when the writer finishes processing pending requests, syncing and closing the WAL file, it closes w.done.
		//if the writer is still working, this line blocks until it finishes. If it has already finished, it returns immediately.
		<-w.done
		return w.closeErr
	}

	//set the closed flag to true
	//makes sure that any subsequent call to Append will return an error
	w.closed = true
	//close the requests channel
	close(w.requests)
	w.submitMu.Unlock()

	//wait for the writer to finish pending requests, sync the active segment, and close the file.
	<-w.done
	return w.closeErr
}

func writeFull(file io.Writer, data []byte) error {
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

func (w *WAL) DurableEnd() Position {
	//getter function
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()

	return w.durableEnd
}

// Stats describes the WAL's footprint on disk.
type Stats struct {
	//number of segment files on disk
	Segments int
	//total size of those segment files in bytes
	Bytes int64
	//end of the synchronized data
	DurableEnd Position
}

// Stats lists the segment files on disk. It is safe to call concurrently with appends.
func (w *WAL) Stats() (Stats, error) {
	w.segmentMu.Lock()
	defer w.segmentMu.Unlock()

	segments, err := listSegments(w.path)
	if err != nil {
		return Stats{}, err
	}

	stats := Stats{Segments: len(segments), DurableEnd: w.DurableEnd()}
	for _, seg := range segments {
		info, err := os.Stat(seg.path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return Stats{}, err
		}
		stats.Bytes += info.Size()
	}

	return stats, nil
}

// BytesAfter returns how many durable bytes lie after position, across segments.
// The shipper uses it to report how far its checkpoint trails the WAL.
func (w *WAL) BytesAfter(position Position) (int64, error) {
	w.segmentMu.Lock()
	defer w.segmentMu.Unlock()

	durable := w.DurableEnd()
	segments, err := listSegments(w.path)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, seg := range segments {
		if seg.id < position.SegmentId || seg.id > durable.SegmentId {
			continue
		}

		end := durable.Offset
		if seg.id != durable.SegmentId {
			info, err := os.Stat(seg.path)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return 0, err
			}
			end = info.Size()
		}

		start := int64(0)
		if seg.id == position.SegmentId {
			start = position.Offset
		}
		if end > start {
			total += end - start
		}
	}

	return total, nil
}
