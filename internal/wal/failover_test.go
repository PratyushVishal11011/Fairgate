package wal

import (
	"FairGate/internal/wire"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var errInjected = errors.New("injected fault")

// faultyFS opens real files but fails the next N writes, syncs, or opens on request.
type faultyFS struct {
	mu        sync.Mutex
	failWrite int
	failSync  int
	failOpen  int
}

func (fs *faultyFS) take(counter *int) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if *counter > 0 {
		*counter--
		return true
	}
	return false
}

func (fs *faultyFS) set(write, sync, open int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.failWrite, fs.failSync, fs.failOpen = write, sync, open
}

func (fs *faultyFS) open(name string, flag int, perm os.FileMode) (File, error) {
	if fs.take(&fs.failOpen) {
		return nil, errInjected
	}
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultyFile{File: file, fs: fs}, nil
}

type faultyFile struct {
	*os.File
	fs *faultyFS
}

// Write fails after writing half of p, leaving a torn record behind as a real failure can.
func (f *faultyFile) Write(p []byte) (int, error) {
	if f.fs.take(&f.fs.failWrite) {
		n, _ := f.File.Write(p[:len(p)/2])
		return n, errInjected
	}
	return f.File.Write(p)
}

func (f *faultyFile) Sync() error {
	if f.fs.take(&f.fs.failSync) {
		return errInjected
	}
	return f.File.Sync()
}

func testEvent(seq uint64) wire.Event {
	return wire.Event{
		ProducerId:    "producer-1",
		ProducerEpoch: "1",
		EventTime:     time.Unix(1793000000, 0).UTC(),
		Seq:           seq,
		EventType:     "test",
		Payload:       fmt.Sprintf("payload-%03d-%s", seq, "abcdefghijklmnopqrstuvwxyz0123456789"),
	}
}

func openFaulty(t *testing.T, path string, fs *faultyFS) *WAL {
	t.Helper()
	w, err := OpenWithOptions(path, Options{OpenFile: fs.open})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func expectSeqs(t *testing.T, events []wire.Event, want ...uint64) {
	t.Helper()
	got := make([]uint64, len(events))
	for i, event := range events {
		got[i] = event.Seq
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("recovered seqs %v, want %v", got, want)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestSyncFailureFailsOverToFreshSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	fs := &faultyFS{}
	w := openFaulty(t, path, fs)

	if err := w.Append(testEvent(1)); err != nil {
		t.Fatal(err)
	}
	durable := fileSize(t, path)

	fs.set(0, 1, 0)
	if err := w.Append(testEvent(2)); err != nil {
		t.Fatalf("a failed fsync must be retried on a fresh segment, got %v", err)
	}
	if w.Failovers() != 1 || w.Health() != nil {
		t.Fatalf("expected one failover and a healthy WAL, got %d, %v", w.Failovers(), w.Health())
	}
	if err := w.Append(testEvent(3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	//the failed segment is cut back to what was durable before the failed batch.
	if size := fileSize(t, path); size != durable {
		t.Fatalf("failed segment is %d bytes, want %d", size, durable)
	}
	recovery, err := RecoverWithOptions(path, RecoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.Damage) != 0 {
		t.Fatalf("expected a clean WAL, got %v", recovery.Damage)
	}
	expectSeqs(t, recovery.Events, 1, 2, 3)
}

func TestTornWriteFailsOverWithoutDamage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	fs := &faultyFS{}
	w := openFaulty(t, path, fs)

	if err := w.Append(testEvent(1)); err != nil {
		t.Fatal(err)
	}
	fs.set(1, 0, 0)
	if err := w.Append(testEvent(2)); err != nil {
		t.Fatalf("a failed write must be retried on a fresh segment, got %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	recovery, err := RecoverWithOptions(path, RecoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.Damage) != 0 {
		t.Fatalf("the torn record must have been cut back, got %v", recovery.Damage)
	}
	expectSeqs(t, recovery.Events, 1, 2)
}

func TestWALDegradesAndResumes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	fs := &faultyFS{}
	w := openFaulty(t, path, fs)
	defer w.Close()
	w.resumeInterval = 20 * time.Millisecond

	if err := w.Append(testEvent(1)); err != nil {
		t.Fatal(err)
	}

	//the sync fails, and so does opening a fresh segment, then two resume attempts.
	fs.set(0, 1, 3)
	err := w.Append(testEvent(2))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
	if !errors.Is(w.Health(), ErrUnavailable) {
		t.Fatalf("expected a degraded WAL, got %v", w.Health())
	}
	if err := w.Append(testEvent(3)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a degraded WAL must refuse appends, got %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for w.Health() != nil {
		if time.Now().After(deadline) {
			t.Fatalf("WAL did not resume: %v", w.Health())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := w.Append(testEvent(2)); err != nil {
		t.Fatalf("append after resume: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	events, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	expectSeqs(t, events, 1, 2)
}

// writeLegacySegment writes records in the v1 format: length | crc32(payload) | payload.
func writeLegacySegment(t *testing.T, path string, events ...wire.Event) {
	t.Helper()
	var data []byte
	for _, event := range events {
		req, err := encodeRecord(event)
		if err != nil {
			t.Fatal(err)
		}
		var header [v1HeaderSize]byte
		copy(header[0:4], req.header[4:8])
		putUint32(header[4:8], recordChecksum(formatLegacy, nil, req.payload))
		data = append(data, header[:]...)
		data = append(data, req.payload...)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func putUint32(b []byte, v uint32) {
	b[0], b[1], b[2], b[3] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v)
}

func TestLegacySegmentsStayReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	writeLegacySegment(t, path, testEvent(1), testEvent(2))

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	//appends never mix formats in one segment.
	if w.segmentId != 1 {
		t.Fatalf("expected appends to start segment 1 after a legacy segment, got %d", w.segmentId)
	}
	if err := w.Append(testEvent(3)); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(w, Position{})
	for _, want := range []uint64{1, 2, 3} {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read seq %d: %v", want, err)
		}
		if record.Event.Seq != want {
			t.Fatalf("read seq %d, want %d", record.Event.Seq, want)
		}
	}
	_ = reader.closeFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	events, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	expectSeqs(t, events, 1, 2, 3)

	reports, err := Inspect(path, Position{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 || reports[0].Format != "v1" || reports[1].Format != "v2" || reports[0].Records != 2 {
		t.Fatalf("unexpected inspect report %+v", reports)
	}
}
