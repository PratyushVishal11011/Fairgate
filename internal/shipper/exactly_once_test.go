package shipper

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

// crashOnCallStore records every batch and panics on one chosen call, after recording it,
// to simulate a crash after the destination accepted the batch but before the checkpoint commit.
type crashOnCallStore struct {
	mu      sync.Mutex
	crashAt int
	batches [][]wire.Event
	tokens  []string
}

func (s *crashOnCallStore) InsertBatch(ctx context.Context, events []wire.Event, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.batches = append(s.batches, append([]wire.Event(nil), events...))
	s.tokens = append(s.tokens, token)

	if len(s.batches) == s.crashAt {
		panic("simulated crash after destination accepted batch")
	}

	return nil
}

func (s *crashOnCallStore) Close() error {
	return nil
}

func (s *crashOnCallStore) snapshot() ([][]wire.Event, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batches := make([][]wire.Event, len(s.batches))
	for i, batch := range s.batches {
		batches[i] = append([]wire.Event(nil), batch...)
	}

	return batches, append([]string(nil), s.tokens...)
}

func walPositions(t *testing.T, w *wal.WAL) []wal.Record {
	t.Helper()

	reader := wal.NewReader(w, wal.Position{})
	var records []wal.Record
	for {
		record, err := reader.Next()
		if err != nil {
			return records
		}
		records = append(records, record)
	}
}

func TestShipperReplaysExactPendingRangeAfterCrash(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "events.wal")
	checkpointStore := NewCheckpointStore(filepath.Join(dir, "checkpoint.json"))

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	first := []wire.Event{
		testShipperEvent(1, "event-1"),
		testShipperEvent(2, "event-2"),
	}
	for _, event := range first {
		if err := w.Append(event); err != nil {
			_ = w.Close()
			t.Fatal(err)
		}
	}
	records := walPositions(t, w)

	// Crash on the second batch, so the replayed batch does not start at the run's first position.
	storage := &crashOnCallStore{crashAt: 2}
	firstShipper, err := New(storage, w, checkpointStore, newTestBackoff(), Config{
		BatchSize: 1, PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}

	crash := make(chan any, 1)
	go func() {
		defer func() { crash <- recover() }()
		_ = firstShipper.Run(context.Background())
	}()

	select {
	case recovered := <-crash:
		if recovered == nil {
			_ = w.Close()
			t.Fatal("expected simulated crash")
		}
	case <-time.After(5 * time.Second):
		_ = w.Close()
		t.Fatal("timed out waiting for simulated crash")
	}

	checkpoint, err := checkpointStore.Load()
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	wantCrashed := Checkpoint{
		Segment: records[0].End.SegmentId,
		Offset:  records[0].End.Offset,
		Pending: &PendingBatch{Segment: records[1].End.SegmentId, Offset: records[1].End.Offset},
	}
	if !reflect.DeepEqual(checkpoint, wantCrashed) {
		_ = w.Close()
		t.Fatalf("checkpoint after crash: got %+v pending %+v, want %+v pending %+v",
			checkpoint, checkpoint.Pending, wantCrashed, wantCrashed.Pending)
	}

	// More records become durable before the restart, and the batch size grows.
	// Without the pending range the replay would cover event-2 and event-3 under a new token.
	if err := w.Append(testShipperEvent(3, "event-3")); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	secondShipper, err := New(storage, w, checkpointStore, newTestBackoff(), Config{
		BatchSize: 10, PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- secondShipper.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		batches, _ := storage.snapshot()
		if len(batches) >= 4 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("restarted shipper stopped early: %v", err)
		case <-deadline:
			t.Fatalf("timed out waiting for replay; got %d batches", len(batches))
		case <-time.After(time.Millisecond):
		}
	}

	all := walPositions(t, w)
	waitForCommittedEnd(t, checkpointStore, all[len(all)-1].End)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	batches, tokens := storage.snapshot()
	if len(batches) != 4 {
		t.Fatalf("expected 4 inserts, got %d", len(batches))
	}
	if !reflect.DeepEqual(batches[2], batches[1]) {
		t.Fatalf("replay did not resend the pending range: got %+v, want %+v", batches[2], batches[1])
	}
	if tokens[2] != tokens[1] {
		t.Fatalf("replay token changed: first=%q replay=%q", tokens[1], tokens[2])
	}
	if len(batches[3]) != 1 || batches[3][0].Payload != "event-3" {
		t.Fatalf("expected event-3 after the replay, got %+v", batches[3])
	}

	checkpoint, err = checkpointStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Pending != nil {
		t.Fatalf("pending batch left behind after commit: %+v", checkpoint.Pending)
	}
}

func TestShipperTokensDeriveFromEachBatchRange(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := uint64(1); i <= 3; i++ {
		if err := w.Append(testShipperEvent(i, "event")); err != nil {
			t.Fatal(err)
		}
	}
	records := walPositions(t, w)

	checkpointStore := NewCheckpointStore(filepath.Join(dir, "checkpoint.json"))
	storage := &fakeStore{}
	s, err := New(storage, w, checkpointStore, newTestBackoff(), Config{
		BatchSize: 1, PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitForCommittedEnd(t, checkpointStore, records[2].End)
	cancel()
	<-done

	tokens := storage.Tokens()
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	for i, record := range records {
		if want := batchToken(record.Start, record.End); tokens[i] != want {
			t.Fatalf("batch %d token is not derived from its own range: got %q want %q", i, tokens[i], want)
		}
	}
}

func TestShipperRejectsPendingEndOffRecordBoundary(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append(testShipperEvent(1, "event-1")); err != nil {
		t.Fatal(err)
	}
	end := w.DurableEnd()

	tests := map[string]PendingBatch{
		"inside record": {Segment: end.SegmentId, Offset: end.Offset - 1},
		"past durable":  {Segment: end.SegmentId, Offset: end.Offset + 100},
	}

	for name, pending := range tests {
		t.Run(name, func(t *testing.T) {
			checkpointStore := NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json"))
			pending := pending
			if err := checkpointStore.Commit(Checkpoint{Pending: &pending}); err != nil {
				t.Fatal(err)
			}

			storage := &fakeStore{}
			s, err := New(storage, w, checkpointStore, newTestBackoff(), Config{
				BatchSize: 10, PollInterval: 5 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = s.Run(ctx)
			if err == nil || !strings.Contains(err.Error(), "pending batch end") {
				t.Fatalf("expected pending batch error, got %v", err)
			}
			if storage.Calls() != 0 {
				t.Fatalf("store must not be called with a misaligned pending batch, got %d calls", storage.Calls())
			}
		})
	}
}

func waitForCommittedEnd(t *testing.T, checkpointStore *CheckpointStore, end wal.Position) {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		checkpoint, err := checkpointStore.Load()
		if err != nil {
			t.Fatal(err)
		}
		if checkpoint.Pending == nil && checkpointPosition(checkpoint) == end {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for checkpoint %+v; got %+v", end, checkpoint)
		case <-time.After(time.Millisecond):
		}
	}
}

type recordingObserver struct {
	mu          sync.Mutex
	inserted    []int
	failures    []int
	checkpoints []wal.Position
}

func (o *recordingObserver) BatchInserted(events int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inserted = append(o.inserted, events)
}

func (o *recordingObserver) InsertFailed(consecutive int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures = append(o.failures, consecutive)
}

func (o *recordingObserver) CheckpointCommitted(position wal.Position) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.checkpoints = append(o.checkpoints, position)
}

func TestShipperReportsToObserver(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := uint64(1); i <= 2; i++ {
		if err := w.Append(testShipperEvent(i, "event")); err != nil {
			t.Fatal(err)
		}
	}
	end := w.DurableEnd()

	checkpointStore := NewCheckpointStore(filepath.Join(dir, "checkpoint.json"))
	observer := &recordingObserver{}
	s, err := New(&fakeStore{failuresRemaining: 2}, w, checkpointStore, newTestBackoff(), Config{
		BatchSize: 10, PollInterval: 5 * time.Millisecond, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitForCommittedEnd(t, checkpointStore, end)
	cancel()
	<-done

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if !reflect.DeepEqual(observer.failures, []int{1, 2}) {
		t.Errorf("failures: got %v, want [1 2]", observer.failures)
	}
	if !reflect.DeepEqual(observer.inserted, []int{2}) {
		t.Errorf("inserted: got %v, want [2]", observer.inserted)
	}
	if !reflect.DeepEqual(observer.checkpoints, []wal.Position{end}) {
		t.Errorf("checkpoints: got %v, want [%v]", observer.checkpoints, end)
	}
	if s.Committed() != end {
		t.Errorf("Committed() = %+v, want %+v", s.Committed(), end)
	}
}
