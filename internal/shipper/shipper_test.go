package shipper

import (
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu sync.Mutex

	failuresRemaining int
	insertCalls       int
	batches           [][]wire.Event
}

func (f *fakeStore) InsertBatch(
	ctx context.Context,
	events []wire.Event,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.insertCalls++

	batchCopy := append([]wire.Event(nil), events...)
	f.batches = append(f.batches, batchCopy)

	if f.failuresRemaining > 0 {
		f.failuresRemaining--
		return errors.New("temporary store failure")
	}

	return nil
}

func (f *fakeStore) Close() error {
	return nil
}

func (f *fakeStore) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.insertCalls
}

func (f *fakeStore) Batches() [][]wire.Event {
	f.mu.Lock()
	defer f.mu.Unlock()

	batches := make([][]wire.Event, len(f.batches))

	for i, batch := range f.batches {
		batches[i] = append([]wire.Event(nil), batch...)
	}

	return batches
}

func testShipperEvent(seq uint64, payload string) wire.Event {
	return wire.Event{
		ProducerId: "producer-1",
		Seq:        seq,
		Idx:        uint16(seq),
		EventType:  "test",
		Payload:    payload,
	}
}

func newTestBackoff() *Backoff {
	return NewBackoff(
		time.Millisecond,
		10*time.Millisecond,
		2,
		0,
		42,
	)
}

func TestShipperReadsFromWALAndAdvancesCheckpoint(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "events.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	events := []wire.Event{
		testShipperEvent(1, "event-1"),
		testShipperEvent(2, "event-2"),
	}

	for _, event := range events {
		if err := w.Append(event); err != nil {
			t.Fatal(err)
		}
	}

	checkpointPath := filepath.Join(dir, "checkpoint.json")
	checkpointStore := NewCheckpointStore(checkpointPath)

	storage := &fakeStore{}

	shipper, err := New(
		storage,
		w,
		checkpointStore,
		newTestBackoff(),
		Config{
			BatchSize:     2,
			FlushInterval: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := shipper.Run(context.Background()); err != nil {
		t.Fatalf("shipper.Run() failed: %v", err)
	}

	if storage.Calls() != 1 {
		t.Fatalf(
			"expected 1 InsertBatch call, got %d",
			storage.Calls(),
		)
	}

	batches := storage.Batches()

	if len(batches) != 1 {
		t.Fatalf(
			"expected 1 stored batch, got %d",
			len(batches),
		)
	}

	if len(batches[0]) != 2 {
		t.Fatalf(
			"expected batch of 2 events, got %d",
			len(batches[0]),
		)
	}

	if batches[0][0].Seq != 1 || batches[0][1].Seq != 2 {
		t.Fatalf("events were not read from WAL in order")
	}

	checkpoint, err := checkpointStore.Load()
	if err != nil {
		t.Fatal(err)
	}

	if checkpoint.Offset <= 0 {
		t.Fatalf(
			"expected checkpoint offset to advance, got %d",
			checkpoint.Offset,
		)
	}
}

func TestShipperRetriesFailedInsert(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "events.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	events := []wire.Event{
		testShipperEvent(1, "event-1"),
		testShipperEvent(2, "event-2"),
	}

	for _, event := range events {
		if err := w.Append(event); err != nil {
			t.Fatal(err)
		}
	}

	checkpointPath := filepath.Join(dir, "checkpoint.json")
	checkpointStore := NewCheckpointStore(checkpointPath)

	storage := &fakeStore{
		failuresRemaining: 2,
	}

	shipper, err := New(
		storage,
		w,
		checkpointStore,
		newTestBackoff(),
		Config{
			BatchSize:     2,
			FlushInterval: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()

	if err := shipper.Run(context.Background()); err != nil {
		t.Fatalf("shipper.Run() failed: %v", err)
	}

	elapsed := time.Since(start)

	if storage.Calls() != 3 {
		t.Fatalf(
			"expected 3 InsertBatch calls, got %d",
			storage.Calls(),
		)
	}

	if elapsed < 2*time.Millisecond {
		t.Fatalf(
			"expected backoff delays, elapsed time was %v",
			elapsed,
		)
	}

	batches := storage.Batches()

	if len(batches) != 3 {
		t.Fatalf(
			"expected 3 attempts, got %d",
			len(batches),
		)
	}

	for i, batch := range batches {
		if len(batch) != 2 {
			t.Fatalf(
				"attempt %d: expected 2 events, got %d",
				i+1,
				len(batch),
			)
		}

		if batch[0].Seq != 1 || batch[1].Seq != 2 {
			t.Fatalf(
				"attempt %d: retry did not submit the same batch",
				i+1,
			)
		}
	}

	checkpoint, err := checkpointStore.Load()
	if err != nil {
		t.Fatal(err)
	}

	if checkpoint.Offset <= 0 {
		t.Fatalf(
			"checkpoint did not advance after successful retry",
		)
	}
}

func TestShipperDoesNotAdvanceCheckpointOnCanceledRetry(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "events.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	event := testShipperEvent(1, "event-1")

	if err := w.Append(event); err != nil {
		t.Fatal(err)
	}

	checkpointPath := filepath.Join(dir, "checkpoint.json")
	checkpointStore := NewCheckpointStore(checkpointPath)

	storage := &fakeStore{
		failuresRemaining: 100,
	}

	backoff := NewBackoff(
		50*time.Millisecond,
		100*time.Millisecond,
		2,
		0,
		42,
	)

	shipper, err := New(
		storage,
		w,
		checkpointStore,
		backoff,
		Config{
			BatchSize:     1,
			FlushInterval: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- shipper.Run(ctx)
	}()

	deadline := time.After(time.Second)

	for {
		if storage.Calls() >= 1 {
			break
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for InsertBatch")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"expected context.Canceled, got %v",
				err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("shipper did not stop after context cancellation")
	}

	checkpoint, err := checkpointStore.Load()
	if err != nil {
		t.Fatal(err)
	}

	if checkpoint.Offset != 0 {
		t.Fatalf(
			"checkpoint advanced despite failed storage: offset=%d",
			checkpoint.Offset,
		)
	}
}
