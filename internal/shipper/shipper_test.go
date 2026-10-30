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
	tokens            []string
}

func (f *fakeStore) InsertBatch(
	ctx context.Context,
	events []wire.Event,
	token string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.insertCalls++

	batchCopy := append([]wire.Event(nil), events...)
	f.batches = append(f.batches, batchCopy)
	f.tokens = append(f.tokens, token)

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

func (f *fakeStore) Tokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.tokens...)
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

	checkpointStore := NewCheckpointStore(
		filepath.Join(dir, "checkpoint.json"),
	)
	storage := &fakeStore{}

	shipper, err := New(
		storage,
		w,
		checkpointStore,
		newTestBackoff(),
		Config{
			BatchSize:    2,
			PollInterval: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- shipper.Run(ctx)
	}()

	waitForCheckpoint(t, checkpointStore, storage, 1)
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if got := storage.Calls(); got != 1 {
		t.Fatalf("expected 1 InsertBatch call, got %d", got)
	}

	batches := storage.Batches()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("expected one batch of 2 events, got %#v", batches)
	}

	if batches[0][0].Seq != 1 || batches[0][1].Seq != 2 {
		t.Fatal("events were not read from WAL in order")
	}

	assertNonEmptyTokens(t, storage.Tokens(), 1)

	checkpoint, err := checkpointStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Offset <= 0 {
		t.Fatalf("expected checkpoint to advance, got %d", checkpoint.Offset)
	}
}

func TestShipperRetriesFailedInsert(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "events.wal"))
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

	checkpointStore := NewCheckpointStore(
		filepath.Join(dir, "checkpoint.json"),
	)
	storage := &fakeStore{failuresRemaining: 2}

	shipper, err := New(
		storage,
		w,
		checkpointStore,
		newTestBackoff(),
		Config{
			BatchSize:    2,
			PollInterval: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		done <- shipper.Run(ctx)
	}()

	waitForCheckpoint(t, checkpointStore, storage, 3)
	elapsed := time.Since(startTime)
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if got := storage.Calls(); got != 3 {
		t.Fatalf("expected 3 InsertBatch calls, got %d", got)
	}
	if elapsed < 2*time.Millisecond {
		t.Fatalf("expected backoff delays, elapsed %v", elapsed)
	}

	batches := storage.Batches()
	if len(batches) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(batches))
	}

	for i, batch := range batches {
		if len(batch) != 2 ||
			batch[0].Seq != 1 ||
			batch[1].Seq != 2 {
			t.Fatalf("attempt %d did not submit the original batch", i+1)
		}
	}

	tokens := storage.Tokens()
	assertNonEmptyTokens(t, tokens, 3)

	for i, token := range tokens {
		if token != tokens[0] {
			t.Fatalf(
				"attempt %d used token %q; expected %q",
				i+1,
				token,
				tokens[0],
			)
		}
	}

	checkpoint, err := checkpointStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Offset <= 0 {
		t.Fatal("checkpoint did not advance after successful retry")
	}
}

func TestShipperDoesNotAdvanceCheckpointOnCanceledRetry(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append(testShipperEvent(1, "event-1")); err != nil {
		t.Fatal(err)
	}

	checkpointStore := NewCheckpointStore(
		filepath.Join(dir, "checkpoint.json"),
	)
	storage := &fakeStore{failuresRemaining: 100}

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
			BatchSize:    1,
			PollInterval: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- shipper.Run(ctx)
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for storage.Calls() < 1 {
		select {
		case <-deadline.C:
			cancel()
			t.Fatal("timed out waiting for InsertBatch")
		case <-ticker.C:
		}
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shipper did not stop after cancellation")
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

	tokens := storage.Tokens()
	assertNonEmptyTokens(t, tokens, len(tokens))
}

func assertNonEmptyTokens(t *testing.T, tokens []string, want int) {
	t.Helper()

	if len(tokens) != want {
		t.Fatalf("expected %d tokens, got %d", want, len(tokens))
	}

	for i, token := range tokens {
		if token == "" {
			t.Fatalf("attempt %d had an empty idempotency token", i+1)
		}
	}
}

func waitForCheckpoint(
	t *testing.T,
	checkpointStore *CheckpointStore,
	storage *fakeStore,
	wantCalls int,
) {
	t.Helper()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		checkpoint, err := checkpointStore.Load()
		if err != nil {
			t.Fatalf("load checkpoint while waiting: %v", err)
		}

		if checkpoint.Offset > 0 && storage.Calls() >= wantCalls {
			return
		}

		select {
		case <-deadline.C:
			t.Fatalf(
				"timed out waiting for checkpoint and %d insert calls",
				wantCalls,
			)
		case <-ticker.C:
		}
	}
}

// A shipper waiting at the durable WAL end ships a new record as soon as it is woken,
// instead of at its next poll.
func TestShipperWakeShipsWithoutWaitingForPoll(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	storage := &fakeStore{}
	shipper, err := New(storage, w, NewCheckpointStore(filepath.Join(dir, "checkpoint.json")), newTestBackoff(),
		Config{BatchSize: 10, PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- shipper.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	//let the shipper reach the empty WAL end and start its hourly wait.
	time.Sleep(50 * time.Millisecond)
	if err := w.Append(testShipperEvent(1, "late")); err != nil {
		t.Fatal(err)
	}
	shipper.Wake()

	deadline := time.Now().Add(2 * time.Second)
	for storage.Calls() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("woken shipper did not ship the new record")
		}
		time.Sleep(5 * time.Millisecond)
	}
	//Wake never blocks, even when a wake-up is already pending.
	shipper.Wake()
	shipper.Wake()
}
