package shipper

import (
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type crashRecoveryStore struct {
	mu        sync.Mutex
	calls     int
	batches   [][]wire.Event
	tokens    []string
	crashOnce bool
}

func (s *crashRecoveryStore) InsertBatch(
	ctx context.Context,
	events []wire.Event,
	token string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	s.batches = append(s.batches, append([]wire.Event(nil), events...))
	s.tokens = append(s.tokens, token)

	if s.crashOnce {
		s.crashOnce = false
		panic("simulated crash after destination accepted batch")
	}

	return nil
}

func (s *crashRecoveryStore) Close() error {
	return nil
}

func (s *crashRecoveryStore) snapshot() (int, [][]wire.Event, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batches := make([][]wire.Event, len(s.batches))
	for i, batch := range s.batches {
		batches[i] = append([]wire.Event(nil), batch...)
	}

	tokens := append([]string(nil), s.tokens...)

	return s.calls, batches, tokens
}

func TestShipperCrashRecoveryBeforeCheckpointCommit(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "events.wal")
	checkpointPath := filepath.Join(dir, "checkpoint.json")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	events := []wire.Event{
		testShipperEvent(1, "event-1"),
		testShipperEvent(2, "event-2"),
	}

	for _, event := range events {
		if err := w.Append(event); err != nil {
			_ = w.Close()
			t.Fatal(err)
		}
	}

	checkpointStore := NewCheckpointStore(checkpointPath)
	storage := &crashRecoveryStore{
		crashOnce: true,
	}

	newShipper := func(w *wal.WAL) *Shipper {
		t.Helper()

		s, err := New(
			storage,
			w,
			checkpointStore,
			newTestBackoff(),
			Config{
				BatchSize:    2,
				PollInterval: 5 * time.Millisecond,
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		return s
	}

	firstShipper := newShipper(w)

	crash := make(chan any, 1)

	go func() {
		defer func() {
			crash <- recover()
		}()

		_ = firstShipper.Run(context.Background())
	}()

	select {
	case recovered := <-crash:
		if recovered == nil {
			_ = w.Close()
			t.Fatal("expected simulated crash, but shipper returned normally")
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

	if checkpoint.Offset != 0 || checkpoint.Segment != 0 {
		_ = w.Close()
		t.Fatalf(
			"checkpoint advanced before simulated crash: %+v",
			checkpoint,
		)
	}

	calls, batches, tokens := storage.snapshot()

	if calls != 1 {
		_ = w.Close()
		t.Fatalf("expected 1 insert before crash, got %d", calls)
	}

	if !reflect.DeepEqual(batches[0], events) {
		_ = w.Close()
		t.Fatal("first inserted batch does not match WAL events")
	}

	if len(tokens) != 1 || tokens[0] == "" {
		_ = w.Close()
		t.Fatalf("expected a non-empty idempotency token, got %v", tokens)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	secondShipper := newShipper(w)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- secondShipper.Run(ctx)
	}()

	deadline := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()

	for {
		checkpoint, err = checkpointStore.Load()
		if err != nil {
			cancel()
			t.Fatal(err)
		}

		calls, batches, tokens = storage.snapshot()

		if checkpoint.Offset > 0 && calls >= 2 {
			break
		}

		select {
		case <-deadline.C:
			cancel()
			t.Fatal("timed out waiting for replay and checkpoint commit")
		case <-ticker.C:
		}
	}

	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context cancellation after recovery")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restarted shipper did not stop")
	}

	if calls != 2 {
		t.Fatalf("expected 2 insert attempts, got %d", calls)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 recorded batches, got %d", len(batches))
	}

	if !reflect.DeepEqual(batches[0], batches[1]) {
		t.Fatal("replayed batch differs from the original batch")
	}

	if !reflect.DeepEqual(batches[1], events) {
		t.Fatal("replayed events do not match the WAL events")
	}

	if len(tokens) != 2 {
		t.Fatalf("expected 2 idempotency tokens, got %d", len(tokens))
	}

	if tokens[0] != tokens[1] {
		t.Fatalf(
			"idempotency token changed after restart: first=%q replay=%q",
			tokens[0],
			tokens[1],
		)
	}

	if tokens[0] == "" {
		t.Fatal("idempotency token must not be empty")
	}

	if checkpoint.Offset <= 0 {
		t.Fatalf("checkpoint was not committed after recovery: %+v", checkpoint)
	}
}
