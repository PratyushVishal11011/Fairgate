package shipper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

type checkpointFailStore struct {
	inner      *fakeStore
	parentPath string
	failedOnce bool
}

func (s *checkpointFailStore) InsertBatch(
	ctx context.Context,
	events []wire.Event,
	token string,
) error {
	if err := s.inner.InsertBatch(ctx, events, token); err != nil {
		return err
	}

	if !s.failedOnce {
		s.failedOnce = true

		if err := os.RemoveAll(s.parentPath); err != nil {
			return err
		}

		if err := os.WriteFile(
			s.parentPath,
			[]byte("not a directory"),
			0600,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *checkpointFailStore) Close() error {
	return s.inner.Close()
}

func TestShipperReplaysBatchWhenCheckpointCommitFails(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(filepath.Join(dir, "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	event := testShipperEvent(1, "event-1")
	if err := w.Append(event); err != nil {
		t.Fatal(err)
	}

	blockingPath := filepath.Join(dir, "checkpoint-parent")
	if err := os.MkdirAll(blockingPath, 0755); err != nil {
		t.Fatal(err)
	}

	brokenCheckpoint := NewCheckpointStore(
		filepath.Join(blockingPath, "checkpoint.json"),
	)

	innerStore := &fakeStore{}
	storage := &checkpointFailStore{
		inner:      innerStore,
		parentPath: blockingPath,
	}

	firstShipper, err := New(
		storage,
		w,
		brokenCheckpoint,
		newTestBackoff(),
		Config{
			BatchSize:    1,
			PollInterval: 5 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()

	firstDone := make(chan error, 1)

	go func() {
		runErr := firstShipper.Run(firstCtx)
		if runErr != nil {
			t.Logf("first shipper exited: %v", runErr)
		}
		firstDone <- runErr
	}()

	// Wait for the store call independently of the shipper's exit signal.
	waitForCalls(t, innerStore, 1)

	select {
	case runErr := <-firstDone:
		if runErr == nil {
			t.Fatal("expected checkpoint commit to fail")
		}
	case <-time.After(5 * time.Second):
		cancelFirst()
		t.Fatal("first shipper did not exit after checkpoint commit failure")
	}

	cancelFirst()

	if got := innerStore.Calls(); got != 1 {
		t.Fatalf("expected one store call before restart, got %d", got)
	}

	// The first checkpoint was not committed. Restart from a valid
	// checkpoint path so the same WAL batch is replayed.
	validCheckpoint := NewCheckpointStore(
		filepath.Join(dir, "checkpoint.json"),
	)

	secondShipper, err := New(
		innerStore,
		w,
		validCheckpoint,
		newTestBackoff(),
		Config{
			BatchSize:    1,
			PollInterval: 5 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)

	go func() {
		secondDone <- secondShipper.Run(secondCtx)
	}()

	waitForCheckpoint(t, validCheckpoint, innerStore, 2)
	cancelSecond()

	select {
	case runErr := <-secondDone:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restarted shipper did not stop after cancellation")
	}

	batches := innerStore.Batches()
	if len(batches) != 2 {
		t.Fatalf("expected 2 store calls, got %d", len(batches))
	}

	for i, batch := range batches {
		if len(batch) != 1 ||
			batch[0].Seq != event.Seq ||
			batch[0].Payload != event.Payload {
			t.Fatalf(
				"call %d did not replay the original event: %#v",
				i+1,
				batch,
			)
		}
	}

	tokens := innerStore.Tokens()
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}

	if tokens[0] == "" || tokens[0] != tokens[1] {
		t.Fatalf("expected identical non-empty replay tokens, got %#v", tokens)
	}

	checkpoint, err := validCheckpoint.Load()
	if err != nil {
		t.Fatal(err)
	}

	if checkpoint.Offset <= 0 {
		t.Fatalf(
			"expected checkpoint to advance after replay, got %d",
			checkpoint.Offset,
		)
	}
}

func waitForCalls(t *testing.T, storage *fakeStore, want int) {
	t.Helper()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for storage.Calls() < want {
		select {
		case <-deadline.C:
			t.Fatalf(
				"timed out waiting for %d store calls; got %d",
				want,
				storage.Calls(),
			)
		case <-ticker.C:
		}
	}
}
