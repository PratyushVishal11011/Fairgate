package deduplication

import (
	"path/filepath"
	"testing"
	"time"

	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

func TestIdempotencyAcrossRestart(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "restart.wal")

	event := wire.Event{
		ProducerId:    "producer-restart",
		ProducerEpoch: "epoch-001",
		EventTime:     time.Now().UTC(),
		Seq:           1,
		Idx:           0,
		EventType:     "test_event",
		Payload:       "original-payload",
	}

	// First process: persist the event.
	firstWAL, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := firstWAL.Append(event); err != nil {
		_ = firstWAL.Close()
		t.Fatal(err)
	}

	if err := firstWAL.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate restart: recover the event from the WAL.
	recovered, err := wal.Recover(walPath)
	if err != nil {
		t.Fatalf("recover WAL: %v", err)
	}

	if len(recovered) != 1 {
		t.Fatalf("expected 1 recovered event, got %d", len(recovered))
	}

	// Rebuild the deduplication index from recovered events.
	manager, err := NewManager(recovered)
	if err != nil {
		t.Fatalf("initialize dedup manager: %v", err)
	}

	// Second process: retry the same event.
	secondWAL, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer secondWAL.Close()

	persistCalls := 0

	duplicate, err := manager.PersistIfNew(event, func() error {
		persistCalls++
		return secondWAL.Append(event)
	})
	if err != nil {
		t.Fatalf("retry event: %v", err)
	}

	if !duplicate {
		t.Fatal("expected recovered event to be recognized as a duplicate")
	}

	if persistCalls != 0 {
		t.Fatalf("expected no WAL append for duplicate, got %d", persistCalls)
	}

	// Reusing the same identity with different contents must fail.
	conflicting := event
	conflicting.Payload = "different-payload"

	_, err = manager.PersistIfNew(conflicting, func() error {
		t.Fatal("conflicting event must not be persisted")
		return nil
	})

	if err != ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// Verify that the duplicate retry did not add another WAL record.
	if err := secondWAL.Close(); err != nil {
		t.Fatal(err)
	}

	finalEvents, err := wal.Recover(walPath)
	if err != nil {
		t.Fatalf("recover WAL after retry: %v", err)
	}

	if len(finalEvents) != 1 {
		t.Fatalf("expected 1 WAL event after retry, got %d", len(finalEvents))
	}
}
