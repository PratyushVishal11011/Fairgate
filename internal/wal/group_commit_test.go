package wal

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func TestConcurrentAppendAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := openWithSegmentSize(path, 4096)
	if err != nil {
		t.Fatal(err)
	}

	const eventCount = 200

	var wg sync.WaitGroup
	errs := make(chan error, eventCount)

	for i := 0; i < eventCount; i++ {
		wg.Add(1)

		go func(seq int) {
			defer wg.Done()

			event := wire.Event{
				ProducerId: "producer-001",
				EventTime:  time.Now().UTC(),
				Seq:        uint64(seq + 1),
				Idx:        uint16(seq + 1),
				EventType:  "test",
				Payload:    fmt.Sprintf("event-%d", seq+1),
			}

			if err := w.Append(event); err != nil {
				errs <- fmt.Errorf("append %d: %w", seq+1, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	events, err := Recover(path)
	if err != nil {
		t.Fatalf("recover WAL: %v", err)
	}

	if len(events) != eventCount {
		t.Fatalf("expected %d events, got %d", eventCount, len(events))
	}

	seen := make(map[uint64]bool, eventCount)
	for _, event := range events {
		if seen[event.Seq] {
			t.Errorf("duplicate event sequence: %d", event.Seq)
		}
		seen[event.Seq] = true
	}

	for seq := uint64(1); seq <= eventCount; seq++ {
		if !seen[seq] {
			t.Errorf("missing event sequence: %d", seq)
		}
	}
}

func TestAppendAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	err = w.Append(wire.Event{
		ProducerId: "producer-001",
		EventTime:  time.Now().UTC(),
		Seq:        1,
		EventType:  "test",
		Payload:    "after-close",
	})

	if err == nil {
		t.Fatal("expected append to fail after WAL close")
	}
}

func TestCloseMultipleTimes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
