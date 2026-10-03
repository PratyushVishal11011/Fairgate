package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func TestSegmentRotationAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := openWithSegmentSize(path, 900)
	if err != nil {
		t.Fatal(err)
	}

	const eventCount = 3

	for i := 0; i < eventCount; i++ {
		event := wire.Event{
			ProducerId: "producer-001",
			EventTime:  time.Now().UTC(),
			Seq:        uint64(i + 1),
			Idx:        uint16(i + 1),
			EventType:  "test",
			Payload:    fmt.Sprintf("%0700d", i),
		}

		if err := w.Append(event); err != nil {
			t.Fatalf("append event %d: %v", i+1, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	segments, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(segments) < 2 {
		t.Fatalf("expected WAL rotation, found %d segment(s)", len(segments))
	}

	events, err := Recover(path)
	if err != nil {
		t.Fatalf("recover WAL: %v", err)
	}

	if len(events) != eventCount {
		t.Fatalf("expected %d recovered events, got %d", eventCount, len(events))
	}

	for i, event := range events {
		expectedSeq := uint64(i + 1)
		if event.Seq != expectedSeq {
			t.Errorf("event %d: expected sequence %d, got %d", i, expectedSeq, event.Seq)
		}
	}
}

func TestAppendAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := openWithSegmentSize(path, 900)
	if err != nil {
		t.Fatal(err)
	}

	first := wire.Event{
		ProducerId: "producer-001",
		EventTime:  time.Now().UTC(),
		Seq:        1,
		EventType:  "test",
		Payload:    fmt.Sprintf("%0700d", 1),
	}

	if err := w.Append(first); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = openWithSegmentSize(path, 900)
	if err != nil {
		t.Fatal(err)
	}

	second := wire.Event{
		ProducerId: "producer-001",
		EventTime:  time.Now().UTC(),
		Seq:        2,
		EventType:  "test",
		Payload:    fmt.Sprintf("%0700d", 2),
	}

	if err := w.Append(second); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	events, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 recovered events, got %d", len(events))
	}

	if events[0].Seq != 1 || events[1].Seq != 2 {
		t.Fatalf("unexpected recovery order: %d, %d", events[0].Seq, events[1].Seq)
	}
}

func TestIncompleteTailInFinalSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := openWithSegmentSize(path, 900)
	if err != nil {
		t.Fatal(err)
	}

	event := wire.Event{
		ProducerId: "producer-001",
		EventTime:  time.Now().UTC(),
		Seq:        1,
		EventType:  "test",
		Payload:    "valid",
	}

	if err := w.Append(event); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	// Add an incomplete record header to simulate a crash during append.
	if _, err := file.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	events, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 recovered event, got %d", len(events))
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() == 0 {
		t.Fatal("expected valid record to remain after recovery")
	}
}
