package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func TestAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	event := wire.Event{
		ProducerId: "producer-001",
		EventTime:  time.Now().UTC(),
		Seq:        1,
		Idx:        1,
		EventType:  "test",
		Payload:    "hello",
	}

	if err := w.Append(event); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) < 8 {
		t.Fatal("WAL record is too short")
	}

	length := binary.BigEndian.Uint32(data[:4])
	checksum := binary.BigEndian.Uint32(data[4:8])
	payload := data[8:]

	if uint32(len(payload)) != length {
		t.Fatal("record length mismatch")
	}

	if crc32.ChecksumIEEE(payload) != checksum {
		t.Fatal("checksum mismatch")
	}

	var decoded wire.Event
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.ProducerId != event.ProducerId ||
		decoded.Seq != event.Seq ||
		decoded.Payload != event.Payload {
		t.Fatal("decoded event does not match original")
	}
}

func TestReclaimBeforeKeepsCheckpointAndActiveSegments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")

	w, err := openWithSegmentSize(path, 256) // tiny segments force rotation
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 50; i++ {
		if err := w.Append(wire.Event{ /* fill in fields */ }); err != nil {
			t.Fatal(err)
		}
	}

	durable := w.DurableEnd()
	if durable.SegmentId < 2 {
		t.Fatalf("need at least 3 segments, got active id %d", durable.SegmentId)
	}

	// Checkpoint on the active segment: everything older may go, nothing else.
	if err := w.ReclaimBefore(durable); err != nil {
		t.Fatal(err)
	}
	segs, _ := listSegments(path)
	if !containsSegment(segs, durable.SegmentId) {
		t.Fatalf("active/checkpoint segment %d was reclaimed", durable.SegmentId)
	}
	if segs[0].id < durable.SegmentId {
		// Only the active segment and newer should remain.
		t.Fatalf("older segment %d survived reclaim", segs[0].id)
	}

	// A checkpoint beyond the durable end must be rejected.
	ahead := Position{SegmentId: durable.SegmentId + 1}
	if err := w.ReclaimBefore(ahead); !errors.Is(err, ErrCheckpointAhead) {
		t.Fatalf("expected ErrCheckpointAhead, got %v", err)
	}
}

func containsSegment(segs []segment, id uint64) bool {
	for _, s := range segs {
		if s.id == id {
			return true
		}
	}
	return false
}
