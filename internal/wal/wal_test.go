package wal

import (
	"encoding/binary"
	"encoding/json"
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
