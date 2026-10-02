package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func TestRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	originalEvents := []wire.Event{
		{
			ProducerId: "producer-1",
			EventTime:  time.Unix(1727600001, 0).UTC(),
			Seq:        1,
			Idx:        0,
			EventType:  "temperature",
			Payload:    "25.6",
		},
		{
			ProducerId: "producer-2",
			EventTime:  time.Unix(1727600000, 0).UTC(),
			Seq:        2,
			Idx:        0,
			EventType:  "vibration",
			Payload:    "0.85",
		},
	}

	for _, event := range originalEvents {
		if err := w.Append(event); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	recoveredEvents, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(recoveredEvents, originalEvents) {
		t.Fatalf("recovered events do not match original events\n got: %+v\nwant: %+v",
			recoveredEvents, originalEvents)
	}
}

func TestRecoverEmptyWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")

	if err := os.WriteFile(path, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	events, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestRecoverIncompleteHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incomplete_header.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	event := wire.Event{
		ProducerId: "producer-1",
		EventTime:  time.Unix(1727600000, 0).UTC(),
		Seq:        1,
		Idx:        0,
		EventType:  "temperature",
		Payload:    "25.6",
	}

	if err := w.Append(event); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	validSize := info.Size()

	// Append an incomplete 8-byte header.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.Write([]byte{0x00, 0x00, 0x00}); err != nil {
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

	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != validSize {
		t.Fatalf("expected WAL size %d after truncation, got %d",
			validSize, info.Size())
	}
}

func TestRecoverIncompletePayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incomplete_payload.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	event := wire.Event{
		ProducerId: "producer-1",
		EventTime:  time.Unix(1727600000, 0).UTC(),
		Seq:        1,
		Idx:        0,
		EventType:  "temperature",
		Payload:    "25.6",
	}

	if err := w.Append(event); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	validSize := info.Size()

	// Append a header declaring 10 payload bytes, but write only 3.
	var header [8]byte
	binary.BigEndian.PutUint32(header[0:4], 10)
	binary.BigEndian.PutUint32(header[4:8], 12345)

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("abc")); err != nil {
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

	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != validSize {
		t.Fatalf("expected WAL size %d after truncation, got %d",
			validSize, info.Size())
	}
}

func TestRecoverChecksumMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad_checksum.wal")

	payload := []byte("{}")

	var header [8]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], 12345)

	data := append(header[:], payload...)

	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	_, err := Recover(path)
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
}
