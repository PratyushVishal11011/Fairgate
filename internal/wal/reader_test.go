package wal

import (
	"FairGate/internal/wire"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func testReaderEvent(producer string, seq uint64, idx uint16, payload string) wire.Event {
	return wire.Event{
		ProducerId: producer,
		Seq:        seq,
		Idx:        idx,
		EventType:  "test",
		Payload:    payload,
	}
}

func TestReaderNext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	events := []wire.Event{
		testReaderEvent("producer-001", 1, 0, "hello"),
		testReaderEvent("producer-001", 2, 0, "world"),
		testReaderEvent("producer-002", 1, 0, "fairgate"),
	}

	for _, event := range events {
		if err := w.Append(event); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}

	reader := NewReader(w, Position{})

	var previousEnd Position

	for i, expected := range events {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("Next() failed for record %d: %v", i, err)
		}

		if !reflect.DeepEqual(record.Event, expected) {
			t.Fatalf(
				"record %d mismatch:\nexpected: %+v\ngot:      %+v",
				i,
				expected,
				record.Event,
			)
		}

		if i == 0 {
			if record.Start != (Position{}) {
				t.Fatalf("first record started at %+v", record.Start)
			}
		} else {
			if record.Start != previousEnd {
				t.Fatalf(
					"record %d starts at %+v, expected %+v",
					i,
					record.Start,
					previousEnd,
				)
			}
		}

		if record.End.offset <= record.Start.offset {
			t.Fatalf(
				"record %d has invalid boundaries: start=%+v end=%+v",
				i,
				record.Start,
				record.End,
			)
		}

		previousEnd = record.End
	}
}

func TestReaderReturnsEOFAtDurableEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	event := testReaderEvent("producer-001", 1, 0, "hello")

	if err := w.Append(event); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(w, Position{})

	record, err := reader.Next()
	if err != nil {
		t.Fatalf("first Next() failed: %v", err)
	}

	positionAfterRecord := reader.position

	_, err = reader.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	if reader.position != positionAfterRecord {
		t.Fatalf(
			"reader advanced after EOF: before=%+v after=%+v",
			positionAfterRecord,
			reader.position,
		)
	}

	if record.End != positionAfterRecord {
		t.Fatalf(
			"record end %v does not match reader position %v",
			record.End,
			positionAfterRecord,
		)
	}
}

func TestReaderDetectsChecksumMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	event := testReaderEvent("producer-001", 1, 0, "hello")

	if err := w.Append(event); err != nil {
		w.Close()
		t.Fatal(err)
	}

	// Close the writer before deliberately corrupting the WAL.
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

	checksum := binary.BigEndian.Uint32(data[4:8])
	binary.BigEndian.PutUint32(data[4:8], checksum+1)

	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	// Reopen the WAL so its durable state reflects the existing file.
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	reader := NewReader(w, Position{})

	_, err = reader.Next()
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}

	if !contains(err.Error(), "WAL checksum mismatch") {
		t.Fatalf("expected checksum mismatch, got: %v", err)
	}

	// Validation failure must not advance the reader.
	if reader.position != (Position{}) {
		t.Fatalf(
			"reader advanced after checksum failure: %+v",
			reader.position,
		)
	}
}

func TestReaderAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.wal")

	wal, err := openWithSegmentSize(path, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	events := []wire.Event{
		testReaderEvent("producer-1", 1, 1, "first event"),
		testReaderEvent("producer-1", 2, 2, "second event"),
		testReaderEvent("producer-1", 3, 3, "third event"),
	}

	for _, event := range events {
		if err := wal.Append(event); err != nil {
			t.Fatal(err)
		}
	}

	reader := NewReader(wal, Position{})

	for i, expected := range events {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("reading event %d: %v", i, err)
		}

		if !reflect.DeepEqual(record.Event, expected) {
			t.Fatalf(
				"event %d mismatch: expected %+v, got %+v",
				i,
				expected,
				record.Event,
			)
		}
	}

	_, err = reader.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReaderStartsFromPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.wal")

	wal, err := openWithSegmentSize(path, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	events := []wire.Event{
		testReaderEvent("producer-1", 1, 1, "first event"),
		testReaderEvent("producer-1", 2, 2, "second event"),
		testReaderEvent("producer-1", 3, 3, "third event"),
	}

	for _, event := range events {
		if err := wal.Append(event); err != nil {
			t.Fatal(err)
		}
	}

	reader := NewReader(wal, Position{
		segmentId: 1,
		offset:    0,
	})

	record, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(record.Event, events[1]) {
		t.Fatalf(
			"expected second event, got %+v",
			record.Event,
		)
	}
}

func contains(value, target string) bool {
	return len(value) >= len(target) && value[:len(target)] == target
}
