package store

import (
	"testing"
	"time"

	"FairGate/internal/wire"
)

func testStoreEvent(seq uint64, payload string) wire.Event {
	return wire.Event{
		ProducerId:    "producer-1",
		ProducerEpoch: "epoch-1",
		EventTime:     time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC),
		Seq:           seq,
		EventType:     "test",
		Payload:       payload,
	}
}

func TestUniqueRowsKeepsFirstEventPerIdentity(t *testing.T) {
	events := []wire.Event{
		testStoreEvent(1, "first"),
		testStoreEvent(2, "second"),
		testStoreEvent(1, "first again"),
	}

	rows := uniqueRows(events)

	if len(rows) != 2 {
		t.Fatalf("expected 2 unique rows, got %d", len(rows))
	}
	if rows[0].event.Payload != "first" || rows[1].event.Payload != "second" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if rows[0].id != eventID(events[0]) {
		t.Fatalf("row ID does not match event ID")
	}
}

func TestWithoutExistingDropsStoredEvents(t *testing.T) {
	rows := uniqueRows([]wire.Event{
		testStoreEvent(1, "a"),
		testStoreEvent(2, "b"),
		testStoreEvent(3, "c"),
	})

	remaining := withoutExisting(rows, map[string]struct{}{
		rows[0].id: {},
		rows[2].id: {},
	})

	if len(remaining) != 1 || remaining[0].id != rows[1].id {
		t.Fatalf("expected only the unstored event, got %+v", remaining)
	}
	if got := withoutExisting(rows, nil); len(got) != len(rows) {
		t.Fatalf("expected all rows without existing IDs, got %d", len(got))
	}
}

func TestInsertTokenBindsBatchTokenToRows(t *testing.T) {
	rows := uniqueRows([]wire.Event{
		testStoreEvent(1, "a"),
		testStoreEvent(2, "b"),
	})

	token := insertToken("batch", rows)

	if token != insertToken("batch", uniqueRows([]wire.Event{testStoreEvent(1, "a"), testStoreEvent(2, "b")})) {
		t.Fatal("the same rows must produce the same token")
	}
	if token == insertToken("other", rows) {
		t.Fatal("a different batch token must produce a different insert token")
	}
	// A replay that sends only the rows that did not land must not reuse the original token,
	// or ClickHouse would discard it as a duplicate block.
	if token == insertToken("batch", rows[1:]) {
		t.Fatal("a subset of rows must produce a different insert token")
	}
}
