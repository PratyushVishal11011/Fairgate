package deduplication

import (
	"errors"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func testEvent() wire.Event {
	return wire.Event{
		ProducerId:    "producer-001",
		ProducerEpoch: "epoch-001",
		EventTime:     time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC),
		Seq:           1,
		Idx:           0,
		EventType:     "bearing_fault",
		Payload:       `{"severity":"high"}`,
	}
}

func TestPersistIfNew(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent()
	persistCalls := 0

	persist := func() error {
		persistCalls++
		return nil
	}

	duplicate, err := m.PersistIfNew(event, persist)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate {
		t.Fatal("first event was marked as duplicate")
	}

	duplicate, err = m.PersistIfNew(event, persist)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate {
		t.Fatal("retry was not marked as duplicate")
	}
	if persistCalls != 1 {
		t.Fatalf("persist called %d times; want 1", persistCalls)
	}
}

func TestRejectConflictingContents(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent()
	_, err = m.PersistIfNew(event, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	event.Payload = `{"severity":"low"}`
	_, err = m.PersistIfNew(event, func() error {
		t.Fatal("conflicting event must not be persisted")
		return nil
	})

	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got error %v; want ErrConflict", err)
	}
}

func TestRecoveredEventIsDuplicate(t *testing.T) {
	event := testEvent()

	m, err := NewManager([]wire.Event{event})
	if err != nil {
		t.Fatal(err)
	}

	called := false
	duplicate, err := m.PersistIfNew(event, func() error {
		called = true
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if !duplicate {
		t.Fatal("recovered event was not marked as duplicate")
	}
	if called {
		t.Fatal("recovered event was persisted again")
	}
}

func TestPersistenceFailureDoesNotRecordIdentity(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent()
	writeErr := errors.New("simulated WAL failure")

	_, err = m.PersistIfNew(event, func() error {
		return writeErr
	})
	if !errors.Is(err, writeErr) {
		t.Fatalf("got error %v; want %v", err, writeErr)
	}

	duplicate, err := m.PersistIfNew(event, func() error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate {
		t.Fatal("event should not be marked as duplicate after persistence failure")
	}
}
