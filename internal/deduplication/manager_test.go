package deduplication

import (
	"errors"
	"sync"
	"sync/atomic"
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

func TestConcurrentRetriesPersistOnce(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent()
	var persistCalls atomic.Int32
	var duplicates atomic.Int32

	const retries = 50
	var wg sync.WaitGroup
	for i := 0; i < retries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			duplicate, err := m.PersistIfNew(event, func() error {
				persistCalls.Add(1)
				time.Sleep(time.Millisecond)
				return nil
			})
			if err != nil {
				t.Error(err)
			}
			if duplicate {
				duplicates.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := persistCalls.Load(); got != 1 {
		t.Fatalf("persist called %d times; want 1", got)
	}
	if got := duplicates.Load(); got != retries-1 {
		t.Fatalf("got %d duplicates; want %d", got, retries-1)
	}
}

func TestDistinctIdentitiesPersistConcurrently(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}

	first := testEvent()
	second := testEvent()
	second.Seq = 2

	release := make(chan struct{})
	started := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := m.PersistIfNew(first, func() error {
			close(started)
			<-release
			return nil
		})
		firstDone <- err
	}()
	<-started

	//the second identity must not wait for the first one's persist, e.g. its WAL fsync.
	secondDone := make(chan error, 1)
	go func() {
		_, err := m.PersistIfNew(second, func() error { return nil })
		secondDone <- err
	}()

	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("persist of a distinct identity was blocked by another persist")
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestRetryPersistsAfterInflightFailure(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent()
	writeErr := errors.New("simulated WAL failure")
	release := make(chan struct{})
	started := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := m.PersistIfNew(event, func() error {
			close(started)
			<-release
			return writeErr
		})
		firstDone <- err
	}()
	<-started

	retried := make(chan bool, 1)
	retryDone := make(chan error, 1)
	go func() {
		duplicate, err := m.PersistIfNew(event, func() error {
			retried <- true
			return nil
		})
		if duplicate {
			err = errors.New("retry was marked as duplicate after the first persist failed")
		}
		retryDone <- err
	}()

	close(release)
	if err := <-firstDone; !errors.Is(err, writeErr) {
		t.Fatalf("got error %v; want %v", err, writeErr)
	}
	if err := <-retryDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-retried:
	default:
		t.Fatal("retry did not persist the event")
	}
}
