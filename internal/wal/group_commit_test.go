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

	w, err := OpenWithSegmentSize(path, 4096)
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

// gatedObserver blocks the writer inside its first BatchCommitted call until release is closed,
// so appends submitted meanwhile pile up in the request channel.
type gatedObserver struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu      sync.Mutex
	batches []int
}

func (o *gatedObserver) BatchCommitted(records int, _ int64, _ time.Duration) {
	o.mu.Lock()
	o.batches = append(o.batches, records)
	o.mu.Unlock()

	o.once.Do(func() {
		close(o.entered)
		<-o.release
	})
}

func (o *gatedObserver) SegmentRotated() {}

func TestQueuedAppendsShareOneSync(t *testing.T) {
	w, err := Open(filepath.Join(t.TempDir(), "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	observer := &gatedObserver{entered: make(chan struct{}), release: make(chan struct{})}
	w.SetObserver(observer)

	event := func(seq int) wire.Event {
		return wire.Event{
			ProducerId: "producer-001",
			EventTime:  time.Now().UTC(),
			Seq:        uint64(seq),
			EventType:  "test",
			Payload:    fmt.Sprintf("event-%d", seq),
		}
	}

	//the first append holds the writer in the observer after its sync.
	firstDone := make(chan error, 1)
	go func() { firstDone <- w.Append(event(1)) }()
	<-observer.entered

	//well over the old 64-record batch limit.
	const queued = 300
	var wg sync.WaitGroup
	errs := make(chan error, queued)
	for i := 0; i < queued; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			if err := w.Append(event(seq)); err != nil {
				errs <- err
			}
		}(i + 2)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(w.requests) < queued {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d appends queued", len(w.requests), queued)
		}
		time.Sleep(time.Millisecond)
	}

	close(observer.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.batches) != 2 || observer.batches[1] != queued {
		t.Fatalf("got group commits of %v records; want [1 %d]", observer.batches, queued)
	}
}
