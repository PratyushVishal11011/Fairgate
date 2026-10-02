package sched

import (
	"FairGate/internal/wire"
	"testing"
	"time"
)

func TestQueueCapacity(t *testing.T) {
	queues, err := NewQueues(2)
	if err != nil {
		t.Fatal(err)
	}

	event1 := wire.Event{ProducerId: "producer-A", Seq: 1}
	event2 := wire.Event{ProducerId: "producer-A", Seq: 2}
	event3 := wire.Event{ProducerId: "producer-A", Seq: 3}

	if !queues.Enqueue(event1) {
		t.Fatal("expected first event to be enqueued")
	}

	if !queues.Enqueue(event2) {
		t.Fatal("expected second event to be enqueued")
	}

	result := make(chan bool, 1)

	// Third enqueue should block because the queue is full.
	go func() {
		result <- queues.Enqueue(event3)
	}()

	select {
	case <-result:
		t.Fatal("expected third enqueue to block")
	case <-time.After(50 * time.Millisecond):
		// Expected: enqueue is blocked.
	}

	// Remove one event to free up queue capacity.
	received := <-queues.Queue("producer-A")
	if received.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", received.Seq)
	}

	// Third enqueue should now complete.
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("expected third event to be enqueued")
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue did not resume after space became available")
	}

	// Verify the remaining events are in FIFO order.
	received = <-queues.Queue("producer-A")
	if received.Seq != 2 {
		t.Fatalf("expected seq 2, got %d", received.Seq)
	}

	received = <-queues.Queue("producer-A")
	if received.Seq != 3 {
		t.Fatalf("expected seq 3, got %d", received.Seq)
	}
}
