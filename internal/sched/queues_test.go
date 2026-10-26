package sched

import (
	"context"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func TestQueueCapacity(t *testing.T) {
	queues, err := NewQueues(2)
	if err != nil {
		t.Fatal(err)
	}

	event1 := wire.Event{
		ProducerId:    "producer-A",
		ProducerEpoch: "epoch-001",
		Seq:           1,
		Idx:           0,
	}
	event2 := wire.Event{
		ProducerId:    "producer-A",
		ProducerEpoch: "epoch-001",
		Seq:           2,
		Idx:           0,
	}
	event3 := wire.Event{
		ProducerId:    "producer-A",
		ProducerEpoch: "epoch-001",
		Seq:           3,
		Idx:           0,
	}

	if !queues.Enqueue(event1) {
		t.Fatal("expected first event to be enqueued")
	}

	if !queues.Enqueue(event2) {
		t.Fatal("expected second event to be enqueued")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan bool, 1)

	// The third enqueue should block while the queue is full.
	go func() {
		result <- queues.EnqueueContext(ctx, event3)
	}()

	select {
	case ok := <-result:
		t.Fatalf("expected enqueue to block, returned %v", ok)
	case <-time.After(50 * time.Millisecond):
		// Expected: the queue is full, so enqueue is waiting.
	}

	// Remove one event to free queue capacity.
	received := <-queues.Queue("producer-A")
	if received.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", received.Seq)
	}

	// The third enqueue should now complete.
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("expected third event to be enqueued")
		}
	case <-time.After(time.Second):
		cancel()
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

func TestQueueCapacityCancellation(t *testing.T) {
	queues, err := NewQueues(1)
	if err != nil {
		t.Fatal(err)
	}

	first := wire.Event{
		ProducerId:    "producer-A",
		ProducerEpoch: "epoch-001",
		Seq:           1,
	}
	second := wire.Event{
		ProducerId:    "producer-A",
		ProducerEpoch: "epoch-001",
		Seq:           2,
	}

	if !queues.Enqueue(first) {
		t.Fatal("expected first event to be enqueued")
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan bool, 1)

	go func() {
		result <- queues.EnqueueContext(ctx, second)
	}()

	// Give the enqueue goroutine time to reach the full queue.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case ok := <-result:
		if ok {
			t.Fatal("expected enqueue to fail after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue did not return after context cancellation")
	}

	// The original event should remain in the queue.
	received := <-queues.Queue("producer-A")
	if received.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", received.Seq)
	}
}

func TestQueueCapacityOverride(t *testing.T) {
	queues, err := NewQueuesWithCapacities(1, map[string]int{"big": 3})
	if err != nil {
		t.Fatal(err)
	}

	if got := cap(queues.Queue("big")); got != 3 {
		t.Fatalf("expected overridden capacity 3, got %d", got)
	}
	if got := cap(queues.Queue("other")); got != 1 {
		t.Fatalf("expected default capacity 1, got %d", got)
	}

	if _, err := NewQueuesWithCapacities(1, map[string]int{"bad": -1}); err == nil {
		t.Fatal("expected negative capacity to be rejected")
	}
}
