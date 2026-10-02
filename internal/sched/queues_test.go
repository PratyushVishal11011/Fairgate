package sched

import (
	"FairGate/internal/wire"
	"testing"
)

func TestQueueCapacity(t *testing.T) {
	queues, err := NewQueues(2)
	if err != nil {
		t.Fatal(err)
	}

	event := wire.Event{ProducerId: "producer-A"}

	if !queues.Enqueue(event) {
		t.Fatal("expected first event to be enqueued")
	}

	if !queues.Enqueue(event) {
		t.Fatal("expected second event to be enqueued")
	}

	if queues.Enqueue(event) {
		t.Fatal("expected third event to be rejected")
	}
}

func TestQueueProducerIsolation(t *testing.T) {
	queues, err := NewQueues(1)
	if err != nil {
		t.Fatal(err)
	}

	if !queues.Enqueue(wire.Event{ProducerId: "producer-A"}) {
		t.Fatal("expected producer A event to be enqueued")
	}

	if !queues.Enqueue(wire.Event{ProducerId: "producer-B"}) {
		t.Fatal("expected producer B to have independent capacity")
	}
}

func TestQueueRejectsEmptyProducer(t *testing.T) {
	queues, err := NewQueues(2)
	if err != nil {
		t.Fatal(err)
	}

	if queues.Enqueue(wire.Event{}) {
		t.Fatal("expected empty producer ID to be rejected")
	}
}

func TestQueueRetrievesEvent(t *testing.T) {
	queues, err := NewQueues(2)
	if err != nil {
		t.Fatal(err)
	}

	event := wire.Event{
		ProducerId: "producer-A",
		Seq:        42,
	}

	if !queues.Enqueue(event) {
		t.Fatal("expected event to be enqueued")
	}

	received := <-queues.Queue("producer-A")

	if received.Seq != 42 {
		t.Fatalf("expected seq 42, got %d", received.Seq)
	}
}

func TestInvalidQueueCapacity(t *testing.T) {
	if _, err := NewQueues(0); err == nil {
		t.Fatal("expected error for zero capacity")
	}
}
