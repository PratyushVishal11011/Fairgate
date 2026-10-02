package sched

import (
	"context"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func TestSchedulerDrainsQueueBeyondCapacity(t *testing.T) {
	const (
		capacity = 256
		total    = 600
	)

	queues, err := NewQueues(capacity)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := NewDRRScheduler(8)
	processed := make(chan wire.Event, total)
	schedulerDone := make(chan error, 1)

	go func() {
		schedulerDone <- scheduler.Run(ctx, queues, func(event wire.Event) error {
			processed <- event
			return nil
		})
	}()

	enqueueDone := make(chan bool, 1)

	go func() {
		for i := 0; i < total; i++ {
			event := wire.Event{
				ProducerId: "producer-1",
				EventTime:  time.Now(),
				Seq:        uint64(i + 1),
				Idx:        0,
				EventType:  "test",
				Payload:    "recovery-test",
			}

			if !queues.Enqueue(event) {
				enqueueDone <- false
				return
			}
		}
		enqueueDone <- true
	}()

	select {
	case success := <-enqueueDone:
		if !success {
			t.Fatal("failed to enqueue events")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("enqueue blocked; scheduler may not be draining the queue")
	}

	for i := 0; i < total; i++ {
		select {
		case <-processed:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after processing %d of %d events", i, total)
		}
	}

	cancel()

	select {
	case <-schedulerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not stop after cancellation")
	}
}
