package sched

import (
	"FairGate/internal/wire"
	"context"
	"errors"
	"testing"
)

func TestDRRFairness(t *testing.T) {
	queues, err := NewQueues(20)
	if err != nil {
		t.Fatal(err)
	}

	producers := []string{"producer-A", "producer-B", "producer-C"}

	for _, producer := range producers {
		for i := 0; i < 10; i++ {
			if !queues.Enqueue(wire.Event{
				ProducerId: producer,
				Seq:        uint64(i + 1),
			}) {
				t.Fatalf("failed to enqueue for %s", producer)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := NewDRRScheduler(8)
	counts := make(map[string]int)
	total := 0

	err = scheduler.Run(ctx, queues, func(event wire.Event) error {
		counts[event.ProducerId]++
		total++

		if total == 30 {
			cancel()
		}

		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	for _, producer := range producers {
		if counts[producer] != 10 {
			t.Errorf("%s processed %d events, expected 10",
				producer, counts[producer])
		}
	}
}
