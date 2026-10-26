package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"FairGate/internal/wire"
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
				ProducerId:    producer,
				ProducerEpoch: "epoch-001",
				Seq:           uint64(i + 1),
				Idx:           0,
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
			t.Errorf(
				"%s processed %d events, expected 10",
				producer,
				counts[producer],
			)
		}

	}
}

func fillQueue(t *testing.T, queues *Queues, producer string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if !queues.Enqueue(wire.Event{
			ProducerId:    producer,
			ProducerEpoch: "epoch-001",
			Seq:           uint64(i + 1),
		}) {
			t.Fatalf("failed to enqueue for %s", producer)
		}
	}
}

// runUntil processes events until total have been served and returns the count per producer.
func runUntil(t *testing.T, scheduler *DRRScheduler, queues *Queues, total int) map[string]int {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	counts := make(map[string]int)
	served := 0
	err := scheduler.Run(ctx, queues, func(event wire.Event) error {
		counts[event.ProducerId]++
		served++
		if served == total {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	return counts
}

func TestWeightedDRRSharesRoundsByWeight(t *testing.T) {
	queues, err := NewQueues(500)
	if err != nil {
		t.Fatal(err)
	}
	fillQueue(t, queues, "heavy", 400)
	fillQueue(t, queues, "light", 400)

	scheduler := NewWeightedDRRScheduler(8, 1, map[string]float64{"heavy": 3})

	// Five full rounds: heavy gets 8*3 per round, light gets 8.
	counts := runUntil(t, scheduler, queues, 160)

	if counts["heavy"] != 120 || counts["light"] != 40 {
		t.Fatalf("expected heavy=120 light=40, got %v", counts)
	}
}

func TestWeightedDRRCarriesFractionalCredit(t *testing.T) {
	queues, err := NewQueues(100)
	if err != nil {
		t.Fatal(err)
	}
	fillQueue(t, queues, "half", 50)
	fillQueue(t, queues, "whole", 50)

	// With quantum 1, a weight of 0.5 earns one event every second round.
	scheduler := NewWeightedDRRScheduler(1, 1, map[string]float64{"half": 0.5})
	counts := runUntil(t, scheduler, queues, 30)

	if counts["half"] != 10 || counts["whole"] != 20 {
		t.Fatalf("expected half=10 whole=20, got %v", counts)
	}
}

func TestWeightedDRRDefaultWeightAppliesToUnlistedProducers(t *testing.T) {
	queues, err := NewQueues(100)
	if err != nil {
		t.Fatal(err)
	}
	fillQueue(t, queues, "listed", 60)
	fillQueue(t, queues, "unlisted", 60)

	scheduler := NewWeightedDRRScheduler(4, 2, map[string]float64{"listed": 1})
	counts := runUntil(t, scheduler, queues, 36)

	if counts["listed"] != 12 || counts["unlisted"] != 24 {
		t.Fatalf("expected listed=12 unlisted=24, got %v", counts)
	}
}

func TestWeightedDRRDoesNotBankCreditWhileIdle(t *testing.T) {
	queues, err := NewQueues(100)
	if err != nil {
		t.Fatal(err)
	}
	// Create the producer's queue without any events in it.
	_ = queues.Queue("idle")

	scheduler := NewWeightedDRRScheduler(1, 1, map[string]float64{"idle": 0.5})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = scheduler.Run(ctx, queues, func(wire.Event) error { return nil })

	if credit := scheduler.deficits["idle"]; credit != 0 {
		t.Fatalf("idle producer banked %v credit", credit)
	}
}
