package sched

import (
	"FairGate/internal/wire"
	"context"
	"time"
)

type DRRScheduler struct {
	//The number of events a producer of weight 1 is allowed to process in each round
	//Assumed equal cost for each event for now - will update later
	quantum int
	//weight applied to producers that are not listed in weights
	defaultWeight float64
	//per-producer weights; a producer's share of each round is quantum * weight
	weights map[string]float64
	//credit a producer has left; fractional credit carries over while the producer stays backlogged
	deficits map[string]float64
}

func NewDRRScheduler(quantum int) *DRRScheduler {
	return NewWeightedDRRScheduler(quantum, 1, nil)
}

// NewWeightedDRRScheduler serves each backlogged producer quantum * weight events per round on average.
// Producers missing from weights get defaultWeight. Non-positive weights fall back to the default.
func NewWeightedDRRScheduler(quantum int, defaultWeight float64, weights map[string]float64) *DRRScheduler {
	//Creates a new scheduler and if the provided quantum <= 0 then it makes it 8 (Arbitrary number)
	if quantum <= 0 {
		quantum = 8
	}
	if defaultWeight <= 0 {
		defaultWeight = 1
	}

	resolved := make(map[string]float64, len(weights))
	for producerId, weight := range weights {
		if weight > 0 {
			resolved[producerId] = weight
		}
	}

	return &DRRScheduler{
		quantum:       quantum,
		defaultWeight: defaultWeight,
		weights:       resolved,
		deficits:      make(map[string]float64),
	}
}

func (s *DRRScheduler) weight(producerId string) float64 {
	if weight, ok := s.weights[producerId]; ok {
		return weight
	}
	return s.defaultWeight
}

func (s *DRRScheduler) Run(
	//Main Scheduler Loop
	ctx context.Context, //Allows the scheduler to be stopped or canceled
	queues *Queues, //Gives access to each events producer queue
	process func(wire.Event) error) error { //A callback function that handles each event selected by the scheduler

	//The outer for loop runs indefinitely until the context is canceled or an error occurs
	for {
		select {
		//ctx.Done() is a channel that becomes ready when the context is canceled or its deadline expires.
		case <-ctx.Done():
			//If the context is canceled, the scheduler returns the context error.
			return ctx.Err()
		//Otherwise, the default case lets it continue immediately without blocking.
		default:
		}
		//returns the IDs of producers that have queues.
		producerIds := queues.ProducerIds()
		//this tracks whether at least one event was processed during this pass through the producers.
		//updated to true after the scheduler successfully processes an event
		didWork := false

		//iterate through each producer
		//For every backlogged producer, we add its weighted quantum to its deficit and get the producer's receive-only event channel.
		for _, producerId := range producerIds {
			queue := queues.Queue(producerId)

			//an idle producer does not bank credit, as in classic DRR.
			if len(queue) == 0 {
				s.deficits[producerId] = 0
				continue
			}

			s.deficits[producerId] += float64(s.quantum) * s.weight(producerId)

			//this continues while the current producer has credit for at least one more event.
			for s.deficits[producerId] >= 1 {
				//here, the code checks cancellation again. This gives it another opportunity to stop between event-processing operations.
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				select {
				//The queue contains an event, it is passed to the process callback
				case event := <-queue:
					//If processing fails, the scheduler stops and returns the error.
					if err := process(event); err != nil {
						return err
					}
					//else, producer's deficit decreases by one because one event's worth of processing allowance has been consumed
					s.deficits[producerId]--
					//didWork is set to true to indicate some work was performed
					didWork = true
				//because of non-blocking select, the scheduler doesn't wait for an event to arrive.
				//instead, it sets producer's deficit to zero and stops using that producer's current allowance
				//prevents unused allowance from accumulating while the producer has no queued events.
				default:
					//no events available for this producer
					s.deficits[producerId] = 0
					break
				}

			}
		}
		//once the scheduler has visited all producers, it checks whether any work was done
		//if no events were processed, the scheduler waits for 1 millisecond or until the context is canceled.
		//prevents the scheduler from continuously looping at full CPU usage when all queues are empty
		//if work was done, it immediately starts another pass.
		if !didWork {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
	}
}
