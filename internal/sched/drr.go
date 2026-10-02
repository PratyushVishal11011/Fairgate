package sched

import (
	"FairGate/internal/wire"
	"context"
	"time"
)

type DRRScheduler struct {
	//The number of events a producer is allowed to process in each round
	//Assumed equal cost for each event for now - will update later
	quantum  int
	deficits map[string]int
}

func NewDRRScheduler(quantum int) *DRRScheduler {
	//Creates a new scheduler and if the provided quantum <= 0 then it makes it 8 (Arbitrary number)
	if quantum <= 0 {
		quantum = 8
	}

	return &DRRScheduler{
		quantum:  quantum,
		deficits: make(map[string]int),
	}
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
		//For every producer, we add the quantum to that producer's deficit and get the producer's receive-only event channel.
		for _, producerId := range producerIds {
			s.deficits[producerId] = s.quantum
			queue := queues.Queue(producerId)

			//this continues as long as the current producer has a positive deficit.
			for s.deficits[producerId] > 0 {
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
