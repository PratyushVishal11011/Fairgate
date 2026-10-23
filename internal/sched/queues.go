package sched

import (
	"FairGate/internal/wire"
	"context"
	"errors"
	"sort"
	"sync"
)

type EventKey struct {
	ProducerId    string
	ProducerEpoch string
	Seq           uint64
	Idx           uint16
}

type Queues struct {
	mu       sync.Mutex
	capacity int
	//used chan (channel here) to map queues to producer ids
	//channels are a safe way for goroutines to communicate and pass data
	queues map[string]chan wire.Event
	//Added another event key property for idempotency
	seen map[EventKey]struct{}
}

func NewQueues(capacity int) (*Queues, error) {
	if capacity <= 0 {
		return nil, errors.New("queue capacity must be greater than zero")
	}

	return &Queues{
		capacity: capacity,
		queues:   make(map[string]chan wire.Event),
		seen:     make(map[EventKey]struct{}),
	}, nil
}

func (q *Queues) getOrCreate(producerId string) chan wire.Event {
	//remove mutex lock from this function to the enqueueContext function
	//this prevents an indefinite lock

	//Check if producerId has a queue
	queue, exists := q.queues[producerId]

	//Create a queue if it does not exist
	if !exists {
		queue = make(chan wire.Event, q.capacity)
		q.queues[producerId] = queue
	}

	return queue
}

func (q *Queues) EnqueueContext(ctx context.Context, event wire.Event) bool {
	//add a check for producer epoch as well
	if event.ProducerId == "" || event.ProducerEpoch == "" {
		return false
	}

	key := EventKey{
		ProducerId:    event.ProducerId,
		ProducerEpoch: event.ProducerEpoch,
		Seq:           event.Seq,
		Idx:           event.Idx,
	}

	//Get or create queue depending on availability
	//obtain a mutex lock
	q.mu.Lock()

	//release the mutex if it has already been added
	if _, exists := q.seen[key]; exists {
		q.mu.Unlock()
		return true
	}

	queue := q.getOrCreate(event.ProducerId)
	//reserve this identity so concurrent retries cannot enqueue it twice.
	q.seen[key] = struct{}{}

	//unlock the mutex once the queue has been created
	q.mu.Unlock()

	//Convert the enqueue from a non-blocking to a blocking step
	//Ensures no processes are rejected due to a full queue
	//TODO: Overflow Buffer + Non blocking queue to mitigate this issue to some extent
	//update: idempotency implemented
	//Might come up with a better solution later so haven't implemented for now

	//Implemented a non-blocking procedure to ensure graceful shutdowns
	select {
	case queue <- event:
		return true
	//if the context is canceled or its deadline expires while waiting to enqueue, the select statement can exit without waiting for space in the queue.
	//remove the event's identity from the deduplication map so a subsequent retry can attempt to enqueue the event again.
	case <-ctx.Done():
		q.mu.Lock()
		delete(q.seen, key)
		q.mu.Unlock()
		return false
	}
}

func (q *Queues) Enqueue(event wire.Event) bool {
	return q.EnqueueContext(context.Background(), event)
}

func (q *Queues) Queue(producerId string) <-chan wire.Event {
	// returns the queue (channel) belonging to a specific producer.
	// If the producer doesn't have a queue yet, it creates one.
	if producerId == "" {
		return nil
	}
	return q.getOrCreate(producerId)
}

func (q *Queues) ProducerIds() []string {
	// returns a sorted snapshot of producer IDs.
	// Not necessary but sorting makes the processing order deterministic
	// This makes it easier to debug and im too lazy to break my head later
	q.mu.Lock()
	defer q.mu.Unlock()

	ids := make([]string, 0, len(q.queues))

	for id := range q.queues {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
