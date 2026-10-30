package sched

import (
	"FairGate/internal/wire"
	"context"
	"errors"
	"fmt"
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
	//per-producer capacities that replace the default capacity.
	capacities map[string]int
}

func NewQueues(capacity int) (*Queues, error) {
	return NewQueuesWithCapacities(capacity, nil)
}

// NewQueuesWithCapacities gives every producer a queue of capacity events, except those listed in capacities.
// A zero entry keeps the default capacity.
func NewQueuesWithCapacities(capacity int, capacities map[string]int) (*Queues, error) {
	if capacity <= 0 {
		return nil, errors.New("queue capacity must be greater than zero")
	}

	resolved := make(map[string]int, len(capacities))
	for producerId, producerCapacity := range capacities {
		if producerCapacity < 0 {
			return nil, fmt.Errorf("queue capacity for producer %q cannot be negative", producerId)
		}
		if producerCapacity == 0 {
			producerCapacity = capacity
		}
		resolved[producerId] = producerCapacity
	}

	return &Queues{
		capacity:   capacity,
		queues:     make(map[string]chan wire.Event),
		seen:       make(map[EventKey]struct{}),
		capacities: resolved,
	}, nil
}

func (q *Queues) getOrCreate(producerId string) chan wire.Event {
	//remove mutex lock from this function to the enqueueContext function
	//this prevents an indefinite lock

	//Check if producerId has a queue
	queue, exists := q.queues[producerId]

	//Create a queue if it does not exist
	if !exists {
		capacity := q.capacity
		if producerCapacity, ok := q.capacities[producerId]; ok {
			capacity = producerCapacity
		}
		queue = make(chan wire.Event, capacity)
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
	//getOrCreate expects the caller to hold the mutex, as EnqueueContext does.
	q.mu.Lock()
	defer q.mu.Unlock()
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

// Depth returns how many events producerId has queued and its queue capacity, without creating a queue.
func (q *Queues) Depth(producerId string) (depth, capacity int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if queue, exists := q.queues[producerId]; exists {
		return len(queue), cap(queue)
	}
	if producerCapacity, ok := q.capacities[producerId]; ok {
		return 0, producerCapacity
	}
	return 0, q.capacity
}

// Len returns the number of events queued across all producers.
func (q *Queues) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	total := 0
	for _, queue := range q.queues {
		total += len(queue)
	}
	return total
}

// QueueStat is a point-in-time view of one producer queue.
type QueueStat struct {
	ProducerId string
	Depth      int
	Capacity   int
}

// Stats returns the depth and capacity of every producer queue, sorted by producer ID.
func (q *Queues) Stats() []QueueStat {
	q.mu.Lock()
	defer q.mu.Unlock()

	stats := make([]QueueStat, 0, len(q.queues))
	for id, queue := range q.queues {
		stats = append(stats, QueueStat{ProducerId: id, Depth: len(queue), Capacity: cap(queue)})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].ProducerId < stats[j].ProducerId })
	return stats
}
