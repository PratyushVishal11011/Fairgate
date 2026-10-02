package sched

import (
	"FairGate/internal/wire"
	"errors"
	"sort"
	"sync"
)

type EventKey struct {
	ProducerId string
	Seq        uint64
	idx        uint16
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
	//Lock mutex to prevent dirty read / writes
	q.mu.Lock()
	//make sure the mutex unlocks when the function returns
	defer q.mu.Unlock()

	//Check if producerId has a queue
	queue, exists := q.queues[producerId]

	//Create a queue if it does not exist
	if !exists {
		queue = make(chan wire.Event, q.capacity)
		q.queues[producerId] = queue
	}

	return queue
}

func (q *Queues) Enqueue(event wire.Event) bool {
	if event.ProducerId == "" {
		return false
	}

	//Get or create queue depending on availability
	queue := q.getOrCreate(event.ProducerId)

	//Convert the enqueue from a non-blocking to a blocking step
	//Ensures no processes are rejected due to a full queue
	//TODO: Implement Idempotency + Overflow Buffer + Non blocking queue to mitigate this issue to some extent
	//Might come up with a better solution later so haven't implemented for now
	queue <- event
	return true
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
