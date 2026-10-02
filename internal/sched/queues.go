package sched

import (
	"FairGate/internal/wire"
	"errors"
	"sort"
	"sync"
)

type Queues struct {
	mu       sync.Mutex
	capacity int
	//used chan (channel here) to map queues to producer ids
	//channels are a safe way for goroutines to communicate and pass data
	queues map[string]chan wire.Event
}

func NewQueues(capacity int) (*Queues, error) {
	if capacity <= 0 {
		return nil, errors.New("queue capacity must be greater than zero")
	}

	return &Queues{
		capacity: capacity,
		queues:   make(map[string]chan wire.Event),
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

	//Checks which channels are ready to proceed
	//if multiple are ready, go chooses one among them
	select {
	//send event to queue channel
	//If the channel has available buffer space, the send can proceed immediately.
	//The event is added to queue and returns true
	case queue <- event:
		return true
	//queue is full, sending the event isn't possible immediately.
	//Instead of waiting for space to become available, the function returns false.
	default:
		return false
	}
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
