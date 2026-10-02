package admit

import (
	"errors"
	"sync"
	"time"
)

type TokenBucket struct {
	mu         sync.RWMutex //Using a mutex to protect the bucket from concurrent access by multiple goroutines.
	rate       float64      //Number of tokens replenished per second
	capacity   float64      //Max number of tokens the bucket can hold
	tokens     float64      //Number of tokens currently available
	lastRefill time.Time    //Tracks the last time tokens were replenished
	now        func() time.Time
}

func NewTokenBucket(ratePerSecond float64, burst int) (*TokenBucket, error) {
	if ratePerSecond <= 0 || burst <= 0 {
		return nil, errors.New("rate per second must be greater than 0 and less than 0")
	}

	now := time.Now

	return &TokenBucket{
		rate:       ratePerSecond,
		capacity:   float64(burst),
		tokens:     float64(burst),
		lastRefill: now(),
		now:        now,
	}, nil
}
func (b *TokenBucket) Allow() bool {
	//Lock grants exclusive access to the current goroutine trying to access the mutex
	//This prevents dirty reads/writes when multiple goroutines try to access the mutex concurrently
	b.mu.Lock()
	//makes sure that the mutex is released once the function is returns regardless of acceptance or rejection aka (the status of my internship applications)
	defer b.mu.Unlock()

	currentTime := b.now()
	elapsed := currentTime.Sub(b.lastRefill).Seconds()

	//replenish tokens based on the elapsed time
	if elapsed > 0 {
		b.tokens += elapsed * b.rate

		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.lastRefill = currentTime
	}

	//if fewer than one token is available, request is rejected
	if b.tokens < 1 {
		return false
	}
	//deduct from the total number of available tokens and return true if token is allocated
	b.tokens--
	return true
}
