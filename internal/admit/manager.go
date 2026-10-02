package admit

import (
	"errors"
	"sync"
)

type Manager struct {
	mu      sync.RWMutex //protects concurrent access to the map.
	rate    float64
	burst   int
	buckets map[string]*TokenBucket //map that associates each producer ID with its own token bucket.
}

func NewManager(ratePerSecond float64, burst int) (*Manager, error) {
	if ratePerSecond <= 0 {
		return nil, errors.New("rate per second must be greater than zero")
	}
	if burst <= 0 {
		return nil, errors.New("burst must be greater than zero")
	}

	return &Manager{
		rate:    ratePerSecond,
		burst:   burst,
		buckets: make(map[string]*TokenBucket),
	}, nil
}

func (m *Manager) Allow(ProducerId string) bool {
	//Return false if producer Id is not given
	if ProducerId == "" {
		return false
	}
	//Lock mutex to prevent dirty reads / writes
	m.mu.Lock()
	//check if producers token bucket exists
	bucket, exists := m.buckets[ProducerId]

	//create the bucket if it does not exist
	if !exists {
		var err error
		bucket, err = NewTokenBucket(m.rate, m.burst)
		if err != nil {
			//Unlock mutex if creation fails and return false
			m.mu.Unlock()
			return false
		}
		m.buckets[ProducerId] = bucket
	}
	m.mu.Unlock()
	return bucket.Allow()
}
