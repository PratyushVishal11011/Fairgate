package admit

import (
	"errors"
	"fmt"
	"sync"
)

type Manager struct {
	mu      sync.RWMutex //protects concurrent access to the map.
	rate    float64
	burst   int
	buckets map[string]*TokenBucket //map that associates each producer ID with its own token bucket.
	//per-producer limits that replace the default rate and burst.
	limits map[string]Limit
}

// Limit is a token-bucket limit for one producer.
// A zero Rate or Burst keeps the manager's default for that field.
type Limit struct {
	Rate  float64
	Burst int
}

func NewManager(ratePerSecond float64, burst int) (*Manager, error) {
	return NewManagerWithLimits(ratePerSecond, burst, nil)
}

// NewManagerWithLimits applies ratePerSecond and burst to every producer except those listed in limits.
func NewManagerWithLimits(ratePerSecond float64, burst int, limits map[string]Limit) (*Manager, error) {
	if ratePerSecond <= 0 {
		return nil, errors.New("rate per second must be greater than zero")
	}
	if burst <= 0 {
		return nil, errors.New("burst must be greater than zero")
	}

	resolved := make(map[string]Limit, len(limits))
	for producerId, limit := range limits {
		if limit.Rate < 0 || limit.Burst < 0 {
			return nil, fmt.Errorf("limit for producer %q cannot be negative", producerId)
		}
		if limit.Rate == 0 {
			limit.Rate = ratePerSecond
		}
		if limit.Burst == 0 {
			limit.Burst = burst
		}
		resolved[producerId] = limit
	}

	return &Manager{
		rate:    ratePerSecond,
		burst:   burst,
		buckets: make(map[string]*TokenBucket),
		limits:  resolved,
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
		rate, burst := m.rate, m.burst
		if limit, ok := m.limits[ProducerId]; ok {
			rate, burst = limit.Rate, limit.Burst
		}

		var err error
		bucket, err = NewTokenBucket(rate, burst)
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
