package shipper

import (
	"context"
	"math/rand"
	"time"
)

type Backoff struct {
	initial    time.Duration
	max        time.Duration
	multiplier float64
	jitter     float64
	rng        *rand.Rand
}

func NewBackoff(
	initial time.Duration,
	max time.Duration,
	multiplier float64,
	jitter float64,
	seed int64,
) *Backoff {
	return &Backoff{
		initial:    initial,
		max:        max,
		multiplier: multiplier,
		jitter:     jitter,
		rng:        rand.New(rand.NewSource(seed)),
	}
}

func (b *Backoff) Duration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := float64(b.initial)

	for i := 0; i < attempt; i++ {
		delay *= b.multiplier
		if delay > float64(b.max) {
			delay = float64(b.max)
			break
		}
	}
	if delay > float64(b.max) {
		delay = float64(b.max)
	}

	//jitter is applied symmetrically around the base delay.
	//jitter=0.2 means the final delay is in: [80% of base, 120% of base]
	if b.jitter > 0 {
		factor := 1 + ((b.rng.Float64()*2 - 1) * b.jitter)
		delay *= factor
	}

	if delay > float64(b.max) {
		delay = float64(b.max)
	}

	if delay < 0 {
		delay = 0
	}

	return time.Duration(delay)
}

func (b *Backoff) Wait(ctx context.Context, attempt int) error {
	delay := b.Duration(attempt)

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
