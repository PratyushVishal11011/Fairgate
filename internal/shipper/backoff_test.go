package shipper

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoffCapsAtMaximum(t *testing.T) {
	backoff := NewBackoff(
		100*time.Millisecond,
		1*time.Second,
		2.0,
		0,
		42,
	)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1 * time.Second},
		{5, 1 * time.Second},
		{10, 1 * time.Second},
	}

	for _, test := range tests {
		got := backoff.Duration(test.attempt)

		if got != test.want {
			t.Fatalf(
				"attempt %d: expected %v, got %v",
				test.attempt,
				test.want,
				got,
			)
		}
	}
}

func TestBackoffJitterStaysWithinRange(t *testing.T) {
	backoff := NewBackoff(
		1*time.Second,
		10*time.Second,
		2.0,
		0.2,
		42,
	)

	base := 1 * time.Second

	for i := 0; i < 100; i++ {
		got := backoff.Duration(0)

		min := time.Duration(float64(base) * 0.8)
		max := time.Duration(float64(base) * 1.2)

		if got < min || got > max {
			t.Fatalf(
				"jittered delay %v outside [%v, %v]",
				got,
				min,
				max,
			)
		}
	}
}

func TestBackoffNegativeAttemptUsesInitialDelay(t *testing.T) {
	backoff := NewBackoff(
		100*time.Millisecond,
		1*time.Second,
		2.0,
		0,
		42,
	)

	if got := backoff.Duration(-1); got != 100*time.Millisecond {
		t.Fatalf("expected initial delay, got %v", got)
	}
}

func TestBackoffWaitHonorsContext(t *testing.T) {
	backoff := NewBackoff(
		1*time.Second,
		1*time.Second,
		2.0,
		0,
		42,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := backoff.Wait(ctx, 0)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestBackoffWaitSucceeds(t *testing.T) {
	backoff := NewBackoff(
		1*time.Millisecond,
		1*time.Millisecond,
		2.0,
		0,
		42,
	)

	start := time.Now()

	if err := backoff.Wait(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	if elapsed := time.Since(start); elapsed < 1*time.Millisecond {
		t.Fatalf("wait returned too early: %v", elapsed)
	}
}
