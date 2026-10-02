package admit

import (
	"testing"
	"time"
)

func TestTokenBucketBurst(t *testing.T) {
	bucket, err := NewTokenBucket(2, 3)
	if err != nil {
		t.Fatal(err)
	}

	current := time.Now()
	bucket.now = func() time.Time { return current }

	for i := 0; i < 3; i++ {
		if !bucket.Allow() {
			t.Fatalf("expected token %d to be allowed", i+1)
		}
	}

	if bucket.Allow() {
		t.Fatal("expected request to be rejected when bucket is empty")
	}
}

func TestTokenBucketRefill(t *testing.T) {
	bucket, err := NewTokenBucket(2, 3)
	if err != nil {
		t.Fatal(err)
	}

	current := time.Now()
	bucket.now = func() time.Time { return current }

	for i := 0; i < 3; i++ {
		bucket.Allow()
	}

	current = current.Add(time.Second)

	if !bucket.Allow() {
		t.Fatal("expected request to be allowed after refill")
	}

	if !bucket.Allow() {
		t.Fatal("expected second request to be allowed after refill")
	}

	if bucket.Allow() {
		t.Fatal("expected request to be rejected after consuming refilled tokens")
	}
}

func TestTokenBucketInvalidConfig(t *testing.T) {
	if _, err := NewTokenBucket(0, 5); err == nil {
		t.Fatal("expected error for zero rate")
	}

	if _, err := NewTokenBucket(5, 0); err == nil {
		t.Fatal("expected error for zero burst")
	}
}
