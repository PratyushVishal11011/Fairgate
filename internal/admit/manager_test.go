package admit

import "testing"

func TestManagerProducerIsolation(t *testing.T) {
	manager, err := NewManager(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	if !manager.Allow("producer-A") {
		t.Fatal("expected producer A to be allowed")
	}

	if manager.Allow("producer-A") {
		t.Fatal("expected producer A to be rate-limited")
	}

	if !manager.Allow("producer-B") {
		t.Fatal("expected producer B to have an independent bucket")
	}
}

func TestManagerRejectsEmptyProducer(t *testing.T) {
	manager, err := NewManager(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	if manager.Allow("") {
		t.Fatal("expected empty producer ID to be rejected")
	}
}

func TestManagerInvalidConfig(t *testing.T) {
	if _, err := NewManager(0, 1); err == nil {
		t.Fatal("expected error for zero rate")
	}

	if _, err := NewManager(1, 0); err == nil {
		t.Fatal("expected error for zero burst")
	}
}

func TestManagerProducerLimitOverride(t *testing.T) {
	manager, err := NewManagerWithLimits(1, 1, map[string]Limit{
		"bursty": {Burst: 3},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if !manager.Allow("bursty") {
			t.Fatalf("expected bursty event %d to be allowed by its own burst", i+1)
		}
	}
	if manager.Allow("bursty") {
		t.Fatal("expected bursty to be limited after its burst")
	}

	if !manager.Allow("default") || manager.Allow("default") {
		t.Fatal("expected unlisted producer to keep the default burst of 1")
	}
}

func TestManagerRejectsNegativeLimit(t *testing.T) {
	if _, err := NewManagerWithLimits(1, 1, map[string]Limit{"bad": {Rate: -1}}); err == nil {
		t.Fatal("expected negative rate to be rejected")
	}
}
