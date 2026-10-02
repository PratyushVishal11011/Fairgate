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
