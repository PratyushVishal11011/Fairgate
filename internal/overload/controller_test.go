package overload

import (
	"errors"
	"testing"

	"FairGate/internal/sched"
)

// fakeBacklog reports fixed queue depths with one capacity for every producer.
type fakeBacklog struct {
	capacity int
	depths   map[string]int
}

func (f *fakeBacklog) Depth(producerId string) (int, int) {
	return f.depths[producerId], f.capacity
}

func (f *fakeBacklog) Stats() []sched.QueueStat {
	stats := make([]sched.QueueStat, 0, len(f.depths))
	for id, depth := range f.depths {
		stats = append(stats, sched.QueueStat{ProducerId: id, Depth: depth, Capacity: f.capacity})
	}
	return stats
}

func reason(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		return ""
	}
	var rejection *Rejection
	if !errors.As(err, &rejection) {
		t.Fatalf("expected a *Rejection, got %v", err)
	}
	return rejection.Reason
}

func TestQueueHighWater(t *testing.T) {
	backlog := &fakeBacklog{capacity: 10, depths: map[string]int{}}
	c, err := New(Config{QueueHighWater: 0.9, ResumeRatio: 0.8}, backlog, nil)
	if err != nil {
		t.Fatal(err)
	}

	for depth, want := range map[int]string{0: "", 8: "", 9: ReasonQueueBacklogged, 10: ReasonQueueBacklogged} {
		backlog.depths["a"] = depth
		if got := reason(t, c.Check("a")); got != want {
			t.Fatalf("depth %d: got %q, want %q", depth, got, want)
		}
	}
}

func TestQueueHighWaterZeroDisables(t *testing.T) {
	backlog := &fakeBacklog{capacity: 10, depths: map[string]int{"a": 10}}
	c, err := New(Config{ResumeRatio: 0.8}, backlog, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Check("a"); err != nil {
		t.Fatalf("a full queue was refused with the high-water check disabled: %v", err)
	}
}

func TestGatewayBacklogRefusesOnlyProducersAboveTheirShare(t *testing.T) {
	// heavy and light share a 100-event limit with weights 3 and 1: shares of 75 and 25.
	backlog := &fakeBacklog{capacity: 1000, depths: map[string]int{"heavy": 90, "light": 10}}
	c, err := New(Config{
		MaxQueuedEvents: 100,
		ResumeRatio:     0.8,
		DefaultWeight:   1,
		Weights:         map[string]float64{"heavy": 3},
	}, backlog, nil)
	if err != nil {
		t.Fatal(err)
	}

	c.Sample()
	if !c.Status().GatewayBacklogged {
		t.Fatal("expected the gateway to be backlogged at 100 queued events")
	}
	if got := reason(t, c.Check("heavy")); got != ReasonGatewayBacklogged {
		t.Fatalf("heavy holds 90 of a 75-event share: got %q", got)
	}
	if err := c.Check("light"); err != nil {
		t.Fatalf("light holds 10 of a 25-event share and should be admitted: %v", err)
	}
	if err := c.Check("idle"); err != nil {
		t.Fatalf("a producer with nothing queued should be admitted: %v", err)
	}

	// A producer that started queueing after the sample gets a share of the sampled weight: 1/4 of 100.
	backlog.depths["late"] = 20
	if err := c.Check("late"); err != nil {
		t.Fatalf("late holds 20 of a 25-event share and should be admitted: %v", err)
	}
	backlog.depths["late"] = 25
	if got := reason(t, c.Check("late")); got != ReasonGatewayBacklogged {
		t.Fatalf("late holds 25 of a 25-event share: got %q", got)
	}
}

func TestGatewayBacklogHysteresis(t *testing.T) {
	backlog := &fakeBacklog{capacity: 1000, depths: map[string]int{"a": 99}}
	c, err := New(Config{MaxQueuedEvents: 100, ResumeRatio: 0.8}, backlog, nil)
	if err != nil {
		t.Fatal(err)
	}

	steps := []struct {
		depth int
		want  bool
	}{
		{99, false}, // below the limit
		{100, true}, // reaches the limit
		{85, true},  // still above 80, the resume point
		{80, true},  // at the resume point
		{79, false}, // below it: clears
		{95, false}, // must reach the limit again
		{100, true},
	}
	for i, step := range steps {
		backlog.depths["a"] = step.depth
		c.Sample()
		if got := c.Status().GatewayBacklogged; got != step.want {
			t.Fatalf("step %d (depth %d): backlogged %v, want %v", i, step.depth, got, step.want)
		}
	}
}

func TestStorageBacklogRefusesEveryProducer(t *testing.T) {
	backlog := &fakeBacklog{capacity: 10, depths: map[string]int{}}
	lag := int64(0)
	c, err := New(Config{MaxShipperLagBytes: 1000, ResumeRatio: 0.5}, backlog, func() (int64, error) {
		return lag, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	lag = 1000
	c.Sample()
	if got := reason(t, c.Check("anyone")); got != ReasonStorageBacklogged {
		t.Fatalf("got %q, want %q", got, ReasonStorageBacklogged)
	}

	lag = 600
	c.Sample()
	if !c.Status().StorageBacklogged {
		t.Fatal("storage backlog cleared above the resume point")
	}

	lag = 499
	c.Sample()
	if err := c.Check("anyone"); err != nil {
		t.Fatalf("storage backlog did not clear below the resume point: %v", err)
	}
	if got := c.Status().ShipperLagBytes; got != 499 {
		t.Fatalf("shipper lag %d, want 499", got)
	}
}

func TestStorageLagErrorKeepsPreviousState(t *testing.T) {
	backlog := &fakeBacklog{capacity: 10, depths: map[string]int{}}
	var lagErr error
	c, err := New(Config{MaxShipperLagBytes: 100, ResumeRatio: 0.8}, backlog, func() (int64, error) {
		return 200, lagErr
	})
	if err != nil {
		t.Fatal(err)
	}

	c.Sample()
	lagErr = errors.New("segment listing failed")
	c.Sample()
	if !c.Status().StorageBacklogged {
		t.Fatal("a failed lag read cleared the storage backlog")
	}
}

func TestNewValidates(t *testing.T) {
	backlog := &fakeBacklog{capacity: 10}
	cases := map[string]Config{
		"high water above 1":   {QueueHighWater: 1.5, ResumeRatio: 0.8},
		"negative queue limit": {MaxQueuedEvents: -1, ResumeRatio: 0.8},
		"lag without source":   {MaxShipperLagBytes: 10, ResumeRatio: 0.8},
		"zero resume ratio":    {},
	}
	for name, cfg := range cases {
		if _, err := New(cfg, backlog, nil); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := New(Config{ResumeRatio: 0.8}, nil, nil); err == nil {
		t.Error("expected an error without a backlog")
	}
}
