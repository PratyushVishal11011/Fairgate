package loadgen

import (
	"context"
	"sync"
	"time"
)

// Result is the outcome of a run.
type Result struct {
	Config    Config
	Started   time.Time
	Finished  time.Time
	Producers []ProducerResult
}

type ProducerResult struct {
	ID  string
	Hot bool
	//distinct events generated.
	Events int
	//event frames written, including retries.
	Sent int
	//events acknowledged as accepted, including Duplicates.
	Accepted int
	//accepted ACKs for retries of events the gateway had already persisted.
	Duplicates int
	//events that never got an accepted or invalid ACK.
	Unacked int
	//rejected ACKs by message; each was retried.
	Rejected map[string]int
	//invalid ACKs by message for real events; these are not retried.
	Invalid map[string]int
	//error and unknown ACKs by "status:message"; each was retried.
	Errors map[string]int
	//malformed frames sent, and how many the gateway answered as invalid.
	Malformed         int
	MalformedRejected int
	//ACKs that matched no in-flight frame.
	UnmatchedAcks int
	Disconnects   int
	DialFailures  int
	LastError     string
	FirstSend     time.Time
	LastAck       time.Time

	outcomes []Outcome
	samples  []sample
	buckets  []bucket
}

// Throughput returns the producer's accepted events per second between its first send and last ACK.
func (p ProducerResult) Throughput() float64 {
	if p.Accepted == 0 || !p.LastAck.After(p.FirstSend) {
		return 0
	}
	return float64(p.Accepted) / p.LastAck.Sub(p.FirstSend).Seconds()
}

// Run drives the gateway until every producer has sent its events or cfg.Duration elapses,
// then waits up to cfg.DrainTimeout for outstanding events. Canceling ctx stops new events early;
// outstanding events are still drained.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	start := time.Now()
	var sendCtx context.Context
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		sendCtx, cancel = context.WithDeadline(ctx, start.Add(cfg.Duration))
	} else {
		sendCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	producers := make([]*producer, cfg.Producers)
	var wg sync.WaitGroup
	for i := range producers {
		producers[i] = newProducer(cfg, i, start)
		wg.Add(1)
		go func(p *producer) {
			defer wg.Done()
			p.run(sendCtx)
		}(producers[i])
	}
	wg.Wait()

	result := &Result{
		Config:    cfg,
		Started:   start,
		Finished:  time.Now(),
		Producers: make([]ProducerResult, len(producers)),
	}
	for i, p := range producers {
		result.Producers[i] = p.result
	}
	return result, nil
}

// Manifest returns the per-event outcomes of the run for reconciliation.
func (r *Result) Manifest() *Manifest {
	m := &Manifest{
		Version:      ManifestVersion,
		GeneratedAt:  r.Finished.UTC(),
		Epoch:        r.Config.Epoch,
		EventType:    r.Config.EventType,
		PayloadBytes: r.Config.PayloadBytes,
		Producers:    make([]ManifestProducer, len(r.Producers)),
	}
	for i, p := range r.Producers {
		m.Producers[i] = ManifestProducer{
			ID:       p.ID,
			Accepted: RangesOf(p.outcomes, OutcomeAccepted),
			Unacked:  RangesOf(p.outcomes, OutcomeUnacked),
			Invalid:  RangesOf(p.outcomes, OutcomeInvalid),
		}
	}
	return m
}
