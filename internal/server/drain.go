package server

import (
	"log/slog"
	"time"
)

// drainOutcome says how one stage of the shutdown drain ended.
type drainOutcome string

const (
	drainComplete drainOutcome = "complete"
	drainTimedOut drainOutcome = "timed_out"
	//the stage's goroutine exited, after an error, before its backlog was empty.
	drainStopped drainOutcome = "stopped"
	//server.drain_timeout is 0.
	drainSkipped drainOutcome = "skipped"
)

const drainPollInterval = 10 * time.Millisecond

// waitDrained polls drained until it reports true, stopped is closed, or deadline passes.
func waitDrained(deadline time.Time, stopped <-chan struct{}, drained func() bool) drainOutcome {
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()

	for {
		if drained() {
			return drainComplete
		}
		select {
		case <-stopped:
			return drainStopped
		default:
		}
		if !time.Now().Before(deadline) {
			return drainTimedOut
		}
		select {
		case <-ticker.C:
		case <-stopped:
		}
	}
}

// drainReport is what the shutdown drain confirmed.
type drainReport struct {
	//scheduler: events queued when the drain started, and still queued when it stopped.
	queue         drainOutcome
	queuedAtStart int
	queuedLeft    int
	//shipper: durable WAL bytes not yet in the store when the drain started, and when it stopped.
	shipper          drainOutcome
	unshippedAtStart int64
	unshippedLeft    int64
	elapsed          time.Duration
}

func (r drainReport) complete() bool {
	return r.queue == drainComplete && r.shipper == drainComplete
}

func (r drainReport) log(logger *slog.Logger) {
	switch {
	case r.queue == drainSkipped:
		logger.Info("Shutdown drain skipped because server.drain_timeout is 0",
			"queued_events", r.queuedLeft,
			"unshipped_bytes", r.unshippedLeft,
		)
	case r.complete():
		logger.Info("Shutdown drain complete: producer queues are empty and every WAL record is in the store",
			"queued_events_processed", r.queuedAtStart,
			"shipped_bytes", r.unshippedAtStart,
			"duration", r.elapsed,
		)
	default:
		//every queued or unshipped event was appended to the WAL before it was acknowledged.
		logger.Warn("Shutdown drain incomplete; remaining events stay in the WAL and are processed and shipped after the next start",
			"queue_drain", r.queue,
			"queued_events_left", r.queuedLeft,
			"shipper_drain", r.shipper,
			"unshipped_bytes", r.unshippedLeft,
			"duration", r.elapsed,
		)
	}
}
