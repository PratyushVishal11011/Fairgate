// Package loadgen drives a FairGate gateway with simulated producers over the wire protocol.
//
// Each producer holds one TCP connection, pipelines up to a window of events, and retries
// rejected or unacknowledged events with the same identity, so every retry is idempotent.
// A run records, per event, whether the gateway accepted it; the resulting manifest is what
// the reconcile tool checks against storage.
package loadgen

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	//gateway ingestion address.
	Addr string
	//number of simulated producers, each with its own connection.
	Producers int
	//events per producer; 0 sends until Duration elapses.
	Events int
	//how long producers send new events; 0 sends until Events are sent.
	Duration time.Duration
	//events per second per producer; 0 sends as fast as the window allows.
	Rate float64
	//events in flight per connection before the producer waits for an ACK.
	Window int
	//minimum payload size in bytes; payloads are padded deterministically to this size.
	PayloadBytes int
	EventType    string
	//producer IDs are Prefix-0001, Prefix-0002, ...
	Prefix string
	//producer_epoch shared by all producers in this run; part of every event identity.
	Epoch string
	//producer starts are spread evenly across this period.
	Ramp time.Duration
	//the first Hot producers are noisy neighbours with their own rate and window.
	Hot       int
	HotRate   float64
	HotWindow int
	//probability of sending an extra malformed frame after each new event.
	MalformedRatio float64
	//time without an ACK for the oldest in-flight event before the connection is dropped and retried.
	AckTimeout  time.Duration
	DialTimeout time.Duration
	//cap on the backoff after a rejection or a failed connection.
	MaxBackoff time.Duration
	//time after sending stops to collect outstanding ACKs and finish retries.
	DrainTimeout time.Duration
	//seeds jitter and malformed-frame injection.
	Seed int64
	//send events without a hello first, as clients written before protocol versioning do.
	Legacy bool
}

// DefaultConfig returns settings that suit a local Compose stack.
func DefaultConfig() Config {
	return Config{
		Addr:         "127.0.0.1:9001",
		Producers:    50,
		Events:       1000,
		Window:       1,
		PayloadBytes: 256,
		EventType:    "loadgen",
		Prefix:       "loadgen",
		HotWindow:    32,
		AckTimeout:   10 * time.Second,
		DialTimeout:  5 * time.Second,
		MaxBackoff:   2 * time.Second,
		DrainTimeout: 60 * time.Second,
		Seed:         1,
	}
}

// NewEpoch returns a producer epoch that is unique per run.
func NewEpoch() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

// Validate reports every invalid setting at once.
func (c Config) Validate() error {
	var problems []string
	check := func(ok bool, format string, args ...any) {
		if !ok {
			problems = append(problems, fmt.Sprintf(format, args...))
		}
	}

	check(c.Addr != "", "addr is required")
	check(c.Producers > 0, "producers must be greater than zero")
	check(c.Events >= 0, "events cannot be negative")
	check(c.Duration >= 0, "duration cannot be negative")
	check(c.Events > 0 || c.Duration > 0, "set events, duration, or both")
	check(c.Rate >= 0, "rate cannot be negative")
	check(c.Window > 0, "window must be greater than zero")
	check(c.PayloadBytes >= 0, "payload bytes cannot be negative")
	check(c.EventType != "", "event type is required")
	check(c.Prefix != "", "prefix is required")
	check(c.Epoch != "", "epoch is required")
	check(c.Ramp >= 0, "ramp cannot be negative")
	check(c.Hot >= 0 && c.Hot <= c.Producers, "hot must be between 0 and producers")
	check(c.HotRate >= 0, "hot rate cannot be negative")
	check(c.Hot == 0 || c.HotWindow > 0, "hot window must be greater than zero")
	check(c.MalformedRatio >= 0 && c.MalformedRatio <= 1, "malformed ratio must be between 0 and 1")
	check(c.AckTimeout > 0, "ack timeout must be greater than zero")
	check(c.DialTimeout > 0, "dial timeout must be greater than zero")
	check(c.MaxBackoff > 0, "max backoff must be greater than zero")
	check(c.DrainTimeout >= 0, "drain timeout cannot be negative")

	if len(problems) > 0 {
		return errors.New("invalid load configuration:\n  - " + strings.Join(problems, "\n  - "))
	}
	return nil
}

func (c Config) producerID(i int) string {
	return ProducerID(c.Prefix, i)
}

func (c Config) isHot(i int) bool {
	return i < c.Hot
}

func (c Config) rateFor(i int) float64 {
	if c.isHot(i) {
		return c.HotRate
	}
	return c.Rate
}

func (c Config) windowFor(i int) int {
	if c.isHot(i) {
		return c.HotWindow
	}
	return c.Window
}
