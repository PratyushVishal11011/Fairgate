// Package overload decides when the gateway refuses new events because work is backing up.
//
// It reads three backlogs. A producer's own queue is read live on every check. The total
// queued across all producers and the shipper's delivery lag are sampled in the background,
// and each turns on at its limit and off again only once it falls below ResumeRatio of it,
// so a backlog hovering at its limit does not flap.
package overload

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	"FairGate/internal/sched"
)

// Reasons a Controller refuses an event. Each is sent as the message of a rejected ACK.
const (
	//the producer's own queue is at its high-water mark.
	ReasonQueueBacklogged = "queue_backlogged"
	//the gateway holds too many queued events and this producer holds more than its weighted share of them.
	ReasonGatewayBacklogged = "gateway_backlogged"
	//the shipper trails the WAL by too many bytes, so every new event is refused.
	ReasonStorageBacklogged = "storage_backlogged"
)

// Reasons lists every reason a Controller can return.
var Reasons = []string{ReasonQueueBacklogged, ReasonGatewayBacklogged, ReasonStorageBacklogged}

// Rejection is returned when the controller refuses an event.
type Rejection struct {
	Reason string
}

func (r *Rejection) Error() string { return "overloaded: " + r.Reason }

// Config sets the backlog limits. A zero limit disables that check.
type Config struct {
	//refuse a producer's events once its queue holds this fraction of its capacity, from 0 to 1.
	QueueHighWater float64
	//events queued across all producers before producers above their weighted share are refused.
	MaxQueuedEvents int
	//durable WAL bytes the shipper may trail by before every new event is refused.
	MaxShipperLagBytes int64
	//a gateway-wide signal clears once its backlog falls below this fraction of its limit.
	ResumeRatio float64
	//how often Run samples the gateway-wide backlogs.
	SampleInterval time.Duration
	//DRR weights used to work out each producer's share of MaxQueuedEvents.
	DefaultWeight float64
	Weights       map[string]float64
	//Logger reports signals turning on and off; nil discards them.
	Logger *slog.Logger
}

// Backlog is the queue view the controller reads. *sched.Queues implements it.
type Backlog interface {
	Depth(producerId string) (depth, capacity int)
	Stats() []sched.QueueStat
}

// Status is the state of the gateway-wide signals at the last sample.
type Status struct {
	//MaxQueuedEvents is reached; producers above their share are refused.
	GatewayBacklogged bool
	//MaxShipperLagBytes is reached; every new event is refused.
	StorageBacklogged bool
	//events queued across all producers.
	QueuedEvents int
	//bytes the shipper trails the durable WAL end by.
	ShipperLagBytes int64
}

type Controller struct {
	cfg    Config
	queues Backlog
	lag    func() (int64, error)
	logger *slog.Logger

	mu     sync.RWMutex
	status Status
	//sum of the weights of the producers that had queued events at the last sample.
	activeWeight float64
}

// New returns a controller over queues. lag reports the shipper's delivery lag in bytes;
// it may be nil when MaxShipperLagBytes is zero.
func New(cfg Config, queues Backlog, lag func() (int64, error)) (*Controller, error) {
	if queues == nil {
		return nil, errors.New("overload controller needs a queue backlog")
	}
	if cfg.QueueHighWater < 0 || cfg.QueueHighWater > 1 {
		return nil, errors.New("queue high-water mark must be between 0 and 1")
	}
	if cfg.MaxQueuedEvents < 0 || cfg.MaxShipperLagBytes < 0 {
		return nil, errors.New("backlog limits cannot be negative")
	}
	if cfg.MaxShipperLagBytes > 0 && lag == nil {
		return nil, errors.New("a shipper lag limit needs a lag source")
	}
	if cfg.ResumeRatio <= 0 || cfg.ResumeRatio > 1 {
		return nil, errors.New("resume ratio must be greater than 0 and at most 1")
	}
	if cfg.DefaultWeight <= 0 {
		cfg.DefaultWeight = 1
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Controller{cfg: cfg, queues: queues, lag: lag, logger: logger}, nil
}

func (c *Controller) weight(producerId string) float64 {
	if weight, ok := c.cfg.Weights[producerId]; ok && weight > 0 {
		return weight
	}
	return c.cfg.DefaultWeight
}

// Check returns a *Rejection if an event from producerId should be refused, and nil to admit it.
// It runs before the WAL append, so a refused event is never persisted.
func (c *Controller) Check(producerId string) error {
	c.mu.RLock()
	status := c.status
	activeWeight := c.activeWeight
	c.mu.RUnlock()

	if status.StorageBacklogged {
		return &Rejection{Reason: ReasonStorageBacklogged}
	}

	depth, capacity := c.queues.Depth(producerId)
	//a producer with nothing queued is not adding to any backlog.
	if depth == 0 {
		return nil
	}

	if c.cfg.QueueHighWater > 0 {
		limit := int(math.Ceil(c.cfg.QueueHighWater * float64(capacity)))
		if limit < 1 {
			limit = 1
		}
		if depth >= limit {
			return &Rejection{Reason: ReasonQueueBacklogged}
		}
	}

	if status.GatewayBacklogged {
		weight := c.weight(producerId)
		//a producer that became backlogged after the sample still counts towards the total weight.
		total := math.Max(activeWeight, weight)
		share := weight / total * float64(c.cfg.MaxQueuedEvents)
		if float64(depth) >= share {
			return &Rejection{Reason: ReasonGatewayBacklogged}
		}
	}

	return nil
}

// Status returns the gateway-wide signals at the last sample.
func (c *Controller) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Sample refreshes the gateway-wide backlogs.
func (c *Controller) Sample() {
	queued := 0
	activeWeight := 0.0
	for _, stat := range c.queues.Stats() {
		if stat.Depth == 0 {
			continue
		}
		queued += stat.Depth
		activeWeight += c.weight(stat.ProducerId)
	}

	var lagBytes int64
	lagKnown := false
	if c.cfg.MaxShipperLagBytes > 0 {
		lag, err := c.lag()
		if err != nil {
			c.logger.Warn("Could not read shipper lag; keeping the previous storage backlog state", "error", err)
		} else {
			lagBytes, lagKnown = lag, true
		}
	}

	c.mu.Lock()
	previous := c.status
	c.activeWeight = activeWeight
	c.status.QueuedEvents = queued
	if c.cfg.MaxQueuedEvents > 0 {
		c.status.GatewayBacklogged = c.hysteresis(previous.GatewayBacklogged, float64(queued), float64(c.cfg.MaxQueuedEvents))
	}
	if lagKnown {
		c.status.ShipperLagBytes = lagBytes
		c.status.StorageBacklogged = c.hysteresis(previous.StorageBacklogged, float64(lagBytes), float64(c.cfg.MaxShipperLagBytes))
	}
	current := c.status
	c.mu.Unlock()

	if current.GatewayBacklogged != previous.GatewayBacklogged {
		c.logger.Warn("Gateway queue backlog changed",
			"backlogged", current.GatewayBacklogged,
			"queued_events", current.QueuedEvents,
			"max_queued_events", c.cfg.MaxQueuedEvents,
		)
	}
	if current.StorageBacklogged != previous.StorageBacklogged {
		c.logger.Warn("Storage backlog changed",
			"backlogged", current.StorageBacklogged,
			"shipper_lag_bytes", current.ShipperLagBytes,
			"max_shipper_lag_bytes", c.cfg.MaxShipperLagBytes,
		)
	}
}

// hysteresis turns a signal on at limit and off below ResumeRatio * limit.
func (c *Controller) hysteresis(active bool, value, limit float64) bool {
	if active {
		return value >= c.cfg.ResumeRatio*limit
	}
	return value >= limit
}

// Run samples every SampleInterval until ctx is canceled.
func (c *Controller) Run(ctx context.Context) {
	if c.cfg.SampleInterval <= 0 {
		return
	}
	ticker := time.NewTicker(c.cfg.SampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Sample()
		}
	}
}
