package shipper

import (
	"FairGate/internal/store"
	"FairGate/internal/wire"
	"context"
	"errors"
	"time"
)

var ErrClosed = errors.New("shipper closed")

type Config struct {
	//Maximum number of events collected before triggering a batch write.
	BatchSize int
	//Maximum amount of time to wait before flushing a batch, regardless of whether batch size has been reached
	FlushInterval time.Duration
	//Capacity of the internal buffered channel that holds events waiting to be processed by the shipper.
	QueueSize int
}

type Shipper struct {
	//Store interface used to persist event batches.
	//keeps shipper independent of clickhouse
	store  store.Store
	config Config
	//Buffered channel that temporarily holds events submitted to the shipper before they are written to storage.
	events chan wire.Event
}

func New(storage store.Store, config Config) (*Shipper, error) {
	if storage == nil {
		return nil, errors.New("the storage cannot be nil")
	}
	if config.BatchSize <= 0 {
		return nil, errors.New("the batch size must be greater than zero")
	}
	if config.FlushInterval <= 0 {
		return nil, errors.New("the flush interval must be greater than zero")
	}
	if config.QueueSize <= 0 {
		return nil, errors.New("the queue size must be greater than zero")
	}

	return &Shipper{
		store:  storage,
		config: config,
		events: make(chan wire.Event, config.QueueSize),
	}, nil
}

// Submit sends an event to the shipper.
// It blocks when the internal buffer is full, unless the context is canceled.
func (s *Shipper) Submit(ctx context.Context, event wire.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	//Attempts to send the event to the shipper's events channel.
	//If the channel has available buffer space, the event is queued and the function returns nil
	//If the buffer is full, the send blocks until space becomes available or the context is canceled.
	case s.events <- event:
		return nil
	}
}
