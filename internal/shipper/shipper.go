package shipper

import (
	"FairGate/internal/store"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"context"
	"errors"
	"io"
	"time"
)

var ErrClosed = errors.New("shipper closed")

type Config struct {
	//Maximum number of events collected before triggering a batch write.
	BatchSize int
	//Maximum amount of time to wait before flushing a batch, regardless of whether batch size has been reached
	FlushInterval time.Duration
	//Capacity of the internal buffered channel that holds events waiting to be processed by the shipper.
	QueueSize    int
	PollInterval time.Duration
}

type Shipper struct {
	//Store interface used to persist event batches.
	//keeps shipper independent of clickhouse
	store  store.Store
	config Config
	//REMOVED: Buffered channel that temporarily holds events submitted to the shipper before they are written to storage.
	//Now implemented as WAL -> Reader -> Shipper
	wal        *wal.WAL
	checkpoint *CheckpointStore
	backoff    *Backoff
}

func New(
	storage store.Store,
	w *wal.WAL,
	checkpoint *CheckpointStore,
	backoff *Backoff,
	config Config,
) (*Shipper, error) {
	if storage == nil {
		return nil, errors.New("the storage cannot be nil")
	}
	if w == nil {
		return nil, errors.New("the WAL cannot be nil")
	}
	if checkpoint == nil {
		return nil, errors.New("the checkpoint store cannot be nil")
	}
	if backoff == nil {
		return nil, errors.New("the backoff cannot be nil")
	}
	if config.BatchSize <= 0 {
		return nil, errors.New("the batch size must be greater than zero")
	}
	if config.PollInterval <= 0 {
		return nil, errors.New("the poll interval must be greater than zero")
	}

	return &Shipper{
		store:      storage,
		wal:        w,
		checkpoint: checkpoint,
		backoff:    backoff,
		config:     config,
	}, nil
}

func checkpointPosition(checkpoint Checkpoint) wal.Position {
	return wal.Position{
		SegmentId: checkpoint.Segment,
		Offset:    checkpoint.Offset,
	}
}

func positionCheckpoint(position wal.Position) Checkpoint {
	return Checkpoint{
		Segment: position.SegmentId,
		Offset:  position.Offset,
	}
}

func (s *Shipper) Run(ctx context.Context) error {
	checkpoint, err := s.checkpoint.Load()
	if err != nil {
		return err
	}

	reader := wal.NewReader(s.wal, checkpointPosition(checkpoint))
	poll := time.NewTicker(s.config.PollInterval)
	defer poll.Stop()

	for {
		batch, end, err := s.readBatch(ctx, reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			//readBatch returns io.EOF only when it has no records to return.
			//keep the same reader so it can observe later durable appends.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-poll.C:
				continue
			}
		}

		if err := s.writeBatch(ctx, batch); err != nil {
			return err
		}

		if err := s.checkpoint.Commit(positionCheckpoint(end)); err != nil {
			return err
		}
	}
}

func (s *Shipper) readBatch(
	ctx context.Context,
	reader *wal.Reader,
) ([]wire.Event, wal.Position, error) {
	batch := make([]wire.Event, 0, s.config.BatchSize)

	var end wal.Position

	for len(batch) < s.config.BatchSize {
		select {
		case <-ctx.Done():
			return nil, end, ctx.Err()
		default:
		}

		record, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(batch) == 0 {
					return nil, end, io.EOF
				}

				return batch, end, nil
			}

			return nil, end, err
		}

		batch = append(batch, record.Event)
		end = record.End
	}

	return batch, end, nil
}

func (s *Shipper) writeBatch(
	ctx context.Context,
	batch []wire.Event,
) error {
	for attempt := 0; ; attempt++ {
		err := s.store.InsertBatch(ctx, batch)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := s.backoff.Wait(ctx, attempt); err != nil {
			return err
		}
	}
}
