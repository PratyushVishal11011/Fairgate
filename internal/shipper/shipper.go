package shipper

import (
	"FairGate/internal/store"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	if err := s.wal.ReclaimBefore(checkpointPosition(checkpoint)); err != nil {
		return err
	}
	start := checkpointPosition(checkpoint)
	reader := wal.NewReader(s.wal, start)
	poll := time.NewTicker(s.config.PollInterval)
	defer poll.Stop()

	//a pending batch may already be in the store, possibly only partially.
	//replay exactly that WAL range first so the store sees the same rows under the same token.
	if checkpoint.Pending != nil {
		end := wal.Position{
			SegmentId: checkpoint.Pending.Segment,
			Offset:    checkpoint.Pending.Offset,
		}

		batch, err := s.readPendingBatch(ctx, reader, end)
		if err != nil {
			return err
		}

		if err := s.shipBatch(ctx, batch, start, end); err != nil {
			return err
		}

		start = end
	}

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

		if err := s.shipBatch(ctx, batch, start, end); err != nil {
			return err
		}

		//the next batch starts where this one ended, so its token is derived from its own range.
		start = end
	}
}

// shipBatch records the batch as pending, inserts it, then commits its end position.
func (s *Shipper) shipBatch(
	ctx context.Context,
	batch []wire.Event,
	start wal.Position,
	end wal.Position,
) error {
	intent := positionCheckpoint(start)
	intent.Pending = &PendingBatch{
		Segment: end.SegmentId,
		Offset:  end.Offset,
	}

	if err := s.checkpoint.Commit(intent); err != nil {
		return err
	}

	if err := s.writeBatch(ctx, batch, start, end); err != nil {
		return err
	}

	if err := s.checkpoint.Commit(positionCheckpoint(end)); err != nil {
		return err
	}

	return s.wal.ReclaimBefore(end)
}

// readPendingBatch reads every record up to and including the one ending at end.
// The batch was formed from durable records, so the range must still be readable and end on a record boundary.
func (s *Shipper) readPendingBatch(
	ctx context.Context,
	reader *wal.Reader,
	end wal.Position,
) ([]wire.Event, error) {
	var batch []wire.Event

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		record, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf(
					"pending batch end %d:%d is beyond the durable WAL end",
					end.SegmentId,
					end.Offset,
				)
			}

			return nil, err
		}

		batch = append(batch, record.Event)

		if record.End == end {
			return batch, nil
		}

		if positionAfter(record.End, end) {
			return nil, fmt.Errorf(
				"pending batch end %d:%d is not a WAL record boundary",
				end.SegmentId,
				end.Offset,
			)
		}
	}
}

func positionAfter(a, b wal.Position) bool {
	if a.SegmentId != b.SegmentId {
		return a.SegmentId > b.SegmentId
	}

	return a.Offset > b.Offset
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
	start wal.Position,
	end wal.Position,
) error {
	for attempt := 0; ; attempt++ {
		token := batchToken(start, end)
		err := s.store.InsertBatch(ctx, batch, token)
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

func batchToken(start, end wal.Position) string {
	value := fmt.Sprintf(
		"fairgate:%d:%d:%d:%d",
		start.SegmentId,
		start.Offset,
		end.SegmentId,
		end.Offset,
	)

	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
