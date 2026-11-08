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
	"log/slog"
	"sync"
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
	//Observer receives instrumentation events; nil disables them.
	Observer Observer
	//Logger records failed insert attempts with their error; nil discards them.
	Logger *slog.Logger
}

// Observer receives shipper events for instrumentation. Methods run on the shipper goroutine and must be cheap.
type Observer interface {
	//BatchInserted reports a successful store insert and how long that attempt took.
	BatchInserted(events int, duration time.Duration)
	//InsertFailed reports a failed insert attempt and how many attempts in a row have failed for this batch.
	InsertFailed(consecutiveFailures int)
	//CheckpointCommitted reports that the checkpoint advanced to position.
	CheckpointCommitted(position wal.Position)
}

type noopObserver struct{}

func (noopObserver) BatchInserted(int, time.Duration) {}
func (noopObserver) InsertFailed(int)                 {}
func (noopObserver) CheckpointCommitted(wal.Position) {}

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
	observer   Observer
	logger     *slog.Logger

	//last committed position, read by metrics while Run advances it.
	committedMu sync.Mutex
	committed   wal.Position

	//wake cuts short the poll wait at the durable WAL end.
	wake chan struct{}
}

// Wake makes a shipper that is waiting at the durable WAL end check for new records now,
// instead of at its next poll. It never blocks; the shutdown drain uses it.
func (s *Shipper) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Committed returns the last checkpoint position this shipper loaded or committed.
func (s *Shipper) Committed() wal.Position {
	s.committedMu.Lock()
	defer s.committedMu.Unlock()
	return s.committed
}

func (s *Shipper) setCommitted(position wal.Position) {
	s.committedMu.Lock()
	s.committed = position
	s.committedMu.Unlock()
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

	observer := config.Observer
	if observer == nil {
		observer = noopObserver{}
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return &Shipper{
		store:      storage,
		wal:        w,
		checkpoint: checkpoint,
		backoff:    backoff,
		config:     config,
		observer:   observer,
		logger:     logger,
		wake:       make(chan struct{}, 1),
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
	s.setCommitted(start)
	reader := wal.NewReader(s.wal, start)

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

	//the next batch is read and decoded while the current one is being inserted. Inserts stay one at a
	//time: the existence check before each insert must see every row of the batch before it.
	readCtx, stopReading := context.WithCancel(ctx)
	batches := make(chan readResult, 1)
	var reading sync.WaitGroup
	reading.Add(1)
	go func() {
		defer reading.Done()
		s.readAhead(readCtx, reader, batches)
	}()
	//deferred, so it also runs while a panic unwinds: the reader is never used after Run ends.
	defer func() {
		stopReading()
		reading.Wait()
	}()

	//next is a batch already read whose range is already recorded as pending.
	var next *readResult
	for {
		batch := next
		next = nil
		recorded := batch != nil
		if batch == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case result := <-batches:
				batch = &result
			}
		}
		if batch.err != nil {
			return batch.err
		}
		end := batch.end

		if !recorded {
			if err := s.checkpoint.Commit(pendingCheckpoint(start, end)); err != nil {
				return err
			}
		}

		if err := s.writeBatch(ctx, batch.events, start, end); err != nil {
			return err
		}

		//when the following batch is already read, one commit records this batch as shipped and that
		//one as pending, so a restart replays exactly its range under the same token.
		commit := positionCheckpoint(end)
		select {
		case result := <-batches:
			if result.err == nil {
				commit = pendingCheckpoint(end, result.end)
			}
			next = &result
		default:
		}

		if err := s.checkpoint.Commit(commit); err != nil {
			return err
		}
		s.setCommitted(end)
		s.observer.CheckpointCommitted(end)
		if err := s.wal.ReclaimBefore(end); err != nil {
			return err
		}

		//the next batch starts where this one ended, so its token is derived from its own range.
		start = end
	}
}

// readResult is one batch read ahead of the insert in flight, or the error that stopped reading.
type readResult struct {
	events []wire.Event
	end    wal.Position
	err    error
}

// readAhead owns reader: it reads batches and sends them to out until ctx ends or a read fails.
// At the durable WAL end it waits for the poll interval or a Wake, keeping the same reader so it
// observes later durable appends.
func (s *Shipper) readAhead(ctx context.Context, reader *wal.Reader, out chan<- readResult) {
	poll := time.NewTicker(s.config.PollInterval)
	defer poll.Stop()

	for {
		events, end, err := s.readBatch(ctx, reader)
		if errors.Is(err, io.EOF) {
			select {
			case <-ctx.Done():
				return
			case <-poll.C:
			case <-s.wake:
			}
			continue
		}
		if err != nil && ctx.Err() != nil {
			return
		}

		select {
		case out <- readResult{events: events, end: end, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// pendingCheckpoint commits start as shipped and records the batch ending at end as possibly in the store.
func pendingCheckpoint(start, end wal.Position) Checkpoint {
	checkpoint := positionCheckpoint(start)
	checkpoint.Pending = &PendingBatch{
		Segment: end.SegmentId,
		Offset:  end.Offset,
	}
	return checkpoint
}

// shipBatch records the batch as pending, inserts it, then commits its end position.
func (s *Shipper) shipBatch(
	ctx context.Context,
	batch []wire.Event,
	start wal.Position,
	end wal.Position,
) error {
	if err := s.checkpoint.Commit(pendingCheckpoint(start, end)); err != nil {
		return err
	}

	if err := s.writeBatch(ctx, batch, start, end); err != nil {
		return err
	}

	if err := s.checkpoint.Commit(positionCheckpoint(end)); err != nil {
		return err
	}
	s.setCommitted(end)
	s.observer.CheckpointCommitted(end)

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
		attemptStart := time.Now()
		err := s.store.InsertBatch(ctx, batch, token)
		if err == nil {
			s.observer.BatchInserted(len(batch), time.Since(attemptStart))
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		//the metric only counts failures; without the error a stuck shipper gives no hint why.
		s.logger.Warn("WAL shipper insert failed; retrying after backoff",
			"attempt", attempt+1, "events", len(batch), "error", err)
		s.observer.InsertFailed(attempt + 1)

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
