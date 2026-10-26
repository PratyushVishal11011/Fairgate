package store

import (
	"FairGate/internal/wire"
	"context"
)

type Store interface {
	// InsertBatch modified the function to be exported outside the store package
	//updated to accept an Idempotency Token
	//implementations must be idempotent: the shipper replays a batch after a crash or failed checkpoint,
	//and each event identity must end up stored exactly once however many times it is submitted.
	InsertBatch(ctx context.Context, events []wire.Event, token string) error
	Close() error
}
