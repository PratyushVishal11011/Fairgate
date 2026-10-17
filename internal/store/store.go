package store

import (
	"FairGate/internal/wire"
	"context"
)

type Store interface {
	// InsertBatch modified the function to be exported outside the store package
	//updated to accept an Idempotency Token
	InsertBatch(ctx context.Context, events []wire.Event, token string) error
	Close() error
}
