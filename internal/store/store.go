package store

import (
	"FairGate/internal/wire"
	"context"
)

type Store interface {
	// InsertBatch modified the function to be exported outside the store package
	InsertBatch(ctx context.Context, event []wire.Event) error
	Close() error
}
