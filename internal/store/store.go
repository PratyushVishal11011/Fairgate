package store

import (
	"FairGate/internal/wire"
	"context"
)

type Store interface {
	insertBatch(ctx context.Context, event []wire.Event) error
	Close() error
}
