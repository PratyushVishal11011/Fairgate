package store

import (
	"FairGate/internal/wire"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseStore struct {
	conn clickhouse.Conn
}

func NewClickHouseStore(
	addr []string,
	database string,
	username string,
	password string,
) (*ClickHouseStore, error) {
	if len(addr) == 0 {
		return nil, errors.New("address is required")
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: addr,
		Auth: clickhouse.Auth{Database: database, Username: username, Password: password}, DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}

	// Verify that ClickHouse is reachable before returning the store.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse connection: %w", err)
	}

	return &ClickHouseStore{conn: conn}, nil
}

// InsertBatch inserts a batch of events into ClickHouse.
func (s *ClickHouseStore) InsertBatch(ctx context.Context, events []wire.Event, token string) error {
	if len(events) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO fairgate.events
		(event_id, producer_id, event_time, seq, idx, event_type, payload)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, event := range events {
		err := batch.Append(
			eventID(event),
			event.ProducerId,
			event.EventTime,
			event.Seq,
			event.Idx,
			event.EventType,
			event.Payload,
		)
		if err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append event: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send ClickHouse batch: %w", err)
	}
	return nil
}

// Close releases the ClickHouse connection.
func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}

func eventID(event wire.Event) string {
	identity := fmt.Sprintf(
		"%s:%d:%d",
		event.ProducerId,
		event.Seq,
		event.Idx,
	)

	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}
