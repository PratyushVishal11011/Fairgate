package store

import (
	"FairGate/internal/wire"
	"context"
	"crypto/sha256"
	"encoding/binary"
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

	if token == "" {
		return errors.New("token is required")
	}

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"insert_deduplication_token": token}))

	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO fairgate.events
		(event_id, producer_id, producer_epoch,event_time, seq, idx, event_type, payload)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	defer batch.Close()

	for _, event := range events {
		err := batch.Append(
			eventID(event),
			event.ProducerId,
			event.ProducerEpoch,
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
	//previous event identity was constructed using formatted text
	//updated version uses length-prefixed strings and fixed-width numeric fields
	//prevents ambiguous string combinations from producing the same encoded identity.
	//creates an empty byte slice with a pre-allocated capacity.
	//18 - two 4-byte length prefixes (8 bytes), a 8-byte sequence number, and a 2-byte index (10 bytes).
	identity := make([]byte, 0, len(event.ProducerId)+len(event.ProducerEpoch)+18)

	identity = appendLengthPrefixed(identity, event.ProducerId)
	identity = appendLengthPrefixed(identity, event.ProducerEpoch)

	//encode sequence number and index
	var numbers [10]byte

	binary.BigEndian.PutUint64(numbers[:8], event.Seq)
	binary.BigEndian.PutUint16(numbers[8:], event.Idx)

	identity = append(identity, numbers[:]...)
	sum := sha256.Sum256(identity)
	return hex.EncodeToString(sum[:])
}

func appendLengthPrefixed(dst []byte, value string) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	dst = append(dst, length[:]...)
	return append(dst, value...)
}
