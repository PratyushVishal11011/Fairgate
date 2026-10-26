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

// InsertBatch inserts a batch of events into ClickHouse exactly once per event identity.
//
// The shipper is the only writer for its events, so a read-then-insert is race free:
// rows whose event_id is already stored are dropped before the insert. This covers a batch
// replayed after a crash, a partially committed multi-partition insert, and an event that was
// accepted again after its WAL segment was reclaimed, without depending on ClickHouse's
// insert-deduplication window. The insert itself carries a token derived from the batch token
// and the exact rows sent, so an insert that is still in flight when a replay starts is
// deduplicated by ClickHouse rather than written twice.
func (s *ClickHouseStore) InsertBatch(ctx context.Context, events []wire.Event, token string) error {
	if len(events) == 0 {
		return nil
	}

	if token == "" {
		return errors.New("token is required")
	}

	candidates := uniqueRows(events)

	existing, err := s.existingEventIDs(ctx, candidates)
	if err != nil {
		return err
	}

	rows := withoutExisting(candidates, existing)
	if len(rows) == 0 {
		return nil
	}

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplicate":         1,
		"insert_deduplication_token": insertToken(token, rows),
		//a synchronous insert is visible to the next existence check once Send returns.
		"async_insert": 0,
	}))

	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO fairgate.events
		(event_id, producer_id, producer_epoch,event_time, seq, idx, event_type, payload)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	defer batch.Close()

	for _, row := range rows {
		event := row.event
		err := batch.Append(
			row.id,
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

type row struct {
	id    string
	event wire.Event
}

// uniqueRows computes each event's ID and keeps the first event for each ID.
// The WAL can hold the same identity twice when it was accepted again after reclamation.
func uniqueRows(events []wire.Event) []row {
	rows := make([]row, 0, len(events))
	seen := make(map[string]struct{}, len(events))

	for _, event := range events {
		id := eventID(event)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		rows = append(rows, row{id: id, event: event})
	}

	return rows
}

func withoutExisting(rows []row, existing map[string]struct{}) []row {
	if len(existing) == 0 {
		return rows
	}

	remaining := make([]row, 0, len(rows))
	for _, row := range rows {
		if _, ok := existing[row.id]; !ok {
			remaining = append(remaining, row)
		}
	}

	return remaining
}

func (s *ClickHouseStore) existingEventIDs(ctx context.Context, rows []row) (map[string]struct{}, error) {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.id
	}

	result, err := s.conn.Query(ctx,
		`SELECT DISTINCT event_id FROM fairgate.events
		WHERE event_id IN (SELECT arrayJoin(?))`,
		ids,
	)
	if err != nil {
		return nil, fmt.Errorf("query existing events: %w", err)
	}
	defer result.Close()

	existing := make(map[string]struct{})
	for result.Next() {
		var id string
		if err := result.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan existing event: %w", err)
		}
		existing[id] = struct{}{}
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("read existing events: %w", err)
	}

	return existing, nil
}

// insertToken binds the batch token to the exact rows being inserted.
// A replay of the same rows reuses the token, while a replay that sends fewer rows
// (because some already landed) gets a new one and is not discarded as a duplicate block.
func insertToken(token string, rows []row) string {
	hash := sha256.New()
	hash.Write([]byte(token))
	for _, row := range rows {
		hash.Write([]byte{0})
		hash.Write([]byte(row.id))
	}
	return hex.EncodeToString(hash.Sum(nil))
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
