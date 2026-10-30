package reconcile

import (
	"FairGate/internal/wire"
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// DefaultTable is the table the gateway's ClickHouse adapter writes to.
const DefaultTable = "fairgate.events"

var tablePattern = regexp.MustCompile(`^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$`)

// ClickHouse reads stored events from the gateway's ClickHouse table.
//
// Rows are counted as stored, without FINAL: the table is a ReplacingMergeTree ordered by event_id,
// so a background merge can collapse duplicate rows of one identity. A duplicate that was merged
// away before reconciliation runs is not detected.
type ClickHouse struct {
	conn  clickhouse.Conn
	table string
}

func OpenClickHouse(addr []string, database, username, password, table string) (*ClickHouse, error) {
	if len(addr) == 0 {
		return nil, errors.New("clickhouse address is required")
	}
	if !tablePattern.MatchString(table) {
		return nil, fmt.Errorf("invalid table name %q", table)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        addr,
		Auth:        clickhouse.Auth{Database: database, Username: username, Password: password},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &ClickHouse{conn: conn, table: table}, nil
}

func (c *ClickHouse) Close() error {
	return c.conn.Close()
}

func (c *ClickHouse) Count(ctx context.Context, epoch string, producers []string) (uint64, error) {
	var count uint64
	err := c.conn.QueryRow(ctx,
		`SELECT uniqExact(producer_id, seq, idx) FROM `+c.table+`
		WHERE producer_epoch = ? AND producer_id IN (SELECT arrayJoin(?))`,
		epoch, producers,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count stored events: %w", err)
	}
	return count, nil
}

func (c *ClickHouse) Scan(ctx context.Context, epoch string, producers []string, content bool, fn func(Row) error) error {
	columns := "producer_id, seq, idx, count()"
	if content {
		columns += ", uniqExact(event_type, payload), any(event_type), any(payload)"
	}
	rows, err := c.conn.Query(ctx,
		`SELECT `+columns+` FROM `+c.table+`
		WHERE producer_epoch = ? AND producer_id IN (SELECT arrayJoin(?))
		GROUP BY producer_id, seq, idx`,
		epoch, producers,
	)
	if err != nil {
		return fmt.Errorf("scan stored events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row Row
		dest := []any{&row.ProducerID, &row.Seq, &row.Idx, &row.Copies}
		if content {
			dest = append(dest, &row.Variants, &row.EventType, &row.Payload)
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("read stored event: %w", err)
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan stored events: %w", err)
	}
	return nil
}

// Memory is an in-memory Source, for tests and for checking a run against a store fake.
// It keeps every row it is given, so a duplicate insert shows up as an extra copy.
type Memory struct {
	mu   sync.Mutex
	rows []wire.Event
}

// Add records stored events.
func (m *Memory) Add(events ...wire.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = append(m.rows, events...)
}

type memoryKey struct {
	producer string
	seq      uint64
	idx      uint16
}

func (m *Memory) group(epoch string, producers []string) (map[memoryKey][]wire.Event, []memoryKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wanted := make(map[string]struct{}, len(producers))
	for _, id := range producers {
		wanted[id] = struct{}{}
	}
	groups := make(map[memoryKey][]wire.Event)
	var keys []memoryKey
	for _, event := range m.rows {
		if _, ok := wanted[event.ProducerId]; !ok || event.ProducerEpoch != epoch {
			continue
		}
		key := memoryKey{event.ProducerId, event.Seq, event.Idx}
		if _, ok := groups[key]; !ok {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], event)
	}
	return groups, keys
}

func (m *Memory) Count(_ context.Context, epoch string, producers []string) (uint64, error) {
	_, keys := m.group(epoch, producers)
	return uint64(len(keys)), nil
}

func (m *Memory) Scan(_ context.Context, epoch string, producers []string, content bool, fn func(Row) error) error {
	groups, keys := m.group(epoch, producers)
	for _, key := range keys {
		events := groups[key]
		row := Row{ProducerID: key.producer, Seq: key.seq, Idx: uint64(key.idx), Copies: uint64(len(events))}
		if content {
			variants := make(map[[2]string]struct{})
			for _, event := range events {
				variants[[2]string{event.EventType, event.Payload}] = struct{}{}
			}
			row.Variants = uint64(len(variants))
			row.EventType = events[0].EventType
			row.Payload = events[0].Payload
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}
