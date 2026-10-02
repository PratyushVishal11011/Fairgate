CREATE DATABASE IF NOT EXISTS fairgate;

CREATE TABLE IF NOT EXISTS fairgate.events (
    producer_id String,
    event_time DateTime64(3, 'UTC'),
    seq UInt64,
    idx UInt16,
    event_type LowCardinality(String),
    payload String,
    received_at DateTime64(3, 'UTC') DEFAULT now64(3)
)

-- ReplaceMergeTree helps support deduplication of events with matching sorting keys during background merges.
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_time)
-- ORDER BY organizes events by producer, event time, sequence number and index.
ORDER BY (producer_id, event_time, seq, idx)
-- Automatically expire old data
TTL event_time + INTERVAL 30 DAY;