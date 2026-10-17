CREATE DATABASE IF NOT EXISTS fairgate;

CREATE TABLE IF NOT EXISTS fairgate.events(
    event_id   String,
    producer_id String,
    event_time  DateTime64(3),
    seq         UInt64,
    idx         UInt64,
    event_type  String,
    payload     String
    )

-- ReplaceMergeTree helps support deduplication of events with matching sorting keys during background merges.
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_time)
-- ORDER BY organizes events by producer, event time, sequence number and index.
ORDER BY (producer_id, event_time, seq, idx)
-- Automatically expire old data
TTL event_time + INTERVAL 30 DAY
SETTINGS
    non_replicated_deduplication_window = 1000;