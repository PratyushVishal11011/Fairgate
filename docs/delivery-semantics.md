[FairGate](../README.md) › [Docs](README.md) › Delivery semantics

# Delivery semantics

| Property | Status |
|---|---|
| Event persisted to the WAL before acknowledgment | Yes |
| Disk synchronization completed (per batch) before acknowledgment | Yes |
| Acknowledgment sent after enqueue | Yes |
| Event identity includes producer ID, producer epoch, sequence number, and index | Yes |
| Identical retry acknowledged without a second WAL append or enqueue | Yes |
| Conflicting reuse of an event identity rejected | Yes |
| Identity recorded only after WAL persistence succeeds | Yes |
| Event appended to the WAL but not yet enqueued at shutdown reaches the live queue | No; replayed from the WAL on the next startup |
| Per-producer memory bounded | Yes, by queue capacity |
| WAL split into segments with rotation | Yes |
| Persisted records scanned across segments on restart | Yes |
| Sparse segment set (after reclamation) handled on restart | Yes |
| Damaged records that were not shipped refused on restart | Yes, unless `wal.on_corruption` is `quarantine` or `fairgate wal repair` is run |
| Torn final record, and damage to records already shipped, handled on restart without intervention | Yes |
| Recovered events replayed into queues on restart | Yes, from segments still on disk |
| WAL write or sync failure surfaced to callers | Only if failover to a fresh segment also fails; then as a retryable `rejected` / `storage_unavailable` |
| Failed fsync retried on the same file | No; the batch is rewritten to a fresh segment |
| WAL kept open until all handlers and the shipper exit during shutdown | Yes |
| Explicit queue-drain confirmation on shutdown | Yes, within `server.drain_timeout`: queues emptied and the shipper caught up to the durable WAL end, with the outcome logged |
| Shipper and ClickHouse adapter available | Yes |
| Shipper started by the gateway runtime | Yes; always enabled |
| Shipper stopped and joined before the store and WAL close | Yes |
| Shipper reads only synchronized (durable) WAL records | Yes |
| Shipper resumes from a durable WAL checkpoint after restart | Yes |
| Shipper follows new WAL appends continuously | Yes, by polling at the durable end |
| Checkpoint written atomically after a successful insert | Yes |
| Failed storage batches retried with backoff | Yes, until success or context cancellation |
| Stable SHA-256 batch token passed to the store | Yes; same token on retry and replay |
| Deterministic `event_id` written with each ClickHouse row | Yes; hash of producer ID, epoch, sequence number, and index |
| Empty batch token rejected before insert | Yes |
| Batch token passed to ClickHouse as `insert_deduplication_token` | Yes |
| Batch recorded as pending before insert | Yes; a restart replays exactly that WAL range with the same token |
| Each batch token derived from that batch's own WAL range | Yes |
| Events already stored skipped before insert | Yes; by `event_id`, independent of the deduplication window |
| Duplicate identities within a batch inserted once | Yes |
| Replayed batch deduplicated by ClickHouse | Yes; by the existence check, with the row-bound token covering an insert still in flight |
| Duplicate suppression on replay after a checkpoint failure verified by integration tests | Yes, against a Docker-hosted ClickHouse; subject to the deduplication window and storage behavior |
| Batch may be submitted again after a crash before checkpoint commit | Yes; already stored events are skipped, so no duplicate rows result |
| Lost or rewound checkpoint causes duplicate rows | No; the re-shipped events are skipped |
| WAL segments reclaimed after shipping | Yes; segments older than the committed checkpoint segment are removed |
| Reclamation runs only after the checkpoint commit (and at startup against the checkpoint on disk) | Yes |
| Reclamation rejects checkpoints beyond the durable end | Yes (`ErrCheckpointAhead`) |
| Checkpoint segment or active segment reclaimed | No |
| Unshipped records removed by reclamation | No |
| WAL retains full event history | No |
| Acknowledgment implies database storage | No; it implies WAL durability, and the shipper later stores the event exactly once |
| End-to-end exactly-once delivery (per event identity) | Yes, within the scope below |

Exactly-once here means that every acknowledged event identity (`producer_id`, `producer_epoch`, `seq`, `idx`) appears as exactly one row in `fairgate.events`, without `FINAL` and without waiting for merges. The producer side is at-least-once: it retries until it receives an `accepted` ACK. The gateway turns this into exactly-once storage in two layers. The first is the deduplication manager, which covers identities still in the WAL. The second, authoritative layer is the adapter's existence check, which covers everything else.

The guarantee holds under these conditions:

- **Single writer per identity.** Each event identity is written by a single gateway, with one WAL and one shipper. The existence check reads before it inserts, so two gateways shipping the same identity at the same moment could both insert it. Route each producer to one gateway; see [Running more than one gateway](deployment.md#running-more-than-one-gateway).
- **First write wins for identity reuse after reclamation.** Identity reuse with different content is rejected only while the original is still in the WAL. After reclamation, a conflicting event is acknowledged and then dropped by the adapter, because the first stored version wins.
- **TTL expiry.** Rows removed by the table's 30-day TTL no longer count as stored. A resend of an expired event would be stored again, and then expire on the same TTL.

Validate these conditions under your own load and failure modes before relying on them in production, the single-writer condition in particular.
