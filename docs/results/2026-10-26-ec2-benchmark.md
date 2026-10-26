[FairGate](../../README.md) › [Docs](../README.md) › Results › EC2 benchmark, 26 October 2026

# EC2 benchmark, 26 October 2026

Analysis of `benchmark-results-20261026-133551/ec2_run_20261026_081113`, compared with the 19 October run (`ec2_run_20261019_063227`) from the benchmark repository.

## Summary

- **The gateway saturates at about 7,000 events/s, and every producer waits behind all the others.** Median latency grows linearly with the number of connected producers, at about 0.14 ms each, while throughput stays flat. At 750 producers that is about 100 ms per event.
- **The likely ceiling is the WAL group commit cap.** The writer makes at most 64 records durable per fsync (`maxBatchSize` in `internal/wal/wal.go`). 64 records per ~9 ms fsync cycle gives about 7,000 events/s, and 750 producers make about 12 batches to wait through. This is inferred from client-side data only and needs one server-side measurement to confirm (see [Next steps](#next-steps)).
- **This run included the deduplication lock fix, and performance is back to the 19 October baseline.** The fix was uncommitted in the working tree when the run started. Under the old global lock the gateway could not have exceeded a few hundred events/s, so the measured 6,400 events/s shows the fix took effect. No EC2 run of the code with the lock exists, so the size of that regression on EC2 is inferred, not measured.
- **The ~100 ms median is not new.** The 19 October build, which predates deduplication, shows the same ceiling and the same median. Tail latency improved since then: normal-producer p99.9 fell from 442 ms to 341 ms, and short stalls fell from 34 to 4.
- **Overall p99 (1.6 s) and p99.9 (4.4 s) come from the test design, not from gateway hiccups.** Every non-wave event over 1 s came from a burst-profile producer queueing its own pipelined events. For normal producers, p99 is 236 ms.
- **Oversized events are misreported as WAL failures.** Events between 1 MiB and 5 MiB pass framing, fail the WAL record limit, increment `fairgate_wal_append_errors_total`, and close the connection. That fired the critical `FairGateWALAppendErrors` alert 62 times during the run, even though the WAL was healthy.
- **Admission control was never exercised.** No event was rate limited. Each connection is handled one event at a time, so a single producer cannot exceed about 10 events/s here, far below the 500 events/s limit.

## Setup

| | |
|---|---|
| Gateway host | EC2 `t3.large` (2 vCPU, 8 GiB), 30 GiB gp3. Runs the full Compose stack: FairGate, ClickHouse, Prometheus, Alertmanager, and Grafana share the host and the volume. |
| Client host | EC2 `t3.small`, 2 Python asyncio processes, same subnet |
| Gateway config | `deploy/fairgate.yaml` defaults: admission 500/s, burst 1,000; queue capacity 512; DRR quantum 8; shipper batch 200 |
| Load | 750 producers × 2,000 events = 1,500,000 events. Normal producers send one event at a time and wait for its ACK, with about 4 ms between events. 5 s ramp, 10 s warmup. |
| Anomalies | 40 producers, 8 each of burst, slow, disconnect, malformed, and oversized, each active for 110–300 of its events |
| Waves | At 8 s and 16 s, for 3 s, 450 producers (60%) switch to pipelining 50 events per round trip |
| Code | Working tree at 13:35 IST, including the uncommitted deduplication lock fix |

## Headline results

| Metric | 26 Oct | 19 Oct | Change |
|---|---:|---:|---|
| Run duration | 234.3 s | 240.9 s | −2.7% |
| Accepted throughput | 6,400 /s | 6,225 /s | +2.8% |
| Normal producers p50 | 102.1 ms | 96.2 ms | +6% |
| Normal producers p99 | 235.7 ms | 255.4 ms | −8% |
| Normal producers p99.9 | 341.5 ms | 442.5 ms | −23% |
| Normal producers max | 578 ms | 847 ms | −32% |
| All events p50 / p99 / p99.9 | 103 / 1,649 / 4,353 ms | 97 / 1,579 / 5,151 ms | |
| Stalls (100 ms windows under 20% of median ACK rate) | 4 | 34 | longest 0.1 s vs 0.8 s |
| 100 ms windows with no ACKs at all | 1 | 11 | |
| Jain fairness, normal producers' throughput | 0.99991 | 0.99997 | |
| Client event-loop lag, median / p99 | 7.5 / 128 ms | 20.3 / 155 ms | client improved too |

Both runs accepted 1,499,677 of 1,500,000 events; the 323 others are the malformed and oversized events covered below. The client also changed between runs (its event-loop lag fell), so small differences in the median are within the noise of comparing the two.

## Latency is queueing at a fixed-capacity stage

Median latency for normal producers rises in proportion to the number of active producers, during both the ramp-up and the ramp-down, while throughput stays around 7,000 events/s:

| Second | Active producers | Accepted /s | Measured p50 | 0.1435 ms × producers |
|---:|---:|---:|---:|---:|
| 0 | up to 150 (ramping) | 4,014 | 13.8 ms | ≤ 21.5 ms |
| 1 | 300 | 6,910 | 26.5 ms | 43.0 ms |
| 2 | 449 | 6,997 | 41.5 ms | 64.4 ms |
| 3 | 599 | 7,468 | 62.3 ms | 86.0 ms |
| 4 | 749 | 6,698 | 88.1 ms | 107.5 ms |
| 30–200 (steady) | 750 | 6,687 (median) | 102.8 ms | 107.6 ms |
| 229 | 348 | 6,031 | 33.9 ms | 49.9 ms |
| 230 | 126 | 1,789 | 16.4 ms | 18.1 ms |
| 231–234 | 2–8 | < 600 | 7.2–9.1 ms | ~1 ms |

The line is fitted over every non-wave second. "Active producers" counts every producer that sent at least one event in that second. During ramp-up and ramp-down, producers start or finish partway through the second, so the average number in flight is lower than the count shown. That is why measured values sit below the line in those rows. At 2–8 producers the line no longer applies, because the idle floor described below dominates.

Two observations follow:

- **Little's law holds.** 6,687 events/s × 0.103 s = 688 events in flight, out of 750 producers. The other 8% of the time producers spend in their own 4 ms pause between events. Nearly every producer is waiting on the gateway at any moment.
- **An idle gateway still takes 7–9 ms per event.** In the first 100 ms (15 producers) the median was 8.4 ms, and at the end of the run (2–8 producers) 7.2–9.1 ms. In-VPC round trips are well under 1 ms, so this floor is the WAL: the writer waits up to 2 ms (`batchDelay`) for more records and then fsyncs. That puts an idle fsync at about 5–7 ms on this volume.

### Why the WAL is the likely bottleneck

The WAL writer is a single goroutine. It takes at most 64 queued appends (`maxBatchSize`), writes them, fsyncs, and acknowledges them. With around 700 handlers waiting, it works through roughly 11 batches before reaching the newest request. The numbers line up:

- 0.1435 ms per producer × 64 records per batch = **9.2 ms per batch cycle**, a plausible fsync time on a gp3 volume that ClickHouse merges and Prometheus also write to.
- 64 records / 9.2 ms = **about 7,000 events/s**, the observed ceiling.
- The idle floor of 7–9 ms is one batch cycle, as expected.

One observation does not fit cleanly: during waves, throughput reached 9,600–9,700 events/s, which would need a 6.6 ms cycle. Fsync time may vary with ClickHouse activity, or CPU contention on the shared 2-vCPU host may also play a part. The client's event-loop lag barely correlates with latency (0.26 with p99, −0.05 with p50), so the client is not what sets the median.

The data cannot separate "fsync-bound at 64 records per batch" from "CPU-bound on a crowded host" with certainty, because the run collected no gateway metrics. Two gateway histograms that already exist would settle it:

- `fairgate_wal_group_commit_records` stuck at the 64 bucket means the batch cap is the limit.
- `fairgate_wal_sync_duration_seconds` gives the cycle time directly.

If the cap is the limit, the model predicts that lifting it (so one fsync covers every waiting append) would bring the 750-producer median from about 100 ms down to one or two fsync cycles, roughly 10–20 ms.

## Waves and burst producers: queueing inside the producer's own connection

The gateway reads, processes, and acknowledges one event at a time per connection. A producer that pipelines N events therefore waits for N gateway cycles for its last ACK, and the client measures every event in a batch from the moment the batch was written.

| Group | Pipeline depth | p50 | p99 | Throughput per producer |
|---|---:|---:|---:|---:|
| Wave members during waves (450 producers) | 50 | 2,214 / 2,298 ms | 5,086 / 4,783 ms | 15.8 events/s |
| Everyone else during waves (300 producers) | 1 | 101 / 101 ms | 210 / 267 ms | 7.6 events/s |
| Burst anomaly producers (8) | ~30 | 1,487 ms | 3,916 ms | 10.3 events/s (median) |
| Normal producers, whole run | 1 | 101 ms | 233 ms | 8.85 events/s |

- **Non-members were unaffected by the waves.** Their median stayed at 101 ms, because the WAL queue never holds more than one append per connection however deep a client's pipeline is. Aggregate throughput rose to 9,700 events/s.
- **Pipelining is rewarded.** During waves, members got about twice the per-producer throughput of non-members, which drops Jain fairness to 0.90–0.91. Pipelining removes the client's pause and round trip between events. DRR orders processing after enqueue and has no say over who gets acknowledged first.
- **Bursting gains nothing.** Burst producers achieved the same ~10 events/s as normal producers; their extra events only waited longer in their own pipelines.
- **The non-wave p99 spikes are burst producers.** The summary flags seconds 30, 39, 44, 51, and 55 with p99 between 0.72 and 1.39 s. In those seconds every event over 500 ms came from a burst producer, and the normal-producer p99 was 152–292 ms. All 1,068 non-wave events over 1 s were burst events.
- **Admission control never triggered.** No event was rate limited. The token bucket allows 500 events/s per producer with a burst of 1,000, while one-at-a-time handling caps a connection at about 1 / (latency + pause), roughly 10 events/s here. The good isolation in this run comes from per-connection serialization, not from admission control or DRR. A producer that opens many connections, or a gateway that drops below saturation, would behave differently.

## Bad input handling

### Oversized events

| Frame size | Count | Result |
|---|---:|---|
| 528 KB – 1.03 MB | 31 | accepted |
| 1.05 – 2.09 MB | 62 | `error` / `wal_append_failed`, connection closed |
| 698 KB – 1.90 MB | 4 | client error (`IncompleteReadError`, `BrokenPipeError`) on a connection the gateway had already closed |

The frame limit is 5 MiB (`wire.MaxFrameSize`), but the WAL rejects records over 1 MiB (`maxRecordSize`). An event in between passes framing and decoding, then fails inside `WAL.Append`. The handler treats that as a WAL failure:

- it logs at error level and increments `fairgate_wal_append_errors_total`;
- that counter drives the critical `FairGateWALAppendErrors` alert, whose description says the WAL has failed fatally, which it has not;
- it sends `wal_append_failed` and closes the connection, so the oversized producers reconnected 61 times.

This is a client error and should be rejected as `invalid` before the WAL, without closing the connection or counting a WAL error. Either check the payload size at decode or lower `MaxFrameSize` to the WAL record limit.

### Malformed events

| Kind (as generated by the client) | Result |
|---|---|
| empty `producer_id`, empty `event_type`, missing `event_type` | `invalid` / `invalid_payload` (199 events) |
| `seq: 0` | accepted (52 events) |

The decoder does not require a non-zero `seq`, so the client's "zero seq" case is accepted and persisted. Whether `seq` 0 is valid is a protocol decision. The benchmark expects it to be rejected, and `docs/event-model.md` does not list it as required.

### ACK sequence mismatches are a client artifact

The summary reports 251 `ack_seq_mismatches`, exactly the number of malformed events sent. No valid event had a mismatched ACK.

- `invalid` ACKs carry no event identity. `wire.Ack.Seq` has no `omitempty`, so it serializes as `"seq": 0`, and the client compares that against the sequence it sent.
- For the accepted `seq: 0` events, the client's exception `(mal and s == 0)` checks its own loop sequence instead of the ACK's sequence, so those are flagged too.

## Stalls

The 26 October run is noticeably smoother. Binning ACK arrivals into 100 ms windows, it had 4 windows below 20% of the median rate (each 0.1 s long) and 1 window with no ACKs, at 16.3 s inside a wave. The 19 October run had 34 such windows, the longest 0.8 s, and 11 windows with no ACKs.

A few short dips remain in the per-second series, the largest at 225 s (2,638 events/s, normal p99 481 ms) and smaller ones at 22, 30, and 34 s. Without gateway metrics, the cause cannot be pinned down; fsync outliers, ClickHouse inserts and merges on the shared volume, and Go GC are the candidates.

## Limits of this analysis

- **Client-side data only.** The run kept no Prometheus data and no host CPU or disk metrics, so the bottleneck is inferred from the latency model, not observed directly.
- **Shared host.** ClickHouse, Prometheus, Grafana, and Alertmanager share the gateway's 2 vCPUs and its gp3 volume. Absolute numbers will differ on a dedicated host.
- **Uncommitted code.** The run used the working tree, including the deduplication fix and the `producer_epoch` documentation change, not a commit.
- **No run of the regressed code.** The impact of the old deduplication lock on EC2 is a prediction from the code (one fsync plus the 2 ms batch window per event, globally serialized), not a measurement.
- **The client changed between runs.** Its event-loop lag fell from a 20 ms to a 7.5 ms median, which affects the comparison with 19 October.

## Next steps

In priority order:

1. **Confirm the WAL bottleneck.** Rerun the same benchmark and keep `fairgate_wal_group_commit_records`, `fairgate_wal_sync_duration_seconds`, and host CPU. The simplest way is to save Prometheus's data volume, or to scrape `/metrics` from the client before and after the run.
2. **Lift the group commit cap if confirmed.** Let one batch take every queued append, or raise `maxBatchSize` well above the expected concurrency. Also skip the 2 ms collection wait when appends are already queued, since that wait currently sets the floor even on an idle gateway. Separately, the request channel is sized with `maxRecordSize` (1,048,576 slots) rather than a batch-related constant, which looks unintended.
3. **Reject oversized events before the WAL.** Return `invalid`, keep the connection open, and stop counting them as WAL errors, so `FairGateWALAppendErrors` only fires for real disk problems.
4. **Decide whether `seq` 0 is valid**, then align the decoder, `docs/event-model.md`, and the benchmark.
5. **Fix the benchmark harness.** Correct the ACK sequence check, and add a scenario that exceeds the admission rate (a producer pushing above 500 events/s across several connections) so admission control is actually tested.
6. **Consider lowering per-event log lines to debug.** Every accepted event writes two info lines (`Event admitted` and `Event processed by DRR`), about 14,000 lines/s at this load through Docker's log driver on a 2-vCPU host.
