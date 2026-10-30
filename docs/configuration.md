[FairGate](../README.md) › [Docs](README.md) › Configuration

# Configuration

FairGate reads an optional YAML config file. Settings are resolved in three layers, each overriding the one before:

1. Built-in defaults.
2. The config file, passed with `-config` or `FAIRGATE_CONFIG`. `-config` wins if both are set.
3. Environment variables (see [Environment overrides](#environment-overrides)).

Without a config file, the gateway runs with the defaults below, which match its behavior before the file existed, except that the [overload](#overload) limits are on by default. Every key in the file is optional, and a key you leave out keeps its default.

```bash
fairgate -config /etc/fairgate/fairgate.yaml
FAIRGATE_CONFIG=/etc/fairgate/fairgate.yaml fairgate
```

[`deploy/fairgate.yaml`](../deploy/fairgate.yaml) is the reference file. It lists every setting with its default and a comment, and Docker Compose mounts it into the gateway container. A test checks that its values match the built-in defaults. Copy it as a starting point and remove the settings you don't change.

Unknown keys are rejected, so a misspelled setting stops startup instead of being silently ignored. Invalid values are also rejected at startup, and every problem is reported at once.

## Settings

| Key | Default | Description |
|---|---|---|
| `server.listen_addr` | `:9001` | TCP address for event ingestion. |
| `server.metrics_addr` | `:9100` | HTTP address for `/health` and `/metrics`. If you change it, update the Compose port mapping, healthcheck, and Prometheus target to match. |
| `server.shutdown_grace_period` | `5s` | Time active handlers get to finish before their connections are closed. |
| `server.max_frame_bytes` | `5242880` (5 MiB) | Largest frame accepted, counting the type and flags bytes, from 1 KiB to 64 MiB. A larger frame is answered `invalid` / `frame_too_large`: skipped if it is up to four times this size, otherwise the connection is closed. Events over the WAL's 1 MiB record limit are refused as `payload_too_large` whatever this is. See [Error handling](wire-protocol.md#error-handling). |
| `server.idle_timeout` | `0s` | Close a connection that starts no new frame for this long. `0` keeps idle connections open. |
| `server.frame_timeout` | `30s` | Time to receive the rest of a frame once its first byte arrives. A stalled frame closes the connection. |
| `server.write_timeout` | `10s` | Time to write one ACK. A client that stops reading ACKs is disconnected. |
| `server.allow_legacy_clients` | `true` | Accept clients that send events without a hello first (protocol version 0). Set `false` once every producer negotiates a version; their events are then answered `invalid` / `hello_required`. See [Handshake and versioning](wire-protocol.md#handshake-and-versioning). |
| `server.drain_timeout` | `10s` | After handlers stop, time to empty the producer queues and ship every WAL record to ClickHouse before exiting. Whatever is left stays in the WAL for the next start. `0` skips the drain. See [Queue-drain confirmation](server-lifecycle.md#queue-drain-confirmation). |
| `admission.rate` | `500` | Token-bucket refill rate per producer, in events per second. |
| `admission.burst` | `1000` | Token-bucket capacity per producer. |
| `queues.capacity` | `512` | Events buffered per producer. See also `overload.queue_high_water`. |
| `scheduler.quantum` | `8` | Events served per DRR round to a producer of weight 1. |
| `fairness.default_weight` | `1` | DRR weight for producers without an entry under `fairness.producers`. |
| `fairness.producers.<id>.weight` | `default_weight` | The producer's share of each DRR round: `quantum × weight` events. Fractions are allowed. |
| `fairness.producers.<id>.rate` | `admission.rate` | Admission rate for the producer, in events per second. |
| `fairness.producers.<id>.burst` | `admission.burst` | Admission burst for the producer. |
| `fairness.producers.<id>.queue_capacity` | `queues.capacity` | Events buffered for the producer before enqueueing blocks. |
| `overload.queue_high_water` | `0.9` | Refuse a producer's events once its queue holds this fraction of its capacity, from 0 to 1. `0` disables the check. |
| `overload.max_queued_events` | `100000` | Events queued across all producers. Past it, producers holding more than their weighted share of this total are refused. `0` disables the check. |
| `overload.max_shipper_lag_bytes` | `1073741824` (1 GiB) | Durable WAL bytes not yet delivered to ClickHouse. Past it, every new event is refused. `0` disables the check. |
| `overload.resume_ratio` | `0.8` | A gateway-wide limit clears once its backlog drops below this fraction of it. Greater than 0 and at most 1. |
| `overload.sample_interval` | `100ms` | How often the total queued events and the shipper lag are sampled. |
| `wal.path` | `data/fairgate.wal` | Path of the first WAL segment. Later segments are created next to it. |
| `wal.segment_size` | `67108864` (64 MiB) | Segment size in bytes before the writer rotates. Reclamation deletes whole segments, so smaller segments free disk space sooner. |
| `wal.on_corruption` | `refuse` | What startup does with damaged WAL records the shipper has not delivered yet: `refuse` stops startup, `quarantine` copies the segment to `quarantine/`, cuts it at the damage, and starts. A torn final record, or damage to records already shipped, is handled automatically either way. See [Recovery](wal.md#recovery). |
| `wal.degraded_exit_after` | `1m` | If a WAL write or sync fails and moving to a fresh segment fails too, events are refused as `rejected` / `storage_unavailable` and `/ready` reports not ready. After this long the gateway exits with status `3` so a supervisor can restart it. `0` never exits. See [Write and sync failures](wal.md#write-and-sync-failures). |
| `shipper.checkpoint_path` | `data/checkpoint.json` | Checkpoint file. Keep it on the same persistent volume as the WAL. |
| `shipper.batch_size` | `200` | Maximum events per ClickHouse insert. |
| `shipper.poll_interval` | `1s` | How often the shipper checks for new durable WAL records once it has caught up. |
| `shipper.retry.initial_delay` | `1s` | First retry delay after a failed insert. |
| `shipper.retry.max_delay` | `30s` | Cap on the retry delay. Must be at least `initial_delay`. |
| `shipper.retry.multiplier` | `2` | Growth factor between attempts. Must be at least 1. |
| `shipper.retry.jitter` | `0.2` | Symmetric jitter as a fraction of each delay, between 0 and 1. |
| `clickhouse.addr` | `[localhost:9000]` | One or more ClickHouse native-protocol addresses. |
| `clickhouse.database` | `fairgate` | Default database of the ClickHouse connection. The adapter always writes to the `fairgate.events` table. |
| `clickhouse.username` | `fairgate` | ClickHouse user. |
| `clickhouse.password` | `fairgate_dev_password` | ClickHouse password. Prefer `CLICKHOUSE_PASSWORD` to keep it out of the file. |
| `log.level` | `info` | One of `debug`, `info`, `warn`, `error`. |
| `profiling.enabled` | `false` | Serve the `pprof` endpoints under `/debug/pprof/`. |
| `profiling.addr` | `127.0.0.1:6060` | Listener for the `pprof` endpoints, separate from `server.metrics_addr`. |
| `profiling.block_profile_rate` | `0` | Sample one blocking event per this many nanoseconds blocked; `0` leaves the block profile empty. |
| `profiling.mutex_profile_fraction` | `0` | Sample one in this many mutex contention events; `0` leaves the mutex profile empty. |

## Fairness

The `fairness` section sets how producers share admission, buffering, and scheduling. `<id>` is a `producer_id`. Every field of an entry is optional: an omitted or zero field keeps the default named in the table above. Values must not be negative, and `default_weight` must be greater than zero.

```yaml
fairness:
  default_weight: 1
  producers:
    payments-service:
      weight: 4
      rate: 2000
      burst: 4000
      queue_capacity: 2048
    batch-importer:
      weight: 0.25
```

See [Backpressure, scheduling, and fairness](scheduling.md) for how weights translate into each producer's share.

Producers listed under `fairness.producers` also get their own `producer` label in the per-producer metrics. Every other producer is reported as `other`, so list a producer here (even with no overrides, as `my-producer: {}`) to monitor it individually. See [Observability](observability.md#per-producer-labels).

## Overload

The `overload` section sets when the gateway refuses new events because work is backing up. A refused event gets a `rejected` ACK whose message names the backlog, and it is never appended to the WAL, so the producer can retry it after backing off. See [Overload handling](scheduling.md#overload-handling) for how each check works.

```yaml
overload:
  queue_high_water: 0.9
  max_queued_events: 100000
  max_shipper_lag_bytes: 1073741824
  resume_ratio: 0.8
  sample_interval: 100ms
```

Set a limit to `0` to turn that check off. With all three at `0`, a full queue blocks the handler after the WAL append, as it did before this section existed.

## Paths and durations

Durations use Go syntax, for example `500ms`, `5s`, or `1m30s`. Relative paths resolve against the gateway's working directory, which is `/app` in the Docker image.

These limits are not configurable:

- The maximum frame size (5 MiB) is part of the wire protocol.
- WAL group commit has no batch size limit, and its 2 ms collection delay for a lone request is fixed; see [Write-Ahead Log](wal.md#writer-and-group-commit).
- Segment reclamation has no separate switch: it is driven by the shipper's checkpoint.

## Environment overrides

These environment variables override the config file. They suit secrets and per-host values, and they are the same variables FairGate read before the file existed.

| Variable | Overrides |
|---|---|
| `FAIRGATE_CONFIG` | Config file path, used when `-config` is not given. |
| `FAIRGATE_METRICS_ADDR` | `server.metrics_addr` |
| `FAIRGATE_CHECKPOINT_PATH` | `shipper.checkpoint_path` |
| `CLICKHOUSE_ADDR` | `clickhouse.addr` (comma-separated for several addresses) |
| `CLICKHOUSE_DATABASE` | `clickhouse.database` |
| `CLICKHOUSE_USER` | `clickhouse.username` |
| `CLICKHOUSE_PASSWORD` | `clickhouse.password` |

An empty variable counts as unset. `FAIRGATE_SHIPPER_ENABLED` is no longer read. The shipper always runs, and the setting was removed from Docker Compose; it can be deleted from existing environments.
