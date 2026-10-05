# FairGate

**A fair-share event ingestion gateway written in Go.**

FairGate is a modular event-ingestion gateway. It accepts framed event streams over TCP, applies admission control, persists accepted events in a segmented Write-Ahead Log (WAL) with group commit, and schedules events from per-producer queues using Deficit Round Robin (DRR).

The project explores fair resource sharing in event-ingestion systems: keeping producers isolated from one another through bounded buffering, admission limits, and fair scheduling. FairGate is being developed as an open-source foundation that can be extended with different storage backends and downstream processing components.

> [!IMPORTANT]
> **Beta status.** FairGate is under active development. The current implementation provides TCP ingestion, event decoding and validation, admission control, per-producer bounded queues, DRR scheduling, a segmented WAL with group commit and ordered recovery, acknowledgments, and context-aware graceful shutdown. The shipper can read durable WAL records from a persisted checkpoint, batch them for a `Store`, retry failed inserts with capped exponential backoff and jitter, and atomically persist progress after successful inserts. The gateway runtime starts the shipper when `FAIRGATE_SHIPPER_ENABLED=true`, and a Docker Compose stack runs FairGate with ClickHouse. WAL segment reclamation, advanced overload control, and production observability are not implemented.

## Table of Contents

- [Design goals](#design-goals)
- [Features](#features)
- [Recent updates](#recent-updates)
- [Architecture](#architecture)
- [Event lifecycle](#event-lifecycle)
- [Event model](#event-model)
- [Wire protocol](#wire-protocol)
- [Write-Ahead Log](#write-ahead-log)
- [Backpressure and scheduling](#backpressure-and-scheduling)
- [Server lifecycle and graceful shutdown](#server-lifecycle-and-graceful-shutdown)
- [Delivery semantics](#delivery-semantics)
- [Configuration](#configuration)
- [Storage integration](#storage-integration)
- [Roadmap](#roadmap)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [Docker deployment](#docker-deployment)
- [Development status and limitations](#development-status-and-limitations)
- [Contributing](#contributing)
- [License](#license)

## Design goals

| Goal | Approach |
|---|---|
| Producer isolation | Each producer has its own bounded queue, so one producer cannot consume another producer's buffer space. |
| Fair service | DRR scheduling visits producer queues in rounds and serves a bounded amount from each. |
| Bounded resource usage | Every queue has an explicit capacity, and a full queue applies backpressure instead of growing. |
| Durability before acknowledgment | Events are appended to the WAL and synchronized to disk (via group commit) before an acknowledgment is sent. |
| Extensibility | Core ingestion and scheduling are independent of event meaning. Storage and processing attach through well-defined seams. |

## Features

### Currently implemented

- **TCP event ingestion.** Accepts client connections and reads length-prefixed frames.
- **Framed wire protocol.** Identifies event and acknowledgment frames using frame types.
- **JSON event decoding.** Decodes events and checks required fields.
- **Admission control.** Applies token-bucket-based limits to incoming events.
- **Per-producer queues.** Keeps producer events in separate bounded channels.
- **DRR scheduling.** Selects queued events in producer rounds using a configurable quantum.
- **Segmented Write-Ahead Log.** Appends events with length and CRC metadata across multiple segment files, rotating to a new segment when the configured segment size would be exceeded.
- **Group commit.** A dedicated writer goroutine batches concurrent append requests and issues a single `Sync()` per batch, bounded by a maximum request count and a short collection delay.
- **WAL recovery.** Discovers segments, scans them in order on startup, truncates an incomplete trailing record in the final segment only, rejects incomplete records in earlier segments, and replays recovered events into the producer queues.
- **WAL fatal error handling.** Write, rotation, and synchronization failures are recorded as fatal; affected requests receive an error and later batches are rejected.
- **Durable WAL reader.** Reads records only up to the WAL's synchronized durable end, verifies length, CRC32, and JSON, returns each event with its start and end positions, and advances across segment boundaries from any saved position.
- **WAL-backed batch shipper.** `Shipper.Run(ctx)` loads the checkpoint, reads durable records in order up to `BatchSize`, and sends each batch to the configured `Store`. At the durable WAL end it polls for new records and keeps running until its context is canceled.
- **Gateway shipper runtime.** When `FAIRGATE_SHIPPER_ENABLED=true`, `RunContext` starts the shipper alongside the server, and shutdown stops and joins it before the store and WAL are closed.
- **Retry with backoff.** Retries failed batch inserts with capped exponential delay, configurable jitter, and context-aware waiting. The same batch is retried until it succeeds or the context is canceled.
- **Durable checkpoints.** Stores the last successfully inserted record position as a segment and offset. Checkpoints are written through a synced temporary file followed by an atomic rename; a missing checkpoint starts reading at the beginning of the WAL, and invalid JSON or negative offsets are rejected.
- **Store interface.** `InsertBatch(ctx, events)` and `Close()` separate storage from the processing pipeline.
- **ClickHouse adapter.** Official Go client with configurable authentication, `Ping` health checks, batched inserts through `PrepareBatch` and `Send`, and graceful cleanup.
- **Optional ClickHouse integration test.** Environment-gated test covering connectivity, batch insertion, and retrieval.
- **Docker deployment.** A `Dockerfile` and Compose stack run FairGate with ClickHouse, with the shipper enabled and FairGate started only after ClickHouse reports healthy. `install-docker-ubuntu.sh` installs Docker Engine and the Compose plugin on Ubuntu.
- **Acknowledgments.** Sends an accepted acknowledgment after the event has been appended to the WAL and enqueued.
- **Concurrent connections.** Handles client connections in separate goroutines and tracks them for coordinated shutdown.
- **Context-aware lifecycle.** `RunContext` lets the caller control server lifetime through a `context.Context`.
- **Graceful shutdown.** Stops accepting clients, waits for active handlers within a grace period, forces closure of blocked connections afterward, and stops the scheduler before cleanup.
- **Cancelable enqueueing.** A handler blocked on a full producer queue can be interrupted during shutdown.
- **Scheduler error propagation.** An unexpected scheduler failure is reported to the caller instead of being silently lost.
- **Tests.** Includes unit and integration tests for core behavior.

### Planned

- WAL segment reclamation after safely shipped segments are no longer needed
- Configurable producer weights and richer fairness controls
- Backlog-aware overload handling and load shedding
- Prometheus metrics, profiling endpoints, and dashboards
- Load-generation, reconciliation, and chaos-testing tools
- Additional protocol hardening
- Explicit queue-drain confirmation during shutdown

Planned capabilities are not implied to be available in this beta.

## Recent updates

### 05th October, 2026: Gateway shipper runtime and Docker deployment

The gateway can now start the WAL shipper as part of `RunContext` when `FAIRGATE_SHIPPER_ENABLED=true`. The shipper polls the durable WAL end for new records, retries ClickHouse insert failures, and persists its checkpoint after successful batches. Docker Compose passes the ClickHouse connection settings and waits for ClickHouse health before starting FairGate. The `install-docker-ubuntu.sh` script installs Docker Engine and the Compose plugin on Ubuntu.

For remote access, publish TCP port `9001` on a non-loopback interface and allow inbound TCP `9001` through the host firewall.

See [Docker deployment](#docker-deployment) and [Storage integration](#storage-integration).

### 05th October, 2026: WAL shipping, durable checkpoints, and retry backoff

The latest changes connect the storage building blocks to a WAL-reading shipper. They add record-position tracking across segments, persistent progress, and retry handling for failed storage writes.

- **Durable WAL reader.** Reads only records up to the WAL's synchronized durable end, verifies record length, CRC32, and JSON, and returns each event with its start and end positions. It advances across segment boundaries and can start from a saved position.
- **Checkpoint-based resume.** The checkpoint records a segment ID and byte offset. A missing checkpoint starts at the beginning of the WAL; invalid JSON and negative offsets are rejected.
- **Atomic checkpoint commit.** Each successful batch advances the checkpoint by writing and syncing a temporary JSON file, then renaming it over the checkpoint path.
- **WAL-backed batch shipping.** `Shipper.Run(ctx)` loads the checkpoint, reads events in WAL order up to `BatchSize`, calls `Store.InsertBatch`, and commits the batch end position only after insertion succeeds. It returns when it reaches the current durable WAL end.
- **Retry and backoff.** Failed inserts retry the same batch using exponential delays capped at a maximum delay, with optional symmetric jitter. Waiting respects context cancellation; retries otherwise continue until success.
- **Cross-segment recovery validation.** Recovery now rejects incomplete records in non-final segments instead of truncating them; only an incomplete tail in the final segment can be truncated.
- **Storage interface export.** `Store.InsertBatch` is exported so the shipper can call storage implementations across package boundaries.

At this stage the shipper was a standalone component: it was not started by the gateway command, `Run` returned at the current durable end instead of following later appends, and there was no automatic WAL segment reclamation. The gateway runtime wiring and polling were added in the update above. A crash after a successful insert but before the checkpoint commit can replay that batch, so these changes do not provide exactly-once delivery.

See [Storage integration](#storage-integration) for the shipper flow and [Write-Ahead Log](#write-ahead-log) for recovery behavior.

### 03rd October, 2026: Asynchronous shipper, Store interface, and ClickHouse adapter

Earlier versions stopped at the scheduler, whose callback only logged events, and had no way to hand events to a database. The latest changes add an asynchronous shipping layer and a storage abstraction, with ClickHouse as the first backend, so event scheduling is decoupled from database insertion.

- **Asynchronous shipper.** A bounded in-memory channel buffers incoming events, so callers are not blocked on database inserts. `BatchSize`, `FlushInterval`, and `QueueSize` control batch formation, flushing frequency, and buffer capacity.
- **Context-aware submission.** `Submit(ctx, event)` hands an event to the shipper and respects cancellation, so a caller waiting on a full buffer can stop waiting.
- **`Store` interface.** Storage operations are separated from the event processing pipeline behind `InsertBatch(ctx, events)` and `Close()`. Different backends can be implemented without modifying the shipper.
- **ClickHouse adapter.** Built on the official Go client, with connection initialization, configurable authentication, connection health checks through `Ping`, and graceful connection cleanup.
- **Batched insertion.** The adapter uses `PrepareBatch` and `Send` to reduce per-event insertion overhead.
- **`fairgate.events` persistence.** Events are stored with their producer ID, event timestamp, sequence number, index, event type, and payload. The adapter follows the existing ClickHouse schema, including `ReplacingMergeTree`, monthly partitioning, and a 30-day TTL.
- **Optional integration test.** An environment-gated ClickHouse test validates connectivity, batch insertion, and retrieval of persisted events. It does not run unless ClickHouse is configured.

These changes provide the shipping and storage building blocks. Durable checkpoints, retry and backoff, and WAL segment reclamation are still separate upcoming steps, so database delivery guarantees are not yet documented. See [Storage integration](#storage-integration).

### 03rd October, 2026: Segmented WAL and group commit

Earlier versions of the WAL stored all events in a single file and performed a disk synchronization (`Sync()`) for every appended event. The latest changes introduce WAL segmentation and group commit to manage log growth and reduce the overhead of frequent disk synchronization.

- **Segmented WAL.** The WAL is now split into multiple files, with a configurable segment size. When the active segment reaches its size limit, the writer rotates to a new segment instead of continuing to grow a single file.
- **Ordered recovery.** Recovery discovers WAL segments and processes them in order, reconstructing events across multiple files while preserving the existing record format.
- **Incomplete record handling.** Recovery can truncate an incomplete trailing record in the final segment to the last valid record boundary, while treating corruption in completed records as an error.
- **Dedicated writer goroutine.** A separate `writerLoop` now owns WAL file operations. Instead of each `Append()` call writing directly to disk, requests are submitted to a shared channel and processed by the writer.
- **Group commit.** Multiple append requests are collected into batches, allowing the WAL to perform a single `Sync()` for the batch rather than synchronizing after every event.
- **Bounded batching.** Batches are limited by a maximum request count and a short collection delay, allowing the writer to balance synchronization overhead with append latency.
- **Asynchronous request submission.** Concurrent handlers can submit append requests to the writer goroutine, while each caller waits for confirmation that its batch has been written and synchronized.
- **Segment rotation during batching.** The writer checks each record's size before writing and rotates the active segment when the next record would exceed the configured segment limit.
- **Fatal error handling.** Write, rotation, and synchronization failures are recorded as fatal WAL errors. Affected requests receive an error, and subsequent batches are rejected rather than being reported as successfully persisted.
- **Graceful WAL closure.** Closing the WAL stops new submissions, allows queued requests to be processed, and then synchronizes and closes the active segment before the writer exits.

These changes establish a WAL that supports multiple segments and batched disk synchronization while retaining the existing record framing and recovery format. Queue-drain confirmation during server shutdown and ClickHouse persistence are still separate upcoming steps.

See [Write-Ahead Log](#write-ahead-log) for the writer flow and recovery behavior.

### 03rd October, 2026: Graceful shutdown and context-aware event handling

Earlier versions accepted connections and processed events concurrently, but had no coordinated way to stop accepting clients, let in-flight handlers finish, and release resources safely. The latest changes add a controlled shutdown sequence built on `context`, `sync.WaitGroup`, `sync.Mutex`, and channels.

- **`RunContext`.** A new context-aware entry point gives the caller control over server lifetime. `Run` remains as a thin wrapper that uses a background context, so existing callers are unaffected.
- **Connection tracking.** Active connections are registered in a mutex-protected map and counted with a `WaitGroup`, so the server knows which handlers are running and can wait for them.
- **Listener watcher.** A watcher goroutine closes the TCP listener when the context is canceled, which unblocks `Accept` and begins shutdown. It exits cleanly if the accept loop ends first.
- **Cancelable enqueueing.** `EnqueueContext` replaces the blocking enqueue in the connection handler, so a handler stuck on a full queue can stop waiting when shutdown is forced.
- **Separate handler and scheduler contexts.** Handlers can be canceled independently, which lets the scheduler keep processing queued events while handlers drain.
- **Scheduler lifecycle.** The server waits for the scheduler goroutine to exit before returning, and an unexpected scheduler failure closes the listener and is returned to the caller.
- **Five-second grace period.** After the accept loop stops, handlers get a fixed grace period. If it expires, active connections are closed and the handler context is canceled to interrupt blocked reads and enqueues.
- **WAL safety.** The WAL is not closed until every tracked handler has exited, so a handler cannot append to a closed log.

See [Server lifecycle and graceful shutdown](#server-lifecycle-and-graceful-shutdown) for diagrams, behavior details, and limitations.

## Architecture

### Current implementation

The diagram below shows the ingestion path and the separate, opt-in WAL shipping path in the gateway runtime.

```mermaid
flowchart TD
    P["Event producers"] -->|"TCP, length-prefixed frames"| S

    subgraph GW["FairGate (current implementation)"]
        direction TB
        S["TCP server<br/>frame decoding and event validation"]
        A["Admission control<br/>token-bucket limits"]
        W[("Segmented Write-Ahead Log<br/>length + CRC32, group commit")]
        Q["Per-producer bounded queues"]
        D["DRR scheduler<br/>configurable quantum"]
        C["Event callback<br/>currently logs producer ID and sequence"]
        SH["WAL shipper<br/>opt-in: FAIRGATE_SHIPPER_ENABLED"]

        S --> A
        A --> W
        W --> Q
        Q --> D
        D --> C
        W -.->|"recovered events at startup"| Q
        W -.->|"durable records, polled"| SH
    end

    Q -.->|"accepted ACK"| P
    SH -->|"batched inserts"| CHDB[("ClickHouse")]
```

Accepted events are appended to the WAL before they are enqueued and acknowledged to the client. The scheduler callback logs selected events. When `FAIRGATE_SHIPPER_ENABLED=true`, the server starts a WAL shipper that writes batches to ClickHouse independently of the scheduler.

The server's goroutine and context structure, including the shutdown path, is described in [Server lifecycle and graceful shutdown](#server-lifecycle-and-graceful-shutdown).

### WAL shipping component

The server starts the shipper when `FAIRGATE_SHIPPER_ENABLED=true`. It reads durable WAL records from its checkpoint, inserts each batch through the `Store` interface, and commits the checkpoint only after insertion succeeds. It polls at the WAL end for new durable records. Segment reclamation remains planned.

```mermaid
flowchart LR
    W[("Write-Ahead Log")]
    SH["Shipper<br/>WAL reader, batching, retry, backoff"]
    ST["Store interface"]
    CH[("ClickHouse")]
    CK["Durable checkpoint"]
    R["WAL segment reclamation"]

    W --> SH
    SH --> ST
    ST --> CH
    SH --> CK
    CK -.->|"gates"| R

    classDef planned stroke-dasharray: 5 5,stroke-width:2px;
    class R planned;
```

### Component responsibilities

| Package | Responsibility |
|---|---|
| `internal/server` | TCP listener, per-connection goroutines, connection tracking, frame handling, acknowledgment delivery, server lifecycle (`RunContext`), optional shipper startup and shutdown, and graceful shutdown. |
| `internal/wire` | Frame codec, event and acknowledgment types, validation. |
| `internal/admit` | Token-bucket admission control. |
| `internal/sched` | Per-producer bounded queues (including context-aware `EnqueueContext`) and the DRR scheduler. |
| `internal/wal` | Segmented WAL, writer goroutine with group commit, segment rotation, synchronization, and ordered recovery with cross-segment validation. |
| `internal/shipper` | Durable WAL reader, checkpoint load and atomic commit, batching, polling at the durable end, and retry with capped exponential backoff and jitter. |
| `internal/store` | `Store` interface and the ClickHouse adapter. |

## Event lifecycle

The sequence below shows the path of a single event through the current implementation.

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant S as TCP server
    participant A as Admission control
    participant W as WAL
    participant Q as Producer queue
    participant D as DRR scheduler
    participant C as Event callback

    P->>S: Event frame
    S->>S: Decode frame and validate event
    S->>A: Admit event for producer
    alt Event admitted
        A-->>S: Allow
        S->>W: Submit append request (batched with others)
        W-->>S: Durable (after batch sync)
        S->>Q: EnqueueContext (waits for space or cancellation)
        Q-->>S: Enqueued
        S-->>P: ACK with status accepted
        D->>Q: Dequeue within quantum
        Q-->>D: Event
        D->>C: Deliver event
    else Event not admitted
        A-->>S: Reject
        S-->>P: Rejection handled per server implementation
    end
```

An `accepted` ACK reflects WAL durability and queue admission. It does not reflect downstream storage. See [Delivery semantics](#delivery-semantics).

If shutdown cancels the handler while it is waiting for queue space, the event has already been appended to the WAL but is not acknowledged. It remains available for recovery on the next startup.

When the shipper is enabled, it picks the event up from the WAL separately, after the event is durable and independently of the scheduler.

## Event model

Events are represented as JSON with the following fields:

```json
{
  "producer_id": "sensor-01",
  "event_time": "2026-10-02T08:00:00Z",
  "seq": 1,
  "idx": 0,
  "event_type": "temperature",
  "payload": "{\"value\":24.7}"
}
```

| Field | Type | Description |
|---|---|---|
| `producer_id` | string | Identifier of the event producer |
| `event_time` | timestamp | Event timestamp |
| `seq` | unsigned integer | Producer sequence number |
| `idx` | unsigned integer | Event index |
| `event_type` | string | Event category |
| `payload` | string | Event payload |

The decoder requires a non-empty producer ID, a non-zero event time, and a non-empty event type.

```mermaid
classDiagram
    class Event {
        +string producer_id
        +timestamp event_time
        +uint seq
        +uint idx
        +string event_type
        +string payload
    }
    class Ack {
        +string producer_id
        +uint seq
        +uint idx
        +string status
        +string message
    }
    Event "1" --> "1" Ack : acknowledged by
```

The `message` field of an acknowledgment is optional.

## Wire protocol

FairGate uses a length-prefixed frame format. Fields are transmitted in the order shown.

```mermaid
flowchart LR
    subgraph FRAME["Frame"]
        direction LR
        L["Length<br/>4 bytes<br/>big-endian"]
        T["Type<br/>1 byte"]
        F["Flags<br/>1 byte"]
        PL["Payload<br/>variable length"]
        L --> T --> F --> PL
    end
```

| Field | Size | Description |
|---|---|---|
| Length | 4 bytes, big-endian | Frame length prefix used to delimit frames on the TCP stream. |
| Type | 1 byte | Identifies the frame kind (see below). |
| Flags | 1 byte | Per-frame flags. |
| Payload | Variable | Frame body, interpreted according to the type. |

How frames are demultiplexed on a connection:

```mermaid
flowchart TD
    R["Read length prefix"] --> RB["Read type, flags, and payload"]
    RB --> TY{"Frame type"}
    TY -->|"1"| EV["Event frame<br/>decode and validate JSON event"]
    TY -->|"2"| AK["ACK frame<br/>producer ID, seq, idx, status, message"]
    TY -->|"Other"| UN["Unrecognized type<br/>handled per server implementation"]
```

Event frames flow from producer to server and ACK frames flow from server to producer.

The current frame types are:

| Type | Purpose |
|---|---|
| `1` | Event |
| `2` | Acknowledgment (ACK) |

An ACK contains the producer ID, sequence number, event index, status, and an optional message. An `accepted` ACK indicates that the event was written to the WAL and enqueued. It does **not** indicate that a downstream database has stored or queried the event.

Refer to the `wire` package for the current protocol implementation and exact encoding behavior.

## Write-Ahead Log

The WAL stores serialized event records with a length field and a CRC32 checksum. The layout below is conceptual and is unchanged by segmentation and group commit. Refer to the `wal` package for the exact encoding.

```text
+----------------+----------------+---------------------------+
| Length         | CRC32          | Serialized event          |
+----------------+----------------+---------------------------+
```

### Segments

The log is stored as multiple segment files rather than one growing file. Each segment holds a sequence of records in the format above. The segment size is configurable. Before writing a record, the writer checks whether it would push the active segment past the limit, and if so rotates to a new segment first.

A position in the log is identified by a segment ID and a byte offset within that segment. The shipper uses these positions for its checkpoint.

### Writer and group commit

A dedicated `writerLoop` goroutine owns all WAL file operations. Callers of `Append()` submit a request to a shared channel and wait for the result, rather than writing to disk themselves. The writer collects requests into a batch, bounded by a maximum request count and a short collection delay, writes the records, and performs one `Sync()` for the whole batch. Every caller in the batch is then released, so no `Append()` returns before its record has been synchronized.

```mermaid
flowchart TD
    H["Handler goroutines<br/>Append()"] -->|"submit request"| CH["Shared request channel"]
    CH --> WL["writerLoop"]
    WL --> BT["Collect batch<br/>max requests or short delay"]
    BT --> SZ{"Next record fits<br/>in active segment?"}
    SZ -->|"No"| RT["Rotate to new segment"]
    SZ -->|"Yes"| WR["Write record"]
    RT --> WR
    WR --> MORE{"More records<br/>in batch?"}
    MORE -->|"Yes"| SZ
    MORE -->|"No"| SY["Sync once for the batch"]
    SY --> RES["Return result to each caller"]
    RES --> WL
```

If a write, rotation, or sync fails, the error is recorded as fatal. Requests in the affected batch receive an error, and later batches are rejected instead of being reported as persisted.

Closing the WAL stops new submissions, lets queued requests finish, then synchronizes and closes the active segment before the writer exits.

### Recovery

On startup the WAL discovers its segments and scans them in order, record by record. An incomplete trailing record in the **final** segment is truncated to the last valid record boundary. An incomplete record in any earlier segment is rejected as an error rather than truncated, because a completed segment should never end mid-record. Corrupt records and checksum mismatches in completed records are also reported as errors.

```mermaid
flowchart TD
    A["Start recovery"] --> B["Discover segments<br/>and order them"]
    B --> B2["Open next segment"]
    B2 --> C{"Another record<br/>available?"}
    C -->|"No: end of segment"| L{"More segments?"}
    L -->|"Yes"| B2
    L -->|"No"| H["Recovery complete"]
    C -->|"Yes"| D{"Record complete?"}
    D -->|"No: incomplete record"| FS{"Final segment?"}
    FS -->|"Yes: incomplete tail"| E["Truncate to last<br/>valid record boundary"]
    E --> H
    FS -->|"No"| G2["Report error"]
    D -->|"Yes"| F{"CRC32 matches?"}
    F -->|"No"| G["Report error"]
    F -->|"Yes"| I["Accept record"]
    I --> C
```

Recovered events are replayed into the producer queues. The scheduler is started before replay so that it consumes events while the queues fill, which avoids a startup deadlock when a producer has more recovered events than its queue capacity. Automated segment reclamation is a planned extension, so completed segments are currently retained even after the shipper has checkpointed past them.

### Durable reader

The shipper reads the WAL through a reader that stops at the WAL's synchronized durable end, so it never returns a record that has not been synced. For each record it verifies the length, CRC32, and JSON, and returns the event together with its start and end positions. It moves across segment boundaries automatically and can begin at any saved position.

## Backpressure and scheduling

Each producer has its own bounded queue. The queue capacity is configured when the queue manager is created, and the server currently uses 256 events per producer. Enqueueing blocks when a producer's queue is full, applying backpressure to the caller instead of allowing the queue to grow without bound. In the connection handler the wait is context-aware (`EnqueueContext`), so a blocked handler can stop waiting if shutdown is forced.

The DRR scheduler visits producer queues and processes available events up to its configured quantum (currently 8 in the server). The scheduler is independent of event meaning, and its callback is responsible for downstream handling. At this beta stage, the callback only logs events; database delivery is handled by the separate WAL shipper.

```mermaid
flowchart TD
    S["Select next producer queue"] --> E{"Queue empty?"}
    E -->|"Yes"| N["Advance to next producer"]
    E -->|"No"| T["Dequeue up to quantum events"]
    T --> CB["Invoke callback for each event"]
    CB --> N
    N --> S
```

```mermaid
flowchart LR
    subgraph PA["Producer A (high rate)"]
        QA["Queue A<br/>bounded"]
    end
    subgraph PB["Producer B"]
        QB["Queue B<br/>bounded"]
    end
    subgraph PC["Producer C"]
        QC["Queue C<br/>bounded"]
    end

    QA --> DRR(("DRR<br/>scheduler"))
    QB --> DRR
    QC --> DRR
    DRR --> OUT["Callback"]
```

Because each queue is separate and bounded, a high-rate producer fills only its own queue. Other producers continue to be served in each scheduling round.

## Server lifecycle and graceful shutdown

`RunContext(ctx, addr, logger)` owns the server's lifetime. Cancelling `ctx` starts an orderly shutdown. `Run(addr, logger)` is retained as a wrapper that calls `RunContext` with `context.Background()`.

### Goroutines and contexts

```mermaid
flowchart TD
    CALLER["Caller<br/>RunContext(ctx, addr, logger)"]
    WATCH["Watcher goroutine<br/>closes listener on ctx.Done()"]
    ACC["Accept loop"]
    H["Handler goroutines<br/>one per connection<br/>controlled by handlerCtx"]
    SCH["Scheduler goroutine<br/>controlled by schedulerCtx"]
    SHP["Shipper goroutine (optional)<br/>FAIRGATE_SHIPPER_ENABLED"]
    TRK["Tracking state<br/>handlerWG and activeConns<br/>guarded by connMu"]

    CALLER --> WATCH
    CALLER --> ACC
    CALLER --> SCH
    CALLER -.-> SHP
    ACC -->|"registers and spawns"| H
    ACC --> TRK
    H -->|"deregisters on exit"| TRK
    SCH -.->|"unexpected error:<br/>closes listener"| ACC
```

| Mechanism | Purpose |
|---|---|
| `handlerWG` | Counts active handler goroutines so the server can wait for them. |
| `activeConns` and `connMu` | Track live connections so they can be closed if handlers do not finish in time. |
| `handlerCtx` | Cancelled only when the grace period expires, to interrupt blocked enqueues. |
| `schedulerCtx` | Cancelled after handlers exit, so queued events keep being processed while handlers drain. |
| `schedulerDone` and `schedulerErr` | Signal scheduler exit and report unexpected scheduler failures without blocking. |
| Watcher and `watchStop` | Close the listener on cancellation, and let the watcher exit if the accept loop ends first. |

### Shutdown sequence

```mermaid
flowchart TD
    A["1. Context canceled"] --> B["2. Watcher closes the listener"]
    B --> C["3. Accept loop exits<br/>no new connections"]
    C --> D{"4. Handlers finish within<br/>the 5-second grace period?"}
    D -->|"Yes"| F["5. All handlers have exited"]
    D -->|"No"| E["Close active connections<br/>and cancel handler context"]
    E --> F
    F --> G["6. Cancel scheduler<br/>and wait for it to exit"]
    G --> G2["7. Stop and join the shipper<br/>(if enabled)"]
    G2 --> H["8. Return from RunContext<br/>deferred cleanup closes the store, WAL, and listener"]
```

The ordering is deliberate. The WAL is closed only after every tracked handler has exited, so no handler can append to a closed log, and only after the shipper has stopped, so the shipper never reads from a closed log or writes to a closed store. Closing the WAL then drains queued append requests and synchronizes the active segment before the writer exits. The scheduler keeps running during the grace period so queued events continue to be processed while handlers drain.

### Behavior details

- **Closing a connection interrupts a blocked read.** Handlers waiting in `wire.ReadFrame()` exit once their connection is closed, instead of waiting indefinitely for the client.
- **Accept-loop errors are classified.** A closed listener or a canceled context is treated as normal shutdown. Any other listener error, or a scheduler failure, is returned to the caller.
- **Scheduler failure is fatal.** If the scheduler stops unexpectedly, the listener is closed and the error is returned from `RunContext`.
- **Select ordering.** If a queue send and context cancellation are ready at the same moment, Go's `select` may pick either. A handler may therefore enqueue one more event after cancellation.

### Embedding the server

From within the module, for example in a `cmd/` entry point:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := server.RunContext(ctx, ":9000", logger); err != nil {
    logger.Error("server stopped", "error", err)
}
```

The listen address is chosen by the caller. The Docker Compose stack publishes port `9001`; see [Docker deployment](#docker-deployment).

### Limitations

- The five-second grace period bounds the wait before forced connection closure. It does not guarantee that shutdown completes within five seconds, because a handler blocked in an operation that responds to neither connection closure nor context cancellation could still delay it.
- There is no explicit queue-drain confirmation. The scheduler callback only logs events, so shutdown does not prove that every queued event was delivered downstream. Events appended to the WAL remain recoverable on the next startup.
- When enabled, the server stops and joins the shipper during shutdown before closing the store and WAL. Events not yet shipped at that point stay in the WAL and are picked up from the checkpoint on the next start.

## Delivery semantics

| Property | Current beta |
|---|---|
| Event persisted to the WAL before acknowledgment | Yes |
| Disk synchronization completed (per batch) before acknowledgment | Yes |
| Acknowledgment sent after enqueue | Yes |
| Per-producer memory bounded | Yes, by queue capacity |
| WAL split into segments with rotation | Yes |
| Persisted records scanned across segments on restart | Yes |
| Incomplete record in a non-final segment rejected on restart | Yes |
| Recovered events replayed into queues on restart | Yes |
| WAL write or sync failure surfaced to callers | Yes |
| WAL kept open until all handlers exit during shutdown | Yes |
| Explicit queue-drain confirmation on shutdown | No |
| Shipper and ClickHouse adapter available | Yes |
| Shipper reads only synchronized (durable) WAL records | Yes |
| Shipper resumes from a durable WAL checkpoint after restart | Yes, when `FAIRGATE_SHIPPER_ENABLED=true` |
| Checkpoint written atomically after a successful insert | Yes |
| Failed storage batches retried with backoff | Yes, until success or context cancellation |
| Batch may be inserted again after a crash before checkpoint commit | Yes (duplicates possible) |
| Shipper started by the gateway runtime | Yes, when `FAIRGATE_SHIPPER_ENABLED=true` |
| Shipper stopped and joined before the store and WAL close | Yes |
| Shipper follows new WAL appends continuously | Yes, by polling at the durable end |
| Events are shipped when the shipper is not enabled | No |
| WAL segments reclaimed after shipping | No |
| Acknowledgment implies database storage | No |
| End-to-end exactly-once delivery | No |

Do not treat the current beta as a production-ready, exactly-once event delivery system.

## Configuration

The following values are currently fixed in the server and are expected to become configurable.

| Setting | Current value | Description |
|---|---|---|
| Queue capacity | 256 events per producer | Bound on each producer's queue. |
| DRR quantum | 8 | Maximum events served from a producer queue per visit. |
| Shutdown grace period | 5 seconds | Time active handlers have to finish before connections are forcibly closed. |

The WAL segment size is configurable. Group commit batching is bounded by a maximum request count and a short collection delay; refer to the `wal` package for the current options and defaults.

### Shipper and ClickHouse

The shipper is configured through environment variables.

| Variable | Description |
|---|---|
| `FAIRGATE_SHIPPER_ENABLED` | Set to `true` to start the shipper with the gateway. When unset or not `true`, events stay in the WAL and are not shipped. |
| `FAIRGATE_CHECKPOINT_PATH` | Checkpoint file location. Defaults to `data/checkpoint.json`. |
| `CLICKHOUSE_ADDR` | ClickHouse address. |
| `CLICKHOUSE_DATABASE` | ClickHouse database. |
| `CLICKHOUSE_USER` | ClickHouse user. |
| `CLICKHOUSE_PASSWORD` | ClickHouse password. |

When enabled, the gateway runtime configures the shipper with a batch size of 100 and a one-second WAL polling interval. The retry policy starts at one second, caps at 30 seconds, doubles between attempts, and applies 20% symmetric jitter. These shipper values are currently fixed in the runtime.

## Storage integration

ClickHouse is the initial storage backend. The `Store` interface, ClickHouse adapter, and WAL-backed shipper are implemented and can be started by the gateway runtime with `FAIRGATE_SHIPPER_ENABLED=true`. The remaining storage work is automatic WAL segment reclamation after segments are safely shipped.

### Current building blocks

| Component | Behavior |
|---|---|
| Shipper | Reads from the WAL at the saved position, forms batches up to `BatchSize`, and inserts them through the `Store`. `Run(ctx)` polls for newly durable records and runs until its context is canceled. |
| Retry policy | Retries insertion errors with exponential backoff capped at a maximum delay, optional symmetric jitter, and context-aware cancellation. |
| Checkpoint | JSON segment and byte offset. Missing files mean the beginning of the WAL; writes sync a temporary file and atomically rename it after each successful batch. |
| `Store` interface | `InsertBatch(ctx, events)` and `Close()`. Backends plug in without changes to the shipper. |
| ClickHouse adapter | Official Go client, configurable authentication, `Ping` health check, batched inserts with `PrepareBatch` and `Send`, graceful close. |
| Integration test | Optional and environment-gated. Validates connectivity, batch insertion, and retrieval. |

> [!NOTE]
> **Database delivery guarantees remain limited.** The shipper can resume from its checkpoint and retries insertion failures when enabled. A process crash after a successful insert but before checkpoint commit can result in the batch being inserted again. Exactly-once delivery is not provided, and completed WAL segments are not reclaimed.

### Shipper ordering

Producer acknowledgments remain based on WAL durability. The shipper loads its checkpoint, reads batches from that position up to the durable WAL end, and delivers each batch to the store. It writes the checkpoint only after a batch insert succeeds, then polls for later appends.

```mermaid
sequenceDiagram
    autonumber
    participant W as WAL
    participant SH as Shipper
    participant DB as ClickHouse
    participant CK as Checkpoint

    SH->>CK: Load checkpoint (start of WAL if missing)
    loop While running
        SH->>W: Read next batch from current position
        alt Records available
            W-->>SH: Durable records (up to batch size or WAL end)
            loop Until insert succeeds or context is canceled
                SH->>DB: Insert batch
                alt Insert fails
                    DB-->>SH: Error
                    SH->>SH: Wait with exponential backoff and jitter
                else Insert succeeds
                    DB-->>SH: OK
                end
            end
            SH->>CK: Atomically persist batch end position
        else At durable end
            SH->>SH: Wait for poll interval
        end
    end
```

### Duplicate handling

A crash between a successful insert and the checkpoint write can cause the same batch to be inserted again after restart. The current schema uses `ReplacingMergeTree`, but deduplication is subject to ClickHouse merge behavior and does not make insertion exactly once.

### Schema

The adapter persists events into the existing `fairgate.events` table, which uses `ReplacingMergeTree`, monthly partitioning, and a 30-day TTL. Persisted fields include the producer ID, event timestamp, sequence number, index, event type, and payload. Refer to the schema definition in the repository for the exact columns, partition expression, and TTL clause.

## Roadmap

The roadmap lists planned work in dependency order. Items are subject to change.

```mermaid
flowchart TD
    P1["Phase 1: Core ingestion (implemented)<br/>TCP server, wire protocol, admission control<br/>Per-producer queues, DRR scheduling<br/>Segmented WAL with group commit and ordered recovery<br/>Context-aware lifecycle and graceful shutdown<br/>Store interface, ClickHouse adapter"]
    P2["Phase 2: Durable delivery (in progress)<br/>WAL shipping, checkpoints, retry and runtime wiring implemented<br/>WAL segment reclamation planned"]
    P3["Phase 3: Fairness and overload control (planned)<br/>Producer weights, backlog-aware overload handling<br/>Load shedding"]
    P4["Phase 4: Operability and hardening (planned)<br/>Metrics, profiling, dashboards<br/>Load, reconcile, and chaos tools, protocol hardening"]

    P1 --> P2 --> P3 --> P4

    classDef done stroke-width:3px;
    classDef active stroke-dasharray: 2 2,stroke-width:3px;
    classDef planned stroke-dasharray: 5 5,stroke-width:2px;
    class P1 done;
    class P2 active;
    class P3,P4 planned;
```

A solid border marks implemented work, a finely dashed thick border marks work in progress, and a coarsely dashed border marks planned work.

| Area | Status |
|---|---|
| Ingestion, validation, admission control | Implemented |
| Per-producer queues, DRR scheduling | Implemented |
| WAL append and recovery | Implemented |
| WAL segmentation and group commit | Implemented |
| Cross-segment recovery validation | Implemented |
| Context-aware lifecycle, connection tracking, graceful shutdown | Implemented |
| Cancelable enqueueing and scheduler error propagation | Implemented |
| `Store` interface, ClickHouse adapter | Implemented |
| Optional ClickHouse integration test | Implemented |
| Durable WAL reader and checkpoint-based batch shipping | Implemented |
| Durable checkpoints and retry with backoff | Implemented |
| Shipper runtime wiring and lifecycle | Implemented (opt-in with `FAIRGATE_SHIPPER_ENABLED`) |
| Docker image, Compose stack, Ubuntu install script | Implemented |
| WAL segment reclamation | Planned |
| Producer weights and richer fairness controls | Planned |
| Backlog-aware overload handling | Planned |
| Metrics, profiling, dashboards | Planned |
| Load generator, reconciliation, chaos tooling | Planned |
| Explicit queue-drain confirmation on shutdown | Planned |
| Protocol hardening | Planned |

## Project structure

```text
FairGate/
├── internal/
│   ├── admit/       # Admission control
│   ├── sched/       # Producer queues and DRR scheduler
│   ├── server/      # TCP listener, connection handling, shipper lifecycle
│   ├── shipper/     # WAL reader loop, checkpoints, retry/backoff
│   ├── store/       # Storage interface and ClickHouse adapter
│   ├── wal/         # Segmented WAL, group commit, and recovery
│   └── wire/        # Frame codec, event and ACK types
├── data/            # Local WAL data (runtime; do not commit)
├── Dockerfile
├── docker-compose.yml
├── install-docker-ubuntu.sh
├── go.mod
└── README.md
```

This reflects the current core packages. Additional packages and commands may be added as development continues.

## Getting started

### Requirements

- Go toolchain compatible with the version declared in `go.mod`
- Git
- Docker Engine and Docker Compose plugin for container deployment

### Installation

```bash
git clone https://github.com/PratyushVishal11011/Fairgate.git
cd Fairgate
go mod download
```

### Build and test

```bash
# Build all packages
go build ./...

# Run the test suite
go test ./...

# Run tests with the race detector
go test -race ./...
```

The repository is being developed as a gateway implementation. A stable command-line interface and client SDK are planned. Check the repository's current entry point and configuration before attempting to launch a server, or use the Compose stack described in [Docker deployment](#docker-deployment).

## Docker deployment

The Compose stack starts the FairGate TCP server, ClickHouse, Prometheus, and Grafana. It enables the shipper, waits for ClickHouse's health check, and persists the WAL and checkpoint in the `fairgate_wal` volume.

On an Ubuntu VM, install Docker Engine and the Compose plugin with the included script, then start the stack:

```bash
./install-docker-ubuntu.sh
sudo docker compose up --build -d
sudo docker compose ps
sudo docker compose logs -f fairgate clickhouse
```

For access from outside the host, edit the `fairgate` port mapping in `docker-compose.yml` from the loopback-only binding to:

```yaml
ports:
  - "9001:9001"
```

Recreate the service after changing the mapping:

```bash
sudo docker compose up --build -d --force-recreate fairgate
```

Allow inbound TCP traffic on port `9001` through the host firewall, restricted to the client IP range that needs access. Connect to the host's reachable IP or DNS name on port `9001`; this is FairGate's length-prefixed TCP protocol, not an HTTP endpoint.

The Compose file uses development ClickHouse credentials. Replace them before exposing a non-development deployment. `docker compose down` keeps the named volumes; `docker compose down -v` removes them, including the WAL, checkpoint, and database data.

## Development status and limitations

FairGate is an experimental beta. Interfaces, protocol details, configuration, and storage semantics may change. The project has tests for implemented components, but its end-to-end durability and fairness goals still require broader load, crash-recovery, and downstream-outage validation.

Known limitations of the current beta:

- The WAL-backed shipper starts only when `FAIRGATE_SHIPPER_ENABLED=true`; Compose enables it. Without that setting, events remain in the WAL and are not shipped to ClickHouse.
- A crash after a successful database insert but before checkpoint commit can replay that batch. Storage therefore needs to tolerate duplicates; end-to-end exactly-once delivery is not guaranteed.
- The ClickHouse integration test is optional and runs only when the environment is configured for it.
- The WAL is segmented with group commit, but completed segments are not reclaimed automatically. Checkpoints exist, but reclamation is still unimplemented, so the WAL grows until segments are removed by other means.
- Queue capacity, DRR quantum, the shutdown grace period, and the shipper's batch size, poll interval, and retry policy are fixed in the code and not yet configurable.
- All producers receive equal scheduling treatment. Weights are not yet supported.
- Enqueueing blocks on a full queue (cancelable during shutdown), and no load-shedding policy exists yet.
- A WAL write, rotation, or sync failure is fatal: later appends are rejected until the WAL is reopened.
- Startup recovery fails on an incomplete record in any non-final segment, rather than repairing it.
- Shutdown has no explicit queue-drain confirmation, and a handler stuck in an operation that ignores connection closure and context cancellation can delay it beyond the grace period.
- The Compose file ships with development credentials and, by default, a loopback-only FairGate port; exposing it needs the changes described in [Docker deployment](#docker-deployment).
- FairGate does not yet provide its own metrics, profiling endpoints, or dashboards.

## Contributing

Issues, bug reports, design discussions, and pull requests are welcome. For substantial changes, open an issue first to discuss the proposed behavior or interface.

When contributing:

1. Keep core ingestion and scheduling independent of business-specific logic.
2. Prefer bounded memory and explicit backpressure.
3. Add tests for new behavior and failure paths.
4. Run `go test ./...` and `go test -race ./...` before submitting a pull request.

## License

FairGate is released under the [MIT License](LICENSE).