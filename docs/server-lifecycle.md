[FairGate](../README.md) › [Docs](README.md) › Server lifecycle and graceful shutdown

# Server lifecycle and graceful shutdown

`RunContext(ctx, addr, logger)` owns the server's lifetime. Cancelling `ctx` starts an orderly shutdown. `Run(addr, logger)` is retained as a wrapper that calls `RunContext` with `context.Background()`.

`RunContext` always constructs the ClickHouse store and starts the shipper. Startup is split so that the public entry point supplies the ClickHouse store to an inner startup path that accepts any `Store`; the graceful-shutdown tests use that path with a no-op `Store` and a temporary checkpoint path, so they do not need a live ClickHouse service.

## Goroutines and contexts

```mermaid
flowchart TD
    CALLER["Caller<br/>RunContext(ctx, addr, logger)"]
    WATCH["Watcher goroutine<br/>closes listener on ctx.Done()"]
    ACC["Accept loop"]
    H["Handler goroutines<br/>one per connection<br/>controlled by handlerCtx"]
    SCH["Scheduler goroutine<br/>controlled by schedulerCtx"]
    SHP["Shipper goroutine<br/>always started, joined on shutdown"]
    TRK["Tracking state<br/>handlerWG and activeConns<br/>guarded by connMu"]

    CALLER --> WATCH
    CALLER --> ACC
    CALLER --> SCH
    CALLER --> SHP
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

## Shutdown sequence

```mermaid
flowchart TD
    A["1. Context canceled"] --> B["2. Watcher closes the listener"]
    B --> C["3. Accept loop exits<br/>no new connections"]
    C --> D{"4. Handlers finish within<br/>the 5-second grace period?"}
    D -->|"Yes"| F["5. All handlers have exited"]
    D -->|"No"| E["Close active connections<br/>and cancel handler context"]
    E --> F
    F --> G["6. Cancel scheduler<br/>and wait for it to exit"]
    G --> G2["7. Cancel and join the shipper"]
    G2 --> H["8. Return from RunContext<br/>deferred cleanup closes the store, WAL, and listener"]
```

The ordering is deliberate. The WAL is closed only after every tracked handler has exited, so no handler can append to a closed log, and only after the shipper has stopped, so the shipper never reads from or reclaims in a closed log or writes to a closed store. Closing the WAL then drains queued append requests and synchronizes the active segment before the writer exits. The scheduler keeps running during the grace period so queued events continue to be processed while handlers drain.

## Behavior details

- **Closing a connection interrupts a blocked read.** Handlers waiting in `wire.ReadFrame()` exit once their connection is closed, instead of waiting indefinitely for the client.
- **Accept-loop errors are classified.** A closed listener or a canceled context is treated as normal shutdown. Any other listener error, or a scheduler failure, is returned to the caller.
- **Scheduler failure is fatal.** If the scheduler stops unexpectedly, the listener is closed and the error is returned from `RunContext`.
- **Select ordering.** If a queue send and context cancellation are ready at the same moment, Go's `select` may pick either. A handler may therefore enqueue one more event after cancellation.

## Embedding the server

From within the module, for example in a `cmd/` entry point:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

cfg, err := config.Load("/etc/fairgate/fairgate.yaml") // "" for defaults plus environment overrides
if err != nil {
logger.Error("invalid configuration", "error", err)
return
}

if err := server.RunConfig(ctx, cfg, logger); err != nil {
logger.Error("server stopped", "error", err)
}
```

`RunConfig` takes every setting from the resolved `config.Config`; see [Configuration](configuration.md). `RunContext(ctx, addr, logger)` is still available: it uses the defaults and environment overrides with the given listen address. In both cases the shipper always starts with a ClickHouse store built from the `clickhouse` settings. The Docker Compose stack publishes port `9001`; see [Docker deployment](deployment.md).

## Limitations

- The five-second grace period bounds the wait before forced connection closure. It does not guarantee that shutdown completes within five seconds, because a handler blocked in an operation that responds to neither connection closure nor context cancellation could still delay it.
- There is no explicit queue-drain confirmation. The scheduler callback only logs events, so shutdown does not prove that every queued event was delivered downstream. Events appended to the WAL remain recoverable on the next startup.
- An event that was appended to the WAL but not yet enqueued when shutdown canceled its handler is not processed by the live scheduler. It is recovered on the next startup, and in the meantime a retry of it is acknowledged as a duplicate.
- The server always stops and joins the shipper during shutdown before closing the store and WAL. Events not yet shipped at that point stay in the WAL and are picked up from the checkpoint on the next start.
