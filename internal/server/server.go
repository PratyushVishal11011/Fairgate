package server

import (
	"FairGate/internal/admit"
	"FairGate/internal/config"
	"FairGate/internal/deduplication"
	"FairGate/internal/observability"
	"FairGate/internal/overload"
	"FairGate/internal/sched"
	"FairGate/internal/shipper"
	"FairGate/internal/store"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Version is reported in logs and by fairgate_build_info.
const Version = "1.0.0"

const (
	ackAccepted = "accepted"
	ackRejected = "rejected" // refused by the gateway, retryable after backoff
	ackError    = "error"    // server-side failure, retryable
	ackInvalid  = "invalid"  // malformed request, do not retry unchanged

	ackStorageUnavailable = "storage_unavailable" // the WAL cannot make events durable right now
)

func Run(addr string, logger *slog.Logger) error {
	return RunContext(context.Background(), addr, logger)
}

// RunContext runs the gateway on addr with the default settings and the supported environment overrides.
func RunContext(ctx context.Context, addr string, logger *slog.Logger) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	cfg.Server.ListenAddr = addr

	return RunConfig(ctx, cfg, logger)
}

// RunConfig runs the gateway with a resolved configuration until ctx is canceled.
func RunConfig(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	return runContext(ctx, cfg, logger, func() (store.Store, error) {
		return store.NewClickHouseStore(
			cfg.ClickHouse.Addr,
			cfg.ClickHouse.Database,
			cfg.ClickHouse.Username,
			cfg.ClickHouse.Password,
		)
	})
}

func runContext(ctx context.Context, cfg config.Config, logger *slog.Logger, newStore func() (store.Store, error)) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	addr := cfg.Server.ListenAddr
	labelled := make([]string, 0, len(cfg.Fairness.Producers))
	for producerId := range cfg.Fairness.Producers {
		labelled = append(labelled, producerId)
	}
	metrics := observability.NewWithOptions(observability.Options{
		Version:   Version,
		Producers: labelled,
	})
	metricsServer := &http.Server{Addr: cfg.Server.MetricsAddr, Handler: metrics.Handler()}
	metricsListener, err := net.Listen("tcp", metricsServer.Addr)
	if err != nil {
		return fmt.Errorf("listen for metrics: %w", err)
	}
	metricsErr := make(chan error, 1)
	go func() {
		if err := metricsServer.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsErr <- err
		}
	}()
	defer func() { _ = metricsServer.Close() }()

	if cfg.Profiling.Enabled {
		runtime.SetBlockProfileRate(cfg.Profiling.BlockProfileRate)
		runtime.SetMutexProfileFraction(cfg.Profiling.MutexProfileFraction)

		profilingServer := &http.Server{Addr: cfg.Profiling.Addr, Handler: observability.ProfilingHandler()}
		profilingListener, err := net.Listen("tcp", profilingServer.Addr)
		if err != nil {
			return fmt.Errorf("listen for profiling: %w", err)
		}
		go func() {
			if err := profilingServer.Serve(profilingListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("profiling server stopped", "error", err)
			}
		}()
		defer func() { _ = profilingServer.Close() }()
		logger.Info("profiling enabled", "address", profilingListener.Addr().String())
	}

	// Open a TCP listener on the specified address.
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	walPath := cfg.WAL.Path

	if err := os.MkdirAll(filepath.Dir(walPath), 0700); err != nil {
		return err
	}

	// The committed checkpoint tells recovery which damaged records are already in the store.
	checkpoint := shipper.NewCheckpointStore(cfg.Shipper.CheckpointPath)
	shipped, err := checkpoint.Load()
	if err != nil {
		return fmt.Errorf("load shipper checkpoint: %w", err)
	}

	recovery, err := wal.RecoverWithOptions(walPath, wal.RecoverOptions{
		Shipped:      wal.Position{SegmentId: shipped.Segment, Offset: shipped.Offset},
		OnCorruption: wal.CorruptionPolicy(cfg.WAL.OnCorruption),
	})
	if err != nil {
		return err
	}
	events := recovery.Events
	metrics.RecordWALDamage(recovery.Damage)
	for _, damage := range recovery.Damage {
		if damage.Action == wal.DamageQuarantined {
			logger.Error("Quarantined a damaged WAL segment; records in the damaged bytes are lost",
				"segment", damage.SegmentId,
				"start", damage.Region.Start,
				"end", damage.Region.End,
				"reason", damage.Region.Reason,
				"copy", damage.QuarantinePath,
			)
			continue
		}
		logger.Warn("Recovered past a damaged WAL region",
			"segment", damage.SegmentId,
			"start", damage.Region.Start,
			"end", damage.Region.End,
			"reason", damage.Region.Reason,
			"action", damage.Action,
		)
	}

	logger.Info("WAL recovery complete",
		"recovered_events", len(events),
		"salvaged_events", len(recovery.Salvaged),
		"damaged_regions", len(recovery.Damage),
	)

	//create a deduplication manager instance
	dedupManager, err := deduplication.NewManager(events)
	if err != nil {
		return fmt.Errorf("initialize event deduplication: %w", err)
	}

	walLog, err := wal.OpenWithSegmentSize(walPath, cfg.WAL.SegmentSize)
	if err != nil {
		return err
	}
	defer walLog.Close()
	walLog.SetObserver(metrics)
	metrics.RegisterWAL(walLog)
	metrics.SetReadiness(walLog.Health)

	// Records cut out of a quarantined segment are written again, so they are still shipped.
	for _, event := range recovery.Salvaged {
		if err := walLog.Append(event); err != nil {
			return fmt.Errorf("append salvaged WAL record: %w", err)
		}
	}

	// A WAL that stays degraded for wal.degraded_exit_after stops the gateway, so a supervisor
	// restarts it; startup recovery is the best-tested path back to a healthy WAL.
	walErr := make(chan error, 1)
	walWatchCtx, stopWALWatch := context.WithCancel(context.Background())
	defer stopWALWatch()
	if cfg.WAL.DegradedExitAfter > 0 {
		go watchWAL(walWatchCtx, logger, walLog.Health, cfg.WAL.DegradedExitAfter, time.Second, func(err error) {
			select {
			case walErr <- err:
			default:
			}
			_ = listener.Close()
		})
	}

	shipperErr := make(chan error, 1)
	shipperDone := make(chan struct{})
	cancelShipper := func() {}

	storage, err := newStore()
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}
	defer storage.Close()

	retry := cfg.Shipper.Retry
	backoff := shipper.NewBackoff(
		retry.InitialDelay,
		retry.MaxDelay,
		retry.Multiplier,
		retry.Jitter,
		time.Now().UnixNano(),
	)

	eventShipper, err := shipper.New(
		storage,
		walLog,
		checkpoint,
		backoff,
		shipper.Config{
			BatchSize:    cfg.Shipper.BatchSize,
			PollInterval: cfg.Shipper.PollInterval,
			Observer:     metrics,
		},
	)
	if err != nil {
		return fmt.Errorf("create shipper: %w", err)
	}
	metrics.RegisterShipper(eventShipper, walLog)

	//durable WAL bytes the shipper has not committed to the store yet.
	shipperLag := func() (int64, error) {
		return walLog.BytesAfter(eventShipper.Committed())
	}

	shipperCtx, stopShipper := context.WithCancel(context.Background())
	cancelShipper = stopShipper

	go func() {
		defer close(shipperDone)

		if err := eventShipper.Run(shipperCtx); err != nil &&
			!errors.Is(err, context.Canceled) {
			shipperErr <- err
			_ = listener.Close()
			logger.Error("WAL shipper stopped", "error", err)
		}
	}()

	logger.Info("WAL shipper started")

	// Stop and join the shipper before the deferred store and WAL cleanup.
	defer func() {
		cancelShipper()
		<-shipperDone
	}()

	logger.Info("server listening on", "address", addr)

	// Per-producer fairness overrides replace the defaults at admission, buffering, and scheduling.
	limits := make(map[string]admit.Limit, len(cfg.Fairness.Producers))
	for producerId, producer := range cfg.Fairness.Producers {
		if producer.Rate > 0 || producer.Burst > 0 {
			limits[producerId] = admit.Limit{Rate: producer.Rate, Burst: producer.Burst}
		}
	}

	admission, err := admit.NewManagerWithLimits(cfg.Admission.Rate, cfg.Admission.Burst, limits)
	if err != nil {
		return err
	}

	// Create one shared queue manager with a bounded buffer per producer.
	queues, err := sched.NewQueuesWithCapacities(cfg.Queues.Capacity, cfg.Fairness.QueueCapacities())
	if err != nil {
		return err
	}
	metrics.RegisterQueues(queues)

	// Create a context to control the scheduler's lifecycle.
	schedulerCtx, cancelScheduler := context.WithCancel(context.Background())
	defer cancelScheduler()

	// Create a scheduler that serves each producer quantum * weight events per round.
	scheduler := sched.NewWeightedDRRScheduler(
		cfg.Scheduler.Quantum,
		cfg.Fairness.DefaultWeight,
		cfg.Fairness.Weights(),
	)

	// Signal that the scheduler goroutine has finished executing.
	schedulerDone := make(chan struct{})

	// Report scheduler errors.
	schedulerErr := make(chan error, 1)

	go func() {
		defer close(schedulerDone)

		err := scheduler.Run(schedulerCtx, queues, func(event wire.Event) error {
			metrics.EventProcessed(event.ProducerId)
			logger.Info("Event processed by DRR",
				"producer_id", event.ProducerId,
				"seq", event.Seq,
			)
			return nil
		})

		if err == nil && ctx.Err() == nil {
			err = errors.New("scheduler stopped unexpectedly")
		}

		if err != nil && !errors.Is(err, context.Canceled) {
			// Make sure reporting a scheduler error does not block.
			select {
			case schedulerErr <- err:
			default:
			}

			// Stop accepting new connections and wake the accept loop.
			_ = listener.Close()
			logger.Error("DRR scheduler stopped", "error", err)
		}
	}()

	// Restore previous events.
	for _, event := range events {
		if !queues.Enqueue(event) {
			return fmt.Errorf(
				"queue enqueue failed: producer_id = %s, event_sequence = %d",
				event.ProducerId,
				event.Seq,
			)
		}
	}

	logger.Info("WAL events restored",
		"restored_events", len(events),
	)

	// Refuse new events before the WAL append once producer queues or delivery fall behind.
	overloadCtl, err := overload.New(overload.Config{
		QueueHighWater:     cfg.Overload.QueueHighWater,
		MaxQueuedEvents:    cfg.Overload.MaxQueuedEvents,
		MaxShipperLagBytes: cfg.Overload.MaxShipperLagBytes,
		ResumeRatio:        cfg.Overload.ResumeRatio,
		SampleInterval:     cfg.Overload.SampleInterval,
		DefaultWeight:      cfg.Fairness.DefaultWeight,
		Weights:            cfg.Fairness.Weights(),
		Logger:             logger,
	}, queues, shipperLag)
	if err != nil {
		return err
	}
	metrics.RegisterOverload(overloadCtl)

	// Sample once so a restart with a large backlog refuses events from the first connection.
	overloadCtl.Sample()
	overloadCtx, stopOverload := context.WithCancel(context.Background())
	overloadDone := make(chan struct{})
	go func() {
		defer close(overloadDone)
		overloadCtl.Run(overloadCtx)
	}()
	// Stop sampling before the deferred WAL cleanup.
	defer func() {
		stopOverload()
		<-overloadDone
	}()

	protocol := protocolLimitsFrom(cfg.Server)

	// The handler context is canceled only if the grace period expires.
	handlerCtx, cancelHandler := context.WithCancel(context.Background())
	defer cancelHandler()

	//handlerWG to keep track of how many client-handler goroutines are still running
	var handlerWG sync.WaitGroup
	//mutex protects shared data from concurrent access.
	//prevents data race or a runtime errors.
	var connMu sync.Mutex
	//creates a map that stores all currently active client connections
	activeConns := make(map[net.Conn]struct{})

	//closes all currently active client connections.
	closeActiveConnections := func() {
		//Locks the mutex before accessing activeConns
		connMu.Lock()
		defer connMu.Unlock()

		//Closes each connection.
		//This interrupts network operations such as a handler blocked on wire.ReadFrame(conn).
		for conn := range activeConns {
			_ = conn.Close()
		}
	}

	//Creates an unbuffered channel used to tell the watcher goroutine that it should stop monitoring the context.
	//The struct{} type is empty, so the channel is used only as a signal, without carrying data.
	//Closing this channel signals the watcher that it can exit.
	watchStop := make(chan struct{})

	//This starts a separate goroutine that waits for one of two events.
	go func() {
		select {
		//ctx.Done() is a channel that is closed when the parent context is canceled, expires, or reaches its deadline.
		case <-ctx.Done():
			_ = listener.Close()
		//allows the watcher goroutine to exit when the accept loop has already stopped and the server no longer needs the watcher.
		//no additional code in this case because returning from the anonymous function is sufficient to terminate the goroutine.
		case <-watchStop:
		}
	}()

	var runErr error

	for {
		// Wait for a client to connect.
		conn, err := listener.Accept()

		if err != nil {
			// Check whether the scheduler caused the listener to close.
			select {
			case schedulerError := <-schedulerErr:
				runErr = fmt.Errorf("scheduler error: %v", schedulerError)
			case walError := <-walErr:
				runErr = fmt.Errorf("WAL unavailable for %s: %w", cfg.WAL.DegradedExitAfter, walError)
			case shippingError := <-shipperErr:
				runErr = fmt.Errorf("shipper error: %w", shippingError)
			default:
				// A listener error is unexpected only if the parent
				// context has not been canceled.
				if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					runErr = err
				}
				select {
				case metricsError := <-metricsErr:
					runErr = fmt.Errorf("metrics server: %w", metricsError)
				default:
				}
			}
			break
		}

		//Once a client connects successfully, its connection is added to activeConns.
		//lock the mutex to protect the map.
		connMu.Lock()
		//register the connection.
		activeConns[conn] = struct{}{}
		connMu.Unlock()
		metrics.ConnectionOpened()

		//Increments the WaitGroup counter
		//We do this as we're starting a new goroutine
		handlerWG.Add(1)

		//A new goroutine is started for each client, allowing multiple clients to be handled concurrently
		go func(conn net.Conn) {
			//this ensures the WaitGroup counter is decremented when the goroutine exits,
			//works regardless of how goroutine exits
			defer handlerWG.Done()

			//deferred function to remove the connection from active connections when the function exits
			defer func() {
				metrics.ConnectionClosed()
				connMu.Lock()
				delete(activeConns, conn)
				connMu.Unlock()
			}()

			//added deduplication manager
			handleConn(
				handlerCtx,
				conn,
				logger,
				admission,
				queues,
				walLog,
				metrics,
				dedupManager,
				overloadCtl,
				protocol,
			)
		}(conn)
	}

	// Stop the listener watcher. No more connections will be accepted.
	close(watchStop)
	_ = listener.Close()

	// Allow active handlers to finish while the scheduler continues
	// i.e. draining the queues.
	handlersDone := make(chan struct{})

	go func() {
		handlerWG.Wait()
		close(handlersDone)
	}()

	timer := time.NewTimer(cfg.Server.ShutdownGracePeriod)

	select {
	case <-handlersDone:
		timer.Stop()

	case <-timer.C:
		logger.Warn("Shutdown grace period expired; closing active connections")

		// Closing connections interrupts blocked network reads.
		closeActiveConnections()

		// Context-aware enqueue lets blocked handlers exit too.
		cancelHandler()

		// Wait for all handlers to exit before the WAL is closed.
		<-handlersDone
	}

	// All handlers have stopped, so nothing new reaches the queues or the WAL.
	report := drainPipeline(cfg.Server.DrainTimeout, queues, schedulerDone, cancelScheduler, eventShipper, shipperDone, shipperLag)
	report.log(logger)

	return runErr
}

// drainPipeline confirms the pipeline is empty before the scheduler and shipper stop.
// It waits for the scheduler to empty every producer queue, stops the scheduler, then wakes the
// shipper until its checkpoint reaches the durable WAL end. Both stages share one timeout.
func drainPipeline(
	timeout time.Duration,
	queues *sched.Queues,
	schedulerDone <-chan struct{},
	cancelScheduler context.CancelFunc,
	eventShipper interface{ Wake() },
	shipperDone <-chan struct{},
	shipperLag func() (int64, error),
) drainReport {
	start := time.Now()
	deadline := start.Add(timeout)
	unshipped := func() int64 {
		lag, err := shipperLag()
		if err != nil {
			return -1
		}
		return lag
	}

	report := drainReport{
		queue:            drainSkipped,
		shipper:          drainSkipped,
		queuedAtStart:    queues.Len(),
		unshippedAtStart: unshipped(),
	}

	if timeout > 0 {
		report.queue = waitDrained(deadline, schedulerDone, func() bool {
			return queues.Len() == 0
		})
	}
	cancelScheduler()
	<-schedulerDone
	report.queuedLeft = queues.Len()

	if timeout > 0 {
		report.shipper = waitDrained(deadline, shipperDone, func() bool {
			eventShipper.Wake()
			return unshipped() == 0
		})
	}
	report.unshippedLeft = unshipped()
	report.elapsed = time.Since(start)
	return report
}

func handleConn(
	ctx context.Context,
	conn net.Conn,
	logger *slog.Logger,
	admission *admit.Manager,
	queues *sched.Queues,
	walLog *wal.WAL,
	metrics *observability.Metrics,
	dedupManager *deduplication.Manager,
	overloadCtl *overload.Controller,
	limits protocolLimits,
) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	logger.Info("Client connected", "remote_addr", remoteAddr)

	acks := &ackWriter{conn: conn, timeout: limits.writeTimeout, metrics: metrics}
	frames := newFrameReader(conn, limits, acks, logger, metrics)

	for {
		//next answers and skips frames that are oversized or of another type,
		//and reports false once the connection cannot continue.
		frame, ok := frames.next()
		if !ok {
			return
		}

		received := time.Now()

		event, err := wire.DecodeEvent(frame.Payload)
		if err != nil {
			metrics.EventInvalid()
			if err := acks.send(wire.Ack{
				Status:  ackInvalid,
				Message: "invalid_payload",
			}); err != nil {
				logger.Warn("Failed to send NACK", "error", err)
				return
			}
			continue
		}

		if !admission.Allow(event.ProducerId) {
			metrics.EventRejected(event.ProducerId)
			if err := acks.send(wire.Ack{
				ProducerId: event.ProducerId,
				Seq:        event.Seq,
				Idx:        event.Idx,
				Status:     ackRejected,
				Message:    "rate_limited",
			}); err != nil {
				logger.Warn("Failed to send NACK", "error", err)
				return
			}
			continue
		}

		// The overload check runs inside the persist step, so an identical retry of an event
		// that is already durable is still acknowledged while the gateway is backlogged.
		duplicate, err := dedupManager.PersistIfNew(event, func() error {
			if overloadCtl != nil {
				if err := overloadCtl.Check(event.ProducerId); err != nil {
					return err
				}
			}
			appendStart := time.Now()
			err := walLog.Append(event)
			metrics.ObserveWALAppend(time.Since(appendStart))
			return err
		})
		if err != nil {
			//a backlogged gateway refuses the event before the WAL append; the producer retries after backoff.
			var rejection *overload.Rejection
			if errors.As(err, &rejection) {
				metrics.EventOverloaded(event.ProducerId, rejection.Reason)
				if err := acks.send(wire.Ack{
					ProducerId: event.ProducerId,
					Seq:        event.Seq,
					Idx:        event.Idx,
					Status:     ackRejected,
					Message:    rejection.Reason,
				}); err != nil {
					logger.Warn("Failed to send NACK", "error", err)
					return
				}
				continue
			}

			//a degraded WAL persisted nothing; the producer retries after backoff, possibly on another gateway.
			if errors.Is(err, wal.ErrUnavailable) {
				metrics.WALAppendError(event.ProducerId)
				if err := acks.send(wire.Ack{
					ProducerId: event.ProducerId,
					Seq:        event.Seq,
					Idx:        event.Idx,
					Status:     ackRejected,
					Message:    ackStorageUnavailable,
				}); err != nil {
					logger.Warn("Failed to send NACK", "error", err)
					return
				}
				continue
			}

			if errors.Is(err, deduplication.ErrConflict) ||
				errors.Is(err, deduplication.ErrInvalid) {
				metrics.EventConflict(event.ProducerId)
				if err := acks.send(wire.Ack{
					ProducerId: event.ProducerId,
					Seq:        event.Seq,
					Idx:        event.Idx,
					Status:     ackInvalid,
					Message:    "event_identity_conflict",
				}); err != nil {
					logger.Warn("Failed to send NACK", "error", err)
					return
				}
				continue
			}

			//an event over the WAL record limit is a client error; the WAL is still healthy,
			//so reject the event and keep the connection open.
			if errors.Is(err, wal.ErrRecordTooLarge) {
				metrics.EventInvalid()
				if err := acks.send(wire.Ack{
					ProducerId: event.ProducerId,
					Seq:        event.Seq,
					Idx:        event.Idx,
					Status:     ackInvalid,
					Message:    "payload_too_large",
				}); err != nil {
					logger.Warn("Failed to send NACK", "error", err)
					return
				}
				continue
			}

			metrics.WALAppendError(event.ProducerId)
			logger.Error("Failed to append to WAL",
				"producer_id", event.ProducerId,
				"seq", event.Seq,
				"error", err,
			)
			if err := acks.send(wire.Ack{
				ProducerId: event.ProducerId,
				Seq:        event.Seq,
				Idx:        event.Idx,
				Status:     ackError,
				Message:    "wal_append_failed",
			}); err != nil {
				logger.Warn("Failed to send NACK", "error", err)
			}
			return
		}

		if duplicate {
			if err := acks.send(wire.Ack{
				ProducerId: event.ProducerId,
				Seq:        event.Seq,
				Idx:        event.Idx,
				Status:     ackAccepted,
				Message:    "duplicate_event_already_persisted",
			}); err != nil {
				logger.Warn("Failed to send duplicate ACK", "error", err)
				return
			}
			metrics.EventDuplicate(event.ProducerId)
			metrics.ObserveIngest(time.Since(received))
			continue
		}

		if !queues.EnqueueContext(ctx, event) {
			if ctx.Err() != nil {
				logger.Debug("Handler canceled while enqueueing event",
					"producer_id", event.ProducerId,
					"seq", event.Seq,
				)
				return
			}

			logger.Warn("Event persisted but producer queue is full",
				"producer_id", event.ProducerId,
				"seq", event.Seq,
			)
			continue
		}

		if err := acks.send(wire.Ack{
			ProducerId: event.ProducerId,
			Seq:        event.Seq,
			Idx:        event.Idx,
			Status:     ackAccepted,
			Message:    "Event successfully enqueued",
		}); err != nil {
			logger.Warn("Failed to send ACK", "error", err)
			return
		}

		metrics.EventAccepted(event.ProducerId)
		metrics.ObserveIngest(time.Since(received))
		logger.Info("Event admitted",
			"remote_addr", remoteAddr,
			"producer_id", event.ProducerId,
			"seq", event.Seq,
			"event_type", event.EventType,
		)
	}
}
