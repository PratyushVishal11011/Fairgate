package server

import (
	"FairGate/internal/admit"
	"FairGate/internal/observability"
	"FairGate/internal/sched"
	"FairGate/internal/shipper"
	"FairGate/internal/store"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	ackAccepted         = "accepted"
	ackRejected         = "rejected" // refused by the gateway, retryable after backoff
	ackError            = "error"    // server-side failure, retryable
	ackInvalid          = "invalid"  // malformed request, do not retry unchanged
	shutdownGracePeriod = 5 * time.Second
)

func sendAck(conn net.Conn, ack wire.Ack) error {
	payload, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return wire.WriteFrame(conn, wire.Frame{
		Type:    wire.FrameTypeAck,
		Flags:   0,
		Payload: payload,
	})
}

func Run(addr string, logger *slog.Logger) error {
	return RunContext(context.Background(), addr, logger)
}

func RunContext(ctx context.Context, addr string, logger *slog.Logger) error {
	return runContext(ctx, addr, logger, func() (store.Store, error) {
		return store.NewClickHouseStore(
			[]string{envOrDefault("CLICKHOUSE_ADDR", "localhost:9000")},
			envOrDefault("CLICKHOUSE_DATABASE", "fairgate"),
			envOrDefault("CLICKHOUSE_USER", "fairgate"),
			envOrDefault("CLICKHOUSE_PASSWORD", "fairgate_dev_password"),
		)
	})
}

func runContext(ctx context.Context, addr string, logger *slog.Logger, newStore func() (store.Store, error)) error {
	metrics := observability.New()
	metricsServer := &http.Server{Addr: envOrDefault("FAIRGATE_METRICS_ADDR", ":9100"), Handler: metrics.Handler()}
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

	// Open a TCP listener on the specified address.
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	if err := os.MkdirAll("data", 0700); err != nil {
		return err
	}

	walPath := "data/fairgate.wal"

	events, err := wal.Recover(walPath)
	if err != nil {
		return err
	}

	logger.Info("WAL recovery complete",
		"recovered_events", len(events),
	)

	walLog, err := wal.Open(walPath)
	if err != nil {
		return err
	}
	defer walLog.Close()

	shipperErr := make(chan error, 1)
	shipperDone := make(chan struct{})
	cancelShipper := func() {}

	storage, err := newStore()
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}
	defer storage.Close()

	checkpoint := shipper.NewCheckpointStore(
		envOrDefault("FAIRGATE_CHECKPOINT_PATH", "data/checkpoint.json"),
	)
	backoff := shipper.NewBackoff(
		time.Second,
		30*time.Second,
		2,
		0.2,
		time.Now().UnixNano(),
	)

	eventShipper, err := shipper.New(
		storage,
		walLog,
		checkpoint,
		backoff,
		shipper.Config{
			BatchSize:    200,
			PollInterval: time.Second,
		},
	)
	if err != nil {
		return fmt.Errorf("create shipper: %w", err)
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

	admission, err := admit.NewManager(500, 1000)
	if err != nil {
		return err
	}

	// Create one shared queue manager.
	// For testing, each producer can buffer up to 512 events.
	queues, err := sched.NewQueues(512)
	if err != nil {
		return err
	}

	// Create a context to control the scheduler's lifecycle.
	schedulerCtx, cancelScheduler := context.WithCancel(context.Background())
	defer cancelScheduler()

	// Create a scheduler object with a quantum of 8 events.
	scheduler := sched.NewDRRScheduler(8)

	// Signal that the scheduler goroutine has finished executing.
	schedulerDone := make(chan struct{})

	// Report scheduler errors.
	schedulerErr := make(chan error, 1)

	go func() {
		defer close(schedulerDone)

		err := scheduler.Run(schedulerCtx, queues, func(event wire.Event) error {
			metrics.EventProcessed()
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

			handleConnWithMetrics(
				handlerCtx,
				conn,
				logger,
				admission,
				queues,
				walLog,
				metrics,
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

	timer := time.NewTimer(shutdownGracePeriod)

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

	// All handlers have stopped. The scheduler can now exit.
	cancelScheduler()
	<-schedulerDone

	return runErr
}

func handleConn(
	conn net.Conn,
	logger *slog.Logger,
	admission *admit.Manager,
	queues *sched.Queues,
	walLog *wal.WAL,
) {
	handleConnContext(
		context.Background(),
		conn,
		logger,
		admission,
		queues,
		walLog,
	)
}

func handleConnContext(
	ctx context.Context,
	conn net.Conn,
	logger *slog.Logger,
	admission *admit.Manager,
	queues *sched.Queues,
	walLog *wal.WAL,
) {
	handleConnWithMetrics(ctx, conn, logger, admission, queues, walLog, observability.New())
}

func handleConnWithMetrics(
	ctx context.Context,
	conn net.Conn,
	logger *slog.Logger,
	admission *admit.Manager,
	queues *sched.Queues,
	walLog *wal.WAL,
	metrics *observability.Metrics,
) {
	// Close the connection when the client exits.
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	logger.Info("Client connected", "remoteAddr", remoteAddr)

	for {
		// Convert bytes into frames.
		frame, err := wire.ReadFrame(conn)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				logger.Warn(
					"error reading frame",
					"remoteAddr", remoteAddr,
					"error", err,
				)
			}
			return
		}

		logger.Info(
			"Frame received",
			"remoteAddr", remoteAddr,
			"type", frame.Type,
			"payload_bytes", len(frame.Payload),
		)

		// Type 1 represents an event frame.
		// Unsupported frame types are logged and skipped.
		if frame.Type != 1 {
			logger.Warn(
				"Unsupported Frame Type",
				"Remote Address", remoteAddr,
				"type", frame.Type,
			)
			continue
		}

		// Decode the event and check for errors in the payload.
		event, err := wire.DecodeEvent(frame.Payload)
		if err != nil {
			metrics.EventInvalid()
			logger.Warn("Invalid event payload", "remote_addr", remoteAddr, "error", err)
			if werr := sendAck(conn, wire.Ack{
				Status:  ackInvalid,
				Message: "invalid_payload",
			}); werr != nil {
				logger.Warn("Failed to send NACK", "remote_addr", remoteAddr, "error", werr)
				return
			}
			continue
		}

		// Log the event.
		logger.Info(
			"Event received",
			"remote_addr", remoteAddr,
			"producer_id", event.ProducerId,
			"event_time", event.EventTime,
			"seq", event.Seq,
			"idx", event.Idx,
			"event_type", event.EventType,
		)

		// If the event is not allowed: log, send ack and continue.
		if !admission.Allow(event.ProducerId) {
			metrics.EventRejected()
			logger.Warn("Event rejected by admission control",
				"remote_addr", remoteAddr, "producer_id", event.ProducerId, "seq", event.Seq)
			if werr := sendAck(conn, wire.Ack{
				ProducerId: event.ProducerId,
				Seq:        event.Seq,
				Idx:        event.Idx,
				Status:     ackRejected,
				Message:    "rate_limited",
			}); werr != nil {
				logger.Warn("Failed to send NACK", "remote_addr", remoteAddr, "error", werr)
				return
			}
			continue
		}

		// Append the event to the WAL.
		if err := walLog.Append(event); err != nil {
			metrics.WALAppendError()
			logger.Error("Failed to append to WAL",
				"remote_addr", remoteAddr, "producer_id", event.ProducerId, "seq", event.Seq, "error", err)
			if werr := sendAck(conn, wire.Ack{
				ProducerId: event.ProducerId,
				Seq:        event.Seq,
				Idx:        event.Idx,
				Status:     ackError,
				Message:    "wal_append_failed",
			}); werr != nil {
				logger.Warn("Failed to send NACK", "remote_addr", remoteAddr, "error", werr)
			}
			return // see note below
		}

		// Enqueue the event, allowing shutdown to cancel a blocked enqueue.
		if !queues.EnqueueContext(ctx, event) {
			if ctx.Err() != nil {
				logger.Debug(
					"Handler canceled while enqueueing event",
					"producer_id", event.ProducerId,
					"seq", event.Seq,
				)
				return
			}

			logger.Warn(
				"Event rejected: producer queue full",
				"producer_id", event.ProducerId,
			)
			continue
		}

		if err := sendAck(conn, wire.Ack{
			ProducerId: event.ProducerId,
			Seq:        event.Seq,
			Idx:        event.Idx,
			Status:     ackAccepted,
			Message:    "Event successfully enqueued",
		}); err != nil {
			logger.Warn("Failed to send ACK", "remote_addr", remoteAddr, "error", err)
			return
		}
		metrics.EventAccepted()

		logger.Info(
			"Event enqueued",
			"producer_id", event.ProducerId,
			"seq", event.Seq,
		)

		logger.Info(
			"Event admitted",
			"remote_addr", remoteAddr,
			"producer_id", event.ProducerId,
			"seq", event.Seq,
			"event_type", event.EventType,
		)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
