package server

import (
	"FairGate/internal/admit"
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

const shutdownGracePeriod = 5 * time.Second

func Run(addr string, logger *slog.Logger) error {
	return RunContext(context.Background(), addr, logger)
}

func RunContext(ctx context.Context, addr string, logger *slog.Logger) error {
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

	logger.Info("server listening on", "address", addr)

	admission, err := admit.NewManager(100, 200)
	if err != nil {
		return err
	}

	// Create one shared queue manager.
	// For testing, each producer can buffer up to 256 events.
	queues, err := sched.NewQueues(256)
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
			default:
				// A listener error is unexpected only if the parent
				// context has not been canceled.
				if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					runErr = err
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
				connMu.Lock()
				delete(activeConns, conn)
				connMu.Unlock()
			}()

			handleConnContext(
				handlerCtx,
				conn,
				logger,
				admission,
				queues,
				walLog,
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
			logger.Warn(
				"Invalid Event Payload",
				"remoteAddr", remoteAddr,
				"error", err,
			)
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

		// If the event is not allowed, log and continue.
		if !admission.Allow(event.ProducerId) {
			logger.Warn(
				"Event rejected by admission control",
				"remote_addr", remoteAddr,
				"producer_id", event.ProducerId,
				"seq", event.Seq,
			)
			continue
		}

		// Append the event to the WAL.
		if err := walLog.Append(event); err != nil {
			logger.Error(
				"Failed to append to WAL",
				"producer_id", event.ProducerId,
				"sequence", event.Seq,
				"error", err,
			)
			continue
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

		// Create the acknowledgement object.
		ack := wire.Ack{
			ProducerId: event.ProducerId,
			Seq:        event.Seq,
			Idx:        event.Idx,
			Status:     "accepted",
			Message:    "Event successfully enqueued",
		}

		// Convert the ACK into a JSON object.
		payload, err := json.Marshal(ack)
		if err != nil {
			logger.Error("Failed to encode ACK", "error", err)
			return
		}

		// Send the acknowledgement.
		if err := wire.WriteFrame(conn, wire.Frame{
			Type:    wire.FrameTypeAck,
			Flags:   0,
			Payload: payload,
		}); err != nil {
			logger.Error(
				"Failed to send ACK",
				"producer_id", event.ProducerId,
				"seq", event.Seq,
				"error", err,
			)
			return
		}

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
