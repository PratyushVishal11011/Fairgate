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
)

func Run(addr string, logger *slog.Logger) error {
	//Open a TCP listner on the specified address
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

	logger.Info("server listening on ", "address", addr)
	admission, err := admit.NewManager(100, 200)
	if err != nil {
		return err
	}

	//create one shared queue manager
	//for testing, each producer can buffer upto 256 events
	queues, err := sched.NewQueues(256)
	if err != nil {
		return err
	}

	//context.WithCancel gives us a way to stop the scheduler
	//helps us control the scheduler's lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//create a scheduler object with a quantum of 8 events
	scheduler := sched.NewDRRScheduler(8)

	//Create a channel that helps us report scheduler errors
	schedulerErr := make(chan error, 1)

	go func() {
		err := scheduler.Run(ctx, queues, func(event wire.Event) error {
			logger.Info("Event processed by DRR",
				"producer_id", event.ProducerId,
				"seq", event.Seq,
			)
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			schedulerErr <- err

			//Stop the server from accepting new connections
			listener.Close()
			logger.Error("DRR scheduler stopped", "error", err)
		}
	}()

	//Restore previous events
	for _, event := range events {
		if !queues.Enqueue(event) {
			return fmt.Errorf("queue enqueue failed: producer_id = %s, event_sequence = %d", event.ProducerId, event.Seq)
		}
	}

	logger.Info("WAL events restored",
		"restored_events", len(events),
	)

	for {
		//wait for a client to connect and return a net.Conn for that client.
		conn, err := listener.Accept()

		if err != nil {
			//Check whether the scheduler caused the listener to close
			select {
			case schedulerError := <-schedulerErr:
				return fmt.Errorf("scheduler error: %v", schedulerError)
			}
			return err
		}

		//Start a goroutine, so that one client request doesn't block the others
		go handleConn(conn, logger, admission, queues, walLog)

	}
}

func handleConn(conn net.Conn, logger *slog.Logger, admission *admit.Manager, queues *sched.Queues, walLog *wal.WAL) {
	//close the connection when the client exits
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	logger.Info("Client connected", "remoteAddr", remoteAddr)

	for {
		//convert bytes into frames
		frame, err := wire.ReadFrame(conn)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				logger.Warn("error reading frame", "remoteAddr", remoteAddr, "error", err)
			}
			return
		}
		logger.Info("Frame received", "remoteAddr", remoteAddr, "type", frame.Type, "payload_bytes", len(frame.Payload))

		//Type 1 represents an event frame
		//Only type 1 events are treated as events, the rest are logged and skip (for now - might add support for more later)
		if frame.Type != 1 {
			logger.Warn("Unsupported Frame Type", "Remote Address", remoteAddr, "type", frame.Type)
			continue
		}

		//Decode the event and look for errors in payload
		event, err := wire.DecodeEvent(frame.Payload)
		if err != nil {
			logger.Warn("Invalid Event Payload", "remoteAddr", remoteAddr, "error", err)
			continue
		}

		//Log the event
		logger.Info("Event received",
			"remote_addr", remoteAddr,
			"producer_id", event.ProducerId,
			"event_time", event.EventTime,
			"seq", event.Seq,
			"idx", event.Idx,
			"event_type", event.EventType,
		)

		//If event is not allowed, log and continue
		if !admission.Allow(event.ProducerId) {
			logger.Warn("Event rejected by admission control",
				"remote_addr", remoteAddr,
				"producer_id", event.ProducerId,
				"seq", event.Seq,
			)
			continue
		}

		if err := walLog.Append(event); err != nil {
			logger.Error("Failed to append to WAL", "producer_id", event.ProducerId, "sequence", event.Seq, "error", err)
			continue
		}

		//Try to enqueue event and log if it fails
		if !queues.Enqueue(event) {
			logger.Warn("Event rejected: producer queue full",
				"producer_id", event.ProducerId,
			)
			continue
		}

		//Create acknowledgement object
		ack := wire.Ack{
			ProducerId: event.ProducerId,
			Seq:        event.Seq,
			Idx:        event.Idx,
			Status:     "accepted",
			Message:    "Event successfully enqueued",
		}

		//convert ack into a json object
		payload, err := json.Marshal(ack)
		if err != nil {
			logger.Error("Failed to encode ACK", "error", err)
			return
		}

		//Sends the acknowledgement
		if err := wire.WriteFrame(conn, wire.Frame{
			Type:    wire.FrameTypeAck,
			Flags:   0,
			Payload: payload,
		}); err != nil {
			logger.Error("Failed to send ACK",
				"producer_id", event.ProducerId,
				"seq", event.Seq,
				"error", err,
			)
			return
		}

		logger.Info("Event enqueued",
			"producer_id", event.ProducerId,
			"seq", event.Seq,
		)

		logger.Info("Event admitted",
			"remote_addr", remoteAddr,
			"producer_id", event.ProducerId,
			"seq", event.Seq,
			"event_type", event.EventType,
		)
	}
}
