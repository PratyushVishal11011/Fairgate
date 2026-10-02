package server

import (
	"FairGate/internal/wire"
	"errors"
	"io"
	"log/slog"
	"net"
)

func Run(addr string, logger *slog.Logger) error {
	//Open a TCP listner on the specified address
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}

	defer listener.Close()

	logger.Info("server listening on ", "address", addr)

	for {
		//wait for a client to connect and return a net.Conn for that client.
		conn, err := listener.Accept()

		if err != nil {
			return err
		}

		//Start a goroutine, so that one client request doesn't block the others
		go handleConn(conn, logger)
	}
}

func handleConn(conn net.Conn, logger *slog.Logger) {
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
	}
}
