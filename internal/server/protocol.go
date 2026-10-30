package server

import (
	"FairGate/internal/config"
	"FairGate/internal/observability"
	"FairGate/internal/wire"
	"bufio"
	"errors"
	"io"
	"log/slog"
	"net"
	"time"
)

// ACK messages for frames the gateway cannot handle. They carry no identity: the frame was not
// decoded. Each answers exactly one frame, so a pipelining client stays in step.
const (
	ackMalformedFrame       = "malformed_frame"
	ackFrameTooLarge        = "frame_too_large"
	ackUnsupportedFrameType = "unsupported_frame_type"
)

// An oversized frame up to this many times server.max_frame_bytes is read and discarded so the
// connection stays usable. A larger one is answered and the connection closed, rather than spending
// bandwidth and time on a body that will be thrown away.
const oversizedSkipFactor = 4

// protocolLimits bounds what one connection may send and how long it may stall.
type protocolLimits struct {
	//largest frame accepted; 0 uses wire.MaxFrameSize.
	maxFrameBytes int
	//time to start a new frame; 0 waits indefinitely.
	idleTimeout time.Duration
	//time to finish a frame once its first byte arrives; 0 waits indefinitely.
	frameTimeout time.Duration
	//time to write one ACK; 0 waits indefinitely.
	writeTimeout time.Duration
}

func protocolLimitsFrom(cfg config.Server) protocolLimits {
	return protocolLimits{
		maxFrameBytes: cfg.MaxFrameBytes,
		idleTimeout:   cfg.IdleTimeout,
		frameTimeout:  cfg.FrameTimeout,
		writeTimeout:  cfg.WriteTimeout,
	}
}

func (l protocolLimits) maxFrame() int {
	if l.maxFrameBytes <= 0 {
		return wire.MaxFrameSize
	}
	return l.maxFrameBytes
}

// ackWriter sends ACKs with a write deadline, so a client that stops reading cannot hold its handler.
type ackWriter struct {
	conn    net.Conn
	timeout time.Duration
	metrics *observability.Metrics
}

func (a *ackWriter) send(ack wire.Ack) error {
	if a.timeout > 0 {
		_ = a.conn.SetWriteDeadline(time.Now().Add(a.timeout))
	}
	err := sendAck(a.conn, ack)
	if isTimeout(err) {
		a.metrics.ProtocolError(observability.ProtocolWriteTimeout)
	}
	return err
}

// frameReader reads event frames from one connection and enforces the protocol limits.
// Frames it cannot handle are answered with an invalid ACK and skipped when the stream
// is still in step, or end the connection when it is not.
type frameReader struct {
	conn       net.Conn
	r          *bufio.Reader
	limits     protocolLimits
	acks       *ackWriter
	logger     *slog.Logger
	metrics    *observability.Metrics
	remoteAddr string
}

func newFrameReader(conn net.Conn, limits protocolLimits, acks *ackWriter, logger *slog.Logger, metrics *observability.Metrics) *frameReader {
	return &frameReader{
		conn:       conn,
		r:          bufio.NewReaderSize(conn, 16<<10),
		limits:     limits,
		acks:       acks,
		logger:     logger,
		metrics:    metrics,
		remoteAddr: conn.RemoteAddr().String(),
	}
}

// next returns the next event frame. It returns false when the connection should close.
func (f *frameReader) next() (wire.Frame, bool) {
	for {
		//waiting for a frame to start is bounded by the idle timeout,
		//and everything after its first byte by the frame timeout.
		//a deadline fails to set only on a closed connection, which the next read reports.
		f.setReadDeadline(f.limits.idleTimeout)
		if _, err := f.r.Peek(1); err != nil {
			f.readFailed(err, false)
			return wire.Frame{}, false
		}
		f.setReadDeadline(f.limits.frameTimeout)

		length, err := wire.ReadFrameLength(f.r)
		if err != nil {
			f.readFailed(err, true)
			return wire.Frame{}, false
		}

		if err := wire.CheckFrameLength(length, f.limits.maxFrame()); err != nil {
			var tooLarge *wire.FrameTooLargeError
			if !errors.As(err, &tooLarge) {
				//the length prefix cannot be trusted, so the next frame boundary is unknown.
				f.metrics.ProtocolError(observability.ProtocolMalformedFrame)
				f.logger.Warn("Closing connection after a malformed frame",
					"remote_addr", f.remoteAddr,
					"length", length,
				)
				_ = f.acks.send(wire.Ack{Status: ackInvalid, Message: ackMalformedFrame})
				return wire.Frame{}, false
			}

			f.metrics.ProtocolError(observability.ProtocolFrameTooLarge)
			if uint64(length) > uint64(f.limits.maxFrame())*oversizedSkipFactor {
				f.logger.Warn("Closing connection after an oversized frame too large to skip",
					"remote_addr", f.remoteAddr,
					"length", length,
					"max_frame_bytes", f.limits.maxFrame(),
				)
				_ = f.acks.send(wire.Ack{Status: ackInvalid, Message: ackFrameTooLarge})
				return wire.Frame{}, false
			}
			//discard the body without buffering it, then answer, so the client can read the ACK
			//once it has finished writing.
			if _, err := io.CopyN(io.Discard, f.r, int64(length)); err != nil {
				f.readFailed(err, true)
				return wire.Frame{}, false
			}
			f.logger.Warn("Skipped an oversized frame",
				"remote_addr", f.remoteAddr,
				"length", length,
				"max_frame_bytes", f.limits.maxFrame(),
			)
			if err := f.acks.send(wire.Ack{Status: ackInvalid, Message: ackFrameTooLarge}); err != nil {
				return wire.Frame{}, false
			}
			continue
		}

		frame, err := wire.ReadFrameBody(f.r, length)
		if err != nil {
			f.readFailed(err, true)
			return wire.Frame{}, false
		}

		if frame.Type != wire.FrameTypeEvent {
			f.metrics.ProtocolError(observability.ProtocolUnsupportedFrameType)
			f.logger.Warn("Unsupported frame type",
				"remote_addr", f.remoteAddr,
				"type", frame.Type,
			)
			if err := f.acks.send(wire.Ack{Status: ackInvalid, Message: ackUnsupportedFrameType}); err != nil {
				return wire.Frame{}, false
			}
			continue
		}

		return frame, true
	}
}

func (f *frameReader) setReadDeadline(timeout time.Duration) {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	_ = f.conn.SetReadDeadline(deadline)
}

// readFailed records why reading stopped. inFrame says whether part of a frame had arrived.
func (f *frameReader) readFailed(err error, inFrame bool) {
	switch {
	case errors.Is(err, io.EOF) && !inFrame:
		//the client closed the connection between frames.
	case errors.Is(err, net.ErrClosed):
		//the server closed the connection, during shutdown.
	case isTimeout(err) && !inFrame:
		f.metrics.ProtocolError(observability.ProtocolIdleTimeout)
		f.logger.Info("Closing idle connection", "remote_addr", f.remoteAddr, "idle_timeout", f.limits.idleTimeout)
	case isTimeout(err):
		f.metrics.ProtocolError(observability.ProtocolFrameTimeout)
		f.logger.Warn("Closing connection: frame not completed in time",
			"remote_addr", f.remoteAddr,
			"frame_timeout", f.limits.frameTimeout,
		)
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		f.metrics.ProtocolError(observability.ProtocolTruncatedFrame)
		f.logger.Warn("Connection ended inside a frame", "remote_addr", f.remoteAddr)
	default:
		f.logger.Warn("Error reading frame",
			"remote_addr", f.remoteAddr,
			"error", err,
		)
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
