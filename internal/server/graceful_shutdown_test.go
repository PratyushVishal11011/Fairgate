package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func testServerAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP address: %v", err)
	}

	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release TCP address: %v", err)
	}

	return addr
}

func startTestServer(t *testing.T) (context.CancelFunc, <-chan error, string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	addr := testServerAddress(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)

	go func() {
		done <- RunContext(ctx, addr, logger)
	}()

	// Wait until the server is listening.
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			cancel()
			t.Fatalf("server exited before becoming ready: %v", err)
		default:
		}

		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return cancel, done, addr
		}

		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	t.Fatal("server did not start listening in time")
	return nil, nil, ""
}

func TestRunContextShutdown(t *testing.T) {
	cancel, done, _ := startTestServer(t)
	defer cancel()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunContext returned an unexpected error: %v", err)
		}
	case <-time.After(shutdownGracePeriod + 3*time.Second):
		t.Fatal("RunContext did not return after cancellation")
	}
}

func TestRunContextStopsAcceptingConnections(t *testing.T) {
	cancel, done, addr := startTestServer(t)

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunContext returned an unexpected error: %v", err)
		}
	case <-time.After(shutdownGracePeriod + 3*time.Second):
		t.Fatal("RunContext did not stop")
	}

	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("server accepted a connection after shutdown")
	}
}

func TestRunContextForceClosesStalledHandler(t *testing.T) {
	cancel, done, addr := startTestServer(t)
	defer cancel()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("connect to server: %v", err)
	}
	defer conn.Close()

	// The handler is now blocked waiting for a frame.
	cancel()

	_ = conn.SetReadDeadline(
		time.Now().Add(shutdownGracePeriod + 3*time.Second),
	)

	var buf [1]byte
	_, err = conn.Read(buf[:])
	if err == nil {
		t.Fatal("expected stalled connection to be closed during shutdown")
	}

	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("RunContext returned an unexpected error: %v", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunContext did not return after closing stalled handler")
	}
}
