package chaos

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// echoServer echoes everything it reads and records the size of each read.
type echoServer struct {
	listener net.Listener
	mu       sync.Mutex
	reads    []int
}

func startEcho(t *testing.T) *echoServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &echoServer{listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 4096)
				for {
					n, err := conn.Read(buf)
					if n > 0 {
						e.mu.Lock()
						e.reads = append(e.reads, n)
						e.mu.Unlock()
						if _, err := conn.Write(buf[:n]); err != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return e
}

func startProxy(t *testing.T, upstream string, faults Faults) *Proxy {
	t.Helper()
	p, err := Listen("127.0.0.1:0", upstream, faults)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func roundTrip(t *testing.T, conn net.Conn, msg string) (string, error) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte(msg)); err != nil {
		return "", err
	}
	buf := make([]byte, len(msg))
	_, err := io.ReadFull(conn, buf)
	return string(buf), err
}

func TestProxyForwardsWithLatency(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.listener.Addr().String(), Faults{Latency: 50 * time.Millisecond})

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	start := time.Now()
	got, err := roundTrip(t, conn, "hello")
	if err != nil || got != "hello" {
		t.Fatalf("round trip: %q, %v", got, err)
	}
	//latency applies in each direction.
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("round trip took %s, expected at least 100ms", elapsed)
	}
	stats := p.Stats()
	if stats.Accepted != 1 || stats.BytesUp != 5 || stats.BytesDown != 5 {
		t.Fatalf("unexpected stats %+v", stats)
	}
}

func TestProxyFragmentsWrites(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.listener.Addr().String(), Faults{Fragment: 3, FragmentDelay: 5 * time.Millisecond})

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	msg := strings.Repeat("x", 30)
	got, err := roundTrip(t, conn, msg)
	if err != nil || got != msg {
		t.Fatalf("round trip: %q, %v", got, err)
	}
	echo.mu.Lock()
	defer echo.mu.Unlock()
	//30 bytes in 3-byte fragments arrive as 10 reads; allow the receiver to coalesce a few.
	if len(echo.reads) < 5 {
		t.Fatalf("upstream read the message in %d reads %v; expected it fragmented", len(echo.reads), echo.reads)
	}
}

func TestProxyBlackholeHoldsTrafficUntilHealed(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.listener.Addr().String(), Faults{Blackhole: true})

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 4)
	var netErr net.Error
	if _, err := conn.Read(buf); !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected no data during the partition, got %v", err)
	}

	if err := p.SetFaults(Faults{}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("expected held data after healing, got %q, %v", buf, err)
	}
}

func TestProxyResetsAndRefusesConnections(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.listener.Addr().String(), Faults{ResetProbability: 1})

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roundTrip(t, conn, "boom"); err == nil {
		t.Fatal("expected the connection to be reset")
	}
	_ = conn.Close()

	if err := p.SetFaults(Faults{Refuse: true}); err != nil {
		t.Fatal(err)
	}
	conn, err = net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roundTrip(t, conn, "boom"); err == nil {
		t.Fatal("expected a refused connection")
	}
	_ = conn.Close()

	stats := p.Stats()
	if stats.Resets != 1 || stats.Refused != 1 {
		t.Fatalf("unexpected stats %+v", stats)
	}
}

func TestProxyDisconnectAll(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.listener.Addr().String(), Faults{})

	var conns []net.Conn
	for range 3 {
		conn, err := net.Dial("tcp", p.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := roundTrip(t, conn, "hi"); err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
	}
	if n := p.DisconnectAll(); n != 3 {
		t.Fatalf("disconnected %d connections, want 3", n)
	}
	for _, conn := range conns {
		if _, err := roundTrip(t, conn, "hi"); err == nil {
			t.Fatal("expected a reset connection")
		}
	}
}

func TestAdminAPI(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.listener.Addr().String(), Faults{})
	server := httptest.NewServer(p.Handler())
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPut, server.URL+"/faults", bytes.NewBufferString(`{"latency":"25ms","reset_probability":0.5}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT /faults: %s", resp.Status)
	}
	if f := p.Faults(); f.Latency != 25*time.Millisecond || f.ResetProbability != 0.5 {
		t.Fatalf("faults not applied: %+v", f)
	}

	resp, err = http.Get(server.URL + "/faults")
	if err != nil {
		t.Fatal(err)
	}
	var got Faults
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got != p.Faults() {
		t.Fatalf("GET /faults returned %+v", got)
	}

	req, _ = http.NewRequest(http.MethodPut, server.URL+"/faults", bytes.NewBufferString(`{"reset_probability":2}`))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected an invalid fault to be refused, got %s", resp.Status)
	}

	resp, err = http.Post(server.URL+"/disconnect", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /disconnect: %s", resp.Status)
	}
}
