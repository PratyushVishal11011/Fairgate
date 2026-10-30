package wire

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
)

func TestDecodeHelloValidates(t *testing.T) {
	cases := map[string]string{
		"not json":       `{"min_version":`,
		"zero min":       `{"min_version":0,"max_version":1}`,
		"inverted range": `{"min_version":3,"max_version":2}`,
		"long client":    `{"min_version":1,"max_version":1,"client":"` + strings.Repeat("c", MaxIdentifierLength+1) + `"}`,
	}
	for name, payload := range cases {
		if _, err := DecodeHello([]byte(payload)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	//unknown fields are ignored, so later versions can extend the hello.
	hello, err := DecodeHello([]byte(`{"min_version":1,"max_version":4,"client":"c/1","future":true}`))
	if err != nil || hello.MaxVersion != 4 || hello.Client != "c/1" {
		t.Fatalf("got %+v, %v", hello, err)
	}
}

func TestNegotiatePicksHighestCommonVersion(t *testing.T) {
	cases := []struct {
		clientMin, clientMax, serverMin, serverMax, want int
		ok                                               bool
	}{
		{1, 1, 1, 1, 1, true},
		{1, 5, 1, 2, 2, true},
		{2, 5, 1, 3, 3, true},
		{3, 5, 1, 2, 0, false},
		{1, 1, 2, 3, 0, false},
	}
	for _, c := range cases {
		got, ok := Negotiate(Hello{MinVersion: c.clientMin, MaxVersion: c.clientMax}, c.serverMin, c.serverMax)
		if got != c.want || ok != c.ok {
			t.Errorf("client %d-%d server %d-%d: got %d %v, want %d %v",
				c.clientMin, c.clientMax, c.serverMin, c.serverMax, got, ok, c.want, c.ok)
		}
	}
}

// serveOneHello reads a hello on conn and answers with welcome.
func serveOneHello(t *testing.T, conn net.Conn, frameType uint8, welcome Welcome) <-chan Hello {
	t.Helper()
	got := make(chan Hello, 1)
	go func() {
		frame, err := ReadFrame(conn)
		if err != nil || frame.Type != FrameTypeHello {
			close(got)
			return
		}
		hello, _ := DecodeHello(frame.Payload)
		got <- hello
		payload, _ := json.Marshal(welcome)
		_ = WriteFrame(conn, Frame{Type: frameType, Payload: payload})
	}()
	return got
}

func TestHandshake(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		hellos := serveOneHello(t, server, FrameTypeWelcome, Welcome{Version: ProtocolVersion, MaxFrameBytes: 1024})
		welcome, err := Handshake(client, "test/1")
		if err != nil || welcome.Version != ProtocolVersion || welcome.MaxFrameBytes != 1024 {
			t.Fatalf("got %+v, %v", welcome, err)
		}
		hello := <-hellos
		if hello.MinVersion != MinProtocolVersion || hello.MaxVersion != ProtocolVersion || hello.Client != "test/1" {
			t.Fatalf("server saw hello %+v", hello)
		}
	})

	failures := map[string]struct {
		frameType uint8
		welcome   Welcome
		want      string
	}{
		"unsupported":  {FrameTypeWelcome, Welcome{Error: WelcomeUnsupportedVersion, SupportedMinVersion: 7, SupportedMaxVersion: 9}, "7-9"},
		"refused":      {FrameTypeWelcome, Welcome{Error: "nope"}, "nope"},
		"out of range": {FrameTypeWelcome, Welcome{Version: ProtocolVersion + 1}, "outside"},
		"wrong frame":  {FrameTypeAck, Welcome{Version: 1}, "expected a welcome"},
	}
	for name, c := range failures {
		t.Run(name, func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			serveOneHello(t, server, c.frameType, c.welcome)
			_, err := Handshake(client, "test/1")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("expected an error containing %q, got %v", c.want, err)
			}
			if name == "unsupported" && !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
			}
		})
	}
}
