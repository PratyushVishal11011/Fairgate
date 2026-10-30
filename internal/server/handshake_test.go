package server

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"FairGate/internal/observability"
	"FairGate/internal/wire"
)

func (h *protocolHarness) sendHello(t *testing.T, payload string) wire.Welcome {
	t.Helper()
	if err := wire.WriteFrame(h.client, wire.Frame{Type: wire.FrameTypeHello, Payload: []byte(payload)}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	frame, err := wire.ReadFrame(h.client)
	if err != nil {
		t.Fatalf("read welcome: %v", err)
	}
	if frame.Type != wire.FrameTypeWelcome {
		t.Fatalf("expected a welcome frame, got type %d", frame.Type)
	}
	var welcome wire.Welcome
	if err := json.Unmarshal(frame.Payload, &welcome); err != nil {
		t.Fatal(err)
	}
	return welcome
}

// expectMetric checks one sample line of the metrics endpoint.
func (h *protocolHarness) expectMetric(t *testing.T, series string, want string) {
	t.Helper()
	server := httptest.NewServer(h.metrics.Handler())
	defer server.Close()
	resp, err := server.Client().Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, series+" ") {
			if got := strings.TrimPrefix(line, series+" "); got != want {
				t.Fatalf("%s = %s, want %s", series, got, want)
			}
			return
		}
	}
	t.Fatalf("series %s not found", series)
}

func TestHandshakeNegotiatesVersionAndLimits(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{maxFrameBytes: 4096, requireHello: true})
	//a newer client offering versions the server does not know yet settles on the server's newest.
	welcome := h.sendHello(t, `{"min_version":1,"max_version":9,"client":"future-client/3","feature":"x"}`)
	if welcome.Error != "" || welcome.Version != wire.ProtocolVersion {
		t.Fatalf("unexpected welcome %+v", welcome)
	}
	if welcome.MaxFrameBytes != 4096 || welcome.MaxIdentifierBytes != wire.MaxIdentifierLength || !strings.HasPrefix(welcome.Server, "fairgate/") {
		t.Fatalf("welcome must carry the server's limits, got %+v", welcome)
	}
	expectAck(t, h.sendEvent(t, "p", 1), ackAccepted, "Event successfully enqueued")
	h.expectMetric(t, `fairgate_protocol_connections_total{version="1"}`, "1")
}

func TestHandshakeRefusesUnsupportedVersion(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	welcome := h.sendHello(t, `{"min_version":2,"max_version":5}`)
	if welcome.Error != wire.WelcomeUnsupportedVersion || welcome.Version != 0 ||
		welcome.SupportedMinVersion != wire.MinProtocolVersion || welcome.SupportedMaxVersion != wire.ProtocolVersion {
		t.Fatalf("unexpected welcome %+v", welcome)
	}
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolUnsupportedVersion, 1)
}

func TestHandshakeRefusesInvalidHello(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	if welcome := h.sendHello(t, `{"min_version":3,"max_version":1}`); welcome.Error != wire.WelcomeInvalidHello {
		t.Fatalf("unexpected welcome %+v", welcome)
	}
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolInvalidHello, 1)
}

func TestSecondHelloClosesConnection(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	if welcome := h.sendHello(t, `{"min_version":1,"max_version":1}`); welcome.Version != wire.ProtocolVersion {
		t.Fatalf("unexpected welcome %+v", welcome)
	}
	if welcome := h.sendHello(t, `{"min_version":1,"max_version":1}`); welcome.Error != wire.WelcomeUnexpectedHello {
		t.Fatalf("unexpected welcome %+v", welcome)
	}
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolUnexpectedHello, 1)
}

func TestHelloAfterLegacyEventsClosesConnection(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	expectAck(t, h.sendEvent(t, "p", 1), ackAccepted, "Event successfully enqueued")
	if welcome := h.sendHello(t, `{"min_version":1,"max_version":1}`); welcome.Error != wire.WelcomeUnexpectedHello {
		t.Fatalf("unexpected welcome %+v", welcome)
	}
	h.expectClosed(t)
}

func TestLegacyClientsAreAcceptedByDefault(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	expectAck(t, h.sendEvent(t, "p", 1), ackAccepted, "Event successfully enqueued")
	expectAck(t, h.sendEvent(t, "p", 2), ackAccepted, "Event successfully enqueued")
	h.expectMetric(t, `fairgate_protocol_connections_total{version="legacy"}`, "1")
}

func TestLegacyClientsCanBeRefused(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{requireHello: true})
	expectAck(t, h.sendEvent(t, "p", 1), ackInvalid, ackHelloRequired)
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolHelloRequired, 1)
}
