package wire

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	original := Frame{
		Type:    1,
		Flags:   0,
		Payload: []byte("hello fairgate"),
	}

	var buf bytes.Buffer

	if err := WriteFrame(&buf, original); err != nil {
		t.Fatal(err)
	}

	decoded, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Type != original.Type {
		t.Fatalf("expected type %d, got %d", original.Type, decoded.Type)
	}

	if decoded.Flags != original.Flags {
		t.Fatalf("expected flags %d, got %d", original.Flags, decoded.Flags)
	}

	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Fatalf("expected payload %q, got %q", original.Payload, decoded.Payload)
	}
}

func TestRejectInvalidFrameLength(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 1})

	_, err := ReadFrame(&buf)
	if err == nil {
		t.Fatal("expected error for invalid frame length")
	}
}
