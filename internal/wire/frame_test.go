package wire

import (
	"bytes"
	"errors"
	"io"
	"runtime"
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

func TestReadFrameLimitRejectsWithoutReadingBody(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, Frame{Type: FrameTypeEvent, Payload: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("next")

	_, err := ReadFrameLimit(&buf, 50)
	var tooLarge *FrameTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Length != 102 || tooLarge.Limit != 50 {
		t.Fatalf("expected FrameTooLargeError for 102 bytes, got %v", err)
	}
	//only the length prefix was consumed, so a caller can skip exactly the body.
	if buf.Len() != 102+len("next") {
		t.Fatalf("body was consumed: %d bytes left", buf.Len())
	}
}

func TestReadFrameRejectsMalformedLength(t *testing.T) {
	for _, length := range []byte{0, 1} {
		_, err := ReadFrame(bytes.NewReader([]byte{0, 0, 0, length, 1}))
		if !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("length %d: expected ErrMalformedFrame, got %v", length, err)
		}
	}
}

func TestReadFrameReportsTruncation(t *testing.T) {
	if _, err := ReadFrame(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("empty stream: expected io.EOF, got %v", err)
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{0, 0})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("partial length prefix: expected io.ErrUnexpectedEOF, got %v", err)
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{0, 0, 0, 10, 1, 0, 'a'})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("partial body: expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestReadFrameBodyGrowsToLargeFrames(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 50_000)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, Frame{Type: FrameTypeEvent, Flags: 3, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	frame, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != FrameTypeEvent || frame.Flags != 3 || !bytes.Equal(frame.Payload, payload) {
		t.Fatal("large frame did not round-trip")
	}
}

// A peer that claims a large frame but sends little of it must not cost the claimed memory.
func TestReadFrameBodyAllocatesForReceivedBytes(t *testing.T) {
	stream := []byte{0, 0x40, 0, 0, 1, 0, 'x'} // claims 4 MiB
	before := new(runtime.MemStats)
	after := new(runtime.MemStats)
	runtime.ReadMemStats(before)
	_, err := ReadFrame(bytes.NewReader(stream))
	runtime.ReadMemStats(after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected truncation, got %v", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("allocated %d bytes for a 7-byte stream", allocated)
	}
}
