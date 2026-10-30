package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

func TestWALInspectAndRepair(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "fairgate.wal")
	w, err := wal.OpenWithSegmentSize(walPath, 600)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		err := w.Append(wire.Event{
			ProducerId: "p", ProducerEpoch: "1", EventTime: time.Unix(1793000000, 0).UTC(),
			Seq: seq, EventType: "test", Payload: strings.Repeat("x", 60),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	args := func(command string) []string {
		return []string{command, "-wal", walPath, "-checkpoint", filepath.Join(dir, "checkpoint.json")}
	}
	run := func(command string) (int, string) {
		var stdout, stderr bytes.Buffer
		code := runWAL(args(command), &stdout, &stderr)
		return code, stdout.String() + stderr.String()
	}

	if code, out := run("inspect"); code != 0 || !strings.Contains(out, "startup will succeed") {
		t.Fatalf("inspect of a clean WAL: %d\n%s", code, out)
	}

	//damage the second record of the first (non-final) segment.
	data, _ := os.ReadFile(walPath)
	second := bytes.Index(data[1:], []byte("FGW1")) + 1
	data[second+20] ^= 0xff
	if err := os.WriteFile(walPath, data, 0600); err != nil {
		t.Fatal(err)
	}

	if code, out := run("inspect"); code != 1 || !strings.Contains(out, "NOT SHIPPED") {
		t.Fatalf("inspect of a damaged WAL: %d\n%s", code, out)
	}
	if code, out := run("repair"); code != 0 || !strings.Contains(out, "quarantined") {
		t.Fatalf("repair: %d\n%s", code, out)
	}
	if code, out := run("inspect"); code != 0 {
		t.Fatalf("inspect after repair: %d\n%s", code, out)
	}
	if code, _ := run("bogus"); code != 2 {
		t.Fatalf("an unknown subcommand must exit 2, got %d", code)
	}
}
