package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// buildSegments writes events 1..n with a segment size that puts several records in each segment,
// and returns the WAL path and the record boundaries of segment 0.
func buildSegments(t *testing.T, n int) (string, []int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.wal")
	w, err := OpenWithSegmentSize(path, 900)
	if err != nil {
		t.Fatal(err)
	}
	for seq := 1; seq <= n; seq++ {
		if err := w.Append(testEvent(uint64(seq))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scan, err := scanSegment(file, fileSize(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.records) < 4 {
		t.Fatalf("expected at least 4 records in segment 0, got %d", len(scan.records))
	}
	var starts []int64
	for _, record := range scan.records {
		starts = append(starts, record.start)
	}
	return path, starts
}

// corrupt flips a byte inside the payload of the record starting at offset.
func corrupt(t *testing.T, path string, offset int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[offset+v2HeaderSize+5] ^= 0xff
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptUnshippedRecordIsRefused(t *testing.T) {
	path, starts := buildSegments(t, 20)
	corrupt(t, path, starts[1])

	_, err := Recover(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
	if !strings.Contains(err.Error(), "segment 0") || !strings.Contains(err.Error(), "fairgate wal inspect") {
		t.Fatalf("error should name the segment and the inspect command: %v", err)
	}
}

func TestCorruptShippedRecordIsSkipped(t *testing.T) {
	path, starts := buildSegments(t, 20)
	corrupt(t, path, starts[1])
	before, _ := os.ReadFile(path)

	recovery, err := RecoverWithOptions(path, RecoverOptions{Shipped: Position{SegmentId: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.Damage) != 1 || recovery.Damage[0].Action != DamageSkippedShipped {
		t.Fatalf("expected one skipped region, got %+v", recovery.Damage)
	}
	//only the damaged record is missing: the magic lets recovery resume at the next one.
	if len(recovery.Events) != 19 || recovery.Events[1].Seq != 3 {
		t.Fatalf("expected 19 events without seq 2, got %d", len(recovery.Events))
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatal("a skipped region must leave the segment unchanged")
	}
}

func TestCorruptUnshippedRecordIsQuarantined(t *testing.T) {
	path, starts := buildSegments(t, 20)
	corrupt(t, path, starts[1])
	original, _ := os.ReadFile(path)
	segment0Records := len(starts)

	recovery, err := RecoverWithOptions(path, RecoverOptions{OnCorruption: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.Damage) != 1 || recovery.Damage[0].Action != DamageQuarantined {
		t.Fatalf("expected one quarantined region, got %+v", recovery.Damage)
	}
	copied, err := os.ReadFile(recovery.Damage[0].QuarantinePath)
	if err != nil || !bytes.Equal(copied, original) {
		t.Fatalf("quarantine copy must match the original segment: %v", err)
	}
	if size := fileSize(t, path); size != starts[1] {
		t.Fatalf("segment cut at %d, want %d", size, starts[1])
	}
	//records after the damage in segment 0 are salvaged for re-append.
	if len(recovery.Salvaged) != segment0Records-2 || recovery.Salvaged[0].Seq != 3 {
		t.Fatalf("expected %d salvaged records starting at seq 3, got %d", segment0Records-2, len(recovery.Salvaged))
	}
	if len(recovery.Events) != 19 {
		t.Fatalf("expected 19 events, got %d", len(recovery.Events))
	}
}

func TestRepairLetsStartupSucceed(t *testing.T) {
	path, starts := buildSegments(t, 20)
	corrupt(t, path, starts[1])

	reports, err := Inspect(path, Position{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports[0].Damage) != 1 || reports[0].Damage[0].Verdict != VerdictUnshipped {
		t.Fatalf("inspect should report unshipped damage, got %+v", reports[0])
	}

	if _, err := Repair(path, Position{}); err != nil {
		t.Fatal(err)
	}
	events, err := Recover(path)
	if err != nil {
		t.Fatalf("startup recovery after repair: %v", err)
	}
	seen := map[uint64]int{}
	for _, event := range events {
		seen[event.Seq]++
	}
	if len(seen) != 19 || seen[2] != 0 {
		t.Fatalf("expected every seq but 2 after repair, got %v", seen)
	}
	for seq, n := range seen {
		if n != 1 {
			t.Fatalf("seq %d recovered %d times", seq, n)
		}
	}
}

func TestQuarantineRefusesWhenCheckpointIsPastTheDamage(t *testing.T) {
	path, starts := buildSegments(t, 20)
	corrupt(t, path, starts[1])

	//a checkpoint inside the damaged region cannot be honoured by cutting the segment.
	_, err := RecoverWithOptions(path, RecoverOptions{
		Shipped:      Position{SegmentId: 0, Offset: starts[1] + 1},
		OnCorruption: CorruptionQuarantine,
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestNewestSegmentTornTailIsTruncatedButMidFileDamageIsNot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if err := w.Append(testEvent(seq)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	file, _ := os.Open(path)
	scan, err := scanSegment(file, fileSize(t, path))
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}

	//damage to the last record of the newest segment: an unfinished write.
	tail := filepath.Join(t.TempDir(), "tail.wal")
	data, _ := os.ReadFile(path)
	_ = os.WriteFile(tail, data, 0600)
	corrupt(t, tail, scan.records[2].start)
	events, err := Recover(tail)
	if err != nil {
		t.Fatal(err)
	}
	expectSeqs(t, events, 1, 2)
	if size := fileSize(t, tail); size != scan.records[2].start {
		t.Fatalf("torn tail not truncated: %d bytes", size)
	}

	//damage followed by valid records was acknowledged data, not an unfinished write.
	corrupt(t, path, scan.records[1].start)
	if _, err := Recover(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for mid-file damage, got %v", err)
	}
}
