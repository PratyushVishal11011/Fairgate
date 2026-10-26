package wal

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"FairGate/internal/wire"
)

type recordingObserver struct {
	mu        sync.Mutex
	records   int
	bytes     int64
	commits   int
	rotations int
}

func (o *recordingObserver) BatchCommitted(records int, bytes int64, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.records += records
	o.bytes += bytes
	o.commits++
}

func (o *recordingObserver) SegmentRotated() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rotations++
}

func TestStatsBytesAfterAndObserverAcrossSegments(t *testing.T) {
	w, err := OpenWithSegmentSize(filepath.Join(t.TempDir(), "events.wal"), 200)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	observer := &recordingObserver{}
	w.SetObserver(observer)

	const total = 6
	for seq := uint64(1); seq <= total; seq++ {
		if err := w.Append(wire.Event{ProducerId: "p", ProducerEpoch: "1", Seq: seq, EventType: "test", Payload: "payload"}); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := w.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Segments < 2 {
		t.Fatalf("expected the small segment size to rotate, got %d segments", stats.Segments)
	}
	if stats.DurableEnd != w.DurableEnd() {
		t.Fatalf("stats durable end %+v differs from %+v", stats.DurableEnd, w.DurableEnd())
	}

	observer.mu.Lock()
	if observer.records != total {
		t.Errorf("observer saw %d records, want %d", observer.records, total)
	}
	if observer.bytes != stats.Bytes {
		t.Errorf("observer saw %d bytes, segments hold %d", observer.bytes, stats.Bytes)
	}
	if observer.rotations != stats.Segments-1 {
		t.Errorf("observer saw %d rotations for %d segments", observer.rotations, stats.Segments)
	}
	observer.mu.Unlock()

	all, err := w.BytesAfter(Position{})
	if err != nil {
		t.Fatal(err)
	}
	if all != stats.Bytes {
		t.Fatalf("BytesAfter(start) = %d, want all %d durable bytes", all, stats.Bytes)
	}

	// Walk record by record: the lag after each record is everything that follows it.
	reader := NewReader(w, Position{})
	remaining := stats.Bytes
	for {
		record, err := reader.Next()
		if err != nil {
			break
		}
		remaining -= record.End.Offset - record.Start.Offset
		lag, err := w.BytesAfter(record.End)
		if err != nil {
			t.Fatal(err)
		}
		if lag != remaining {
			t.Fatalf("BytesAfter(%+v) = %d, want %d", record.End, lag, remaining)
		}
	}
	if remaining != 0 {
		t.Fatalf("records did not account for every durable byte, %d left", remaining)
	}
}
