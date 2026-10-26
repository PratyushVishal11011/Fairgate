package deduplication

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"FairGate/internal/wal"
)

// BenchmarkPersistIfNewWAL appends distinct events through the manager from many goroutines,
// as concurrent connection handlers do, so that WAL group commit can batch their fsyncs.
func BenchmarkPersistIfNewWAL(b *testing.B) {
	w, err := wal.Open(filepath.Join(b.TempDir(), "events.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	m, err := NewManager(nil)
	if err != nil {
		b.Fatal(err)
	}

	var seq atomic.Uint64
	b.SetParallelism(32)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := testEvent()
			event.Seq = seq.Add(1)
			if _, err := m.PersistIfNew(event, func() error { return w.Append(event) }); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
