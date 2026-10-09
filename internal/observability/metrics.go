package observability

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Metrics keeps the gateway's small, fixed-cardinality metric set. Producer IDs
// and other request data are deliberately never used as labels.
type Metrics struct {
	connections atomic.Int64
	accepted    atomic.Uint64
	rejected    atomic.Uint64
	invalid     atomic.Uint64
	walErrors   atomic.Uint64
	processed   atomic.Uint64
}

func New() *Metrics { return &Metrics{} }

func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "# HELP fairgate_tcp_connections_active Active TCP client connections.\n# TYPE fairgate_tcp_connections_active gauge\nfairgate_tcp_connections_active %d\n", m.connections.Load())
		writeCounter(w, "fairgate_events_accepted_total", "Events durably appended and accepted.", m.accepted.Load())
		writeCounter(w, "fairgate_events_rejected_total", "Events rejected by admission control.", m.rejected.Load())
		writeCounter(w, "fairgate_events_invalid_total", "Events rejected as invalid payloads.", m.invalid.Load())
		writeCounter(w, "fairgate_wal_append_errors_total", "WAL append failures.", m.walErrors.Load())
		writeCounter(w, "fairgate_events_processed_total", "Events processed by the scheduler.", m.processed.Load())
	})
	return mux
}

func writeCounter(w http.ResponseWriter, name, help string, value uint64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
}

func (m *Metrics) ConnectionOpened()  { m.connections.Add(1) }
func (m *Metrics) ConnectionClosed()  { m.connections.Add(-1) }
func (m *Metrics) EventAccepted()     { m.accepted.Add(1) }
func (m *Metrics) EventRejected()     { m.rejected.Add(1) }
func (m *Metrics) EventInvalid()      { m.invalid.Add(1) }
func (m *Metrics) WALAppendError()    { m.walErrors.Add(1) }
func (m *Metrics) EventProcessed()    { m.processed.Add(1) }
