package observability

import (
	"net/http"
	"net/http/pprof"
)

// ProfilingHandler serves the net/http/pprof endpoints under /debug/pprof/.
// It is served on its own listener so profiles are never exposed on the metrics port.
func ProfilingHandler() http.Handler {
	mux := http.NewServeMux()
	//Index also serves the named runtime profiles: heap, allocs, goroutine, block, mutex, threadcreate.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
