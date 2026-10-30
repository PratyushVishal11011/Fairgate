package chaos

import (
	"encoding/json"
	"net/http"
)

// Handler serves a small control API for a running proxy:
//
//	GET  /faults      current faults
//	PUT  /faults      replace the faults with the JSON body, e.g. {"latency":"50ms","reset_probability":0.01}
//	POST /disconnect  reset every open connection
//	GET  /stats       connection and byte counters
func (p *Proxy) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /faults", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, p.Faults())
	})
	mux.HandleFunc("PUT /faults", func(w http.ResponseWriter, r *http.Request) {
		var f Faults
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&f); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := p.SetFaults(f); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, f)
	})
	mux.HandleFunc("POST /disconnect", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]int{"disconnected": p.DisconnectAll()})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, p.Stats())
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
