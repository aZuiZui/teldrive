package services

import (
	"encoding/json"
	"net"
	"net/http"

	"github.com/tgdrive/teldrive/internal/reader"
)

// RefusedPath serves the parts Telegram refused recently (reader.RefusedParts), for phimbo-hub
// through `teldrive refused`. Only to requests from this machine that no proxy passed on: the
// list names files, and the API port can be published (Contabo's is behind Caddy).
const RefusedPath = "/api/stream/refused"

func serveRefused(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() || r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("X-Real-Ip") != "" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"parts": reader.RefusedParts()})
}
