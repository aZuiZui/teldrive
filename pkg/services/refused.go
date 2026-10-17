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

// refusedRoute is RefusedPath as the API handler sees it (cmd/run.go mounts it under /api with
// StripPrefix).
const refusedRoute = "/stream/refused"

// proxyHeaders: chi's RealIP takes the client address from these, so a request carrying any of
// them isn't trusted to come from this machine (one sent straight to the port could claim
// 127.0.0.1 in them).
var proxyHeaders = []string{"X-Forwarded-For", "X-Real-Ip", "True-Client-Ip", "Forwarded"}

func serveRefused(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		http.NotFound(w, r)
		return
	}
	for _, h := range proxyHeaders {
		if r.Header.Get(h) != "" {
			http.NotFound(w, r)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"parts": reader.RefusedParts()})
}
