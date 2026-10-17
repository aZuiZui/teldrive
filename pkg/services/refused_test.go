package services

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

func TestRefusedOnlyAnswersThisMachine(t *testing.T) {
	for _, c := range []struct {
		remote, header string
		want           int
	}{
		{"127.0.0.1:5000", "", http.StatusOK},
		{"[::1]:5000", "", http.StatusOK},
		{"192.168.0.10:5000", "", http.StatusNotFound},     // the LAN
		{"172.20.0.18:5000", "", http.StatusNotFound},      // another container
		{"127.0.0.1:5000", "1.2.3.4", http.StatusNotFound}, // passed on by a proxy
	} {
		req := httptest.NewRequest(http.MethodGet, RefusedPath, nil)
		req.RemoteAddr = c.remote
		if c.header != "" {
			req.Header.Set("X-Forwarded-For", c.header)
		}
		rec := httptest.NewRecorder()
		serveRefused(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s (forwarded %q): %d, want %d", c.remote, c.header, rec.Code, c.want)
		}
		if c.want == http.StatusOK && !strings.Contains(rec.Body.String(), `"parts":`) {
			t.Errorf("body = %q", rec.Body.String())
		}
	}
}

// Through the real router: chi's RealIP would turn a True-Client-IP header into the client
// address, and the API is mounted under /api with StripPrefix.
func TestRefusedThroughTheRouter(t *testing.T) {
	mux := chi.NewRouter()
	mux.Use(chimiddleware.RealIP)
	mux.Mount("/api/", http.StripPrefix("/api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == refusedRoute {
			serveRefused(w, r)
			return
		}
		http.NotFound(w, r)
	})))
	for _, c := range []struct {
		remote, header, value string
		want                  int
	}{
		{"127.0.0.1:5000", "", "", http.StatusOK},
		{"192.168.0.10:5000", "True-Client-IP", "127.0.0.1", http.StatusNotFound},
		{"192.168.0.10:5000", "X-Real-IP", "127.0.0.1", http.StatusNotFound},
		{"192.168.0.10:5000", "X-Forwarded-For", "127.0.0.1", http.StatusNotFound},
	} {
		req := httptest.NewRequest(http.MethodGet, RefusedPath, nil)
		req.RemoteAddr = c.remote
		if c.header != "" {
			req.Header.Set(c.header, c.value)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s with %s %q: %d, want %d", c.remote, c.header, c.value, rec.Code, c.want)
		}
	}
}
