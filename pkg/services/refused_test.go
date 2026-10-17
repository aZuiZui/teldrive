package services

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
