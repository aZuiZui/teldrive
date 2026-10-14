package reader

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/tgerr"
)

// refusedParts remembers parts Telegram refused (-503 Timeout even after the retries) for a
// while. Such a part is usually refused for good, and every read of it costs the full retry
// time; a client that retries itself (rclone: --low-level-retries) then hangs for minutes.
// While remembered, a read of the part fails at once; a read that works clears it.
type refusedParts struct {
	mu    sync.Mutex
	parts map[string]refusal
	now   func() time.Time
}

type refusal struct {
	at  time.Time
	ttl time.Duration
	err error
}

var refused = &refusedParts{parts: map[string]refusal{}, now: time.Now}

func refusedKey(channelID, partID int64) string { return fmt.Sprintf("%d:%d", channelID, partID) }

// isRefusal: Telegram's -503 (a file DC that can't read the document in time).
func isRefusal(err error) bool {
	if err == nil {
		return false
	}
	var rpc *tgerr.Error
	if errors.As(err, &rpc) && rpc.Code == -503 {
		return true
	}
	return strings.Contains(err.Error(), "rpc error code -503")
}

// check returns an error when the part was refused within its ttl.
func (r *refusedParts) check(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	got, ok := r.parts[key]
	if !ok {
		return nil
	}
	age := r.now().Sub(got.at)
	if age >= got.ttl {
		delete(r.parts, key)
		return nil
	}
	return fmt.Errorf("refused by Telegram %s ago, not asked again for %s: %w",
		age.Round(time.Second), (got.ttl - age).Round(time.Second), got.err)
}

func (r *refusedParts) remember(key string, err error, ttl time.Duration) {
	if ttl <= 0 || !isRefusal(err) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for k, v := range r.parts { // keep the map small
		if now.Sub(v.at) >= v.ttl {
			delete(r.parts, k)
		}
	}
	r.parts[key] = refusal{at: now, ttl: ttl, err: err}
}

func (r *refusedParts) forget(key string) {
	r.mu.Lock()
	delete(r.parts, key)
	r.mu.Unlock()
}
