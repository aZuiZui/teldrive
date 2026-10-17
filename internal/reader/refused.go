package reader

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/tgerr"
)

// refusedParts remembers parts Telegram refused (-503 Timeout even after the retries). While a
// part is blocked, a read of it fails at once: every read of a refused part costs the full retry
// time, and a client that retries itself (rclone: --low-level-retries) would hang for minutes.
//
// Most refusals pass on their own (Telegram recovers within the hour), so the block starts short
// (tg.stream.refused-part-first, 2m) and doubles with each refusal of the same part, up to
// tg.stream.refused-part-ttl (10m). A read that works clears the part; one not refused again for
// refusalMemory is forgotten. The list is what `teldrive refused` reports, for phimbo-hub.
type refusedParts struct {
	mu    sync.Mutex
	parts map[string]*refusal
	now   func() time.Time
}

// RefusedPart is one part Telegram refused recently, as `teldrive refused` reports it.
type RefusedPart struct {
	FileID       string    `json:"fileId"`
	FileName     string    `json:"fileName"`
	Part         int       `json:"part"` // 1-based
	MessageID    int64     `json:"messageId"`
	ChannelID    int64     `json:"channelId"`
	Refusals     int       `json:"refusals"` // in a row, each within refusalMemory of the last
	First        time.Time `json:"first"`
	Last         time.Time `json:"last"`
	BlockedUntil time.Time `json:"blockedUntil"`
	Error        string    `json:"error"`
}

// partInfo says which part a chunk source reads, for the report.
type partInfo struct {
	fileID, fileName string
	part             int
	messageID        int64
	channelID        int64
}

type refusal struct {
	RefusedPart
	err error
}

// refusalMemory: a part not refused again for this long starts over (and leaves the report).
const refusalMemory = time.Hour

var refused = &refusedParts{parts: map[string]*refusal{}, now: time.Now}

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

// check returns an error while the part is blocked.
func (r *refusedParts) check(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	got, ok := r.parts[key]
	if !ok {
		return nil
	}
	now := r.now()
	if !now.Before(got.BlockedUntil) {
		return nil // asked again; the history stays until a read works or it's forgotten
	}
	return fmt.Errorf("refused by Telegram %s ago (%d time(s)), not asked again for %s: %w",
		now.Sub(got.Last).Round(time.Second), got.Refusals, got.BlockedUntil.Sub(now).Round(time.Second), got.err)
}

// remember notes a refusal of the part: blocked for first, doubled per refusal in a row, up to max.
func (r *refusedParts) remember(key string, info partInfo, err error, first, max time.Duration) {
	if max <= 0 || !isRefusal(err) {
		return
	}
	if first <= 0 || first > max {
		first = max
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.dropOld(now)
	got, ok := r.parts[key]
	if !ok {
		got = &refusal{RefusedPart: RefusedPart{First: now}}
		r.parts[key] = got
	}
	got.FileID, got.FileName, got.Part, got.MessageID, got.ChannelID =
		info.fileID, info.fileName, info.part, info.messageID, info.channelID
	if ok && now.Before(got.BlockedUntil) {
		// Another chunk of the part refused while it's blocked (a stream reads several at once):
		// the same refusal, not one more.
		got.Last, got.err, got.Error = now, err, err.Error()
		return
	}
	got.Refusals++
	got.Last = now
	got.err = err
	got.Error = err.Error()
	block := first
	for i := 1; i < got.Refusals && block < max; i++ {
		block *= 2
	}
	got.BlockedUntil = now.Add(min(block, max))
}

// forget: a read of the part worked.
func (r *refusedParts) forget(key string) {
	r.mu.Lock()
	delete(r.parts, key)
	r.mu.Unlock()
}

func (r *refusedParts) dropOld(now time.Time) {
	for k, v := range r.parts {
		if now.Sub(v.Last) >= refusalMemory {
			delete(r.parts, k)
		}
	}
}

func (r *refusedParts) list() []RefusedPart {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropOld(r.now())
	out := make([]RefusedPart, 0, len(r.parts))
	for _, v := range r.parts {
		out = append(out, v.RefusedPart)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Last.After(out[j].Last) })
	return out
}

// RefusedParts is the parts Telegram refused within the last hour, newest first.
func RefusedParts() []RefusedPart { return refused.list() }
