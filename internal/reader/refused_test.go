package reader

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
)

func newRefused(now *time.Time) *refusedParts {
	return &refusedParts{parts: map[string]*refusal{}, now: func() time.Time { return *now }}
}

var part = partInfo{fileID: "019ec630-02d8-7849-b6e1-2803b1822e9a", fileName: "14_x.mkv", part: 1, messageID: 51429, channelID: 1}

func TestRefusedPartsBlockStartsShortAndDoubles(t *testing.T) {
	now := time.Unix(1000, 0)
	r := newRefused(&now)
	key := refusedKey(1, 51429)
	telegram := tgerr.New(-503, "Timeout")

	r.remember(key, part, telegram, 2*time.Minute, 10*time.Minute)
	err := r.check(key)
	if err == nil || !errors.Is(err, telegram) || !strings.Contains(err.Error(), "not asked again for 2m0s") {
		t.Fatalf("after the first refusal = %v, want blocked 2m", err)
	}
	for i, want := range []time.Duration{4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute} {
		now = now.Add(r.parts[key].BlockedUntil.Sub(now)) // the block runs out: asked again
		if err := r.check(key); err != nil {
			t.Fatalf("refusal %d: still blocked after its block: %v", i+1, err)
		}
		r.remember(key, part, telegram, 2*time.Minute, 10*time.Minute)
		if got := r.parts[key].BlockedUntil.Sub(now); got != want {
			t.Fatalf("refusal %d: blocked %s, want %s", i+2, got, want)
		}
	}
	if got := r.parts[key].Refusals; got != 5 {
		t.Fatalf("refusals = %d, want 5", got)
	}
}

func TestRefusedPartsChunksOfOneRefusalCountOnce(t *testing.T) {
	now := time.Unix(1000, 0)
	r := newRefused(&now)
	key := refusedKey(1, 51429)
	for range 4 { // a stream reads four chunks of the part at once; all are refused
		r.remember(key, part, tgerr.New(-503, "Timeout"), 2*time.Minute, 10*time.Minute)
	}
	if got := r.parts[key]; got.Refusals != 1 || got.BlockedUntil.Sub(now) != 2*time.Minute {
		t.Fatalf("four chunks of one refusal: %d refusals, blocked %s; want 1, 2m", got.Refusals, got.BlockedUntil.Sub(now))
	}
}

func TestRefusedPartsReportAndForgetting(t *testing.T) {
	now := time.Unix(1000, 0)
	r := newRefused(&now)
	r.remember(refusedKey(1, 1), part, errors.New("retry limit reached after 5 attempts: rpcDoRequest: rpc error code -503: Timeout"),
		2*time.Minute, 10*time.Minute)
	other := part
	other.fileID, other.messageID = "019ec630-2dcf-73b6-88cf-313248f135f8", 2
	now = now.Add(time.Minute)
	r.remember(refusedKey(1, 2), other, tgerr.New(-503, "Timeout"), 2*time.Minute, 10*time.Minute)

	got := r.list()
	if len(got) != 2 || got[0].MessageID != 2 || got[1].FileID != part.fileID || got[1].Part != 1 || got[1].Refusals != 1 {
		t.Fatalf("report = %+v, want both, newest first", got)
	}
	r.forget(refusedKey(1, 2)) // a read of it worked
	if got := r.list(); len(got) != 1 {
		t.Fatalf("after a read worked: %d left, want 1", len(got))
	}
	now = now.Add(refusalMemory)
	if got := r.list(); len(got) != 0 {
		t.Fatalf("an hour without refusals: %d left, want 0", len(got))
	}
}

func TestRefusedPartsIgnoresOtherErrors(t *testing.T) {
	now := time.Unix(1000, 0)
	r := newRefused(&now)
	for _, err := range []error{errors.New("context canceled"), tgerr.New(400, "FILE_REFERENCE_EXPIRED"), nil} {
		r.remember("k", part, err, time.Minute, time.Minute)
		if r.check("k") != nil {
			t.Fatalf("remembered %v", err)
		}
	}
	r.remember("k", part, tgerr.New(-503, "Timeout"), time.Minute, 0)
	if r.check("k") != nil {
		t.Fatal("refused-part-ttl 0 must turn it off")
	}
}
