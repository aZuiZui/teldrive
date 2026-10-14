package reader

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
)

func TestRefusedPartsRemembersTelegramTimeout(t *testing.T) {
	now := time.Unix(1000, 0)
	r := &refusedParts{parts: map[string]refusal{}, now: func() time.Time { return now }}
	key := refusedKey(3848110608, 539793)
	telegram := tgerr.New(-503, "Timeout")

	r.remember(key, telegram, 10*time.Minute)
	err := r.check(key)
	if err == nil || !errors.Is(err, telegram) || !strings.Contains(err.Error(), "not asked again for 10m0s") {
		t.Fatalf("check right after = %v", err)
	}
	now = now.Add(10 * time.Minute)
	if err := r.check(key); err != nil {
		t.Fatalf("check after the ttl = %v", err)
	}
	if len(r.parts) != 0 {
		t.Fatalf("expired entry kept: %v", r.parts)
	}
}

func TestRefusedPartsIgnoresOtherErrors(t *testing.T) {
	r := &refusedParts{parts: map[string]refusal{}, now: time.Now}
	for _, err := range []error{errors.New("context canceled"), tgerr.New(400, "FILE_REFERENCE_EXPIRED"), nil} {
		r.remember("k", err, time.Minute)
		if r.check("k") != nil {
			t.Fatalf("remembered %v", err)
		}
	}
	r.remember("k", tgerr.New(-503, "Timeout"), 0)
	if r.check("k") != nil {
		t.Fatal("ttl 0 must turn it off")
	}
}

func TestRefusedPartsForget(t *testing.T) {
	r := &refusedParts{parts: map[string]refusal{}, now: time.Now}
	r.remember("k", errors.New("retry limit reached after 5 attempts: rpcDoRequest: rpc error code -503: Timeout"), time.Minute)
	if r.check("k") == nil {
		t.Fatal("wrapped -503 text should be remembered")
	}
	r.forget("k")
	if r.check("k") != nil {
		t.Fatal("forget should clear it")
	}
}
