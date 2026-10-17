package cmd

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProbeSpotsReadsFirstAndLastBlock(t *testing.T) {
	var offsets []int64
	fetch := func(_ context.Context, offset int64) error { offsets = append(offsets, offset); return nil }
	if err := probeSpots(context.Background(), fetch, 10_000, time.Second, 3); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 8192 {
		t.Fatalf("offsets = %v", offsets)
	}
}

func TestProbeSpotsSmallPartReadsOnce(t *testing.T) {
	calls := 0
	fetch := func(context.Context, int64) error { calls++; return nil }
	if err := probeSpots(context.Background(), fetch, probeBlock, time.Second, 3); err != nil || calls != 1 {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
}

func TestProbeSpotsRetriesBeforeFailing(t *testing.T) {
	calls := 0
	fetch := func(context.Context, int64) error {
		calls++
		if calls < 3 {
			return errors.New("rpc error code -503: Timeout")
		}
		return nil
	}
	if err := probeSpots(context.Background(), fetch, 100, time.Second, 3); err != nil || calls != 3 {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
}

func TestProbeSpotsFailsWhenEveryAttemptFails(t *testing.T) {
	calls := 0
	fetch := func(context.Context, int64) error { calls++; return errors.New("rpc error code -503: Timeout") }
	err := probeSpots(context.Background(), fetch, 100, time.Second, 2)
	if err == nil || calls != 2 {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
}

func TestLoginFailureNamesTheFloodWait(t *testing.T) {
	cases := map[string]string{
		"offset 0: create connection to DC 1: transfer: export auth to 1: flood wait argument is too big (28m25s > 5s): retry middleware skip: rpc error code 420: FLOOD_WAIT (1705)": "FLOOD_WAIT (1705)",
		"offset 0: create connection to DC 1: transfer: onTransfer: export to 1: export auth to 1: context deadline exceeded":                                                         "timed out",
		"offset 0: create connection to DC 5: dial: refused": "login failed",
	}
	for err, want := range cases {
		if got := loginFailure(err); got != want {
			t.Errorf("loginFailure(%q) = %q, want %q", err, got, want)
		}
	}
}

func TestFloodWaitedMovesToTheNextAccount(t *testing.T) {
	flood := probeFileResult{Parts: []probePartResult{{OK: true}, {Error: "offset 0: flood wait argument is too big (9s > 5s): rpc error code 420: FLOOD_WAIT (9)"}}}
	login := probeFileResult{Parts: []probePartResult{{Error: "not checked: no login to DC 1 (FLOOD_WAIT (1639))"}}}
	refused := probeFileResult{Parts: []probePartResult{{Error: "offset 0: rpc error code -503: Timeout"}}}
	fine := probeFileResult{OK: true, Parts: []probePartResult{{OK: true}}}
	for name, c := range map[string]struct {
		r    probeFileResult
		want bool
	}{"read flood wait": {flood, true}, "DC login flood wait": {login, true}, "-503": {refused, false}, "readable": {fine, false}} {
		if got := floodWaited(c.r); got != c.want {
			t.Errorf("%s: floodWaited = %v, want %v", name, got, c.want)
		}
	}
}
