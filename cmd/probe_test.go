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
