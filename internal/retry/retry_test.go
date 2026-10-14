package retry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

type invokerFunc func(ctx context.Context, input bin.Encoder, output bin.Decoder) error

func (f invokerFunc) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return f(ctx, input, output)
}

var _ tg.Invoker = invokerFunc(nil)

func init() { retryDelay = func(int) time.Duration { return 0 } }

func TestRetriesTelegramTimeout(t *testing.T) {
	calls := 0
	next := invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls++
		if calls < 3 {
			return tgerr.New(-503, "Timeout")
		}
		return nil
	})
	if err := New(5).Handle(next)(context.Background(), nil, nil); err != nil {
		t.Fatalf("err = %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestGivesUpWithLastError(t *testing.T) {
	calls := 0
	next := invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls++
		return tgerr.New(-503, "Timeout")
	})
	err := New(3).Handle(next)(context.Background(), nil, nil)
	if calls != 3 || err == nil || !strings.Contains(err.Error(), "Timeout") {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
}

func TestDoesNotRetryOtherErrors(t *testing.T) {
	calls := 0
	next := invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls++
		return tgerr.New(400, "FILE_REFERENCE_EXPIRED")
	})
	err := New(5).Handle(next)(context.Background(), nil, nil)
	if calls != 1 || err == nil || !strings.Contains(err.Error(), "retry middleware skip") {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
}

func TestMatchesPlainErrorText(t *testing.T) {
	if !isErrorMatch(errors.New("rpc error: connection dead")) {
		t.Fatal("connection dead should match")
	}
	if isErrorMatch(errors.New("FLOOD_WAIT_30")) {
		t.Fatal("flood wait should not match")
	}
}

func TestZeroMaxInvokesOnce(t *testing.T) {
	calls := 0
	next := invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error { calls++; return nil })
	if err := New(0).Handle(next)(context.Background(), nil, nil); err != nil || calls != 1 {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
}
