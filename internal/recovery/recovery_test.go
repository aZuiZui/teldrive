package recovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tgerr"
)

type invokerFunc func(ctx context.Context, input bin.Encoder, output bin.Decoder) error

func (f invokerFunc) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return f(ctx, input, output)
}

func fastBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 5 * time.Millisecond
	b.MaxInterval = 20 * time.Millisecond
	b.MaxElapsedTime = time.Hour // only the contexts end these tests
	return b
}

var errNetwork = errors.New("connection reset by peer")

// The bug behind the warm-client leak: the middleware's context lives for good (a long-lived
// client), the call's own context ends (the player hung up): the call must stop retrying.
func TestRecoveryStopsWhenTheCallsContextEnds(t *testing.T) {
	mw := New(context.Background(), fastBackoff)
	var calls atomic.Int32
	h := mw.Handle(invokerFunc(func(ctx context.Context, _ bin.Encoder, _ bin.Decoder) error {
		calls.Add(1)
		if err := ctx.Err(); err != nil {
			return err
		}
		return errNetwork
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h(ctx, nil, nil) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("got nil, want an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("still retrying 2 s after the call's context ended (%d calls)", calls.Load())
	}
}

// A dropped connection fails a call that is still wanted with an error wrapping context.Canceled:
// it is retried; only the call's (or the middleware's) own context ending stops it.
func TestRecoveryRetriesADroppedConnection(t *testing.T) {
	mw := New(context.Background(), fastBackoff)
	var calls atomic.Int32
	dropped := fmt.Errorf("rpcDoRequest: retryUntilAck: engine forcibly closed: %w", context.Canceled)
	h := mw.Handle(invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		if calls.Add(1) < 3 {
			return dropped
		}
		return nil
	}))
	if err := h(context.Background(), nil, nil); err != nil || calls.Load() != 3 {
		t.Fatalf("err %v after %d calls, want success after 3", err, calls.Load())
	}
}

func TestRecoveryRetriesConnectionErrorsNotRPCErrors(t *testing.T) {
	mw := New(context.Background(), fastBackoff)
	var calls atomic.Int32
	h := mw.Handle(invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		if calls.Add(1) < 3 {
			return errNetwork
		}
		return nil
	}))
	if err := h(context.Background(), nil, nil); err != nil || calls.Load() != 3 {
		t.Fatalf("connection errors: err %v after %d calls, want success after 3", err, calls.Load())
	}
	calls.Store(0)
	rpc := tgerr.New(400, "AUTH_BYTES_INVALID")
	h = mw.Handle(invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls.Add(1)
		return rpc
	}))
	if err := h(context.Background(), nil, nil); !tgerr.Is(err, "AUTH_BYTES_INVALID") || calls.Load() != 1 {
		t.Fatalf("an RPC error: err %v after %d calls, want it at once", err, calls.Load())
	}
}

// Each call has its own backoff: one that gives up doesn't depend on others resetting a shared one.
func TestRecoveryEachCallHasItsOwnBackoff(t *testing.T) {
	limited := func() backoff.BackOff {
		b := fastBackoff().(*backoff.ExponentialBackOff)
		b.MaxElapsedTime = 150 * time.Millisecond
		return b
	}
	mw := New(context.Background(), limited)
	failing := mw.Handle(invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error { return errNetwork }))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ { // other calls keep starting (a shared backoff would be reset by each)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
					_ = failing(ctx, nil, nil)
					cancel()
				}
			}
		}()
	}
	start := time.Now()
	err := failing(context.Background(), nil, nil)
	close(stop)
	wg.Wait()
	if err == nil {
		t.Fatal("got nil, want the connection error")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("gave up after %v; its own 150 ms backoff should have ended it", took)
	}
}
