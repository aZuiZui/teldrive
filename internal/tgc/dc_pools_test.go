package tgc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// fakeInvoker answers every call with err and counts calls and Close.
type fakeInvoker struct {
	dc     int
	calls  atomic.Int32
	closed atomic.Bool
	err    error
}

func (f *fakeInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	f.calls.Add(1)
	return f.err
}

func (f *fakeInvoker) Close() error { f.closed.Store(true); return nil }

type fakeOpener struct {
	mu     sync.Mutex
	opened []*fakeInvoker
	opens  atomic.Int32
	block  chan struct{} // if set, open waits for it
	fail   func(dc int) error
	invErr error
}

func (f *fakeOpener) open(ctx context.Context, dc int, size int64) (telegram.CloseInvoker, error) {
	f.opens.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.fail != nil {
		if err := f.fail(dc); err != nil {
			return nil, err
		}
	}
	inv := &fakeInvoker{dc: dc, err: f.invErr}
	f.mu.Lock()
	f.opened = append(f.opened, inv)
	f.mu.Unlock()
	return inv, nil
}

func getFile(t *testing.T, c *tg.Client) error {
	t.Helper()
	_, err := c.UploadGetFile(context.Background(), &tg.UploadGetFileRequest{Location: &tg.InputDocumentFileLocation{ID: 1}, Limit: 1024})
	return err
}

func TestDCPoolsOpensEachDCOnce(t *testing.T) {
	f := &fakeOpener{block: make(chan struct{})}
	fallback := tg.NewClient(&fakeInvoker{})
	p := newDCPools(context.Background(), 4, f.open, fallback)
	defer p.Close()

	var wg sync.WaitGroup
	apis := make([]*tg.Client, 30)
	for i := range apis {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			api, err := p.API(context.Background(), 4)
			if err != nil {
				t.Error(err)
			}
			apis[i] = api
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(f.block)
	wg.Wait()
	if got := f.opens.Load(); got != 1 {
		t.Fatalf("30 reads of DC 4 opened it %d times, want 1", got)
	}
	for _, api := range apis {
		if api != apis[0] || api == fallback {
			t.Fatal("reads of one DC got different clients, or the fallback")
		}
	}
	if _, err := p.API(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if got := f.opens.Load(); got != 2 {
		t.Fatalf("a second DC: %d opens, want 2", got)
	}
	if p.Open() != 2 {
		t.Fatalf("Open() = %d, want 2", p.Open())
	}
}

func TestDCPoolsReadsGoThroughTheDCAndMiddlewares(t *testing.T) {
	f := &fakeOpener{}
	var seen atomic.Int32
	mw := telegram.MiddlewareFunc(func(next tg.Invoker) telegram.InvokeFunc {
		return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			seen.Add(1)
			return next.Invoke(ctx, input, output)
		}
	})
	fb := &fakeInvoker{}
	p := newDCPools(context.Background(), 4, f.open, tg.NewClient(fb), mw)
	defer p.Close()

	api, _ := p.API(context.Background(), 4)
	_ = getFile(t, api)
	if f.opened[0].calls.Load() != 1 || fb.calls.Load() != 0 {
		t.Fatal("the read didn't go to DC 4's connections")
	}
	if seen.Load() != 1 {
		t.Fatal("the read skipped the client's middlewares")
	}
}

func TestDCPoolsFallBackWhenADCFailsAndRetryLater(t *testing.T) {
	failing := true
	var mu sync.Mutex
	f := &fakeOpener{fail: func(int) error {
		mu.Lock()
		defer mu.Unlock()
		if failing {
			return errors.New("FLOOD_WAIT")
		}
		return nil
	}}
	fallback := tg.NewClient(&fakeInvoker{})
	p := newDCPools(context.Background(), 4, f.open, fallback)
	defer p.Close()
	now := time.Unix(1000, 0)
	p.now = func() time.Time { return now }

	for range 3 {
		api, err := p.API(context.Background(), 4)
		if err != nil || api != fallback {
			t.Fatalf("a DC that failed to open: got %v, %v; want the fallback", api, err)
		}
	}
	if got := f.opens.Load(); got != 1 {
		t.Fatalf("reads within retry-after opened the DC %d times, want 1", got)
	}

	mu.Lock()
	failing = false
	mu.Unlock()
	now = now.Add(p.retryAfter)
	api, _ := p.API(context.Background(), 4)
	if api == fallback || f.opens.Load() != 2 {
		t.Fatalf("after retry-after the DC wasn't opened again (opens %d)", f.opens.Load())
	}
}

func TestDCPoolsWaitEndsWithTheRequest(t *testing.T) {
	f := &fakeOpener{block: make(chan struct{})}
	p := newDCPools(context.Background(), 4, f.open, tg.NewClient(&fakeInvoker{}))
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.API(ctx, 4); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the request's deadline", err)
	}
	// The open goes on for the others: it isn't bound to the request that started it.
	close(f.block)
	api, err := p.API(context.Background(), 4)
	if err != nil || api == nil || f.opens.Load() != 1 {
		t.Fatalf("after the first request went away: %v, opens %d", err, f.opens.Load())
	}
}

func TestDCPoolsDropADCThatLostItsAuthorization(t *testing.T) {
	f := &fakeOpener{invErr: tgerr.New(401, "AUTH_KEY_UNREGISTERED")}
	p := newDCPools(context.Background(), 4, f.open, tg.NewClient(&fakeInvoker{}))
	defer p.Close()

	api, _ := p.API(context.Background(), 4)
	if err := getFile(t, api); err == nil {
		t.Fatal("want the 401")
	}
	if !f.opened[0].closed.Load() {
		t.Fatal("the DC's connections weren't closed")
	}
	_, _ = p.API(context.Background(), 4)
	if got := f.opens.Load(); got != 2 {
		t.Fatalf("the next read opened the DC %d times in all, want 2", got)
	}
}

func TestDCPoolsCloseClosesEveryDC(t *testing.T) {
	f := &fakeOpener{}
	fallback := tg.NewClient(&fakeInvoker{})
	p := newDCPools(context.Background(), 4, f.open, fallback)
	_, _ = p.API(context.Background(), 2)
	_, _ = p.API(context.Background(), 4)
	p.Close()
	for _, inv := range f.opened {
		if !inv.closed.Load() {
			t.Fatalf("DC %d left open", inv.dc)
		}
	}
	if api, _ := p.API(context.Background(), 4); api != fallback {
		t.Fatal("after Close a read should get the fallback")
	}
}

func TestDCPoolsUnknownDCAndNoConnectionsUseTheFallback(t *testing.T) {
	f := &fakeOpener{}
	fallback := tg.NewClient(&fakeInvoker{})
	for _, tc := range []struct{ size, dc int }{{4, 0}, {0, 4}} {
		p := newDCPools(context.Background(), tc.size, f.open, fallback)
		if api, _ := p.API(context.Background(), tc.dc); api != fallback {
			t.Fatalf("size %d dc %d: want the fallback", tc.size, tc.dc)
		}
		p.Close()
	}
	if f.opens.Load() != 0 {
		t.Fatal("nothing should have been opened")
	}
}
