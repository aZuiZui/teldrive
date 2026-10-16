package tgc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
)

// fakeRunner counts client starts; each client is ready at once and runs until its ctx ends
// (or fail returns an error for that start).
type fakeRunner struct {
	starts atomic.Int32
	fail   func(n int32) error
}

func (f *fakeRunner) run(ctx context.Context, token string, ready func(*WarmClient)) error {
	n := f.starts.Add(1)
	if f.fail != nil {
		if err := f.fail(n); err != nil {
			return err
		}
	}
	ready(&WarmClient{Client: &telegram.Client{}})
	<-ctx.Done()
	return ctx.Err()
}

func TestStreamPoolReusesTheBotsClient(t *testing.T) {
	f := &fakeRunner{}
	p := NewStreamPool(f.run, time.Hour)
	defer p.Close(context.Background())

	var wg sync.WaitGroup
	clients := make([]*WarmClient, 20)
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, release, err := p.Acquire(context.Background(), "111:bot")
			if err != nil {
				t.Error(err)
				return
			}
			clients[i] = c
			release()
		}(i)
	}
	wg.Wait()
	if got := f.starts.Load(); got != 1 {
		t.Fatalf("20 requests for one bot started %d clients, want 1", got)
	}
	for _, c := range clients[1:] {
		if c != clients[0] {
			t.Fatal("requests got different clients for the same bot")
		}
	}
	if _, release, err := p.Acquire(context.Background(), "222:bot"); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	if f.starts.Load() != 2 || p.Running() != 2 {
		t.Fatalf("a second bot: starts %d running %d, want 2 and 2", f.starts.Load(), p.Running())
	}
}

func TestStreamPoolClosesIdleClients(t *testing.T) {
	f := &fakeRunner{}
	p := NewStreamPool(f.run, 30*time.Minute)
	defer p.Close(context.Background())
	now := time.Now()
	p.now = func() time.Time { return now }

	_, release, err := p.Acquire(context.Background(), "111:bot")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	p.reapIdle()
	if p.Running() != 1 {
		t.Fatal("a client in use was closed")
	}
	release()
	now = now.Add(10 * time.Minute)
	p.reapIdle()
	if p.Running() != 1 {
		t.Fatal("closed before the idle time")
	}
	now = now.Add(30 * time.Minute)
	p.reapIdle()
	if p.Running() != 0 {
		t.Fatal("an idle client wasn't closed")
	}
	if _, release, err := p.Acquire(context.Background(), "111:bot"); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	if f.starts.Load() != 2 {
		t.Fatalf("after the idle close the next request should start a new client: starts %d", f.starts.Load())
	}
}

func TestStreamPoolRestartsAStoppedClient(t *testing.T) {
	boom := errors.New("AUTH_KEY_UNREGISTERED")
	f := &fakeRunner{fail: func(n int32) error {
		if n == 1 {
			return boom
		}
		return nil
	}}
	p := NewStreamPool(f.run, time.Hour)
	defer p.Close(context.Background())

	if _, _, err := p.Acquire(context.Background(), "111:bot"); !errors.Is(err, boom) {
		t.Fatalf("first start fails: got %v, want %v", err, boom)
	}
	if _, release, err := p.Acquire(context.Background(), "111:bot"); err != nil {
		t.Fatalf("the next request should start a fresh client: %v", err)
	} else {
		release()
	}
	if f.starts.Load() != 2 {
		t.Fatalf("starts %d, want 2", f.starts.Load())
	}
}

func TestStreamPoolAcquireStopsWithTheRequest(t *testing.T) {
	slow := func(ctx context.Context, token string, ready func(*WarmClient)) error {
		<-ctx.Done() // never ready
		return ctx.Err()
	}
	p := NewStreamPool(slow, time.Hour)
	defer p.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := p.Acquire(ctx, "111:bot"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the request's deadline", err)
	}
}

func TestStreamPoolClose(t *testing.T) {
	f := &fakeRunner{}
	p := NewStreamPool(f.run, time.Hour)
	if _, release, err := p.Acquire(context.Background(), "111:bot"); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Acquire(context.Background(), "111:bot"); !errors.Is(err, ErrStreamPoolClosed) {
		t.Fatalf("after Close: got %v", err)
	}
}
