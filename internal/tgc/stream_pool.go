package tgc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
)

// ErrStreamPoolClosed is returned by Acquire after Close.
var ErrStreamPoolClosed = errors.New("stream client pool is closed")

// StreamClientRunner runs one bot's client until ctx ends: it connects, logs the bot in, calls
// ready once the client can be used, then keeps it running. It returns when the client stops.
type StreamClientRunner func(ctx context.Context, token string, ready func(*telegram.Client)) error

// StreamPool keeps one running client per streaming bot, shared by every stream request,
// instead of a new client per request. A new client has to log in to the DC that holds the file
// (auth.exportAuthorization / importAuthorization) before it can read; Telegram rate-limits
// those, so a Plex scan opening hundreds of files ran the bots into FLOOD_WAIT and
// AUTH_BYTES_INVALID. A pooled client logs in to each DC once and keeps the connection.
// Clients unused for the idle time are closed; one that stops (an error, a lost login) is
// dropped and the next request starts a fresh one.
type StreamPool struct {
	run  StreamClientRunner
	idle time.Duration
	now  func() time.Time

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	clients map[string]*pooledClient
	closed  bool
}

type pooledClient struct {
	token    string
	cancel   context.CancelFunc
	ready    chan struct{} // closed once client or err is set
	done     chan struct{} // closed when the client has stopped
	client   *telegram.Client
	err      error
	refs     int
	lastUsed time.Time
}

// NewStreamPool starts the idle reaper; Close stops it and every client.
func NewStreamPool(run StreamClientRunner, idle time.Duration) *StreamPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &StreamPool{run: run, idle: idle, now: time.Now, ctx: ctx, cancel: cancel, clients: map[string]*pooledClient{}}
	if idle > 0 {
		go p.reaper(min(idle, time.Minute))
	}
	return p
}

// Acquire returns the running client of the bot with this token, starting it if needed, and a
// release func to call when the request is done with it. It waits for the client to be ready
// (or ctx to end). The client is shared: use it, don't Run or stop it.
func (p *StreamPool) Acquire(ctx context.Context, token string) (*telegram.Client, func(), error) {
	pc, err := p.lease(token)
	if err != nil {
		return nil, nil, err
	}
	release := sync.OnceFunc(func() { p.release(pc) })
	select {
	case <-pc.ready:
	case <-ctx.Done():
		release()
		return nil, nil, ctx.Err()
	}
	p.mu.Lock()
	client, runErr := pc.client, pc.err
	p.mu.Unlock()
	if runErr != nil || client == nil {
		release()
		if runErr == nil {
			runErr = errors.New("stream client stopped")
		}
		return nil, nil, runErr
	}
	return client, release, nil
}

func (p *StreamPool) lease(token string) (*pooledClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrStreamPoolClosed
	}
	if pc := p.clients[token]; pc != nil && pc.err == nil {
		pc.refs++
		pc.lastUsed = p.now()
		return pc, nil
	}
	runCtx, cancel := context.WithCancel(p.ctx)
	pc := &pooledClient{token: token, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}),
		refs: 1, lastUsed: p.now()}
	p.clients[token] = pc
	go p.start(runCtx, pc)
	return pc, nil
}

func (p *StreamPool) start(ctx context.Context, pc *pooledClient) {
	readyOnce := sync.Once{}
	err := p.run(ctx, pc.token, func(c *telegram.Client) {
		p.mu.Lock()
		pc.client = c
		p.mu.Unlock()
		readyOnce.Do(func() { close(pc.ready) })
	})
	if err == nil {
		err = errors.New("stream client stopped")
	}
	p.mu.Lock()
	pc.client = nil
	pc.err = err
	if p.clients[pc.token] == pc {
		delete(p.clients, pc.token)
	}
	p.mu.Unlock()
	readyOnce.Do(func() { close(pc.ready) })
	close(pc.done)
}

func (p *StreamPool) release(pc *pooledClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pc.refs > 0 {
		pc.refs--
	}
	pc.lastUsed = p.now()
}

func (p *StreamPool) reaper(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
			p.reapIdle()
		}
	}
}

// reapIdle stops the clients nobody used for the idle time.
func (p *StreamPool) reapIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for token, pc := range p.clients {
		if pc.refs == 0 && now.Sub(pc.lastUsed) >= p.idle {
			delete(p.clients, token)
			pc.cancel()
		}
	}
}

// Running is the number of bots with a running (or starting) client.
func (p *StreamPool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.clients)
}

// Close stops every client and waits for them (or ctx).
func (p *StreamPool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	clients := make([]*pooledClient, 0, len(p.clients))
	for _, pc := range p.clients {
		clients = append(clients, pc)
	}
	p.clients = map[string]*pooledClient{}
	p.mu.Unlock()
	p.cancel()
	for _, pc := range clients {
		select {
		case <-pc.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
