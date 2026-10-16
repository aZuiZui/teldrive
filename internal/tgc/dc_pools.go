package tgc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/logging"
	"go.uber.org/zap"
)

const (
	dcPoolOpenTimeout = 30 * time.Second // logging in to a DC (export/import authorization)
	dcPoolRetryAfter  = 30 * time.Second // after a DC failed to open, use the fallback this long
)

var errDCPoolsClosed = errors.New("dc pools closed")

// DCOpener opens a multi-connection invoker to a DC, logged in with the client's authorization.
type DCOpener func(ctx context.Context, dc int, size int64) (telegram.CloseInvoker, error)

// DCPools gives a warm streaming client several connections to each DC it reads files from.
// Without it, gotd sends a read for a file on another DC (FILE_MIGRATE) through one
// sub-connection, which serves one request at a time (a pool.DC of max 1). A warm bot then
// fetched one chunk at a time for every stream it served, and a scan's reads queued behind it,
// holding memory. Each DC is opened once, on first use, with size connections and the client's
// middlewares (rate limit, flood-wait standby, recovery). If a DC can't be opened, reads use the
// client's own API (gotd's single sub-connection) and the DC is tried again after a while.
type DCPools struct {
	ctx         context.Context
	size        int64
	open        DCOpener
	fallback    *tg.Client
	middlewares []telegram.Middleware
	timeout     time.Duration
	retryAfter  time.Duration
	now         func() time.Time
	logger      *zap.Logger

	mu     sync.Mutex
	dcs    map[int]*dcPool
	closed bool
}

type dcPool struct {
	ready    chan struct{} // closed once api or err is set
	api      *tg.Client
	inv      telegram.CloseInvoker
	err      error
	failedAt time.Time
}

// NewDCPools returns the DC pools of a running, logged-in client. ctx is the client's run
// context: the pools' connections live as long as the client.
func NewDCPools(ctx context.Context, client *telegram.Client, size int, middlewares ...telegram.Middleware) *DCPools {
	home := client.Config().ThisDC
	open := func(ctx context.Context, dc int, size int64) (telegram.CloseInvoker, error) {
		if dc == home {
			return client.Pool(size)
		}
		return client.DC(ctx, dc, size)
	}
	return newDCPools(ctx, size, open, client.API(), middlewares...)
}

func newDCPools(ctx context.Context, size int, open DCOpener, fallback *tg.Client, middlewares ...telegram.Middleware) *DCPools {
	return &DCPools{
		ctx: ctx, size: int64(size), open: open, fallback: fallback, middlewares: middlewares,
		timeout: dcPoolOpenTimeout, retryAfter: dcPoolRetryAfter, now: time.Now,
		logger: logging.Component("TG"), dcs: map[int]*dcPool{},
	}
}

// API returns the client for reading files stored on dc, opening that DC's connections on first
// use. It only fails when ctx ends first; dc 0 (unknown) gets the fallback.
func (p *DCPools) API(ctx context.Context, dc int) (*tg.Client, error) {
	if dc <= 0 || p.size < 1 {
		return p.fallback, nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return p.fallback, nil
	}
	e := p.dcs[dc]
	if e != nil && e.err != nil && p.now().Sub(e.failedAt) >= p.retryAfter {
		e = nil
	}
	if e == nil {
		e = &dcPool{ready: make(chan struct{})}
		p.dcs[dc] = e
		go p.start(dc, e)
	}
	p.mu.Unlock()

	select {
	case <-e.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if e.api == nil {
		return p.fallback, nil
	}
	return e.api, nil
}

// start opens dc's connections. It runs on the pools' context, not a request's, so a request that
// goes away doesn't fail the login for the others waiting on it.
func (p *DCPools) start(dc int, e *dcPool) {
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	inv, err := p.open(ctx, dc, p.size)

	p.mu.Lock()
	if err == nil && p.closed {
		_ = inv.Close()
		err = errDCPoolsClosed
	}
	if err != nil {
		e.err = err
		e.failedAt = p.now()
	} else {
		e.inv = inv
		var invoker tg.Invoker = &dcPoolInvoker{next: inv, pools: p, dc: dc, entry: e}
		for i := len(p.middlewares) - 1; i >= 0; i-- {
			invoker = p.middlewares[i].Handle(invoker)
		}
		e.api = tg.NewClient(invoker)
	}
	p.mu.Unlock()
	close(e.ready)

	if err != nil && !errors.Is(err, errDCPoolsClosed) {
		p.logger.Warn("stream.dc_pool_failed", zap.Int("dc", dc), zap.Error(err))
	} else if err == nil {
		p.logger.Debug("stream.dc_pool_opened", zap.Int("dc", dc), zap.Int64("connections", p.size))
	}
}

// drop closes dc's connections after its authorization stopped working; the next read opens it again.
func (p *DCPools) drop(dc int, e *dcPool) {
	p.mu.Lock()
	if p.dcs[dc] != e {
		p.mu.Unlock()
		return
	}
	delete(p.dcs, dc)
	p.mu.Unlock()
	p.logger.Info("stream.dc_pool_dropped", zap.Int("dc", dc))
	_ = e.inv.Close()
}

// Open is the number of DCs with open (or opening) connections.
func (p *DCPools) Open() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.dcs {
		if e.err == nil {
			n++
		}
	}
	return n
}

// Close closes every DC's connections; later reads get the fallback.
func (p *DCPools) Close() {
	p.mu.Lock()
	p.closed = true
	dcs := p.dcs
	p.dcs = map[int]*dcPool{}
	p.mu.Unlock()
	for _, e := range dcs {
		select {
		case <-e.ready:
			if e.inv != nil {
				_ = e.inv.Close()
			}
		default: // still opening: start sees closed and closes it
		}
	}
}

// dcPoolInvoker drops its DC when Telegram says the authorization there is gone.
type dcPoolInvoker struct {
	next  tg.Invoker
	pools *DCPools
	dc    int
	entry *dcPool
}

func (i *dcPoolInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	err := i.next.Invoke(ctx, input, output)
	if err != nil && auth.IsUnauthorized(err) {
		i.pools.drop(i.dc, i.entry)
	}
	return err
}
