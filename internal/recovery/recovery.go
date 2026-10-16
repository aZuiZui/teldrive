package recovery

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// recovery retries a call that failed with a connection error (not an RPC error) until it
// succeeds, the call's own context ends, the middleware's context ends, or its backoff gives up.
//
// Each call gets its own backoff, bound to the call's context. A single backoff shared by every
// call of a client was reset by each new call, so its MaxElapsedTime never ran out while others
// kept calling; and a call whose own context had ended (the player hung up) still counted as
// recoverable as long as the middleware's context lived. For a client per request those were the
// same context; for a long-lived client (tgc.StreamPool, ClientPool) a call could retry for good,
// here an auth.exportAuthorization holding gotd's sub-connection lock, with every read of that
// bot queued behind it (thousands of goroutines, gigabytes of buffers, the OOM killer).
type recovery struct {
	ctx        context.Context
	newBackoff func() backoff.BackOff
}

func New(ctx context.Context, newBackoff func() backoff.BackOff) telegram.Middleware {
	return &recovery{
		ctx:        ctx,
		newBackoff: newBackoff,
	}
}

func (r *recovery) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return backoff.Retry(func() error {
			if err := next.Invoke(ctx, input, output); err != nil {
				if r.shouldRecover(ctx, err) {
					return errors.Wrap(err, "recover")
				}

				return backoff.Permanent(err)
			}

			return nil
		}, backoff.WithContext(r.newBackoff(), ctx))
	}
}

func (r *recovery) shouldRecover(ctx context.Context, err error) bool {
	if ctx.Err() != nil || r.ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	_, ok := tgerr.As(err)

	return !ok
}
