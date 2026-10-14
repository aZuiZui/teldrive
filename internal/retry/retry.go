package retry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// internalErrors are Telegram server-side failures worth another attempt. "Timeout" is the
// -503 a file DC answers when it can't read a document in time.
var internalErrors = []string{
	"Timeout",
	"Timedout",
	"No workers running",
	"RPC_CALL_FAIL",
	"RPC_MCGET_FAIL",
	"WORKER_BUSY_TOO_LONG_RETRY",
	"memory limit exit",
	"connection dead",
	"engine was closed",
	"STORAGE_CHOOSE_VOLUME_FAILED",
}

// retryDelay is the pause before attempt n+1 (n starting at 1), so a busy DC gets a moment.
var retryDelay = func(n int) time.Duration { return time.Duration(n) * 250 * time.Millisecond }

type retry struct {
	max    int
	errors []string
}

func isErrorMatch(err error) bool {
	message := err.Error()
	for _, internalError := range internalErrors {
		if strings.Contains(message, internalError) {
			return true
		}
	}
	return false
}

func (r retry) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		if r.max <= 0 {
			return next.Invoke(ctx, input, output)
		}
		var lastErr error
		for retries := 0; retries < r.max; retries++ {
			if retries > 0 {
				select {
				case <-time.After(retryDelay(retries)):
				case <-ctx.Done():
					return errors.Wrap(lastErr, "retry interrupted")
				}
			}
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			if !tgerr.Is(err, r.errors...) && !isErrorMatch(err) {
				return errors.Wrap(err, "retry middleware skip")
			}
			lastErr = err
		}

		return fmt.Errorf("retry limit reached after %d attempts: %w", r.max, lastErr)
	}
}

func New(max int, errors ...string) telegram.Middleware {
	return retry{
		max:    max,
		errors: append(errors, internalErrors...),
	}
}
