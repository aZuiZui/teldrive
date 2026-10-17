package tgc

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram"
	"github.com/tgdrive/teldrive/internal/logging"
	"go.uber.org/zap"
)

func RunWithAuth(ctx context.Context, client *telegram.Client, token string, f func(ctx context.Context) error) error {
	return runChecked(ctx, client.Run, func(ctx context.Context) error {
		if err := Auth(ctx, client, token); err != nil {
			return err
		}
		return f(ctx)
	})
}

// ErrRunStopped: the client stopped before the callback finished, with no error of its own.
var ErrRunStopped = errors.New("telegram client stopped before the request finished")

// runChecked runs f with run (telegram.Client.Run) and returns nil only when f itself returned
// nil. Client.Run returns nil for any error that wraps context.Canceled, including one from the
// callback: a part upload whose connection was cancelled half way "succeeded" with no message,
// so teldrive answered 200 with an empty part and the file was created without it.
func runChecked(ctx context.Context, run func(context.Context, func(context.Context) error) error, f func(context.Context) error) error {
	var (
		fErr     error
		finished bool
	)
	err := run(ctx, func(ctx context.Context) error {
		fErr = f(ctx)
		finished = fErr == nil
		return fErr
	})
	if err != nil || finished {
		return err
	}
	if fErr != nil {
		return fErr
	}
	return ErrRunStopped
}

func Auth(ctx context.Context, client *telegram.Client, token string) error {
	status, err := client.Auth().Status(ctx)
	if err != nil {
		return err
	}

	logger := logging.Component("TG")

	if token == "" {
		if !status.Authorized {
			return errors.Errorf("not authorized. please login first")
		}
		logger.Debug("session.user",
			zap.Int64("user_id", status.User.ID),
			zap.String("username", status.User.Username))
	} else {
		if !status.Authorized {
			_, err := client.Auth().Bot(ctx, token)
			if err != nil {
				logger.Error("auth.bot_failed", zap.Error(err))
				return err
			}
			status, err = client.Auth().Status(ctx)
			if err != nil {
				return err
			}
			logger.Debug("session.bot",
				zap.Int64("bot_id", status.User.ID),
				zap.String("username", status.User.Username))
		}
	}
	return nil
}
