package tgc

import (
	"context"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/config"
)

// deadBotErrors: what Telegram answers for a bot token that no longer works: the bot was
// banned or deleted, or its token was revoked in @BotFather.
var deadBotErrors = []string{
	"ACCESS_TOKEN_INVALID", "ACCESS_TOKEN_EXPIRED", "BOT_INVALID",
	"USER_DEACTIVATED", "USER_DEACTIVATED_BAN", "AUTH_KEY_UNREGISTERED", "SESSION_REVOKED",
}

// IsDeadBotError: err says the bot's token is dead. A flood wait, a timeout or any other
// answer is not: the bot may work again, so nothing should be removed for it.
func IsDeadBotError(err error) bool {
	if err == nil {
		return false
	}
	if tgerr.Is(err, deadBotErrors...) {
		return true
	}
	rpc, ok := tgerr.As(err)
	return ok && rpc.Code == 401
}

// FreshBotLogin logs the bot in on a new, in-memory session (auth.importBotAuthorization),
// not the one saved in the database: a saved session can outlive or fail apart from the token,
// so only a fresh login tells whether Telegram still takes the token itself. It returns the
// bot's user, or Telegram's error (see IsDeadBotError).
func FreshBotLogin(ctx context.Context, cfg *config.TGConfig, token string, middlewares ...telegram.Middleware) (*tg.User, error) {
	client, err := newClient(ctx, cfg, nil, new(session.StorageMemory), middlewares...)
	if err != nil {
		return nil, err
	}
	var user *tg.User
	err = client.Run(ctx, func(ctx context.Context) error {
		if _, err := client.Auth().Bot(ctx, token); err != nil {
			return err
		}
		user, err = client.Self(ctx)
		return err
	})
	return user, err
}
