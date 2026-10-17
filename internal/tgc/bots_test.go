package tgc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gotd/td/tgerr"
)

func TestIsDeadBotError(t *testing.T) {
	cases := []struct {
		err  error
		dead bool
	}{
		{tgerr.New(400, "ACCESS_TOKEN_INVALID"), true},
		{tgerr.New(401, "ACCESS_TOKEN_EXPIRED"), true},
		{tgerr.New(401, "USER_DEACTIVATED_BAN"), true},
		{fmt.Errorf("login: %w", tgerr.New(401, "AUTH_KEY_UNREGISTERED")), true},
		{tgerr.New(401, "SOMETHING_NEW"), true},
		{tgerr.New(420, "FLOOD_WAIT_300"), false},
		{tgerr.New(500, "INTERNAL"), false},
		{tgerr.New(400, "CHANNEL_INVALID"), false},
		{errors.New("dial tcp: i/o timeout"), false},
		{nil, false},
	}
	for _, c := range cases {
		if got := IsDeadBotError(c.err); got != c.dead {
			t.Errorf("IsDeadBotError(%v) = %v, want %v", c.err, got, c.dead)
		}
	}
}
