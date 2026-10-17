package cmd

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/spf13/cobra"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/database"
	"github.com/tgdrive/teldrive/internal/tgc"
	"github.com/tgdrive/teldrive/pkg/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// maxChannelAdmins: Telegram's limit on a channel's admins, the owner and every bot included.
const maxChannelAdmins = 50

// botsRequest: what `teldrive bots` reads on stdin (tokens stay out of the process list).
type botsRequest struct {
	User   int64    `json:"user"`
	Remove []string `json:"remove"` // banned bots: removed only if a fresh login says the token is dead
	Add    []string `json:"add"`    // new bots: added only if a fresh login works
	// RemoveOthers: admins that are not the user's bots (channels[].others) to demote in every
	// channel, chosen by the person: never one of the user's bots or the owner.
	RemoveOthers []int64 `json:"removeOthers"`
	DryRun       bool    `json:"dryRun"`
}

type botResult struct {
	BotID    int64  `json:"bot_id"`
	Username string `json:"username,omitempty"`
	// Verdict of the fresh login: "dead" (Telegram refuses the token), "alive", or "unclear"
	// (a flood wait, no answer, ...: nothing is done for the bot).
	Login   string  `json:"login"`
	Done    bool    `json:"done"`              // removed / added (in every channel, and its row)
	Planned bool    `json:"planned,omitempty"` // dry run: would be done
	Error   string  `json:"error,omitempty"`
	Changed []int64 `json:"channels,omitempty"` // channels it was removed from / added to
}

type channelResult struct {
	ChannelID int64   `json:"channel_id"`
	Name      string  `json:"name"`
	Admins    int     `json:"admins"`  // after the run
	Missing   []int64 `json:"missing"` // the user's bots not admin here before the run
	Added     []int64 `json:"added"`   // of those, made admin now
	// Others: admins that are neither the owner nor one of the user's bots (an old bot no longer
	// in teldrive.bots, a person). Never changed: they take admin slots, so they are listed.
	Others []otherAdmin `json:"others"`
	Errors []string     `json:"errors,omitempty"`
}

type otherAdmin struct {
	ID       int64  `json:"id"`
	Username string `json:"username,omitempty"`
	Name     string `json:"name,omitempty"`
	Bot      bool   `json:"bot"`
	Deleted  bool   `json:"deleted,omitempty"`
}

type botsResult struct {
	User    int64       `json:"user"`
	DryRun  bool        `json:"dryRun"`
	Removed []botResult `json:"removed"`
	// OthersRemoved: removeOthers demoted, per channel id.
	OthersRemoved map[int64][]int64 `json:"othersRemoved,omitempty"`
	Added         []botResult       `json:"added"`
	Channels      []channelResult   `json:"channels"`
	Bots          int               `json:"bots"` // the user's bots after the run
	Error         string            `json:"error,omitempty"`
}

func NewBotsCmd() *cobra.Command {
	var cfg config.BotsCmdConfig
	loader := config.NewConfigLoader()
	cmd := &cobra.Command{
		Use:   "bots",
		Short: "Replace banned bots and make every bot admin in all of a user's channels",
		Long: `Reads a JSON request on stdin: {"user": <id>, "remove": [<token>...], "add": [<token>...], "dryRun": false}
and prints the result as JSON.

remove: each bot is logged in afresh (a new session, not the saved one). Only when Telegram
refuses the token itself (ACCESS_TOKEN_INVALID, USER_DEACTIVATED_BAN, ... see tgc.IsDeadBotError)
is it demoted in every one of the user's channels and its row deleted; a bot that logs in, or
an unclear answer (flood wait, timeout), is left as it is.

add: each bot must log in. It is made admin in every channel of the user first, and its row is
added only when that worked everywhere (otherwise it is demoted again where it was added).

removeOthers: admins listed under channels[].others (not the owner, not one of the user's bots)
that the person chose to take out, to free their slots.

Then every bot of the user that is not admin in one of the user's channels is made admin there.
Telegram allows 50 admins in a channel (the owner and bots included); removals run first.

Examples:
  echo '{"user": 527047386}' | teldrive bots -c config-online.toml
  echo '{"user": 527047386, "remove": ["123:AA..."], "add": ["456:AA..."]}' | teldrive bots -c config-online.toml`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var req botsRequest
			b, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
			if err != nil {
				return err
			}
			if err := json.Unmarshal(b, &req); err != nil {
				return fmt.Errorf("request on stdin: %w", err)
			}
			if req.User == 0 {
				return fmt.Errorf("request on stdin: no user")
			}
			res := runBotsCmd(cmd.Context(), &cfg, req)
			out := json.NewEncoder(os.Stdout)
			out.SetEscapeHTML(false)
			return out.Encode(res)
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := loader.Load(cmd, &cfg); err != nil {
				return err
			}
			return checkRequiredCheckFlags(&config.CheckCmdConfig{DB: cfg.DB})
		},
	}
	loader.RegisterFlags(cmd.Flags(), reflect.TypeFor[config.BotsCmdConfig]())
	return cmd
}

func tokenBotID(token string) int64 {
	id, _ := strconv.ParseInt(strings.SplitN(token, ":", 2)[0], 10, 64)
	return id
}

// verifyLogin: a fresh login of the bot (tgc.FreshBotLogin). A flood wait longer than 30s isn't
// waited out: the bot is "unclear" and the next run tries again.
func verifyLogin(ctx context.Context, tgCfg *config.TGConfig, token string) (*tg.User, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	mw := tgc.NewMiddleware(tgCfg, tgc.WithFloodWaitLimit(30*time.Second, func(time.Duration) {}), tgc.WithRateLimit())
	user, err := tgc.FreshBotLogin(ctx, tgCfg, token, mw...)
	switch {
	case err == nil:
		return user, "alive", nil
	case tgc.IsDeadBotError(err):
		return nil, "dead", err
	default:
		return nil, "unclear", err
	}
}

// channelAdmins: the channel's admins (owner included) by user id, with what the user's session
// needs to address them, and the users themselves.
func channelAdmins(ctx context.Context, api *tg.Client, ch *tg.InputChannel) (map[int64]*tg.InputUser, map[int64]*tg.User, error) {
	res, err := api.ChannelsGetParticipants(ctx, &tg.ChannelsGetParticipantsRequest{
		Channel: ch, Filter: &tg.ChannelParticipantsAdmins{}, Limit: 200,
	})
	if err != nil {
		return nil, nil, err
	}
	parts, ok := res.(*tg.ChannelsChannelParticipants)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected answer %T", res)
	}
	users := map[int64]*tg.InputUser{}
	info := map[int64]*tg.User{}
	for _, u := range parts.Users {
		if user, ok := u.(*tg.User); ok {
			users[user.ID] = &tg.InputUser{UserID: user.ID, AccessHash: user.AccessHash}
			info[user.ID] = user
		}
	}
	admins := map[int64]*tg.InputUser{}
	for _, p := range parts.Participants {
		var id int64
		switch p := p.(type) {
		case *tg.ChannelParticipantCreator:
			id = p.UserID
		case *tg.ChannelParticipantAdmin:
			id = p.UserID
		default:
			continue
		}
		admins[id] = users[id]
		if admins[id] == nil {
			admins[id] = &tg.InputUser{UserID: id}
		}
	}
	return admins, info, nil
}

// botAdminRights: what teldrive gives its bots (ChannelManager.AddBotsToChannel).
var botAdminRights = tg.ChatAdminRights{
	ChangeInfo: true, PostMessages: true, EditMessages: true, DeleteMessages: true, BanUsers: true,
	InviteUsers: true, PinMessages: true, ManageCall: true, Other: true, ManageTopics: true,
}

func setAdmin(ctx context.Context, api *tg.Client, ch *tg.InputChannel, user *tg.InputUser, admin bool) error {
	req := &tg.ChannelsEditAdminRequest{Channel: ch, UserID: user}
	if admin {
		req.AdminRights, req.Rank = botAdminRights, "bot"
	}
	_, err := api.ChannelsEditAdmin(ctx, req)
	return err
}

func runBotsCmd(ctx context.Context, cfg *config.BotsCmdConfig, req botsRequest) botsResult {
	res := botsResult{User: req.User, DryRun: req.DryRun, Removed: []botResult{}, Added: []botResult{}, Channels: []channelResult{}}
	db, err := database.NewDatabase(ctx, &cfg.DB, &config.DBLoggingConfig{Level: "fatal"}, zap.NewNop())
	if err != nil {
		res.Error = fmt.Sprintf("database: %v", err)
		return res
	}
	var rows []models.Bot
	if err := db.Where("user_id = ?", req.User).Find(&rows).Error; err != nil {
		res.Error = fmt.Sprintf("bots: %v", err)
		return res
	}
	var channels []models.Channel
	if err := db.Where("user_id = ?", req.User).Order("created_at").Find(&channels).Error; err != nil {
		res.Error = fmt.Sprintf("channels: %v", err)
		return res
	}
	var session models.Session
	if err := db.Where("user_id = ?", req.User).Order("created_at desc").First(&session).Error; err != nil {
		res.Error = fmt.Sprintf("no session for user %d: %v", req.User, err)
		return res
	}
	owned := map[int64]string{} // bot id -> token, the user's rows
	for _, r := range rows {
		owned[tokenBotID(r.Token)] = r.Token
	}

	// Fresh logins first, before anything is changed.
	res.Removed = make([]botResult, len(req.Remove))
	remove := make([]*botResult, len(req.Remove))
	for i, t := range req.Remove {
		r := &res.Removed[i]
		r.BotID, remove[i] = tokenBotID(t), r
		if owned[r.BotID] != t {
			r.Login, r.Error = "unclear", "not one of this user's bots (token not in teldrive.bots)"
			continue
		}
		_, r.Login, err = verifyLogin(ctx, &cfg.TG, t)
		if r.Login == "alive" {
			r.Error = "Telegram still takes the token: not banned, not removed"
		} else if r.Login == "unclear" {
			r.Error = fmt.Sprintf("no clear answer (%v): not removed", err)
		}
	}

	usernames := map[int64]string{}
	addTokens := map[int64]string{}
	for _, t := range req.Add {
		r := botResult{BotID: tokenBotID(t)}
		var other models.Bot
		if err := db.Where("token = ?", t).First(&other).Error; err == nil {
			r.Login, r.Error = "unclear", fmt.Sprintf("already a bot of user %d", other.UserId)
			res.Added = append(res.Added, r)
			continue
		}
		user, login, err := verifyLogin(ctx, &cfg.TG, t)
		r.Login = login
		if user != nil {
			r.Username, usernames[r.BotID] = user.Username, user.Username
			addTokens[r.BotID] = t
		} else {
			r.Error = fmt.Sprintf("login failed (%v): not added", err)
		}
		res.Added = append(res.Added, r)
	}

	mw := tgc.NewMiddleware(&cfg.TG, tgc.WithFloodWait(), tgc.WithRateLimit())
	client, err := tgc.AuthClient(ctx, &cfg.TG, session.Session, mw...)
	if err != nil {
		res.Error = fmt.Sprintf("telegram client (user session): %v", err)
		return res
	}
	memCache := cache.NewMemoryCache(10 << 20)
	err = tgc.RunWithAuth(ctx, client, "", func(ctx context.Context) error {
		runBotsChannels(ctx, cfg, db, memCache, client, req, channels, owned, remove, addTokens, usernames, &res)
		return nil
	})
	if err != nil && res.Error == "" {
		res.Error = fmt.Sprintf("user session: %v", err)
	}
	var n int64
	db.Model(&models.Bot{}).Where("user_id = ?", req.User).Count(&n)
	res.Bots = int(n)
	return res
}

func runBotsChannels(ctx context.Context, cfg *config.BotsCmdConfig, db *gorm.DB, c cache.Cacher, client *telegram.Client,
	req botsRequest, channels []models.Channel, owned map[int64]string, remove []*botResult,
	addTokens map[int64]string, usernames map[int64]string, res *botsResult) {
	api := client.API()
	type chState struct {
		input  *tg.InputChannel
		admins map[int64]*tg.InputUser
		users  map[int64]*tg.User
		res    *channelResult
	}
	var chs []*chState
	for _, ch := range channels {
		res.Channels = append(res.Channels, channelResult{ChannelID: ch.ChannelId, Name: ch.ChannelName, Missing: []int64{}, Added: []int64{}, Others: []otherAdmin{}})
	}
	for i, ch := range channels {
		st := &chState{res: &res.Channels[i]}
		chs = append(chs, st)
		input, err := tgc.GetChannelById(ctx, api, ch.ChannelId)
		if err == nil {
			st.input = input
			st.admins, st.users, err = channelAdmins(ctx, api, input)
		}
		if err != nil {
			st.res.Errors = append(st.res.Errors, fmt.Sprintf("reading the channel: %v", err))
			st.input = nil
		}
	}
	readable := func() []*chState {
		var out []*chState
		for _, st := range chs {
			if st.input != nil {
				out = append(out, st)
			}
		}
		return out
	}()
	if len(readable) < len(chs) {
		// A channel that can't be read can't be checked: change nothing, so no bot is left
		// admin in some channels only.
		for _, r := range remove {
			if r.Login == "dead" {
				r.Error = "not removed: a channel could not be read (see channels)"
			}
		}
		for i := range res.Added {
			if addTokens[res.Added[i].BotID] != "" {
				res.Added[i].Error = "not added: a channel could not be read (see channels)"
			}
		}
		for _, st := range chs {
			if st.admins != nil {
				st.res.Admins = len(st.admins)
			}
		}
		return
	}

	// Who else holds admin slots (before anything changes).
	removing := map[int64]bool{}
	for _, r := range remove {
		removing[r.BotID] = true
	}
	for _, st := range chs {
		for id := range st.admins {
			if id == req.User || owned[id] != "" || removing[id] {
				continue
			}
			o := otherAdmin{ID: id}
			if u := st.users[id]; u != nil {
				o.Username, o.Name, o.Bot, o.Deleted = u.Username, strings.TrimSpace(u.FirstName+" "+u.LastName), u.Bot, u.Deleted
			}
			st.res.Others = append(st.res.Others, o)
		}
		slices.SortFunc(st.res.Others, func(a, b otherAdmin) int { return cmp.Compare(a.ID, b.ID) })
	}

	// 0. Other admins the person chose to take out (only ones listed as others above).
	for _, id := range req.RemoveOthers {
		if id == req.User || owned[id] != "" {
			continue
		}
		for _, st := range chs {
			user, ok := st.admins[id]
			if !ok {
				continue
			}
			if !req.DryRun {
				if err := setAdmin(ctx, api, st.input, user, false); err != nil {
					st.res.Errors = append(st.res.Errors, fmt.Sprintf("removing admin %d: %v", id, err))
					continue
				}
				delete(st.admins, id)
			}
			if res.OthersRemoved == nil {
				res.OthersRemoved = map[int64][]int64{}
			}
			res.OthersRemoved[st.res.ChannelID] = append(res.OthersRemoved[st.res.ChannelID], id)
		}
	}

	// 1. Banned bots out of every channel (frees their admin slots), then their rows.
	for _, r := range remove {
		if r.Login != "dead" {
			continue
		}
		failed := false
		for _, st := range chs {
			user, ok := st.admins[r.BotID]
			if !ok {
				continue
			}
			if req.DryRun {
				r.Changed = append(r.Changed, st.res.ChannelID)
				continue
			}
			if err := setAdmin(ctx, api, st.input, user, false); err != nil {
				failed = true
				st.res.Errors = append(st.res.Errors, fmt.Sprintf("removing bot %d: %v", r.BotID, err))
				continue
			}
			delete(st.admins, r.BotID)
			r.Changed = append(r.Changed, st.res.ChannelID)
		}
		switch {
		case req.DryRun:
			r.Planned = true
		case failed:
			r.Error = "still admin in a channel (see channels): row kept, try again"
		default:
			if err := db.Where("user_id = ? AND token = ?", req.User, owned[r.BotID]).Delete(&models.Bot{}).Error; err != nil {
				r.Error = fmt.Sprintf("deleting its row: %v", err)
				continue
			}
			delete(owned, r.BotID)
			r.Done = true
		}
	}

	// username: the bot's @name, for the user's session to find it (a login of the bot, with its
	// saved session, for a bot not logged in by this run).
	username := func(id int64) (string, error) {
		if u := usernames[id]; u != "" {
			return u, nil
		}
		info, err := tgc.GetBotInfo(ctx, db, c, &cfg.TG, owned[id])
		if err != nil {
			return "", err
		}
		usernames[id] = info.UserName
		return info.UserName, nil
	}
	resolve := func(id int64) (*tg.InputUser, error) {
		name, err := username(id)
		if err != nil {
			return nil, fmt.Errorf("bot login: %w", err)
		}
		peer, err := api.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: name})
		if err != nil {
			return nil, fmt.Errorf("resolving @%s: %w", name, err)
		}
		for _, u := range peer.Users {
			if user, ok := u.(*tg.User); ok && user.ID == id {
				return &tg.InputUser{UserID: user.ID, AccessHash: user.AccessHash}, nil
			}
		}
		return nil, fmt.Errorf("@%s is not bot %d", name, id)
	}
	promote := func(st *chState, id int64) error {
		if len(st.admins) >= maxChannelAdmins {
			return fmt.Errorf("channel has %d admins (Telegram's limit)", len(st.admins))
		}
		user, err := resolve(id)
		if err != nil {
			return err
		}
		if err := setAdmin(ctx, api, st.input, user, true); err != nil {
			return err
		}
		st.admins[id] = user
		return nil
	}

	// 2. New bots: admin everywhere first; the row only when that worked in every channel.
	for i := range res.Added {
		r := &res.Added[i]
		token := addTokens[r.BotID]
		if token == "" {
			continue
		}
		if req.DryRun {
			for _, st := range chs {
				if _, ok := st.admins[r.BotID]; !ok {
					r.Changed = append(r.Changed, st.res.ChannelID)
				}
			}
			r.Planned = true
			continue
		}
		var added []*chState
		var failed error
		for _, st := range chs {
			if _, ok := st.admins[r.BotID]; ok {
				continue
			}
			if err := promote(st, r.BotID); err != nil {
				failed = fmt.Errorf("%s: %w", st.res.Name, err)
				break
			}
			added = append(added, st)
		}
		if failed == nil {
			failed = db.Create(&models.Bot{UserId: req.User, Token: token, BotId: r.BotID}).Error
		}
		if failed != nil {
			r.Error = fmt.Sprintf("not added: %v", failed)
			for _, st := range added { // back out, so its admin slots aren't taken for nothing
				if setAdmin(ctx, api, st.input, st.admins[r.BotID], false) == nil {
					delete(st.admins, r.BotID)
				}
			}
			continue
		}
		owned[r.BotID] = token
		for _, st := range added {
			r.Changed = append(r.Changed, st.res.ChannelID)
		}
		r.Done = true
	}

	// 3. Every bot of the user admin in every channel.
	ids := make([]int64, 0, len(owned))
	for id := range owned {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, st := range chs {
		for _, id := range ids {
			if _, ok := st.admins[id]; ok {
				continue
			}
			st.res.Missing = append(st.res.Missing, id)
			if req.DryRun {
				continue
			}
			if err := promote(st, id); err != nil {
				st.res.Errors = append(st.res.Errors, fmt.Sprintf("adding bot %d: %v", id, err))
				continue
			}
			st.res.Added = append(st.res.Added, id)
		}
		st.res.Admins = len(st.admins)
	}
}
