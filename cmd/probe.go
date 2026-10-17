package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"regexp"
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

// probeBlock is how much of a part each read asks for: the smallest read Telegram allows.
const probeBlock = 4096

type probePartResult struct {
	Part      int    `json:"part"`
	MessageID int    `json:"message_id"`
	Size      int64  `json:"size"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	Millis    int64  `json:"ms"`
}

type probeFileResult struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Size      int64             `json:"size"`
	ChannelID int64             `json:"channel_id"`
	Status    string            `json:"status"`
	OK        bool              `json:"ok"`
	Error     string            `json:"error,omitempty"`
	Parts     []probePartResult `json:"parts"`
}

func NewProbeCmd() *cobra.Command {
	var cfg config.ProbeCmdConfig
	loader := config.NewConfigLoader()
	cmd := &cobra.Command{
		Use:   "probe <file-id>...",
		Short: "Check that Telegram still serves every part of the given files",
		Long: `Read the first and the last block of every part of each file straight from Telegram,
with the file owner's session, and print the result as JSON. A part counts as unreadable when
every attempt at a spot fails (Telegram answering -503 Timeout for a document it can no longer
read, a message that is gone, ...).

Examples:
  teldrive probe 019dac72-0e79-7d95-a53b-280565f0aeba
  teldrive probe --attempts 5 --timeout 1m <id> <id>`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			results := runProbeCmd(cmd.Context(), &cfg, args)
			out := json.NewEncoder(os.Stdout)
			out.SetEscapeHTML(false)
			_ = out.Encode(map[string]any{"files": results})
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := loader.Load(cmd, &cfg); err != nil {
				return err
			}
			return checkRequiredCheckFlags(&config.CheckCmdConfig{DB: cfg.DB})
		},
	}
	loader.RegisterFlags(cmd.Flags(), reflect.TypeFor[config.ProbeCmdConfig]())
	return cmd
}

// runProbeCmd probes the files in order with the owner's bots (see probeOwner): each client logs
// in to each DC that holds parts once (auth.exportAuthorization), not once per file, and the
// owner's own account isn't used while there are bots (checking per file got it flood-waited).
func runProbeCmd(ctx context.Context, cfg *config.ProbeCmdConfig, ids []string) []probeFileResult {
	results := make([]probeFileResult, len(ids))
	db, err := database.NewDatabase(ctx, &cfg.DB, &config.DBLoggingConfig{Level: "fatal"}, zap.NewNop())
	if err != nil {
		for i, id := range ids {
			results[i] = probeFileResult{ID: id, Error: fmt.Sprintf("database: %v", err), Parts: []probePartResult{}}
		}
		return results
	}

	byUser := map[int64][]probePending{}
	var users []int64
	for i, id := range ids {
		results[i] = probeFileResult{ID: id, Parts: []probePartResult{}}
		file, ok := loadProbeFile(db, id, &results[i])
		if !ok {
			continue
		}
		if _, seen := byUser[file.UserId]; !seen {
			users = append(users, file.UserId)
		}
		byUser[file.UserId] = append(byUser[file.UserId], probePending{i, file})
	}

	memCache := cache.NewMemoryCache(10 << 20)
	for _, userID := range users {
		probeOwner(ctx, cfg, db, memCache, userID, byUser[userID], results)
	}
	return results
}

type probePending struct {
	index int
	file  models.File
}

// probeAccount is what the probe reads with: one of the owner's bots, or (no bots) their own session.
type probeAccount struct {
	label string
	token string // bot token; "" for the user's session
}

// probeOwner probes one owner's files with their bots, in turn from a random one: a bot that gets
// a flood wait hands the file it was on, and the rest, to the next bot (as streaming puts a
// flood-waited bot on standby). Only when every bot has been tried does a file report the flood
// wait. Without bots it uses the owner's session (which then takes every login and flood wait).
func probeOwner(ctx context.Context, cfg *config.ProbeCmdConfig, db *gorm.DB, c cache.Cacher, userID int64,
	files []probePending, results []probeFileResult) {
	fail := func(queue []probePending, msg string) {
		for _, f := range queue {
			if results[f.index].Error == "" && len(results[f.index].Parts) == 0 {
				results[f.index].Error = msg
			}
		}
	}
	var accounts []probeAccount
	var bots []models.Bot
	db.Where("user_id = ?", userID).Order("bot_id").Find(&bots)
	for _, bot := range bots {
		accounts = append(accounts, probeAccount{label: fmt.Sprintf("bot %d", bot.BotId), token: bot.Token})
	}
	var session models.Session
	if len(accounts) == 0 {
		if err := db.Where("user_id = ?", userID).Order("created_at desc").First(&session).Error; err != nil {
			fail(files, fmt.Sprintf("no bots and no session for user %d: %v", userID, err))
			return
		}
		accounts = append(accounts, probeAccount{label: "user session"})
	}

	base := make(map[int]probeFileResult, len(files))
	for _, f := range files {
		base[f.index] = results[f.index]
	}
	queue := files
	start := rand.Intn(len(accounts))
	for tried := 0; len(queue) > 0 && tried < len(accounts); tried++ {
		acct := accounts[(start+tried)%len(accounts)]
		// No retry middleware: the probe makes its own attempts and must see Telegram's answer. A
		// flood wait longer than a few seconds fails at once, and the next account takes over.
		mw := tgc.NewMiddleware(&cfg.TG, tgc.WithFloodWaitLimit(5*time.Second, func(time.Duration) {}), tgc.WithRateLimit())
		var (
			client *telegram.Client
			err    error
			auth   = acct.token
		)
		if acct.token != "" {
			client, err = tgc.BotClient(ctx, db, c, &cfg.TG, acct.token, mw...)
		} else {
			client, err = tgc.AuthClient(ctx, &cfg.TG, session.Session, mw...)
			auth = session.Session
		}
		if err != nil {
			fail(queue, fmt.Sprintf("telegram client (%s): %v", acct.label, err))
			continue
		}
		done := 0
		err = tgc.RunWithAuth(ctx, client, auth, func(ctx context.Context) error {
			dcDown := map[int]string{}
			for _, f := range queue {
				res := base[f.index]
				res.Parts = []probePartResult{}
				probeParts(ctx, cfg, client.API(), &f.file, &res, dcDown)
				results[f.index] = res
				if floodWaited(res) {
					return nil // this account is flood-waited: the next one takes this file and the rest
				}
				done++
			}
			return nil
		})
		if err != nil && done < len(queue) && results[queue[done].index].Error == "" {
			results[queue[done].index].Error = fmt.Sprintf("%s: %v", acct.label, err)
		}
		queue = queue[done:]
	}
}

// floodWaited: Telegram made this account wait (a read, or a DC login), so another may get through.
func floodWaited(r probeFileResult) bool {
	if isFlood(r.Error) {
		return true
	}
	for _, p := range r.Parts {
		if !p.OK && isFlood(p.Error) {
			return true
		}
	}
	return false
}

func isFlood(err string) bool {
	return strings.Contains(err, "FLOOD_WAIT") || strings.Contains(err, "flood wait")
}

// loadProbeFile reads the file's row; false (with result.Error set) when there is nothing to probe.
func loadProbeFile(db *gorm.DB, id string, result *probeFileResult) (models.File, bool) {
	var file models.File
	if err := db.Where("id = ?", id).First(&file).Error; err != nil {
		result.Error = fmt.Sprintf("file: %v", err)
		return file, false
	}
	result.Name, result.Status = file.Name, file.Status
	if file.Size != nil {
		result.Size = *file.Size
	}
	if file.ChannelId == nil || file.Parts == nil || len(*file.Parts) == 0 {
		result.Error = "file has no Telegram parts"
		return file, false
	}
	result.ChannelID = *file.ChannelId
	return file, true
}

// probeParts reads the first and last block of every part of file into result. dcDown holds the
// DCs this run couldn't log in to (a flood wait, a timeout): their parts aren't asked for again,
// so one refused login doesn't turn into hundreds.
func probeParts(ctx context.Context, cfg *config.ProbeCmdConfig, api *tg.Client, file *models.File, result *probeFileResult, dcDown map[int]string) {
	ids := make([]int, len(*file.Parts))
	for i, part := range *file.Parts {
		ids[i] = part.ID
	}
	messages, err := tgc.GetMessages(ctx, api, ids, *file.ChannelId)
	if err != nil {
		result.Error = fmt.Errorf("get messages: %w", err).Error()
		return
	}
	for i, id := range ids {
		part := probePartResult{Part: i + 1, MessageID: id}
		document := partDocument(messages, id)
		if document == nil {
			part.Error = "message or document not found"
			result.Parts = append(result.Parts, part)
			continue
		}
		part.Size = document.Size
		if reason, down := dcDown[document.DCID]; down {
			part.Error = fmt.Sprintf("not checked: no login to DC %d (%s)", document.DCID, reason)
			result.Parts = append(result.Parts, part)
			continue
		}
		began := time.Now()
		fetch := func(ctx context.Context, offset int64) error {
			_, err := tgc.GetChunk(ctx, api, document.AsInputDocumentFileLocation(), offset, probeBlock)
			return err
		}
		if err := probeSpots(ctx, fetch, document.Size, cfg.Timeout, cfg.Attempts); err != nil {
			part.Error = err.Error()
			if strings.Contains(part.Error, "create connection to DC") {
				dcDown[document.DCID] = loginFailure(part.Error)
			}
		} else {
			part.OK = true
		}
		part.Millis = time.Since(began).Milliseconds()
		result.Parts = append(result.Parts, part)
	}
	result.OK = len(result.Parts) == len(ids)
	for _, part := range result.Parts {
		result.OK = result.OK && part.OK
	}
}

// loginFailure is the short reason a DC login failed: Telegram's flood wait, or the timeout.
func loginFailure(err string) string {
	for _, re := range floodWaitREs {
		if m := re.FindString(err); m != "" {
			return m
		}
	}
	if strings.Contains(err, "deadline exceeded") {
		return "timed out"
	}
	return "login failed"
}

// Telegram's own FLOOD_WAIT (n) first; the middleware's wording when that isn't in the error.
var floodWaitREs = []*regexp.Regexp{regexp.MustCompile(`FLOOD_WAIT \(\d+\)`), regexp.MustCompile(`flood wait argument is too big \([^)]*\)`)}

func partDocument(messages []tg.MessageClass, id int) *tg.Document {
	for _, message := range messages {
		msg, ok := message.(*tg.Message)
		if !ok || msg.ID != id {
			continue
		}
		media, ok := msg.Media.(*tg.MessageMediaDocument)
		if !ok {
			return nil
		}
		document, _ := media.Document.(*tg.Document)
		return document
	}
	return nil
}

// probeSpots reads the first and the last block of a part. A spot fails only when all its
// attempts fail, so one slow answer doesn't mark a part unreadable.
func probeSpots(ctx context.Context, fetch func(context.Context, int64) error, size int64, timeout time.Duration, attempts int) error {
	if attempts < 1 {
		attempts = 1
	}
	spots := []int64{0}
	if last := (size - 1) / probeBlock * probeBlock; last > 0 {
		spots = append(spots, last)
	}
	for _, offset := range spots {
		var err error
		for attempt := 0; attempt < attempts; attempt++ {
			readCtx, cancel := context.WithTimeout(ctx, timeout)
			err = fetch(readCtx, offset)
			cancel()
			if err == nil || ctx.Err() != nil {
				break
			}
		}
		if err != nil {
			return fmt.Errorf("offset %d: %w", offset, err)
		}
	}
	return nil
}
