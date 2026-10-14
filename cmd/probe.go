package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/gotd/td/tg"
	"github.com/spf13/cobra"
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

func runProbeCmd(ctx context.Context, cfg *config.ProbeCmdConfig, ids []string) []probeFileResult {
	results := make([]probeFileResult, 0, len(ids))
	db, err := database.NewDatabase(ctx, &cfg.DB, &config.DBLoggingConfig{Level: "fatal"}, zap.NewNop())
	if err != nil {
		for _, id := range ids {
			results = append(results, probeFileResult{ID: id, Error: fmt.Sprintf("database: %v", err)})
		}
		return results
	}
	for _, id := range ids {
		results = append(results, probeFile(ctx, cfg, db, id))
	}
	return results
}

func probeFile(ctx context.Context, cfg *config.ProbeCmdConfig, db *gorm.DB, id string) probeFileResult {
	result := probeFileResult{ID: id, Parts: []probePartResult{}}
	var file models.File
	if err := db.Where("id = ?", id).First(&file).Error; err != nil {
		result.Error = fmt.Sprintf("file: %v", err)
		return result
	}
	result.Name, result.Status = file.Name, file.Status
	if file.Size != nil {
		result.Size = *file.Size
	}
	if file.ChannelId == nil || file.Parts == nil || len(*file.Parts) == 0 {
		result.Error = "file has no Telegram parts"
		return result
	}
	result.ChannelID = *file.ChannelId

	var session models.Session
	if err := db.Where("user_id = ?", file.UserId).Order("created_at desc").First(&session).Error; err != nil {
		result.Error = fmt.Sprintf("no session for user %d: %v", file.UserId, err)
		return result
	}
	// No retry middleware: the probe makes its own attempts and must see Telegram's answer.
	client, err := tgc.AuthClient(ctx, &cfg.TG, session.Session,
		tgc.NewMiddleware(&cfg.TG, tgc.WithFloodWait(), tgc.WithRateLimit())...)
	if err != nil {
		result.Error = fmt.Sprintf("telegram client: %v", err)
		return result
	}

	ids := make([]int, len(*file.Parts))
	for i, part := range *file.Parts {
		ids[i] = part.ID
	}
	err = tgc.RunWithAuth(ctx, client, session.Session, func(ctx context.Context) error {
		messages, err := tgc.GetMessages(ctx, client.API(), ids, *file.ChannelId)
		if err != nil {
			return fmt.Errorf("get messages: %w", err)
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
			began := time.Now()
			fetch := func(ctx context.Context, offset int64) error {
				_, err := tgc.GetChunk(ctx, client.API(), document.AsInputDocumentFileLocation(), offset, probeBlock)
				return err
			}
			if err := probeSpots(ctx, fetch, document.Size, cfg.Timeout, cfg.Attempts); err != nil {
				part.Error = err.Error()
			} else {
				part.OK = true
			}
			part.Millis = time.Since(began).Milliseconds()
			result.Parts = append(result.Parts, part)
		}
		return nil
	})
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.OK = len(result.Parts) == len(ids)
	for _, part := range result.Parts {
		result.OK = result.OK && part.OK
	}
	return result
}

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
