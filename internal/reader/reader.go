package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/crypt"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/pkg/models"
	"github.com/tgdrive/teldrive/pkg/types"
	"go.uber.org/zap"
)

// PartError says which Telegram message of a file could not be read.
type PartError struct {
	PartNo    int   // 1-based, as in the file's parts list
	MessageID int64 // the part's Telegram message
	Err       error
}

func (e *PartError) Error() string {
	return fmt.Sprintf("part %d (message %d): %v", e.PartNo, e.MessageID, e.Err)
}

func (e *PartError) Unwrap() error { return e.Err }

type Range struct {
	Start, End int64
	PartNo     int64
}

type Reader struct {
	ctx         context.Context
	file        *models.File
	parts       []types.Part
	ranges      []Range
	pos         int
	reader      io.ReadCloser
	remaining   int64
	config      *config.TGConfig
	client      *tg.Client
	dcAPI       DCAPI
	concurrency int
	cache       cache.Cacher
	closeOnce   sync.Once
	closeErr    error
	botID       string
}

func calculatePartByteRanges(start, end, partSize int64) []Range {
	ranges := make([]Range, 0)
	startPart := start / partSize
	endPart := end / partSize

	for part := startPart; part <= endPart; part++ {
		partStart := max(start-part*partSize, 0)
		partEnd := min(partSize-1, end-part*partSize)
		ranges = append(ranges, Range{
			Start:  partStart,
			End:    partEnd,
			PartNo: part,
		})
	}
	return ranges
}

func NewReader(ctx context.Context,
	client *tg.Client,
	cache cache.Cacher,
	file *models.File,
	parts []types.Part,
	start,
	end int64,
	config *config.TGConfig,
	botID string,
	dcAPI DCAPI, // nil: every read goes through client
) (io.ReadCloser, error) {

	size := parts[0].Size
	if *file.Encrypted {
		size = parts[0].DecryptedSize
	}
	r := &Reader{
		ctx:       ctx,
		parts:     parts,
		file:      file,
		remaining: end - start + 1,
		ranges:    calculatePartByteRanges(start, end, size),
		config:    config,
		client:    client,
		dcAPI:     dcAPI,
		cache:     cache,
		botID:     botID,
	}

	if err := r.initializeReader(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}

	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if err != nil && err != io.EOF {
		err = r.partError(err)
	}

	if err == io.EOF && r.remaining > 0 {
		if err := r.moveToNextPart(); err != nil {
			return n, err
		}
		err = nil
	}

	return n, err
}

func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		if r.reader != nil {
			r.closeErr = r.reader.Close()
			r.reader = nil
		}
	})
	return r.closeErr
}

func (r *Reader) initializeReader() error {
	reader, err := r.getPartReader()
	if err != nil {
		return r.partError(err)
	}
	r.reader = reader
	return nil
}

func (r *Reader) partError(err error) error {
	var partErr *PartError
	if errors.As(err, &partErr) || r.pos >= len(r.ranges) {
		return err
	}
	partNo := r.ranges[r.pos].PartNo
	return &PartError{PartNo: int(partNo) + 1, MessageID: r.parts[partNo].ID, Err: err}
}

func (r *Reader) moveToNextPart() error {
	r.reader.Close()
	r.pos++
	if r.pos < len(r.ranges) {
		return r.initializeReader()
	}
	return io.EOF
}

func (r *Reader) getPartReader() (io.ReadCloser, error) {
	currentRange := r.ranges[r.pos]
	partId := r.parts[currentRange.PartNo].ID
	ctx := logging.WithContext(r.ctx, logging.FromContext(r.ctx).With(
		zap.String("file_id", r.file.ID), zap.String("file_name", r.file.Name),
		zap.Int64("part_no", currentRange.PartNo+1), zap.Int64("message_id", partId)))

	chunkSrc := &chunkSource{
		channelId:   *r.file.ChannelId,
		partId:      partId,
		client:      r.client,
		dcAPI:       r.dcAPI,
		concurrency: r.concurrency,
		cache:       r.cache,
		key:         cache.KeyFileLocation(r.config.SessionInstance, r.botID, r.file.ID, partId),
		refusedTTL:  r.config.Stream.RefusedPartTTL,
	}

	var (
		reader io.ReadCloser
		err    error
	)

	reader, err = newTGMultiReader(ctx, currentRange.Start, currentRange.End, r.config, chunkSrc)

	if *r.file.Encrypted {
		salt := r.parts[r.ranges[r.pos].PartNo].Salt
		cipher, _ := crypt.NewCipher(r.config.Uploads.EncryptionKey, salt)
		reader, err = cipher.DecryptDataSeek(ctx,
			func(ctx context.Context,
				underlyingOffset,
				underlyingLimit int64) (io.ReadCloser, error) {
				var end int64

				if underlyingLimit >= 0 {
					end = min(r.parts[r.ranges[r.pos].PartNo].Size-1, underlyingOffset+underlyingLimit-1)
				}

				return newTGMultiReader(ctx, underlyingOffset, end, r.config, chunkSrc)

			}, currentRange.Start, currentRange.End-currentRange.Start+1)
	}

	return reader, err

}
