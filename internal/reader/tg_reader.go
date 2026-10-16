package reader

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-faster/errors"

	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/tgc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	ErrStreamAbandoned = errors.New("stream abandoned")
	ErrChunkTimeout    = errors.New("chunk fetch timed out")
)

// DCAPI returns the client to read a file stored on dc with.
type DCAPI func(ctx context.Context, dc int) (*tg.Client, error)

type ChunkSource interface {
	Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error)
	ChunkSize(start, end int64) int64
}

type chunkSource struct {
	channelId   int64
	partId      int64
	concurrency int
	client      *tg.Client
	dcAPI       DCAPI // set for a warm client: reads go to the part's DC over several connections
	key         string
	cache       cache.Cacher
	refusedTTL  time.Duration // how long a part Telegram refused fails at once (0: never)
}

func (c *chunkSource) ChunkSize(start, end int64) int64 {
	return tgc.CalculateChunkSize(start, end)
}

func (c *chunkSource) Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error) {
	refusedAt := refusedKey(c.channelId, c.partId)
	if err := refused.check(refusedAt); err != nil {
		return nil, err
	}
	chunk, err := c.fetch(ctx, offset, limit)
	if err != nil {
		refused.remember(refusedAt, err, c.refusedTTL)
		return nil, err
	}
	refused.forget(refusedAt)
	return chunk, nil
}

func (c *chunkSource) fetch(ctx context.Context, offset int64, limit int64) ([]byte, error) {
	var location tgc.FileLocation

	// An entry cached before the DC was stored decodes without a location: fetch it again.
	if err := c.cache.Get(ctx, c.key, &location); err != nil || location.Location.ID == 0 {
		loc, err := tgc.GetFileLocation(ctx, c.client, c.channelId, c.partId)
		if err != nil {
			return nil, err
		}
		c.cache.Set(ctx, c.key, loc, 30*time.Minute)
		location = *loc
	}

	client := c.client
	if c.dcAPI != nil {
		var err error
		if client, err = c.dcAPI(ctx, location.DC); err != nil {
			return nil, err
		}
	}
	return tgc.GetChunk(ctx, client, &location.Location, offset, limit)

}

type tgMultiReader struct {
	ctx         context.Context
	cancel      context.CancelFunc
	offset      int64
	limit       int64
	chunkSize   int64
	bufferChan  chan *buffer
	cur         *buffer
	concurrency int
	leftCut     int64
	rightCut    int64
	totalParts  int
	currentPart int
	chunkSrc    ChunkSource
	timeout     time.Duration
	logger      *zap.Logger
	closeOnce   sync.Once
	errMu       sync.Mutex
	err         error // why filling stopped, returned by Read instead of a bare cancel
}

func newTGMultiReader(
	ctx context.Context,
	start int64,
	end int64,
	config *config.TGConfig,
	chunkSrc ChunkSource,
) (*tgMultiReader, error) {
	chunkSize := chunkSrc.ChunkSize(start, end)
	offset := start - (start % chunkSize)

	ctx, cancel := context.WithCancel(ctx)

	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		limit:       end - start + 1,
		bufferChan:  make(chan *buffer, config.Stream.Buffers),
		concurrency: config.Stream.Concurrency,
		leftCut:     start - offset,
		rightCut:    (end % chunkSize) + 1,
		totalParts:  int((end - offset + chunkSize) / chunkSize),
		offset:      offset,
		chunkSize:   chunkSize,
		chunkSrc:    chunkSrc,
		timeout:     config.Stream.ChunkTimeout,
		logger:      logging.FromContext(ctx),
	}

	go r.fillBuffer()
	return r, nil
}

func (r *tgMultiReader) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
	})
	return nil
}

func (r *tgMultiReader) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		return 0, io.EOF
	}

	if r.cur == nil || r.cur.isEmpty() {
		select {
		case cur, ok := <-r.bufferChan:
			if !ok {
				if err := r.fillErr(); err != nil {
					return 0, err
				}
				return 0, ErrStreamAbandoned
			}
			r.cur = cur
		case <-r.ctx.Done():
			if err := r.fillErr(); err != nil {
				return 0, err
			}
			return 0, r.ctx.Err()
		}
	}

	n := copy(p, r.cur.buffer())
	r.cur.increment(n)
	r.limit -= int64(n)

	if r.limit <= 0 {
		return n, io.EOF
	}

	return n, nil
}

func (r *tgMultiReader) fillBuffer() {
	defer close(r.bufferChan)

	for r.currentPart < r.totalParts {
		if err := r.fillBatch(); err != nil {
			if !errors.Is(err, context.Canceled) {
				r.errMu.Lock()
				r.err = err
				r.errMu.Unlock()
			}
			r.cancel()
			return
		}
	}
}

func (r *tgMultiReader) fillErr() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

func (r *tgMultiReader) fillBatch() error {
	g, ctx := errgroup.WithContext(r.ctx)
	g.SetLimit(r.concurrency)

	buffers := make([]*buffer, r.concurrency)

	for i := 0; i < r.concurrency && r.currentPart+i < r.totalParts; i++ {
		g.Go(func() error {
			chunkCtx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()

			chunk, err := r.chunkSrc.Chunk(chunkCtx, r.offset+int64(i)*r.chunkSize, r.chunkSize)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("chunk %d: %w", r.currentPart+i, ErrChunkTimeout)
				}
				return err
			}

			if r.totalParts == 1 {
				chunk = chunk[r.leftCut:r.rightCut]
			} else if r.currentPart+i == 0 {
				chunk = chunk[r.leftCut:]
			} else if r.currentPart+i+1 == r.totalParts {
				chunk = chunk[:r.rightCut]
			}

			buffers[i] = &buffer{buf: chunk}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Error("stream.chunk_failed", zap.Error(err), zap.Int("chunk", r.currentPart), zap.Int("total_chunks", r.totalParts),
				zap.Int64("offset", r.offset))
		}

		return err
	}

	for _, buf := range buffers {
		if buf == nil {
			break
		}
		select {
		case r.bufferChan <- buf:
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	r.currentPart += r.concurrency
	r.offset += r.chunkSize * int64(r.concurrency)

	return nil
}
