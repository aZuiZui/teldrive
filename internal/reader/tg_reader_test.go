package reader

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/tgdrive/teldrive/internal/config"
)

type fakeChunks struct {
	err  error
	data byte
}

func (f fakeChunks) Chunk(_ context.Context, _ int64, limit int64) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []byte(strings.Repeat(string(f.data), int(limit))), nil
}

func (fakeChunks) ChunkSize(start, end int64) int64 { return 1024 }

func testConfig() *config.TGConfig {
	return &config.TGConfig{Stream: config.TGStream{Concurrency: 1, Buffers: 2, ChunkTimeout: time.Second}}
}

func TestMultiReaderReturnsTelegramError(t *testing.T) {
	telegramErr := errors.New("rpc error code -503: Timeout")
	r, err := newTGMultiReader(context.Background(), 0, 4095, testConfig(), fakeChunks{err: telegramErr})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, err = r.Read(make([]byte, 10))
	if !errors.Is(err, telegramErr) {
		t.Fatalf("Read error = %v, want the Telegram error", err)
	}
}

func TestMultiReaderReadsData(t *testing.T) {
	r, err := newTGMultiReader(context.Background(), 0, 2047, testConfig(), fakeChunks{data: 'x'})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || len(got) != 2048 {
		t.Fatalf("read %d bytes, err = %v", len(got), err)
	}
}

func TestPartErrorNamesThePart(t *testing.T) {
	err := (&PartError{PartNo: 1, MessageID: 539901, Err: errors.New("Timeout")}).Error()
	if err != "part 1 (message 539901): Timeout" {
		t.Fatalf("got %q", err)
	}
}
