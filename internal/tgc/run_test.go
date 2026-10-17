package tgc

import (
	"context"
	"errors"
	"testing"
)

// fakeRun behaves like telegram.Client.Run: an error that wraps context.Canceled becomes nil.
func fakeRun(ctx context.Context, f func(context.Context) error) error {
	if err := f(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func TestRunChecked(t *testing.T) {
	boom := errors.New("boom")
	cancelled := errors.Join(errors.New("upload"), context.Canceled)
	for _, c := range []struct {
		name string
		run  func(context.Context, func(context.Context) error) error
		f    error
		want error
	}{
		{"callback ok", fakeRun, nil, nil},
		{"callback error", fakeRun, boom, boom},
		{"callback cancelled", fakeRun, cancelled, cancelled},
		{"client stopped before the callback", func(context.Context, func(context.Context) error) error { return nil }, nil, ErrRunStopped},
	} {
		got := runChecked(context.Background(), c.run, func(context.Context) error { return c.f })
		if !errors.Is(got, c.want) || (c.want == nil) != (got == nil) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
