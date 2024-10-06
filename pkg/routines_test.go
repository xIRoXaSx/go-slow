package ratelimit_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	r "github.com/stretchr/testify/require"
	gs "github.com/xiroxasx/go-slow/pkg"
)

func TestRunNow(t *testing.T) {
	t.Parallel()

	var (
		num   atomic.Int32
		err   error
		limit = 5
		ctx   = context.Background()
		dur   = time.Millisecond * 200
		rl    = gs.New(uint32(limit), dur)
	)

	for i := 0; i < limit-1; i++ {
		err = rl.RunNow(ctx, func() error {
			num.Add(1)
			time.Sleep(dur)
			return nil
		})
		r.NoError(t, err)
	}
	err = rl.RunNow(ctx, func() error {
		num.Add(1)
		time.Sleep(dur * 2)
		rl.Stop()
		return nil
	})
	r.NoError(t, err)

	err = rl.RunNow(ctx, func() error {
		num.Add(1)
		return nil
	})
	r.ErrorIs(t, err, gs.ErrRateLimitExceeded)
	<-rl.StopChan()

	r.EqualValues(t, limit, num.Load())
}

func TestQueue(t *testing.T) {
	t.Parallel()

	var (
		num    atomic.Int32
		limit  = 1
		actual = 5
		ctx    = context.Background()
		dur    = time.Millisecond * 200
		rl     = gs.New(uint32(limit), dur)
	)

	rl.StartScheduler()
	for i := 0; i < actual; i++ {
		rl.Queue(ctx, func() error {
			num.Add(1)
			time.Sleep(dur)
			return nil
		})
	}

	rl.Queue(ctx, func() error {
		num.Add(1)
		rl.Stop()
		return nil
	})

	<-rl.StopChan()
	r.EqualValues(t, int32(actual)+1, num.Load())
}

func TestErrs(t *testing.T) {
	t.Parallel()

	var (
		limit = 5
		ctx   = context.Background()
		dur   = time.Millisecond * 200
		rl    = gs.New(uint32(limit), dur)
	)

	rl.StartScheduler()
	for i := 0; i < limit; i++ {
		func(ind int) {
			rl.Queue(ctx, func() error {
				return fmt.Errorf("test: %d", ind)
			})
		}(i)
	}
	rl.Queue(ctx, func() error {
		time.Sleep(dur)
		rl.Stop()
		return nil
	})

	<-rl.StopChan()

	r.Error(t, rl.Errs())
	errs := strings.Split(rl.Errs().Error(), "\n")
	r.Len(t, errs, limit)
}
