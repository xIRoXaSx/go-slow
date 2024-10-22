package ratelimit_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	r "github.com/stretchr/testify/require"
	gs "github.com/xiroxasx/go-slow/pkg"
)

func TestRunNow(t *testing.T) {
	t.Parallel()

	var (
		num      atomic.Int32
		limit    = 5
		ctx      = context.Background()
		dur      = time.Millisecond * 200
		rl       = gs.New(uint32(limit), int32(limit), dur)
		errTest  = errors.New("test")
		doneChan = make(chan struct{}, 1)
	)

	rl.StartScheduler()
	go func() {
		for range limit {
			select {
			case err := <-rl.ErrChan():
				r.ErrorIs(t, err, errTest)
			case <-rl.DoneChan():
			}
		}
		doneChan <- struct{}{}
	}()

	for range limit - 1 {
		r.NoError(t, rl.RunNow(ctx, func() error {
			num.Add(1)
			time.Sleep(dur)
			return nil
		}))
	}
	r.NoError(t, rl.RunNow(ctx, func() error {
		num.Add(1)
		time.Sleep(dur * 2)
		return errTest
	}))

	r.ErrorIs(t, rl.RunNow(ctx, func() error {
		num.Add(1)
		return nil
	}), gs.ErrRateLimitExceeded)

	rl.StopAndFlush()
	<-doneChan

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
		rl     = gs.New(uint32(actual+1), int32(limit), dur)
	)

	rl.StartScheduler()
	go func() {
		for range actual {
			<-rl.DoneChan()
		}
	}()

	for range actual {
		r.NoError(t, rl.Queue(ctx, func() error {
			num.Add(1)
			time.Sleep(dur)
			return nil
		}))
	}

	r.NoError(t, rl.Queue(ctx, func() error {
		num.Add(1)
		rl.StopNow()
		return nil
	}))

	<-rl.StopChan()
	r.EqualValues(t, int32(actual)+1, num.Load())
}

func TestQueueRoutines(t *testing.T) {
	t.Parallel()

	var (
		num    atomic.Int32
		limit  = 1
		actual = 5
		ctx    = context.Background()
		dur    = time.Millisecond * 200
		rl     = gs.New(uint32(actual+1), int32(limit), dur)
		wg     = &sync.WaitGroup{}
	)

	rl.StartScheduler()
	go func() {
		for range actual + 1 {
			select {
			case err := <-rl.ErrChan():
				r.NoError(t, err)
			case <-rl.DoneChan():
			}
		}
	}()

	for range actual {
		wg.Add(1)

		go func() {
			r.NoError(t, rl.Queue(ctx, func() error {
				num.Add(1)
				return nil
			}))
			wg.Done()
		}()
	}
	wg.Wait()

	r.NoError(t, rl.Queue(ctx, func() error {
		num.Add(1)
		rl.StopAndFlush()
		return nil
	}))

	<-rl.StopChan()
	r.EqualValues(t, int32(actual)+1, num.Load())
}

func TestErrs(t *testing.T) {
	t.Parallel()

	var (
		limit       = 1
		actual      = 5
		timeout     = time.Millisecond * 100
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		dur         = time.Millisecond * 200
		rl          = gs.New(5, int32(limit), dur)
	)
	defer cancel()

	rl.StartScheduler()
	go func() {
		for range actual {
			select {
			case err := <-rl.ErrChan():
				r.ErrorIs(t, errors.Unwrap(errors.Unwrap(err)), context.DeadlineExceeded)
			}
		}
	}()

	for range actual {
		r.NoError(t, rl.Queue(ctx, func() error {
			time.Sleep(timeout * 2)
			return errors.New("timeout not exceeded")
		}))
	}
	rl.StopAndFlush()
	<-rl.StopChan()

	r.Error(t, rl.Errs())
	errs := strings.Split(rl.Errs().Error(), "\n")
	r.Len(t, errs, actual)
}

func TestQueueFull(t *testing.T) {
	t.Parallel()

	var (
		limit  = 1
		actual = 5
		ctx    = context.Background()
		dur    = time.Millisecond * 200
		rl     = gs.New(0, int32(limit), dur)
	)
	rl.StartScheduler()
	go func() {
		for range actual {
			select {
			case <-rl.DoneChan():
			case err := <-rl.ErrChan():
				r.NoError(t, err)
			}
		}
	}()

	n := time.Now()
	for range actual {
		r.ErrorIs(t, rl.Queue(ctx, func() error {
			time.Sleep(time.Second)
			return nil
		}), gs.ErrQueueFull)
	}
	r.WithinDuration(t, time.Now(), n, time.Second*time.Duration(actual))
}

func TestQueueBuffered(t *testing.T) {
	t.Parallel()

	var (
		limit  = 1
		actual = 5
		ctx    = context.Background()
		dur    = time.Millisecond * 200
		rl     = gs.New(5, int32(limit), dur)
	)
	rl.StartScheduler()
	go func() {
		for range actual {
			select {
			case <-rl.DoneChan():
			case err := <-rl.ErrChan():
				r.NoError(t, err)
			}
		}
	}()

	n := time.Now()
	for range actual {
		r.NoError(t, rl.Queue(ctx, func() error {
			time.Sleep(time.Second)
			return nil
		}))
	}
	r.WithinDuration(t, time.Now(), n, time.Second)
}
