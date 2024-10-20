package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
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
		num   atomic.Int32
		err   error
		limit = 5
		ctx   = context.Background()
		dur   = time.Millisecond * 200
		rl    = gs.New(int32(limit), dur)
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
		rl.StopNow()
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
		rl     = gs.New(int32(limit), dur)
	)

	rl.StartScheduler()
	for range actual {
		rl.Queue(ctx, func() error {
			num.Add(1)
			time.Sleep(dur)
			return nil
		})
	}

	rl.Queue(ctx, func() error {
		num.Add(1)
		rl.StopNow()
		return nil
	})

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
		rl     = gs.New(int32(limit), dur)
		wg     = &sync.WaitGroup{}
	)

	rl.StartScheduler()
	go func() {
		for range actual {
			r.NoError(t, <-rl.ErrChan())
		}
	}()

	for range actual {
		wg.Add(1)

		go func() {
			rl.Queue(ctx, func() error {
				num.Add(1)
				return nil
			})
			wg.Done()
		}()
	}
	wg.Wait()

	rl.Queue(ctx, func() error {
		num.Add(1)
		rl.StopWait()
		return nil
	})

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
		rl          = gs.New(int32(limit), dur)
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
		rl.Queue(ctx, func() error {
			time.Sleep(timeout * 2)
			return errors.New("timeout not exceeded")
		})
	}
	rl.StopWait()

	select {
	case err := <-rl.ErrChan():
		r.Error(t, err)
	case <-rl.StopChan():
	}

	r.Error(t, rl.Errs())
	errs := strings.Split(rl.Errs().Error(), "\n")
	r.Len(t, errs, actual)
}

func TestA(t *testing.T) {
	c1 := make(chan error, 5)
	c2 := make(chan struct{}, 1)

	c1 <- errors.New("asdf")
	c1 <- errors.New("asdf")
	c1 <- errors.New("asdf")
	c1 <- errors.New("asdf")
	c1 <- errors.New("asdf")
	for range len(c1) {

		select {
		case <-c1:
			fmt.Println("DRAIN")
		case <-c2:
			fmt.Println("CLOSED")
		}
	}
}
