package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (r *RateLimiter) startScheduler() {
	defer close(r.stopChan)

	for {
		// We need to block all executions if the slots are exhausted.
		if r.num.Load() >= r.limit {
			// Wait for a free slot.
			<-r.qDoneChan
		}

		select {
		case queued, ok := <-r.qChan:
			if !ok {
				return
			}

			r.num.Add(1)
			go func() {
				err := r.run(queued.ctx, queued.f)
				if err != nil {
					r.errChan <- fmt.Errorf("run failed: %w", err)
				} else {
					r.runDoneChan <- struct{}{}
				}
				r.decrementCounterAfterDur()
			}()

		case <-r.stopChan:
			return
		}
	}
}

func (r *RateLimiter) run(ctx context.Context, rf RateLimitFunc) (err error) {
	c := make(chan error, 1)
	go func() { c <- rf() }()

	select {
	case cErr := <-c:
		if cErr != nil {
			err = fmt.Errorf("ratelimited run: %w", cErr)
		}

	case <-ctx.Done():
		cErr := ctx.Err()
		if cErr != nil {
			err = fmt.Errorf("context cancelled: %w", cErr)
		}
	}

	if err == nil {
		return
	}

	r.errMux.Lock()
	defer r.errMux.Unlock()

	r.errs = errors.Join(r.errs, err)
	return
}

func (r *RateLimiter) decrementCounterAfterDur() {
	tick := time.NewTicker(r.dur)
	defer func() {
		tick.Stop()
		r.qDoneChan <- struct{}{}
	}()
	<-tick.C

	num := r.num.Load()
	if num == 0 {
		return
	}
	r.num.Add(-1)
}
