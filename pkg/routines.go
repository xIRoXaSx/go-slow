package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (r *RateLimiter) startScheduler() {
	for {
		select {
		case queued := <-r.qChan:
			// We need to block all executions if the slots are exhausted.
			if r.num.Load() >= r.limit {
				// Wait for a free slot.
				<-r.qDoneChan
			}

			go func() {
				err := r.run(queued.ctx, queued.f)
				if err != nil {
					r.errChan <- fmt.Errorf("run failed: %v", err)
				}
			}()

		case <-r.stopChan:
			return
		}
	}
}

func (r *RateLimiter) run(ctx context.Context, rf RateLimitFunc) (err error) {
	r.num.Add(1)
	defer r.decrement()

	c := make(chan error, 1)
	c <- rf()
	select {
	case cErr := <-c:
		if cErr != nil {
			err = fmt.Errorf("ratelimited run: %v", cErr)
		}

	case <-ctx.Done():
		cErr := ctx.Err()
		if cErr != nil {
			err = fmt.Errorf("context cancelled: %v", cErr)
		}
	}

	r.errMux.Lock()
	defer r.errMux.Unlock()

	r.errs = errors.Join(r.errs, err)
	return
}

func (r *RateLimiter) decrement() {
	defer func() { r.qDoneChan <- struct{}{} }()
	<-time.NewTimer(r.dur).C

	num := r.num.Load()
	if num == 0 {
		return
	}
	r.num.Store(num - 1)
}
