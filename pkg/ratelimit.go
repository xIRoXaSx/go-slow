package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

type RateLimitFunc func() error

type queuedRateLimitItem struct {
	ctx context.Context
	f   RateLimitFunc
}

type RateLimiter struct {
	limit     uint32
	dur       time.Duration
	num       *atomic.Uint32
	errChan   chan error
	qChan     chan queuedRateLimitItem
	qDoneChan chan struct{}
	stopChan  chan struct{}
	errs      error
	errMux    *sync.RWMutex
}

func New(limit uint32, dur time.Duration) RateLimiter {
	return RateLimiter{
		limit:     limit,
		dur:       dur,
		num:       &atomic.Uint32{},
		errChan:   make(chan error),
		qChan:     make(chan queuedRateLimitItem),
		qDoneChan: make(chan struct{}),
		stopChan:  make(chan struct{}, 1),
		errMux:    &sync.RWMutex{},
	}
}

func (r *RateLimiter) StartScheduler() {
	go r.startScheduler()
}

func (r *RateLimiter) Stop() {
	r.stopChan <- struct{}{}
	close(r.stopChan)
}

func (r *RateLimiter) StopChan() <-chan struct{} {
	return r.stopChan
}

// RunNow tries to run rf immediately without queuing it.
// If the limit is exceeded, the returned error is of type [ErrRateLimitExceeded].
func (r *RateLimiter) RunNow(ctx context.Context, rf RateLimitFunc) (err error) {
	if r.num.Load() >= r.limit {
		return ErrRateLimitExceeded
	}
	r.num.Add(1)

	go func() {
		err := r.run(ctx, rf)
		if err != nil {
			r.errChan <- fmt.Errorf("runnow failed: %w", err)
		}
	}()
	return
}

// Queue queues rf to be executed on the next available timeslot.
func (r *RateLimiter) Queue(ctx context.Context, rf RateLimitFunc) {
	r.qChan <- queuedRateLimitItem{ctx: ctx, f: rf}
}

// ErrChan returns the error channel which receives errors on the go.
func (r *RateLimiter) ErrChan() <-chan error {
	return r.errChan
}

// Errs returns all joined errors up until the point of calling this func.
func (r *RateLimiter) Errs() error {
	r.errMux.Lock()
	defer r.errMux.Unlock()

	return r.errs
}
