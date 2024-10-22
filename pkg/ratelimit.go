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
	ErrQueueFull         = errors.New("queue full")
)

type RateLimitFunc func() error

type queuedRateLimitItem struct {
	ctx context.Context
	f   RateLimitFunc
}

type RateLimiter struct {
	limit       int32
	dur         time.Duration
	buffSize    uint32
	num         *atomic.Int32
	errChan     chan error
	runDoneChan chan struct{}
	qChan       chan queuedRateLimitItem
	qDoneChan   chan struct{}
	stopChan    chan struct{}
	errs        error
	errMux      *sync.RWMutex
}

// New creates a new [RateLimiter] with a queue buffer size of buffSize
// and a rate limit of limit/duration.
//
// If buffSize == 0 the queue will be unbuffered and [RateLimiter.Queue] will block
// until the next slot is available.
// If buffSize > 0 the queue will be buffered and [RateLimiter.Queue] will continue
// right after the call as long as enough slots are left.
func New(buffSize uint32, limit int32, dur time.Duration) RateLimiter {
	return RateLimiter{
		limit:       limit,
		dur:         dur,
		buffSize:    buffSize,
		num:         &atomic.Int32{},
		errChan:     make(chan error),
		runDoneChan: make(chan struct{}),
		qChan:       make(chan queuedRateLimitItem, max(0, buffSize)),
		qDoneChan:   make(chan struct{}),
		stopChan:    make(chan struct{}, 1),
		errMux:      &sync.RWMutex{},
	}
}

// StartScheduler starts the scheduler for queuing [RateLimitFunc].
func (r *RateLimiter) StartScheduler() {
	go r.startScheduler()
}

// StopNow stops the scheduler.
func (r *RateLimiter) StopNow() {
	r.stopChan <- struct{}{}
}

// StopAndFlush drains the queue and stops the scheduler afterwards.
func (r *RateLimiter) StopAndFlush() {
	// Drain queue channel.
	close(r.qChan)
}

// StopChan returns the channel which receives the stop signal from [RateLimiter.StopNow].
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
			r.errChan <- fmt.Errorf("run now failed: %w", err)
		} else {
			r.runDoneChan <- struct{}{}
		}

		r.decrementCounterAfterDur()
	}()
	return
}

// Queue queues rf to be executed on the next available time slot.
// If the queue is full [ErrQueueFull] is returned.
func (r *RateLimiter) Queue(ctx context.Context, rf RateLimitFunc) (err error) {
	if int64(len(r.qChan)) >= int64(r.buffSize) {
		return ErrQueueFull
	}

	r.qChan <- queuedRateLimitItem{ctx: ctx, f: rf}
	return
}

// ErrChan returns the error channel which receives errors on the go.
func (r *RateLimiter) ErrChan() <-chan error {
	return r.errChan
}

// DoneChan signals whenever a queue item has been processed successfully.
func (r *RateLimiter) DoneChan() <-chan struct{} {
	return r.runDoneChan
}

// Errs returns all joined errors up until the point of calling this func.
func (r *RateLimiter) Errs() error {
	r.errMux.Lock()
	defer r.errMux.Unlock()

	return r.errs
}
