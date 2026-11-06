package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	// pausedPollInterval is how often a paused Wait checks whether it may continue
	pausedPollInterval = 200 * time.Millisecond
	// defaultMaxPausedWait is how long a Wait stays paused before giving up its batch, so that a long pause doesn't
	// keep a read transaction open on the source and the shadow's receive waiting
	defaultMaxPausedWait = 30 * time.Second
)

// errBudgetPaused is returned by Wait when the budget stayed at zero for longer than maxPausedWait
var errBudgetPaused = errors.New("replication budget paused for too long")

// budget is the byte budget of the replication job: the configured ceiling, or 0 while the copy must wait because the
// ceiling is 0 or the node's requests are slow.
type budget struct {
	// max is the ceiling in bytes per second. It can change at any time, and 0 or less pauses the copy.
	max func() int64
	// slow returns why the node's requests are too slow for the copy to run, empty when they aren't
	slow func() string
	// maxPausedWait is how long a Wait stays paused before giving up its batch
	maxPausedWait time.Duration

	mu      sync.Mutex
	limiter *rate.Limiter
}

func newBudget(maxBytesPerSecond func() int64, slow func() string) *budget {
	return &budget{
		max:           maxBytesPerSecond,
		slow:          slow,
		maxPausedWait: defaultMaxPausedWait,
		limiter:       rate.NewLimiter(rate.Inf, 0), // Wait sets the limit it needs
	}
}

// current returns the budget that applies now, and why it is 0 when it is.
func (b *budget) current() (bytesPerSecond int64, reason string) {
	maxBytes := b.max()
	if maxBytes <= 0 {
		// set on purpose, or a value written with a unit, which the config reads as 0
		return 0, "paused: maxBytesPerSecond is not positive"
	}
	if reason := b.slow(); reason != "" {
		return 0, reason
	}
	return maxBytes, ""
}

// Wait blocks until bytes, a batch's size before compression, fit in the budget. While the budget is 0 it waits for
// it to come back, and gives up with errBudgetPaused after maxPausedWait.
func (b *budget) Wait(ctx context.Context, bytes int) error {
	var pausedSince time.Time
	for bytes > 0 {
		bytesPerSecond, _ := b.current()
		if bytesPerSecond <= 0 {
			if pausedSince.IsZero() {
				pausedSince = time.Now()
			} else if time.Since(pausedSince) > b.maxPausedWait {
				return errBudgetPaused
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pausedPollInterval):
				continue
			}
		}
		pausedSince = time.Time{}

		b.mu.Lock()
		if b.limiter.Limit() != rate.Limit(bytesPerSecond) {
			b.limiter.SetLimit(rate.Limit(bytesPerSecond))
			b.limiter.SetBurst(int(bytesPerSecond))
		}
		step := min(bytes, b.limiter.Burst())
		b.mu.Unlock()

		if err := b.limiter.WaitN(ctx, step); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue // the budget changed under us, retry with the new one
		}
		bytes -= step
	}
	return nil
}

// waitNonZero blocks while the budget is 0, so that a paused copy doesn't keep scanning.
func (b *budget) waitNonZero(ctx context.Context) error {
	for {
		if bytesPerSecond, _ := b.current(); bytesPerSecond > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pausedPollInterval):
		}
	}
}

// latencyTracker follows the node's Get and Put requests, so that the copy waits while they are slow.
type latencyTracker struct {
	inFlight atomic.Int64
	progress atomic.Int64 // unix nanos of the last finished request, or of a start on an idle node
	sum      atomic.Int64 // nanos spent by the requests finished in the current window
	count    atomic.Int64 // requests finished in the current window

	mu          sync.Mutex
	windowStart time.Time
	mean        time.Duration // the mean latency of the last full window
}

// start records that a request started, and returns when.
func (l *latencyTracker) start() time.Time {
	now := time.Now()
	if l.inFlight.Add(1) == 1 {
		l.progress.Store(now.UnixNano()) // an idle node has nothing to wait for
	}
	return now
}

// finish records that the request that started at start finished.
func (l *latencyTracker) finish(start time.Time) {
	now := time.Now()
	l.sum.Add(int64(now.Sub(start)))
	l.count.Add(1)
	l.progress.Store(now.UnixNano())
	l.inFlight.Add(-1)
}

// slow returns why the requests are too slow, or an empty string when they aren't: their mean latency over the last
// second is above max, or requests are running and none has finished for longer than max, as when one is stuck.
func (l *latencyTracker) slow(now time.Time, maxLatency time.Duration) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.windowStart) >= time.Second {
		sum, count := l.sum.Swap(0), l.count.Swap(0)
		l.mean = 0
		if count > 0 {
			l.mean = time.Duration(sum / count)
		}
		l.windowStart = now
	}
	if l.mean > maxLatency {
		return "request latency " + l.mean.String() + " over " + maxLatency.String()
	}
	if l.inFlight.Load() > 0 && now.Sub(time.Unix(0, l.progress.Load())) > maxLatency {
		return "no request finished for over " + maxLatency.String()
	}
	return ""
}
