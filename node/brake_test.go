package node

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"

	pb "github.com/rudderlabs/keydb/proto"
)

func TestBudgetPausedBlocksWait(t *testing.T) {
	var maxBytes atomic.Int64
	b := newBudget(maxBytes.Load, notSlow) // a ceiling of 0 pauses the copy

	rate, reason := b.current()
	require.Zero(t, rate)
	require.Equal(t, "paused: maxBytesPerSecond is not positive", reason)

	done := make(chan error, 1)
	go func() { done <- b.Wait(context.Background(), 1024) }()
	require.Never(t, func() bool { return len(done) > 0 }, 300*time.Millisecond, 50*time.Millisecond,
		"Wait must block while paused")

	maxBytes.Store(32 * bytesize.MB)
	require.Eventually(t, func() bool { return len(done) > 0 }, 3*time.Second, 50*time.Millisecond,
		"Wait must resume once unpaused")
	require.NoError(t, <-done)
	rate, reason = b.current()
	require.EqualValues(t, 32*bytesize.MB, rate)
	require.Empty(t, reason)

	t.Run("cancelled context", func(t *testing.T) {
		maxBytes.Store(0)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- b.Wait(ctx, 1024) }()
		cancel()
		require.Eventually(t, func() bool { return len(done) > 0 }, 3*time.Second, 50*time.Millisecond)
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

func TestBudgetWaitsWhileSlow(t *testing.T) {
	var slow atomic.Bool
	slow.Store(true)
	b := newBudget(func() int64 { return 32 * bytesize.MB }, func() string {
		if slow.Load() {
			return "request latency 80ms over 50ms"
		}
		return ""
	})

	rate, reason := b.current()
	require.Zero(t, rate)
	require.Equal(t, "request latency 80ms over 50ms", reason)

	done := make(chan error, 1)
	go func() { done <- b.waitNonZero(context.Background()) }()
	require.Never(t, func() bool { return len(done) > 0 }, 300*time.Millisecond, 50*time.Millisecond,
		"the copy must wait while the node's requests are slow")
	slow.Store(false)
	require.Eventually(t, func() bool { return len(done) > 0 }, 3*time.Second, 50*time.Millisecond,
		"the copy must resume once they are fast again")
	require.NoError(t, <-done)
}

func TestBudgetWaitLargerThanBurst(t *testing.T) {
	b := newTestBudget(bytesize.MB)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, b.Wait(ctx, int(bytesize.MB+bytesize.MB/2)), "a batch bigger than a second's budget waits")
}

func TestBudgetAbandonsLongPause(t *testing.T) {
	var maxBytes atomic.Int64
	b := newBudget(maxBytes.Load, notSlow)
	b.maxPausedWait = 200 * time.Millisecond
	start := time.Now()
	err := b.Wait(context.Background(), 1024)
	require.ErrorIs(t, err, errBudgetPaused, "a long pause must give up the range instead of holding it open")
	require.Less(t, time.Since(start), 2*time.Second)

	t.Run("waitNonZero blocks until resumed", func(t *testing.T) {
		done := make(chan error, 1)
		go func() { done <- b.waitNonZero(context.Background()) }()
		require.Never(t, func() bool { return len(done) > 0 }, 300*time.Millisecond, 50*time.Millisecond)
		maxBytes.Store(bytesize.MB)
		require.Eventually(t, func() bool { return len(done) > 0 }, 3*time.Second, 50*time.Millisecond)
		require.NoError(t, <-done)
	})
}

func TestLatencyTracker(t *testing.T) {
	const maxLatency = 50 * time.Millisecond
	var l latencyTracker
	now := time.Now()
	require.Empty(t, l.slow(now, maxLatency), "an idle node is never slow")

	l.finish(l.start().Add(-80 * time.Millisecond)) // a request that took 80ms
	require.Empty(t, l.slow(now.Add(500*time.Millisecond), maxLatency), "the mean is taken once a second")
	require.Contains(t, l.slow(now.Add(time.Second), maxLatency), "request latency 80")
	require.Empty(t, l.slow(now.Add(2*time.Second), maxLatency), "only the last second counts")

	t.Run("a request that doesn't finish", func(t *testing.T) {
		var l latencyTracker
		start := l.start()
		require.Empty(t, l.slow(start.Add(maxLatency/2), maxLatency))
		require.Equal(t, "no request finished for over 50ms", l.slow(start.Add(2*maxLatency), maxLatency))
		l.finish(start)
		require.Empty(t, l.slow(start.Add(2*maxLatency), maxLatency))
	})

	t.Run("the first request after an idle spell", func(t *testing.T) {
		var l latencyTracker
		l.progress.Store(time.Now().Add(-time.Hour).UnixNano()) // the last request finished an hour ago
		start := l.start()
		require.Empty(t, l.slow(start.Add(maxLatency/2), maxLatency), "it isn't stuck for running on an idle node")
	})
}

func TestRequestsFeedTheBrake(t *testing.T) {
	ctx := context.Background()
	s := newTestService(ctx, t, Config{NodeID: 0, Addresses: staticList("a:1")}, newTestConf(t))
	for _, tc := range []struct {
		name string
		call func()
	}{
		{"get", func() { _, _ = s.Get(ctx, &pb.GetRequest{Keys: []string{"k"}}) }},
		{"put", func() { _, _ = s.Put(ctx, &pb.PutRequest{Keys: []string{"k"}, TtlSeconds: 60}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.mu.Lock() // the request waits for the lock, as it would behind garbage collection
			unlock := sync.OnceFunc(s.mu.Unlock)
			defer unlock()
			done := make(chan struct{})
			go func() {
				tc.call()
				close(done)
			}()
			require.Eventually(t, func() bool { return s.latency.inFlight.Load() == 1 }, 5*time.Second,
				10*time.Millisecond, "a request counts from the moment it arrives")
			unlock()
			<-done
			require.Zero(t, s.latency.inFlight.Load())
		})
	}
}

func newTestBudget(maxBytes int64) *budget {
	return newBudget(func() int64 { return maxBytes }, notSlow)
}

// notSlow is the request latency check of a node whose requests are never slow
func notSlow() string { return "" }
