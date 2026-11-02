package node

import (
	"context"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
)

func TestBudgetHalvesOnBreach(t *testing.T) {
	b := newTestBudget(32 * bytesize.MB)
	b.tick(BrakeSample{RequestLatency: time.Millisecond}) // baseline

	rate, engaged, _ := b.current()
	require.EqualValues(t, 32*bytesize.MB, rate)
	require.False(t, engaged)

	b.tick(BrakeSample{RequestLatency: time.Millisecond, SchedLatencyP99: 20 * time.Millisecond})
	rate, engaged, reason := b.current()
	require.EqualValues(t, 16*bytesize.MB, rate)
	require.True(t, engaged)
	require.Contains(t, reason, "scheduler latency")

	for range 10 {
		b.tick(BrakeSample{RequestLatency: 6 * time.Millisecond, SchedLatencyP99: 20 * time.Millisecond})
	}
	rate, engaged, reason = b.current()
	require.Zero(t, rate, "below the minimum budget the copy pauses")
	require.True(t, engaged)
	require.Equal(t, "scheduler latency, request latency", reason)
}

func TestBudgetRequestLatencyAgainstBaseline(t *testing.T) {
	b := newTestBudget(32 * bytesize.MB)
	b.tick(BrakeSample{RequestLatency: 10 * time.Millisecond}) // baseline

	b.tick(BrakeSample{RequestLatency: 15 * time.Millisecond})
	rate, _, _ := b.current()
	require.EqualValues(t, 32*bytesize.MB, rate, "1.5x the baseline is healthy")

	b.tick(BrakeSample{RequestLatency: 30 * time.Millisecond})
	rate, _, reason := b.current()
	require.EqualValues(t, 16*bytesize.MB, rate)
	require.Contains(t, reason, "request latency")
}

func TestBudgetBaselineSkipsIdleSamples(t *testing.T) {
	b := newTestBudget(32 * bytesize.MB)
	b.tick(BrakeSample{}) // no traffic yet, no baseline
	b.tick(BrakeSample{RequestLatency: 10 * time.Millisecond})
	b.tick(BrakeSample{RequestLatency: 30 * time.Millisecond})
	_, engaged, reason := b.current()
	require.True(t, engaged, "the baseline must come from the first sample with traffic")
	require.Contains(t, reason, "request latency")
}

func TestBudgetRequestLatencyFloor(t *testing.T) {
	b := newTestBudget(32 * bytesize.MB)
	b.tick(BrakeSample{RequestLatency: 100 * time.Microsecond}) // a quiet-hour baseline
	b.tick(BrakeSample{RequestLatency: time.Millisecond})
	_, engaged, _ := b.current()
	require.False(t, engaged, "10x a tiny baseline is still below the floor")

	b.tick(BrakeSample{RequestLatency: 6 * time.Millisecond})
	_, engaged, reason := b.current()
	require.True(t, engaged)
	require.Contains(t, reason, "request latency")
}

func TestBudgetResetBaseline(t *testing.T) {
	b := newTestBudget(32 * bytesize.MB)
	b.tick(BrakeSample{RequestLatency: 10 * time.Millisecond})
	b.resetBaseline()
	b.tick(BrakeSample{RequestLatency: 50 * time.Millisecond}) // the new job's baseline
	_, engaged, _ := b.current()
	require.False(t, engaged, "after a reset the next sample with traffic is the new baseline")
}

func TestBudgetRecoversAfterHealthySamples(t *testing.T) {
	b := newTestBudget(32 * bytesize.MB)
	b.tick(BrakeSample{}) // baseline
	for range 10 {
		b.tick(BrakeSample{SchedLatencyP99: time.Second})
	}
	rate, _, _ := b.current()
	require.Zero(t, rate)

	healthy := func(n int) {
		for range n {
			b.tick(BrakeSample{})
		}
	}
	healthy(2)
	rate, _, _ = b.current()
	require.Zero(t, rate, "two healthy samples are not enough")
	healthy(1)
	rate, _, _ = b.current()
	require.EqualValues(t, minBudgetBytesPerSecond, rate)
	healthy(3)
	rate, _, _ = b.current()
	require.EqualValues(t, minBudgetBytesPerSecond+4*bytesize.MB, rate, "grows by an eighth of the max")

	healthy(30)
	rate, engaged, _ := b.current()
	require.EqualValues(t, 32*bytesize.MB, rate, "capped at the max")
	require.False(t, engaged)
}

func TestBudgetCappedByMax(t *testing.T) {
	var maxBytes atomic.Int64
	maxBytes.Store(32 * bytesize.MB)
	b := newBudget(maxBytes.Load)
	b.tick(BrakeSample{})

	maxBytes.Store(2 * bytesize.MB)
	rate, _, _ := b.current()
	require.EqualValues(t, 2*bytesize.MB, rate, "a lower max applies at once")
	b.tick(BrakeSample{})
	maxBytes.Store(32 * bytesize.MB)
	rate, _, _ = b.current()
	require.EqualValues(t, 2*bytesize.MB, rate, "a higher max is reached by growing, not at once")
}

func TestBudgetPausedBlocksWait(t *testing.T) {
	var maxBytes atomic.Int64
	b := newBudget(maxBytes.Load) // a ceiling of 0 pauses the copy

	rate, engaged, reason := b.current()
	require.Zero(t, rate)
	require.True(t, engaged)
	require.Equal(t, "paused: maxBytesPerSecond is not positive", reason)
	b.tick(BrakeSample{})

	done := make(chan error, 1)
	go func() { done <- b.Wait(context.Background(), 1024) }()
	require.Never(t, func() bool { return len(done) > 0 }, 300*time.Millisecond, 50*time.Millisecond,
		"Wait must block while paused")

	maxBytes.Store(32 * bytesize.MB)
	require.Eventually(t, func() bool { return len(done) > 0 }, 3*time.Second, 50*time.Millisecond,
		"Wait must resume once unpaused")
	require.NoError(t, <-done)
	rate, _, _ = b.current()
	require.EqualValues(t, 32*bytesize.MB, rate, "a pause doesn't brake the budget")

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

func TestBudgetWaitLargerThanBurst(t *testing.T) {
	b := newTestBudget(bytesize.MB)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, b.Wait(ctx, int(bytesize.MB+bytesize.MB/2)), "a batch bigger than a second's budget waits")
}

func TestRuntimeSignals(t *testing.T) {
	s := newRuntimeSignals(func() time.Duration { return 7 * time.Millisecond })
	require.Equal(t, 7*time.Millisecond, s.Sample().RequestLatency, "request latency comes from the tracker")
}

func TestHistogramP99(t *testing.T) {
	prev := &metrics.Float64Histogram{Counts: []uint64{10, 0, 0}, Buckets: []float64{0, 0.001, 0.01, 0.1}}
	cur := &metrics.Float64Histogram{Counts: []uint64{10, 98, 2}, Buckets: []float64{0, 0.001, 0.01, 0.1}}
	// 100 new samples: 98 under 10ms, 2 under 100ms, so the 99th percentile falls in the last bucket
	require.Equal(t, 100*time.Millisecond, histogramP99(prev, cur))
	require.Zero(t, histogramP99(cur, cur), "no new samples")
	require.Equal(t, 100*time.Millisecond, histogramP99(nil, cur), "no previous sample counts everything")
}

func TestLatencyTracker(t *testing.T) {
	var l latencyTracker
	require.Zero(t, l.meanSinceLast())
	l.observe(2 * time.Millisecond)
	l.observe(4 * time.Millisecond)
	require.Equal(t, 3*time.Millisecond, l.meanSinceLast())
	require.Zero(t, l.meanSinceLast(), "the window resets")
}

func TestBudgetAbandonsLongPause(t *testing.T) {
	var maxBytes atomic.Int64
	b := newBudget(maxBytes.Load)
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

func newTestBudget(maxBytes int64) *budget {
	return newBudget(func() int64 { return maxBytes })
}
