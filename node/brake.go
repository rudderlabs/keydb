package node

import (
	"context"
	"errors"
	"math"
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
)

const (
	// minBudgetBytesPerSecond is the smallest budget the brake keeps, below it the copy pauses
	minBudgetBytesPerSecond = bytesize.MB
	// brakeSchedLatency is the scheduler latency p99 above which the brake engages
	brakeSchedLatency = 10 * time.Millisecond
	// brakeRequestLatencyFactor is how many times the baseline Get/Put latency engages the brake
	brakeRequestLatencyFactor = 2
	// brakeRequestLatencyFloor is the Get/Put latency below which the brake never engages
	brakeRequestLatencyFloor = 5 * time.Millisecond
	// healthySamplesToGrow is how many healthy samples in a row raise the budget by a step
	healthySamplesToGrow = 3
	// pausedPollInterval is how often a paused Wait checks whether it may continue
	pausedPollInterval = 200 * time.Millisecond
	// defaultMaxPausedWait is how long a Wait stays paused before giving up its batch, so that a long pause doesn't
	// keep a read transaction open on the source and the shadow's receive waiting
	defaultMaxPausedWait = 30 * time.Second
)

// errBudgetPaused is returned by Wait when the budget stayed at zero for longer than maxPausedWait
var errBudgetPaused = errors.New("replication budget paused for too long")

// BrakeSample holds the load signals measured since the previous sample.
type BrakeSample struct {
	// SchedLatencyP99 is the 99th percentile of the time runnable goroutines waited for a CPU
	SchedLatencyP99 time.Duration
	// RequestLatency is the mean Get and Put latency
	RequestLatency time.Duration
}

// BrakeSignals measures how loaded the node is. Each call returns the signals since the previous call.
type BrakeSignals interface {
	Sample() BrakeSample
}

// budget is the byte budget of the replication job. It halves on every sample that shows the node under load,
// down to a pause, and grows back in steps once the node is healthy again.
type budget struct {
	// max is the highest budget in bytes per second. It can change at any time, and 0 or less pauses the copy.
	max func() int64
	// maxPausedWait is how long a Wait stays paused before giving up its batch
	maxPausedWait time.Duration

	mu          sync.Mutex
	budgetBytes int64
	baseline    time.Duration // 0 until a sample with traffic
	healthy     int
	reason      string
	limiter     *rate.Limiter
}

func newBudget(maxBytesPerSecond func() int64) *budget {
	return &budget{
		max:           maxBytesPerSecond,
		maxPausedWait: defaultMaxPausedWait,
		budgetBytes:   math.MaxInt64,                // unbraked, only the max applies
		limiter:       rate.NewLimiter(rate.Inf, 0), // Wait sets the limit it needs
	}
}

// effective returns the budget that applies now, 0 meaning paused.
func (b *budget) effective() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return max(min(b.budgetBytes, b.max()), 0)
}

// current returns the budget that applies now, whether the brake (or a pause) holds it below the max, and why.
func (b *budget) current() (bytesPerSecond int64, engaged bool, reason string) {
	maxBytes := b.max()
	if maxBytes <= 0 {
		// set on purpose, or a value written with a unit, which the config reads as 0
		return 0, true, "paused: maxBytesPerSecond is not positive"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bytesPerSecond = min(b.budgetBytes, maxBytes)
	if bytesPerSecond < maxBytes {
		return bytesPerSecond, true, b.reason
	}
	return bytesPerSecond, false, ""
}

// tick takes one brake step and returns the reasons when the sample showed the node under load, none otherwise.
// The first sample with traffic sets the request latency baseline; until then request latency doesn't count.
func (b *budget) tick(sample BrakeSample) []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	maxBytes := b.max()
	if maxBytes <= 0 {
		return nil // paused: the budget resumes where it was
	}
	b.budgetBytes = min(b.budgetBytes, maxBytes)

	var reasons []string
	if sample.SchedLatencyP99 > brakeSchedLatency {
		reasons = append(reasons, "scheduler latency")
	}
	if b.baseline == 0 {
		b.baseline = sample.RequestLatency
	} else if sample.RequestLatency > max(brakeRequestLatencyFactor*b.baseline, brakeRequestLatencyFloor) {
		reasons = append(reasons, "request latency")
	}

	if len(reasons) > 0 {
		b.healthy = 0
		b.budgetBytes /= 2
		if b.budgetBytes < minBudgetBytesPerSecond {
			b.budgetBytes = 0
		}
		b.reason = strings.Join(reasons, ", ")
		return reasons
	}

	b.healthy++
	if b.healthy >= healthySamplesToGrow {
		b.healthy = 0
		if b.budgetBytes == 0 {
			b.budgetBytes = minBudgetBytesPerSecond
		} else {
			b.budgetBytes += maxBytes / 8
		}
		b.budgetBytes = min(b.budgetBytes, maxBytes)
	}
	if b.budgetBytes >= maxBytes {
		b.reason = ""
	}
	return nil
}

// resetBaseline makes the next sample with traffic the new request latency baseline, for a new replication job.
func (b *budget) resetBaseline() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baseline = 0
}

// Wait blocks until n bytes fit in the budget. While the budget is 0 it waits for it to grow or be unpaused, and
// gives up with errBudgetPaused after maxPausedWait.
func (b *budget) Wait(ctx context.Context, n int) error {
	var pausedSince time.Time
	for n > 0 {
		bytesPerSecond := b.effective()
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
		step := min(n, b.limiter.Burst())
		b.mu.Unlock()

		if err := b.limiter.WaitN(ctx, step); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue // the budget changed under us, retry with the new one
		}
		n -= step
	}
	return nil
}

// waitNonZero blocks while the budget is 0, so that a paused or fully braked copy doesn't keep scanning.
func (b *budget) waitNonZero(ctx context.Context) error {
	for b.effective() <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pausedPollInterval):
		}
	}
	return nil
}

// runtimeSignals measures the node's load from the Go runtime and the node's own request latency. CPU throttling
// needs no signal of its own: it shows in both.
type runtimeSignals struct {
	requestLatency func() time.Duration
	samples        []metrics.Sample
	prevSched      *metrics.Float64Histogram
}

func newRuntimeSignals(requestLatency func() time.Duration) *runtimeSignals {
	samples := []metrics.Sample{{Name: "/sched/latencies:seconds"}}
	return &runtimeSignals{requestLatency: requestLatency, samples: samples}
}

func (r *runtimeSignals) Sample() BrakeSample {
	sample := BrakeSample{RequestLatency: r.requestLatency()}
	metrics.Read(r.samples)
	if r.samples[0].Value.Kind() == metrics.KindFloat64Histogram {
		cur := r.samples[0].Value.Float64Histogram()
		cur = &metrics.Float64Histogram{Counts: slices.Clone(cur.Counts), Buckets: slices.Clone(cur.Buckets)}
		sample.SchedLatencyP99 = histogramP99(r.prevSched, cur)
		r.prevSched = cur
	}
	return sample
}

// histogramP99 returns the 99th percentile of the samples recorded between prev and cur.
func histogramP99(prev, cur *metrics.Float64Histogram) time.Duration {
	if cur == nil || len(cur.Counts) == 0 {
		return 0
	}
	deltas := make([]uint64, len(cur.Counts))
	var total uint64
	for i, c := range cur.Counts {
		if prev != nil && i < len(prev.Counts) && prev.Counts[i] <= c {
			c -= prev.Counts[i]
		}
		deltas[i] = c
		total += c
	}
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(total) * 0.99))
	var cumulative uint64
	for i, c := range deltas {
		cumulative += c
		if cumulative >= target {
			upper := cur.Buckets[i+1]
			if math.IsInf(upper, 1) {
				upper = cur.Buckets[i]
			}
			return time.Duration(upper * float64(time.Second))
		}
	}
	return 0
}

// latencyTracker keeps the mean Get and Put latency since it was last read.
type latencyTracker struct {
	sumNanos atomic.Int64
	count    atomic.Int64
}

func (l *latencyTracker) observe(d time.Duration) {
	l.sumNanos.Add(int64(d))
	l.count.Add(1)
}

func (l *latencyTracker) meanSinceLast() time.Duration {
	count := l.count.Swap(0)
	sum := l.sumNanos.Swap(0)
	if count == 0 {
		return 0
	}
	return time.Duration(sum / count)
}
