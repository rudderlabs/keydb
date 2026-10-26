package node

import (
	"bufio"
	"bytes"
	"context"
	"math"
	"os"
	"runtime/metrics"
	"strconv"
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
	// defaultCgroupCPUStatFile is where cgroup v2 reports CPU throttling
	defaultCgroupCPUStatFile = "/sys/fs/cgroup/cpu.stat"
	// pausedPollInterval is how often a paused Wait checks whether it may continue
	pausedPollInterval = 200 * time.Millisecond
)

// BrakeSample holds the load signals measured since the previous sample.
type BrakeSample struct {
	// SchedLatencyP99 is the 99th percentile of the time runnable goroutines waited for a CPU
	SchedLatencyP99 time.Duration
	// RequestLatency is the mean Get and Put latency
	RequestLatency time.Duration
	// ThrottledRatio is the fraction of the window the cgroup was throttled
	ThrottledRatio float64
}

// BrakeSignals measures how loaded the node is. Each call returns the signals since the previous call.
type BrakeSignals interface {
	Sample() BrakeSample
}

type budgetConfig struct {
	// max is the highest budget in bytes per second, it can change at any time
	max func() int64
	// paused stops the copy altogether, it can change at any time
	paused func() bool

	schedLatencyThreshold   time.Duration // 10ms when zero
	requestLatencyFactor    float64       // 2 when zero
	throttledRatioThreshold float64       // 0.1 when zero
	healthySamplesToGrow    int           // 3 when zero
}

// budget is the byte budget of the replication job. It halves on every sample that shows the node under load,
// down to a pause, and grows back in steps once the node is healthy again.
type budget struct {
	cfg budgetConfig

	mu          sync.Mutex
	budgetBytes int64
	baseline    time.Duration
	baselineSet bool
	healthy     int
	reason      string
	limiter     *rate.Limiter
}

func newBudget(cfg budgetConfig) *budget {
	if cfg.schedLatencyThreshold == 0 {
		cfg.schedLatencyThreshold = 10 * time.Millisecond
	}
	if cfg.requestLatencyFactor == 0 {
		cfg.requestLatencyFactor = 2
	}
	if cfg.throttledRatioThreshold == 0 {
		cfg.throttledRatioThreshold = 0.1
	}
	if cfg.healthySamplesToGrow == 0 {
		cfg.healthySamplesToGrow = 3
	}
	maxBytes := max(cfg.max(), 1)
	return &budget{
		cfg:         cfg,
		budgetBytes: maxBytes,
		limiter:     rate.NewLimiter(rate.Limit(maxBytes), int(maxBytes)),
	}
}

// effective returns the budget that applies now, 0 meaning paused.
func (b *budget) effective() int64 {
	if b.cfg.paused() {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return min(b.budgetBytes, b.cfg.max())
}

// current returns the budget that applies now, whether the brake (or a pause) holds it below the max, and why.
func (b *budget) current() (bytesPerSecond int64, engaged bool, reason string) {
	if b.cfg.paused() {
		return 0, true, "paused manually"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	maxBytes := b.cfg.max()
	bytesPerSecond = min(b.budgetBytes, maxBytes)
	if bytesPerSecond < maxBytes {
		return bytesPerSecond, true, b.reason
	}
	return bytesPerSecond, false, ""
}

// tick takes one brake step. The first sample only sets the request latency baseline.
// It returns the reason when the sample showed the node under load, empty otherwise.
func (b *budget) tick(sample BrakeSample) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	maxBytes := b.cfg.max()
	b.budgetBytes = min(b.budgetBytes, maxBytes)
	if !b.baselineSet {
		b.baseline = sample.RequestLatency
		b.baselineSet = true
		return ""
	}

	var reasons []string
	if sample.SchedLatencyP99 > b.cfg.schedLatencyThreshold {
		reasons = append(reasons, "scheduler latency")
	}
	if b.baseline > 0 && float64(sample.RequestLatency) > float64(b.baseline)*b.cfg.requestLatencyFactor {
		reasons = append(reasons, "request latency")
	}
	if sample.ThrottledRatio > b.cfg.throttledRatioThreshold {
		reasons = append(reasons, "cpu throttling")
	}

	if len(reasons) > 0 {
		b.healthy = 0
		b.budgetBytes /= 2
		if b.budgetBytes < minBudgetBytesPerSecond {
			b.budgetBytes = 0
		}
		b.reason = strings.Join(reasons, ", ")
		return b.reason
	}

	b.healthy++
	if b.healthy >= b.cfg.healthySamplesToGrow {
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
	return ""
}

// Wait blocks until n bytes fit in the budget. While the budget is 0 it waits for it to grow or be unpaused.
func (b *budget) Wait(ctx context.Context, n int) error {
	for n > 0 {
		bytesPerSecond := b.effective()
		if bytesPerSecond <= 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pausedPollInterval):
				continue
			}
		}

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

// runtimeSignals measures the node's load from the Go runtime, the node's own request latency and cgroup v2.
type runtimeSignals struct {
	requestLatency func() time.Duration
	cgroupFile     string
	now            func() time.Time

	samples           []metrics.Sample
	prevSched         *metrics.Float64Histogram
	prevThrottledUsec int64
	prevThrottledOK   bool
	prevTime          time.Time
}

func newRuntimeSignals(requestLatency func() time.Duration, cgroupFile string) *runtimeSignals {
	return &runtimeSignals{
		requestLatency: requestLatency,
		cgroupFile:     cgroupFile,
		now:            time.Now,
		samples:        []metrics.Sample{{Name: "/sched/latencies:seconds"}},
	}
}

func (r *runtimeSignals) Sample() BrakeSample {
	sample := BrakeSample{RequestLatency: r.requestLatency()}

	metrics.Read(r.samples)
	if r.samples[0].Value.Kind() == metrics.KindFloat64Histogram {
		cur := r.samples[0].Value.Float64Histogram()
		cur = &metrics.Float64Histogram{
			Counts:  append([]uint64(nil), cur.Counts...),
			Buckets: append([]float64(nil), cur.Buckets...),
		}
		sample.SchedLatencyP99 = histogramP99(r.prevSched, cur)
		r.prevSched = cur
	}

	now := r.now()
	if throttledUsec, ok := readThrottledUsec(r.cgroupFile); ok {
		if r.prevThrottledOK {
			window := now.Sub(r.prevTime)
			if window > 0 {
				delta := time.Duration(throttledUsec-r.prevThrottledUsec) * time.Microsecond
				sample.ThrottledRatio = float64(delta) / float64(window)
			}
		}
		r.prevThrottledUsec, r.prevThrottledOK = throttledUsec, true
	}
	r.prevTime = now
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

// readThrottledUsec reads throttled_usec from a cgroup v2 cpu.stat file.
func readThrottledUsec(file string) (int64, bool) {
	if file == "" {
		return 0, false
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return 0, false
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), " ")
		if !ok || name != "throttled_usec" {
			continue
		}
		usec, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return 0, false
		}
		return usec, true
	}
	return 0, false
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
