package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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
	// ThrottledRatio is the fraction of the cgroup's CPU periods in which it was throttled. It stays at 0 without a
	// CPU limit, since a cgroup without a quota is never throttled.
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
	requestLatencyFloor     time.Duration // 5ms when zero, request latency below it never counts as load
	throttledRatioThreshold float64       // 0.1 when zero
	healthySamplesToGrow    int           // 3 when zero
	maxPausedWait           time.Duration // defaultMaxPausedWait when zero
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
	if cfg.requestLatencyFloor == 0 {
		cfg.requestLatencyFloor = 5 * time.Millisecond
	}
	if cfg.throttledRatioThreshold == 0 {
		cfg.throttledRatioThreshold = 0.1
	}
	if cfg.healthySamplesToGrow == 0 {
		cfg.healthySamplesToGrow = 3
	}
	if cfg.maxPausedWait == 0 {
		cfg.maxPausedWait = defaultMaxPausedWait
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
	return max(min(b.budgetBytes, b.cfg.max()), 0)
}

// current returns the budget that applies now, whether the brake (or a pause) holds it below the max, and why.
func (b *budget) current() (bytesPerSecond int64, engaged bool, reason string) {
	if b.cfg.paused() {
		return 0, true, "paused manually"
	}
	maxBytes := b.cfg.max()
	if maxBytes <= 0 {
		// for example a value written with a unit, which the config reads as 0
		return 0, true, "maxBytesPerSecond is not positive"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bytesPerSecond = min(b.budgetBytes, maxBytes)
	if bytesPerSecond < maxBytes {
		return bytesPerSecond, true, b.reason
	}
	return bytesPerSecond, false, ""
}

// tick takes one brake step and returns the reason when the sample showed the node under load, empty otherwise.
// The first sample with traffic sets the request latency baseline; until then request latency doesn't count.
func (b *budget) tick(sample BrakeSample) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	maxBytes := b.cfg.max()
	b.budgetBytes = min(b.budgetBytes, maxBytes)

	var reasons []string
	if sample.SchedLatencyP99 > b.cfg.schedLatencyThreshold {
		reasons = append(reasons, "scheduler latency")
	}
	switch {
	case !b.baselineSet:
		if sample.RequestLatency > 0 {
			b.baseline, b.baselineSet = sample.RequestLatency, true
		}
	case float64(sample.RequestLatency) > max(
		float64(b.baseline)*b.cfg.requestLatencyFactor, float64(b.cfg.requestLatencyFloor),
	):
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

// resetBaseline makes the next sample with traffic the new request latency baseline, for a new replication job.
func (b *budget) resetBaseline() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baseline, b.baselineSet = 0, false
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
			} else if time.Since(pausedSince) > b.cfg.maxPausedWait {
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

// runtimeSignals measures the node's load from the Go runtime, the node's own request latency and cgroup v2.
type runtimeSignals struct {
	requestLatency func() time.Duration
	cgroupFile     string

	samples   []metrics.Sample
	prevSched *metrics.Float64Histogram
	prevCPU   cgroupCPUStat
	prevCPUOK bool
}

func newRuntimeSignals(requestLatency func() time.Duration, cgroupFile string) *runtimeSignals {
	return &runtimeSignals{
		requestLatency: requestLatency,
		cgroupFile:     cgroupFile,
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

	if cpu, ok := readCgroupCPUStat(r.cgroupFile); ok {
		if r.prevCPUOK {
			if periods := cpu.periods - r.prevCPU.periods; periods > 0 {
				sample.ThrottledRatio = float64(cpu.throttled-r.prevCPU.throttled) / float64(periods)
			}
		}
		r.prevCPU, r.prevCPUOK = cpu, true
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

// cgroupCPUStat holds the CPU period counters of a cgroup v2 cpu.stat file.
type cgroupCPUStat struct {
	periods, throttled int64
}

// readCgroupCPUStat reads nr_periods and nr_throttled from a cgroup v2 cpu.stat file. Both stay at 0 without a CPU
// limit. Throttled time (throttled_usec) isn't used: it adds up across CPUs, so it makes no ratio of the window.
func readCgroupCPUStat(file string) (cgroupCPUStat, bool) {
	if file == "" {
		return cgroupCPUStat{}, false
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return cgroupCPUStat{}, false
	}
	var (
		stat                         cgroupCPUStat
		foundPeriods, foundThrottled bool
	)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), " ")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			continue
		}
		switch name {
		case "nr_periods":
			stat.periods, foundPeriods = n, true
		case "nr_throttled":
			stat.throttled, foundThrottled = n, true
		}
	}
	return stat, foundPeriods && foundThrottled
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
