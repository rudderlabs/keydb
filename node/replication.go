package node

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/keydb/internal/cache/badger"
	pb "github.com/rudderlabs/keydb/proto"
)

const (
	// DefaultReplicationMaxBytesPerSecond is the default byte budget ceiling of the replication job
	DefaultReplicationMaxBytesPerSecond = 32 * bytesize.MB
	// replicationCompressionLevel is the zstd level used to stream ranges to a shadow, the cheapest one
	replicationCompressionLevel = 1
	// replicationBatchSize is the size of the batches streamed to a shadow
	replicationBatchSize = int(256 * bytesize.KB)
	// replicationMaxRetryDelay caps the delay between attempts to send a range that failed
	replicationMaxRetryDelay = 30 * time.Second
	// replicationRPCTimeout bounds the small calls the job makes to the shadow
	replicationRPCTimeout = 5 * time.Second
	// maxPassDelay caps the pause between passes while the brake holds the budget down
	maxPassDelay = time.Minute
)

// ReplicationConfig configures how an active node fills its shadow.
type ReplicationConfig struct {
	// MaxBytesPerSecond returns the byte budget ceiling, DefaultReplicationMaxBytesPerSecond when nil
	MaxBytesPerSecond func() int64
	// Paused returns true to pause the copy without stopping the job, never paused when nil
	Paused func() bool
	// PauseBetweenPasses is the pause between two passes, 5s when zero
	PauseBetweenPasses time.Duration
	// GapThreshold is the largest gap at which the node is ready to switch, 60s when zero
	GapThreshold time.Duration
	// BrakeInterval is how often the brake samples the node's load, 5s when zero
	BrakeInterval time.Duration
	// ReconcileInterval is how often the shadow list is re-read, 1s when zero
	ReconcileInterval time.Duration
	// Signals measures the node's load, the Go runtime, request latency and cgroup throttling when nil
	Signals BrakeSignals

	// BrakeSchedLatencyThreshold is the scheduler latency p99 above which the brake engages, 10ms when zero
	BrakeSchedLatencyThreshold time.Duration
	// BrakeRequestLatencyFactor is how many times the baseline Get/Put latency engages the brake, 2 when zero
	BrakeRequestLatencyFactor float64
	// BrakeRequestLatencyFloor is the Get/Put latency below which the brake never engages, 5ms when zero
	BrakeRequestLatencyFloor time.Duration
	// BrakeThrottledRatioThreshold is the cgroup throttled ratio above which the brake engages, 0.1 when zero
	BrakeThrottledRatioThreshold float64
}

func (c *ReplicationConfig) setDefaults() {
	if c.MaxBytesPerSecond == nil {
		c.MaxBytesPerSecond = func() int64 { return DefaultReplicationMaxBytesPerSecond }
	}
	if c.Paused == nil {
		c.Paused = func() bool { return false }
	}
	if c.PauseBetweenPasses == 0 {
		c.PauseBetweenPasses = 5 * time.Second
	}
	if c.GapThreshold == 0 {
		c.GapThreshold = 60 * time.Second
	}
	if c.BrakeInterval == 0 {
		c.BrakeInterval = 5 * time.Second
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = time.Second
	}
}

// ReplicationStatus is what GET /replication/status returns.
type ReplicationStatus struct {
	Role                   string    `json:"role"`
	ShadowAddress          string    `json:"shadowAddress"`
	State                  string    `json:"state"` // "idle" or "running"
	PassesCompleted        int64     `json:"passesCompleted"`
	CurrentPassRangesDone  int       `json:"currentPassRangesDone"`
	CurrentPassRangesTotal int       `json:"currentPassRangesTotal"`
	LastPassStartedAt      time.Time `json:"lastPassStartedAt"`
	LastPassEndedAt        time.Time `json:"lastPassEndedAt"`
	LastPassDuration       string    `json:"lastPassDuration"`
	LastPassBytes          int64     `json:"lastPassBytes"`
	LastPassRanges         int       `json:"lastPassRanges"`
	GapSeconds             float64   `json:"gapSeconds"` // -1 before the first full pass
	ShadowAllRangesSynced  bool      `json:"shadowAllRangesSynced"`
	BudgetBytesPerSecond   int64     `json:"budgetBytesPerSecond"`
	BrakeEngaged           bool      `json:"brakeEngaged"`
	BrakeReason            string    `json:"brakeReason"`
	Ready                  bool      `json:"ready"`
	NotReadyReasons        []string  `json:"notReadyReasons"`
	LastError              string    `json:"lastError"`
}

// replicator fills the node's shadow, if it has one. It re-reads the shadow list periodically and runs a job
// that sends a full pass of every hash range the node owns, then incremental passes back to back.
type replicator struct {
	s       *Service
	cfg     ReplicationConfig
	budget  *budget
	signals BrakeSignals

	// mu protects the fields below
	mu       sync.Mutex
	target   string
	cancel   context.CancelFunc
	done     chan struct{}
	progress replicationProgress

	// evalMu serialises readiness evaluations
	evalMu sync.Mutex

	metrics struct {
		ready        stats.Gauge
		gap          stats.Gauge
		passDuration stats.Timer
		bytes        stats.Counter
		errors       stats.Counter
		budget       stats.Gauge
	}
}

// replicationProgress is the job's state for the current target
type replicationProgress struct {
	passesCompleted        int64
	currentPassDone        int
	currentPassTotal       int
	lastPassStart          time.Time
	lastPassEnd            time.Time
	lastPassBytes          int64
	lastPassRanges         int
	fullPassDone           bool
	shadowAllRangesSynced  bool
	ready                  bool
	notReadyReasons        []string
	lastError              string
	lastCompletedPassStart time.Time
}

func newReplicator(s *Service) *replicator {
	cfg := s.config.Replication
	cfg.setDefaults()
	r := &replicator{
		s:   s,
		cfg: cfg,
		budget: newBudget(budgetConfig{
			max:                     cfg.MaxBytesPerSecond,
			paused:                  cfg.Paused,
			schedLatencyThreshold:   cfg.BrakeSchedLatencyThreshold,
			requestLatencyFactor:    cfg.BrakeRequestLatencyFactor,
			requestLatencyFloor:     cfg.BrakeRequestLatencyFloor,
			throttledRatioThreshold: cfg.BrakeThrottledRatioThreshold,
		}),
		signals: cfg.Signals,
	}
	if r.signals == nil {
		r.signals = newRuntimeSignals(s.latency.meanSinceLast, defaultCgroupCPUStatFile)
	}
	tags := stats.Tags{"nodeID": strconv.FormatInt(s.config.NodeID, 10)}
	r.metrics.ready = s.stats.NewTaggedStat("keydb_replication_ready", stats.GaugeType, tags)
	r.metrics.gap = s.stats.NewTaggedStat("keydb_replication_gap_seconds", stats.GaugeType, tags)
	r.metrics.passDuration = s.stats.NewTaggedStat("keydb_replication_pass_duration_seconds", stats.TimerType, tags)
	r.metrics.bytes = s.stats.NewTaggedStat("keydb_replication_bytes_total", stats.CountType, tags)
	r.metrics.errors = s.stats.NewTaggedStat("keydb_replication_errors_total", stats.CountType, tags)
	r.metrics.budget = s.stats.NewTaggedStat("keydb_replication_budget_bytes_per_second", stats.GaugeType, tags)
	return r
}

// run re-reads the shadow list until ctx is done, starting and stopping the job to match it.
func (r *replicator) run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		r.reconcile(ctx)
		select {
		case <-ctx.Done():
			r.switchTarget(ctx, "")
			return
		case <-ticker.C:
		}
	}
}

func (r *replicator) reconcile(ctx context.Context) {
	r.s.forgetShadowSource()

	target := ""
	// without its own address a node can't tell whether it is the shadow listed for its index
	if r.s.config.AdvertisedAddress != "" && r.s.role() == pb.NodeRole_ACTIVE {
		target = r.s.shadowAddress()
	}
	if target == r.s.config.AdvertisedAddress {
		target = ""
	}
	r.mu.Lock()
	current := r.target
	r.mu.Unlock()
	if target != current {
		r.switchTarget(ctx, target)
	}
}

// switchTarget stops the running job, if any, and starts one for target unless it is empty.
func (r *replicator) switchTarget(ctx context.Context, target string) {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.target, r.cancel, r.done = "", nil, nil
	r.progress = replicationProgress{} // a stopped copy has no readiness to report, not even while it winds down
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
		r.metrics.ready.Gauge(0)
		r.s.logger.Infon("Replication stopped")
	}
	if target == "" || ctx.Err() != nil {
		return
	}

	jobCtx, jobCancel := context.WithCancel(ctx)
	jobDone := make(chan struct{})
	r.mu.Lock()
	r.target, r.cancel, r.done = target, jobCancel, jobDone
	r.progress = replicationProgress{}
	r.mu.Unlock()
	r.s.logger.Infon("Replication started", logger.NewStringField("shadowAddress", target))
	go func() {
		defer close(jobDone)
		r.job(jobCtx, target)
	}()
}

// job fills the shadow at target until ctx is done.
func (r *replicator) job(ctx context.Context, target string) {
	log := r.s.logger.Withn(logger.NewStringField("shadowAddress", target))
	conn, shadow, err := r.s.connectToNode(target)
	if err != nil {
		log.Errorn("Connecting to the shadow", obskit.Error(err))
		r.recordError(err)
		return
	}
	defer r.s.closeNodeConnection(conn)

	// take the request latency baseline before copying anything, from this job's own window
	r.budget.resetBaseline()
	_ = r.signals.Sample()
	if !sleepCtx(ctx, r.cfg.BrakeInterval) {
		return
	}
	r.budget.tick(r.signals.Sample())

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() { r.brakeLoop(ctx, shadow, log) })

	for ctx.Err() == nil {
		r.pass(ctx, shadow, log)
		r.evaluate(ctx, shadow, log)
		if !sleepCtx(ctx, r.passDelay()) {
			return
		}
	}
}

// brakeLoop samples the node's load, adjusts the byte budget and re-evaluates readiness.
func (r *replicator) brakeLoop(ctx context.Context, shadow pb.NodeServiceClient, log logger.Logger) {
	ticker := time.NewTicker(r.cfg.BrakeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if reason := r.budget.tick(r.signals.Sample()); reason != "" {
			for single := range strings.SplitSeq(reason, ", ") {
				r.s.stats.NewTaggedStat("keydb_replication_brake_events_total", stats.CountType, stats.Tags{
					"nodeID": strconv.FormatInt(r.s.config.NodeID, 10),
					"reason": single,
				}).Increment()
			}
			bytesPerSecond, _, _ := r.budget.current()
			log.Warnn("Replication brake engaged",
				logger.NewStringField("reason", reason),
				logger.NewIntField("budgetBytesPerSecond", bytesPerSecond),
			)
		}
		bytesPerSecond, _, _ := r.budget.current()
		r.metrics.budget.Gauge(bytesPerSecond)
		r.evaluate(ctx, shadow, log)
	}
}

// pass sends every hash range the node owns, retrying a range until it is sent or ctx is done.
func (r *replicator) pass(ctx context.Context, shadow pb.NodeServiceClient, log logger.Logger) {
	r.s.mu.RLock() // the hasher is swapped under the write lock when the degraded nodes change
	ranges := make([]int64, 0)
	for hashRange := range r.s.getCurrentRanges() {
		ranges = append(ranges, hashRange)
	}
	r.s.mu.RUnlock()
	slices.Sort(ranges)

	start := r.s.now()
	r.update(func(p *replicationProgress) {
		p.currentPassDone, p.currentPassTotal = 0, len(ranges)
	})
	r.s.replicating.Store(true)
	defer r.s.replicating.Store(false)

	var passBytes int64
	for i, hashRange := range ranges {
		for attempt := 0; ; attempt++ {
			// a paused or fully braked copy must not even scan: wait before opening the range
			if err := r.budget.waitNonZero(ctx); err != nil {
				return
			}
			n, err := r.sendRange(ctx, shadow, hashRange)
			if err == nil {
				passBytes += n
				break
			}
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errBudgetPaused) {
				// the range was given up so as not to hold it open while paused, it starts over once resumed
				attempt = -1
				continue
			}
			r.recordError(err)
			delay := min(time.Second<<min(attempt, 5), replicationMaxRetryDelay)
			log.Warnn("Sending hash range to the shadow",
				logger.NewIntField("hashRange", hashRange),
				logger.NewIntField("attempt", int64(attempt+1)),
				logger.NewDurationField("retryDelay", delay),
				obskit.Error(err),
			)
			if !sleepCtx(ctx, delay) {
				return
			}
		}
		r.update(func(p *replicationProgress) { p.currentPassDone = i + 1 })
	}

	end := r.s.now()
	r.metrics.passDuration.SendTiming(end.Sub(start))
	r.metrics.bytes.Count(int(passBytes))
	r.update(func(p *replicationProgress) {
		p.passesCompleted++
		p.lastPassStart, p.lastPassEnd = start, end
		p.lastPassBytes, p.lastPassRanges = passBytes, len(ranges)
		p.fullPassDone = true
		p.lastCompletedPassStart = start
		p.lastError = ""
	})
	log.Debugn("Replication pass completed",
		logger.NewIntField("ranges", int64(len(ranges))),
		logger.NewIntField("bytes", passBytes),
		logger.NewDurationField("duration", end.Sub(start)),
	)
}

// sendRange streams the changes of one hash range since the shadow's marker, and returns the bytes sent.
func (r *replicator) sendRange(ctx context.Context, shadow pb.NodeServiceClient, hashRange int64) (int64, error) {
	ctx, cancel := context.WithCancel(ctx) // cancelling it abandons the stream on errors
	defer cancel()

	since, err := shadow.GetSnapshotSince(ctx, &pb.GetSnapshotSinceRequest{HashRange: hashRange})
	if err != nil {
		return 0, fmt.Errorf("getting the shadow's since timestamp: %w", err)
	}
	stream, err := shadow.ReceiveSnapshot(ctx)
	if err != nil {
		return 0, fmt.Errorf("opening the snapshot stream: %w", err)
	}

	source := r.s.config.AdvertisedAddress
	w := &chunkWriter{max: DefaultStreamingChunkSize, send: func(data []byte) error {
		return stream.Send(&pb.SnapshotChunk{HashRange: hashRange, Data: data, SourceAddress: source})
	}}
	maxVersion, _, err := r.s.cache.StreamSnapshot(ctx, hashRange, since.SinceTimestamp, w, badger.StreamOptions{
		CompressionLevel: replicationCompressionLevel,
		BatchSize:        replicationBatchSize,
		Wait:             r.budget.Wait,
	})
	if err != nil {
		return 0, fmt.Errorf("streaming hash range %d: %w", hashRange, err)
	}
	if err := w.flush(); err != nil {
		return 0, fmt.Errorf("sending the last data chunk: %w", err)
	}
	if err := stream.Send(&pb.SnapshotChunk{
		HashRange: hashRange, IsLast: true, Timestamp: maxVersion, SourceAddress: source,
	}); err != nil {
		return 0, fmt.Errorf("sending the last chunk: %w", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, fmt.Errorf("closing the snapshot stream: %w", err)
	}
	if !resp.Success {
		return 0, errors.New(resp.ErrorMessage)
	}
	return w.total, nil
}

// evaluate decides whether the node is ready to switch to its shadow, and logs every change.
func (r *replicator) evaluate(ctx context.Context, shadow pb.NodeServiceClient, log logger.Logger) {
	r.evalMu.Lock()
	defer r.evalMu.Unlock()

	now := r.s.now()
	r.mu.Lock()
	p := r.progress
	r.mu.Unlock()

	var reasons []string
	gap := -1.0
	if !p.fullPassDone {
		reasons = append(reasons, "first full pass not completed")
	} else {
		gap = now.Sub(p.lastCompletedPassStart).Seconds()
		if gap > r.cfg.GapThreshold.Seconds() {
			reasons = append(reasons, "gap above threshold")
		}
	}

	shadowSynced := false
	infoCtx, cancel := context.WithTimeout(ctx, replicationRPCTimeout)
	info, err := shadow.GetNodeInfo(infoCtx, &pb.GetNodeInfoRequest{})
	cancel()
	if err != nil {
		reasons = append(reasons, "shadow unreachable")
	} else {
		shadowSynced = info.AllRangesSynced
		if info.Role != pb.NodeRole_SHADOW {
			reasons = append(reasons, "destination is not a shadow")
		}
		if !shadowSynced {
			reasons = append(reasons, "shadow is missing ranges")
		}
	}

	ready := len(reasons) == 0
	if ctx.Err() != nil {
		return // the job is stopping, its last evaluation means nothing
	}
	r.mu.Lock()
	if r.target == "" {
		r.mu.Unlock()
		return // stopped after the check above
	}
	wasReady := r.progress.ready
	r.progress.ready = ready
	r.progress.notReadyReasons = reasons
	r.progress.shadowAllRangesSynced = shadowSynced
	r.mu.Unlock()

	r.metrics.gap.Gauge(gap)
	if ready {
		r.metrics.ready.Gauge(1)
	} else {
		r.metrics.ready.Gauge(0)
	}
	switch {
	case ready && !wasReady:
		log.Infon("Replication ready to switch",
			logger.NewFloatField("gapSeconds", gap),
			logger.NewIntField("passesCompleted", p.passesCompleted),
		)
	case !ready && wasReady:
		log.Warnn("Replication no longer ready", logger.NewStringField("reasons", strings.Join(reasons, ", ")))
	}
}

// passDelay is the pause before the next pass. It grows as the brake lowers the budget: an incremental pass scans
// every table written since the last one even when it sends little, so the brake has to space passes out too.
func (r *replicator) passDelay() time.Duration {
	bytesPerSecond, _, _ := r.budget.current()
	maxBytes := r.budget.cfg.max()
	if bytesPerSecond <= 0 {
		return maxPassDelay
	}
	if bytesPerSecond >= maxBytes {
		return r.cfg.PauseBetweenPasses
	}
	delay := time.Duration(float64(r.cfg.PauseBetweenPasses) * float64(maxBytes) / float64(bytesPerSecond))
	return min(delay, maxPassDelay)
}

// update changes the progress of the running job. A job that is stopping changes nothing: its target is already
// cleared, and the next job only starts once it has ended.
func (r *replicator) update(f func(p *replicationProgress)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.target == "" {
		return
	}
	f(&r.progress)
}

func (r *replicator) recordError(err error) {
	r.metrics.errors.Increment()
	r.update(func(p *replicationProgress) { p.lastError = err.Error() })
}

// status returns a snapshot of the replication state.
func (r *replicator) status() ReplicationStatus {
	r.mu.Lock()
	target, p := r.target, r.progress
	r.mu.Unlock()

	status := ReplicationStatus{
		Role:                   r.s.role().String(),
		ShadowAddress:          target,
		State:                  "idle",
		PassesCompleted:        p.passesCompleted,
		CurrentPassRangesDone:  p.currentPassDone,
		CurrentPassRangesTotal: p.currentPassTotal,
		LastPassStartedAt:      p.lastPassStart,
		LastPassEndedAt:        p.lastPassEnd,
		LastPassBytes:          p.lastPassBytes,
		LastPassRanges:         p.lastPassRanges,
		GapSeconds:             -1,
		ShadowAllRangesSynced:  p.shadowAllRangesSynced,
		Ready:                  p.ready,
		NotReadyReasons:        p.notReadyReasons,
		LastError:              p.lastError,
	}
	if target != "" {
		status.State = "running"
	}
	if !p.lastPassEnd.IsZero() {
		status.LastPassDuration = p.lastPassEnd.Sub(p.lastPassStart).String()
	}
	if p.fullPassDone {
		status.GapSeconds = r.s.now().Sub(p.lastCompletedPassStart).Seconds()
	}
	status.BudgetBytesPerSecond, status.BrakeEngaged, status.BrakeReason = r.budget.current()
	if !p.ready && status.NotReadyReasons == nil {
		status.NotReadyReasons = []string{"no replication running"}
		if p.passesCompleted == 0 && target != "" {
			status.NotReadyReasons = []string{"first full pass not completed"}
		}
	}
	return status
}

// ReplicationStatus returns the state of the job filling this node's shadow, if any.
func (s *Service) ReplicationStatus() ReplicationStatus {
	return s.replication.status()
}

// chunkWriter sends what is written in chunks of at most max bytes.
type chunkWriter struct {
	send  func(data []byte) error
	max   int
	buf   []byte
	total int64
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for len(w.buf) >= w.max {
		chunk, rest := w.buf[:w.max:w.max], w.buf[w.max:]
		if err := w.send(chunk); err != nil {
			return 0, err
		}
		w.total += int64(w.max)
		// a message handed to gRPC must not change afterwards, so the rest goes to a new buffer
		w.buf = append([]byte(nil), rest...)
	}
	return len(p), nil
}

func (w *chunkWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	if err := w.send(w.buf); err != nil {
		return err
	}
	w.total += int64(len(w.buf))
	w.buf = nil
	return nil
}

// sleepCtx sleeps for d and returns false if ctx is done first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
