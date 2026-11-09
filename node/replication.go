package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
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
	// DefaultReplicationMaxRequestLatency is the default Get/Put latency above which the replication job waits
	DefaultReplicationMaxRequestLatency = 30 * time.Millisecond
	// replicationCompressionLevel is the zstd level used to stream ranges to a shadow, the cheapest one
	replicationCompressionLevel = 1
	// replicationBatchSize is the size of the batches streamed to a shadow
	replicationBatchSize = int(256 * bytesize.KB)
	// replicationMaxRetryDelay caps the delay between attempts to send a range that failed
	replicationMaxRetryDelay = 30 * time.Second
)

// ReplicationConfig configures how an active node fills its shadow.
type ReplicationConfig struct {
	// MaxBytesPerSecond throttles the copy while requests are fast; MaxRequestLatency stops it when they are slow.
	// DefaultReplicationMaxBytesPerSecond when nil, and 0 or less pauses the copy without stopping the job.
	MaxBytesPerSecond func() int64
	// MaxRequestLatency returns the Get/Put latency above which the copy waits, so that it never slows the node's
	// requests down for long. DefaultReplicationMaxRequestLatency when nil.
	MaxRequestLatency func() time.Duration
	// PauseBetweenPasses is the pause between two passes, 5s when zero
	// A pass copies every hash range the node owns to the shadow: all of it at first, then only what changed.
	PauseBetweenPasses time.Duration
	// ReconcileInterval is how often the shadow list is re-read and the gauges published, 1s when zero
	ReconcileInterval time.Duration
}

func (c *ReplicationConfig) setDefaults() {
	if c.MaxBytesPerSecond == nil {
		c.MaxBytesPerSecond = func() int64 { return DefaultReplicationMaxBytesPerSecond }
	}
	if c.MaxRequestLatency == nil {
		c.MaxRequestLatency = func() time.Duration { return DefaultReplicationMaxRequestLatency }
	}
	if c.PauseBetweenPasses == 0 {
		c.PauseBetweenPasses = 5 * time.Second
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = time.Second
	}
}

// ReplicationStatus is what GET /replication/status returns.
// Byte counts are what the copy read from the cache and charged to the budget, before compression.
type ReplicationStatus struct {
	ShadowAddress string `json:"shadowAddress"`
	// State is "running" while this node fills a shadow, "shadow" on a shadow, and "idle" otherwise
	State                  string    `json:"state"`
	PassesCompleted        int64     `json:"passesCompleted"`
	CurrentPassStartedAt   time.Time `json:"currentPassStartedAt"`
	CurrentPassRangesDone  int       `json:"currentPassRangesDone"`
	CurrentPassRangesTotal int       `json:"currentPassRangesTotal"`
	CurrentPassBytes       int64     `json:"currentPassBytes"`
	LastPassStartedAt      time.Time `json:"lastPassStartedAt"`
	LastPassEndedAt        time.Time `json:"lastPassEndedAt"`
	LastPassDuration       string    `json:"lastPassDuration"`
	LastPassBytes          int64     `json:"lastPassBytes"`
	// GapSeconds is how much the shadow may lack now: the time since the start of the last completed pass.
	// -1 before the first pass.
	GapSeconds float64 `json:"gapSeconds"`
	// WorstCaseGapSeconds is the largest gap at the current pace, reached just before a pass ends: two pass
	// durations plus the pause between passes. -1 before the first pass.
	WorstCaseGapSeconds  float64 `json:"worstCaseGapSeconds"`
	BudgetBytesPerSecond int64   `json:"budgetBytesPerSecond"`
	// BrakeReason says why the copy waits, with a budget of 0: paused, or the node's requests are slow
	BrakeReason string `json:"brakeReason"`
	LastError   string `json:"lastError"`
}

// replicator fills the node's shadow, if it has one. It re-reads the shadow list periodically and runs a job
// that sends a full pass of every hash range the node owns, then incremental passes back to back.
type replicator struct {
	s      *Service
	cfg    ReplicationConfig
	budget *budget
	// sourceForgotten is true once this node, active, holds no shadow source marker. Only reconcile uses it.
	sourceForgotten bool

	// mu protects the fields below
	mu     sync.Mutex
	target string
	cancel context.CancelFunc
	done   chan struct{}
	// progress is the status of the running job, without the fields status computes
	progress ReplicationStatus

	metrics struct {
		gap          stats.Gauge
		passDuration stats.Timer
		bytes        stats.Counter
		errors       stats.Counter
		budget       stats.Gauge
	}
}

func newReplicator(s *Service) *replicator {
	cfg := s.config.Replication
	cfg.setDefaults()
	slow := func() string { return s.latency.slow(time.Now(), cfg.MaxRequestLatency()) }
	r := &replicator{s: s, cfg: cfg, budget: newBudget(cfg.MaxBytesPerSecond, slow)}
	tags := stats.Tags{"nodeID": strconv.FormatInt(s.config.NodeID, 10)}
	r.metrics.gap = s.stats.NewTaggedStat("keydb_replication_gap_seconds", stats.GaugeType, tags)
	r.metrics.gap.Gauge(float64(-1)) // no pass is done yet; float64 like in run, a gauge panics if its type changes
	r.metrics.passDuration = s.stats.NewTaggedStat("keydb_replication_pass_duration_seconds", stats.TimerType, tags)
	r.metrics.bytes = s.stats.NewTaggedStat("keydb_replication_bytes_total", stats.CountType, tags)
	r.metrics.errors = s.stats.NewTaggedStat("keydb_replication_errors_total", stats.CountType, tags)
	r.metrics.budget = s.stats.NewTaggedStat("keydb_replication_budget_bytes_per_second", stats.GaugeType, tags)
	return r
}

// run re-reads the shadow list until ctx is done, starting and stopping the job to match it, and publishes the
// budget and the gap.
func (r *replicator) run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		r.reconcile(ctx)
		status := r.status()
		r.metrics.budget.Gauge(status.BudgetBytesPerSecond)
		r.metrics.gap.Gauge(status.GapSeconds)
		select {
		case <-ctx.Done():
			r.switchTarget(ctx, "")
			return
		case <-ticker.C:
		}
	}
}

func (r *replicator) reconcile(ctx context.Context) {
	r.s.awaitPredecessor(ctx)
	if !r.sourceForgotten {
		r.sourceForgotten = r.s.forgetShadowSource()
	}

	target := ""
	// without its own address a node can't tell whether it is the shadow listed for its index
	if r.s.config.AdvertisedAddress != "" && !r.s.isShadow() {
		target = r.s.shadowAddress()
	}
	r.mu.Lock()
	current := r.target
	r.mu.Unlock()
	if target != current {
		r.switchTarget(ctx, target)
	}
}

// switchTarget stops the running job, if any, and starts one for target unless it is empty or the node hands its
// index over, which sends its own pass (see handover).
func (r *replicator) switchTarget(ctx context.Context, target string) {
	if r.s.handingOver.Load() {
		target = ""
	}
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.target, r.cancel, r.done = "", nil, nil
	r.progress = ReplicationStatus{} // a stopped copy reports nothing, not even while it winds down
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
		r.s.logger.Infon("Replication stopped")
	}
	if target == "" || ctx.Err() != nil {
		return
	}

	jobCtx, jobCancel := context.WithCancel(ctx)
	jobDone := make(chan struct{})
	r.mu.Lock()
	r.target, r.cancel, r.done = target, jobCancel, jobDone
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

	for ctx.Err() == nil {
		r.pass(ctx, shadow, log, true)
		if !sleepCtx(ctx, r.cfg.PauseBetweenPasses) {
			return
		}
	}
}

// pass sends every hash range the node owns, retrying a range until it is sent or ctx is done, and returns true once
// every range is sent. Unless throttled, it ignores the budget.
func (r *replicator) pass(ctx context.Context, shadow pb.NodeServiceClient, log logger.Logger, throttled bool) bool {
	r.s.mu.RLock() // the hasher is swapped under the write lock when the degraded nodes change
	ranges := slices.Sorted(maps.Keys(r.s.getCurrentRanges()))
	r.s.mu.RUnlock()

	start := r.s.now()
	r.update(func(p *ReplicationStatus) {
		p.CurrentPassStartedAt, p.CurrentPassRangesDone, p.CurrentPassRangesTotal = start, 0, len(ranges)
		p.CurrentPassBytes = 0
	})
	r.s.replicating.Store(true)
	defer r.s.replicating.Store(false)

	var passBytes int64
	for i, hashRange := range ranges {
		for failures := 0; ; {
			// a paused copy must not even scan: wait before opening the range
			if throttled {
				if err := r.budget.waitNonZero(ctx); err != nil {
					return false
				}
			}
			n, err := r.sendRange(ctx, shadow, hashRange, throttled)
			if err == nil {
				passBytes += n
				break
			}
			if ctx.Err() != nil {
				return false
			}
			if errors.Is(err, errBudgetPaused) {
				continue // the range was given up so as not to hold it open while paused, it starts over once resumed
			}
			r.recordError(err)
			delay := min(time.Second<<min(failures, 5), replicationMaxRetryDelay)
			failures++
			log.Warnn("Sending hash range to the shadow",
				logger.NewIntField("hashRange", hashRange),
				logger.NewIntField("attempt", int64(failures)),
				logger.NewDurationField("retryDelay", delay),
				obskit.Error(err),
			)
			if !sleepCtx(ctx, delay) {
				return false
			}
		}
		r.update(func(p *ReplicationStatus) { p.CurrentPassRangesDone = i + 1 })
	}

	end := r.s.now()
	r.metrics.passDuration.SendTiming(end.Sub(start))
	r.update(func(p *ReplicationStatus) {
		p.PassesCompleted++
		p.LastPassStartedAt, p.LastPassEndedAt, p.LastPassBytes, p.LastError = start, end, passBytes, ""
	})
	log.Infon("Replication pass completed",
		logger.NewIntField("ranges", int64(len(ranges))),
		logger.NewIntField("bytes", passBytes),
		logger.NewDurationField("duration", end.Sub(start)),
	)
	return true
}

// handover sends one last pass to the node taking this node's index over, after stopping the job, so that it holds
// every write this node acknowledged (see Service.Handover).
func (r *replicator) handover(ctx context.Context, successor string) {
	r.switchTarget(ctx, "") // with handingOver set, nothing starts the job again
	log := r.s.logger.Withn(logger.NewStringField("successor", successor))
	if r.cfg.MaxBytesPerSecond() <= 0 {
		log.Warnn("Skipping the handover, replication is paused")
		return
	}
	conn, node, err := r.s.connectToNode(successor)
	if err != nil {
		log.Errorn("Connecting to the successor for the handover", obskit.Error(err))
		return
	}
	defer r.s.closeNodeConnection(conn)
	log.Infon("Handing the node's index over")
	start := r.s.now()
	if !r.pass(ctx, node, log, false) {
		log.Errorn("The handover didn't finish, the successor lacks what it didn't send",
			logger.NewDurationField("duration", r.s.now().Sub(start)), obskit.Error(ctx.Err()))
		return
	}
	log.Infon("Handed the node's index over", logger.NewDurationField("duration", r.s.now().Sub(start)))
}

// sendRange streams the changes of one hash range since the shadow's marker, and returns the bytes it read. Unless
// throttled, it doesn't wait for the budget. The bytes are counted as they go, so that progress shows during a long
// pass.
func (r *replicator) sendRange(
	ctx context.Context, shadow pb.NodeServiceClient, hashRange int64, throttled bool,
) (int64, error) {
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
	send := writerFunc(func(data []byte) (int, error) {
		// gRPC may read a message after Send returns, and the writer reuses its buffer
		chunk := &pb.SnapshotChunk{HashRange: hashRange, Data: bytes.Clone(data), SourceAddress: source}
		if err := stream.Send(chunk); err != nil {
			return 0, err
		}
		return len(data), nil
	})
	var charged int64
	maxVersion, err := r.s.cache.StreamSnapshot(ctx, hashRange, since.SinceTimestamp, send, badger.StreamOptions{
		CompressionLevel: replicationCompressionLevel,
		BatchSize:        replicationBatchSize,
		Wait: func(ctx context.Context, bytes int) error {
			if throttled {
				if err := r.budget.Wait(ctx, bytes); err != nil {
					return err
				}
			}
			charged += int64(bytes)
			r.metrics.bytes.Count(bytes)
			r.update(func(p *ReplicationStatus) { p.CurrentPassBytes += int64(bytes) })
			return nil
		},
	})
	if err != nil {
		return 0, fmt.Errorf("streaming hash range %d: %w", hashRange, err)
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
	return charged, nil
}

// update changes the progress of the running job. A job that is stopping changes nothing: its target is already
// cleared, and the next job only starts once it has ended.
func (r *replicator) update(f func(p *ReplicationStatus)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.target == "" {
		return
	}
	f(&r.progress)
}

func (r *replicator) recordError(err error) {
	r.metrics.errors.Increment()
	r.update(func(p *ReplicationStatus) { p.LastError = err.Error() })
}

// status returns a snapshot of the replication state.
func (r *replicator) status() ReplicationStatus {
	r.mu.Lock()
	status := r.progress
	status.ShadowAddress = r.target
	r.mu.Unlock()

	status.State, status.GapSeconds, status.WorstCaseGapSeconds = "idle", -1, -1
	if r.s.isShadow() {
		// a shadow only receives: the copy and its budget live on the active pod
		status.State = "shadow"
		return status
	}
	if status.ShadowAddress != "" {
		status.State = "running"
		status.BudgetBytesPerSecond, status.BrakeReason = r.budget.current()
	}
	if status.PassesCompleted > 0 {
		lastPass := status.LastPassEndedAt.Sub(status.LastPassStartedAt)
		status.LastPassDuration = lastPass.String()
		status.GapSeconds = r.s.now().Sub(status.LastPassStartedAt).Seconds()
		status.WorstCaseGapSeconds = (2*lastPass + r.cfg.PauseBetweenPasses).Seconds()
	}
	return status
}

// ReplicationStatus returns the state of the job filling this node's shadow, if any.
func (s *Service) ReplicationStatus() ReplicationStatus {
	return s.replication.status()
}

// writerFunc is an io.Writer that calls itself
type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

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
