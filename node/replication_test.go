package node

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	pb "github.com/rudderlabs/keydb/proto"
)

func TestReplicationStartsFromConfig(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	keys := testKeys(1000)
	p.put(t, keys...)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "idle", p.source.ReplicationStatus().State, "no shadow listed, no replication")
	require.False(t, p.onShadow(t, keys[0]))

	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.onShadow(t, keys...) }, 10*time.Second, 50*time.Millisecond,
		"the shadow never received the data")

	status := p.source.ReplicationStatus()
	require.Equal(t, "running", status.State)
	require.Equal(t, p.shadowAddress, status.ShadowAddress)
	require.Equal(t, "ACTIVE", status.Role)
	shadowStatus := p.shadow.ReplicationStatus()
	require.Equal(t, "shadow", shadowStatus.State, "a shadow never replicates")
	require.Equal(t, []string{"this pod is the shadow, the active pod " + p.sourceAddress + " reports readiness"},
		shadowStatus.NotReadyReasons)
}

func TestReplicationIncremental(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	p.put(t, testKeys(5000)...)
	p.shadows.set(p.shadowAddress)

	require.Eventually(t, func() bool { return p.source.ReplicationStatus().PassesCompleted >= 1 },
		10*time.Second, 20*time.Millisecond)
	firstPassBytes := p.source.ReplicationStatus().LastPassBytes
	require.Positive(t, firstPassBytes)

	p.put(t, "new-key")
	passes := p.source.ReplicationStatus().PassesCompleted
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().PassesCompleted >= passes+2 },
		10*time.Second, 20*time.Millisecond)
	require.True(t, p.onShadow(t, "new-key"))
	require.Less(t, p.source.ReplicationStatus().LastPassBytes, firstPassBytes/10,
		"an incremental pass must only send what changed")
}

func TestReplicationReadiness(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	status := p.source.ReplicationStatus()
	require.False(t, status.Ready)
	require.EqualValues(t, -1, status.GapSeconds)

	p.put(t, testKeys(100)...)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().Ready },
		10*time.Second, 20*time.Millisecond, "never became ready")
	status = p.source.ReplicationStatus()
	require.True(t, status.ShadowAllRangesSynced)
	require.Empty(t, status.NotReadyReasons)
	require.GreaterOrEqual(t, status.GapSeconds, float64(0))
}

func TestReplicationStopsOnClear(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().Ready },
		10*time.Second, 20*time.Millisecond)

	p.shadows.set("")
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().State == "idle" },
		10*time.Second, 20*time.Millisecond)
	status := p.source.ReplicationStatus()
	require.False(t, status.Ready, "a stopped copy must not report the old readiness")
	require.Zero(t, status.PassesCompleted)
}

func TestReplicationNeedsAdvertisedAddress(t *testing.T) {
	ctx := context.Background()
	shadowAddress := freeAddress(t)
	s := newTestService(ctx, t, Config{
		NodeID:          0,
		Addresses:       staticList("a:1"),
		ShadowAddresses: staticList(shadowAddress),
		Replication:     fastReplication(nil),
	}, newTestConf(t))
	require.Never(t, func() bool { return s.ReplicationStatus().State != "idle" }, 200*time.Millisecond,
		10*time.Millisecond, "without an advertised address a node can't tell it isn't the shadow itself")
}

func TestReplicationNoRaceWithDegradedChange(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	p.put(t, testKeys(200)...)
	p.shadows.set(p.shadowAddress)
	stop := time.After(300 * time.Millisecond)
	for {
		select {
		case <-stop:
			return
		default:
			p.source.DegradedNodesChanged() // swaps the hasher under the write lock while passes run
		}
	}
}

func TestChunkWriterDoesNotReuseSentChunks(t *testing.T) {
	var sent [][]byte
	w := &chunkWriter{max: 4, send: func(data []byte) error {
		sent = append(sent, data) // keeps the slice, as a message handed to gRPC must not change afterwards
		return nil
	}}
	_, err := w.Write([]byte("aaaabbbbcc"))
	require.NoError(t, err)
	_, err = w.Write([]byte("ccdddd"))
	require.NoError(t, err)
	require.NoError(t, w.flush())
	require.Equal(t, []string{"aaaa", "bbbb", "cccc", "dddd"}, func() []string {
		out := make([]string, len(sent))
		for i, b := range sent {
			out[i] = string(b)
		}
		return out
	}())
}

func TestReplicationStopsOnShutdown(t *testing.T) {
	var paused atomic.Bool
	paused.Store(true) // the pass blocks waiting for the budget
	p := newReplicationPair(t, fastReplication(&paused), stats.NOP)
	p.put(t, testKeys(100)...)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().CurrentPassRangesTotal > 0 },
		10*time.Second, 20*time.Millisecond, "the pass never started")

	closed := make(chan struct{})
	go func() {
		p.source.Close()
		close(closed)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "Close waited for the replication pass")
}

func TestGCSkippedDuringPass(t *testing.T) {
	store, err := memstats.New()
	require.NoError(t, err)
	var paused atomic.Bool
	paused.Store(true) // keeps the pass in progress
	replication := fastReplication(&paused)

	ctx := context.Background()
	shadows := &shadowList{}
	shadows.set("")
	address, shadowAddress := freeAddress(t), freeAddress(t)
	s := newTestServiceWithStats(ctx, t, Config{
		NodeID:                    0,
		Addresses:                 staticList(address),
		AdvertisedAddress:         address,
		ShadowAddresses:           shadows.load,
		GarbageCollectionInterval: 20 * time.Millisecond,
		Replication:               replication,
	}, newTestConf(t), store)
	serveTestService(t, s, address)
	shadow := newTestService(ctx, t, Config{
		NodeID: 0, Addresses: staticList(address), AdvertisedAddress: shadowAddress, ShadowAddresses: shadows.load,
	}, newTestConf(t))
	serveTestService(t, shadow, shadowAddress)

	gcRuns := func() int {
		return len(store.Get("keydb_gc_duration_seconds", stats.Tags{"nodeID": "0"}).Values())
	}
	require.Eventually(t, func() bool { return gcRuns() > 0 }, 5*time.Second, 10*time.Millisecond)

	// with data to send, the pass blocks on the paused budget and stays in progress
	keysByHashRange, err := s.GetKeysByHashRange(testKeys(100))
	require.NoError(t, err)
	require.NoError(t, s.cache.Put(keysByHashRange, time.Hour))
	shadows.set(shadowAddress)
	require.Eventually(t, func() bool { return s.replicating.Load() }, 10*time.Second, 10*time.Millisecond)
	runs := gcRuns()
	time.Sleep(200 * time.Millisecond)
	require.LessOrEqual(t, gcRuns(), runs+1, "garbage collection must skip its ticks while a pass is in progress")

	paused.Store(false)
	shadows.set("")
	require.Eventually(t, func() bool { return gcRuns() > runs+2 }, 5*time.Second, 10*time.Millisecond,
		"garbage collection must resume after the pass")
}

func TestReplicationPausedStopsScanning(t *testing.T) {
	var paused atomic.Bool
	paused.Store(true)
	p := newReplicationPair(t, fastReplication(&paused), stats.NOP)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().State == "running" },
		10*time.Second, 10*time.Millisecond)

	// nothing to send, so no batch ever waits for the budget: the pass itself must not run while paused
	time.Sleep(500 * time.Millisecond)
	status := p.source.ReplicationStatus()
	require.Zero(t, status.PassesCompleted, "a paused copy must not keep scanning")
	require.Zero(t, status.CurrentPassRangesDone)

	paused.Store(false)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().PassesCompleted >= 1 },
		10*time.Second, 10*time.Millisecond, "the copy must resume once unpaused")
}

func TestPassDelayStretchesWithBrake(t *testing.T) {
	var paused atomic.Bool
	r := &replicator{
		cfg:    ReplicationConfig{PauseBetweenPasses: time.Second},
		budget: newTestBudget(32*bytesize.MB, &paused),
	}
	require.Equal(t, time.Second, r.passDelay(), "at full budget the pause is the configured one")

	r.budget.tick(BrakeSample{})
	r.budget.tick(BrakeSample{SchedLatencyP99: time.Second}) // halves the budget
	require.Equal(t, 2*time.Second, r.passDelay(), "at half the budget the pause doubles")

	for range 20 {
		r.budget.tick(BrakeSample{ThrottledRatio: 1})
	}
	require.Equal(t, maxPassDelay, r.passDelay(), "the stretched pause is capped")
}

func TestReadiness(t *testing.T) {
	start := time.Unix(1000, 0)
	const (
		threshold = time.Minute
		pause     = 5 * time.Second
	)
	shadowSynced := &pb.GetNodeInfoResponse{Role: pb.NodeRole_SHADOW, AllRangesSynced: true}
	afterPass := func(duration time.Duration) replicationProgress {
		return replicationProgress{passesCompleted: 3, lastPassStart: start, lastPassEnd: start.Add(duration)}
	}
	tests := []struct {
		name         string
		now          time.Time
		progress     replicationProgress
		info         *pb.GetNodeInfoResponse
		infoErr      error
		reasons      []string
		gap          time.Duration
		worstCaseGap time.Duration
	}{
		{
			name: "ready", now: start.Add(20 * time.Second), progress: afterPass(10 * time.Second),
			info: shadowSynced, gap: 20 * time.Second, worstCaseGap: 25 * time.Second,
		},
		{
			name: "ready through the whole next pass", now: start.Add(44 * time.Second),
			progress: afterPass(20 * time.Second),
			info:     shadowSynced, gap: 44 * time.Second, worstCaseGap: 45 * time.Second,
		},
		{
			name: "no pass yet", now: start, info: shadowSynced,
			reasons: []string{"first pass not completed"}, gap: -1, worstCaseGap: -1,
		},
		{
			name: "the gap runs from the start of the last pass", now: start.Add(61 * time.Second),
			progress: afterPass(time.Second), info: shadowSynced,
			reasons: []string{"gap above threshold"}, gap: 61 * time.Second, worstCaseGap: 7 * time.Second,
		},
		{
			name: "passes too slow for the threshold", now: start.Add(30 * time.Second),
			progress: afterPass(28 * time.Second),
			info:     shadowSynced, reasons: []string{"worst-case gap above threshold"},
			gap: 30 * time.Second, worstCaseGap: 61 * time.Second,
		},
		{
			name: "shadow unreachable", now: start.Add(time.Second), progress: afterPass(time.Second),
			infoErr: errors.New("unavailable"), reasons: []string{"shadow unreachable"},
			gap: time.Second, worstCaseGap: 7 * time.Second,
		},
		{
			name: "destination is not a shadow", now: start.Add(time.Second), progress: afterPass(time.Second),
			info:    &pb.GetNodeInfoResponse{Role: pb.NodeRole_ACTIVE, AllRangesSynced: true},
			reasons: []string{"destination is not a shadow"}, gap: time.Second, worstCaseGap: 7 * time.Second,
		},
		{
			name: "shadow is missing ranges", now: start.Add(time.Second), progress: afterPass(time.Second),
			info: &pb.GetNodeInfoResponse{Role: pb.NodeRole_SHADOW}, reasons: []string{"shadow is missing ranges"},
			gap: time.Second, worstCaseGap: 7 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := readiness(tt.now, tt.progress, pause, threshold, tt.info, tt.infoErr)
			require.Equal(t, tt.reasons, check.reasons)
			require.Equal(t, tt.gap, check.gap)
			require.Equal(t, tt.worstCaseGap, check.worstCaseGap)
		})
	}
}

func TestReplicationMetrics(t *testing.T) {
	store, err := memstats.New()
	require.NoError(t, err)
	p := newReplicationPair(t, fastReplication(nil), store)
	tags := stats.Tags{"nodeID": "0"}
	gauge := func(name string) float64 {
		m := store.Get(name, tags)
		if m == nil {
			return -100 // never set
		}
		return m.LastValue()
	}
	p.put(t, testKeys(100)...)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return gauge("keydb_replication_ready") == 1 },
		10*time.Second, 20*time.Millisecond, "the ready gauge never went up")
	require.GreaterOrEqual(t, gauge("keydb_replication_gap_seconds"), float64(0))
	require.Positive(t, gauge("keydb_replication_bytes_total"))
	require.Positive(t, gauge("keydb_replication_budget_bytes_per_second"))

	p.shadows.set("")
	require.Eventually(t, func() bool { return gauge("keydb_replication_ready") == 0 },
		10*time.Second, 20*time.Millisecond, "the ready gauge must go down once the copy stops")
	require.EqualValues(t, -1, gauge("keydb_replication_gap_seconds"), "a stopped copy has no gap")
	require.Zero(t, gauge("keydb_replication_budget_bytes_per_second"), "a stopped copy has no budget")
}

func TestReplicationProgressDuringPass(t *testing.T) {
	store, err := memstats.New()
	require.NoError(t, err)
	replication := fastReplication(nil)
	replication.MaxBytesPerSecond = func() int64 { return bytesize.MB } // a few seconds for the first pass
	p := newReplicationPair(t, replication, store)
	p.put(t, testKeys(80_000)...)
	p.shadows.set(p.shadowAddress)

	bytesSent := func() float64 {
		m := store.Get("keydb_replication_bytes_total", stats.Tags{"nodeID": "0"})
		if m == nil {
			return 0
		}
		return m.LastValue()
	}
	require.Eventually(t, func() bool {
		status := p.source.ReplicationStatus()
		return status.PassesCompleted == 0 && status.CurrentPassBytes > 0 && bytesSent() > 0
	}, 10*time.Second, 10*time.Millisecond, "bytes must be counted while the pass runs, not only at its end")
	status := p.source.ReplicationStatus()
	require.False(t, status.CurrentPassStartedAt.IsZero())
	require.Equal(t, "running", status.State)
}

func TestGCSkippedOnShadow(t *testing.T) {
	store, err := memstats.New()
	require.NoError(t, err)
	address := freeAddress(t)
	newTestServiceWithStats(context.Background(), t, Config{
		NodeID:                    0,
		Addresses:                 staticList("active:1"),
		AdvertisedAddress:         address,
		ShadowAddresses:           staticList(address),
		GarbageCollectionInterval: 20 * time.Millisecond,
	}, newTestConf(t), store)

	require.Never(t, func() bool {
		return len(store.Get("keydb_gc_duration_seconds", stats.Tags{"nodeID": "0"}).Values()) > 0
	}, 300*time.Millisecond, 20*time.Millisecond, "a shadow must not run garbage collection, even between receives")
}

// healthySignals reports a node under no load
type healthySignals struct{}

func (healthySignals) Sample() BrakeSample { return BrakeSample{} }

// shadowList is a shadow address list that tests can change while the nodes run
type shadowList struct{ v atomic.Pointer[[]string] }

func (l *shadowList) set(list ...string) { l.v.Store(&list) }

func (l *shadowList) load() []string {
	if p := l.v.Load(); p != nil {
		return *p
	}
	return nil
}

// fastReplication is a replication config that runs passes back to back in tests
func fastReplication(paused *atomic.Bool) ReplicationConfig {
	cfg := ReplicationConfig{
		PauseBetweenPasses: 20 * time.Millisecond,
		BrakeInterval:      50 * time.Millisecond,
		ReconcileInterval:  20 * time.Millisecond,
		Signals:            healthySignals{},
	}
	if paused != nil {
		cfg.Paused = paused.Load
	}
	return cfg
}

type replicationPair struct {
	source, shadow               *Service
	sourceAddress, shadowAddress string
	shadows                      *shadowList
}

// newReplicationPair starts an active node 0 and a shadow for it, with no shadow listed yet
func newReplicationPair(t *testing.T, replication ReplicationConfig, stat stats.Stats) *replicationPair {
	t.Helper()
	ctx := context.Background()
	p := &replicationPair{sourceAddress: freeAddress(t), shadowAddress: freeAddress(t), shadows: &shadowList{}}
	p.shadows.set("")
	p.source = newTestServiceWithStats(ctx, t, Config{
		NodeID:            0,
		TotalHashRanges:   4,
		Addresses:         staticList(p.sourceAddress),
		AdvertisedAddress: p.sourceAddress,
		ShadowAddresses:   p.shadows.load,
		Replication:       replication,
	}, newTestConf(t), stat)
	serveTestService(t, p.source, p.sourceAddress)
	p.shadow = newTestService(ctx, t, Config{
		NodeID:            0,
		TotalHashRanges:   4,
		Addresses:         staticList(p.sourceAddress),
		AdvertisedAddress: p.shadowAddress,
		ShadowAddresses:   p.shadows.load,
		Replication:       replication,
	}, newTestConf(t))
	serveTestService(t, p.shadow, p.shadowAddress)
	return p
}

func testKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	return keys
}

func (p *replicationPair) put(t *testing.T, keys ...string) {
	t.Helper()
	keysByHashRange, err := p.source.GetKeysByHashRange(keys)
	require.NoError(t, err)
	require.NoError(t, p.source.cache.Put(keysByHashRange, time.Hour))
}

// onShadow returns true when every key exists on the shadow
func (p *replicationPair) onShadow(t *testing.T, keys ...string) bool {
	t.Helper()
	keysByHashRange, indexes, err := p.shadow.GetKeysByHashRangeWithIndexes(keys)
	require.NoError(t, err)
	exists, err := p.shadow.cache.Get(keysByHashRange, indexes)
	require.NoError(t, err)
	for _, e := range exists {
		if !e {
			return false
		}
	}
	return true
}
