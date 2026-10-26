package node

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	pb "github.com/rudderlabs/keydb/proto"
)

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
	require.Equal(t, "idle", p.shadow.ReplicationStatus().State, "a shadow never replicates")
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

	// a request from a client without the features header blocks the switch
	_, err := p.source.Get(context.Background(), &pb.GetRequest{Keys: []string{"key-1"}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !p.source.ReplicationStatus().Ready },
		10*time.Second, 20*time.Millisecond)
	require.Contains(t, strings.Join(p.source.ReplicationStatus().NotReadyReasons, ","), "old clients")
}

func TestReplicationStopsOnClear(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().State == "running" },
		10*time.Second, 20*time.Millisecond)

	p.shadows.set("")
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().State == "idle" },
		10*time.Second, 20*time.Millisecond)
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
