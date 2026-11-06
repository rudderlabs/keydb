package node

import (
	"context"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
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
	shadowStatus := p.shadow.ReplicationStatus()
	require.Equal(t, "shadow", shadowStatus.State, "a shadow never replicates")
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

func TestReplicationGap(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	status := p.source.ReplicationStatus()
	require.EqualValues(t, -1, status.GapSeconds)
	require.EqualValues(t, -1, status.WorstCaseGapSeconds)

	p.put(t, testKeys(100)...)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().PassesCompleted > 0 },
		10*time.Second, 20*time.Millisecond, "no pass ever completed")
	status = p.source.ReplicationStatus()
	require.GreaterOrEqual(t, status.GapSeconds, float64(0))
	require.GreaterOrEqual(t, status.WorstCaseGapSeconds, float64(0))
}

func TestReplicationStopsOnClear(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().PassesCompleted > 0 },
		10*time.Second, 20*time.Millisecond)

	p.shadows.set("")
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().State == "idle" },
		10*time.Second, 20*time.Millisecond)
	status := p.source.ReplicationStatus()
	require.Zero(t, status.PassesCompleted, "a stopped copy must not report the old progress")
	require.EqualValues(t, -1, status.GapSeconds)
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

func TestReplicationStatusGap(t *testing.T) {
	start := time.Unix(1000, 0)
	now := start
	r := &replicator{
		s:      &Service{now: func() time.Time { return now }},
		cfg:    ReplicationConfig{PauseBetweenPasses: 5 * time.Second},
		budget: newTestBudget(32 * bytesize.MB),
	}
	status := r.status()
	require.EqualValues(t, -1, status.GapSeconds, "no pass yet")
	require.EqualValues(t, -1, status.WorstCaseGapSeconds)

	r.progress = ReplicationStatus{
		PassesCompleted: 3, LastPassStartedAt: start, LastPassEndedAt: start.Add(10 * time.Second),
	}
	now = start.Add(20 * time.Second)
	status = r.status()
	require.EqualValues(t, 20, status.GapSeconds)
	require.EqualValues(t, 25, status.WorstCaseGapSeconds, "two pass durations plus the pause")
	require.Equal(t, "10s", status.LastPassDuration)

	r.progress.LastPassEndedAt = start.Add(time.Second)
	now = start.Add(61 * time.Second)
	status = r.status()
	require.EqualValues(t, 61, status.GapSeconds, "the gap runs from the start of the last pass")
	require.EqualValues(t, 7, status.WorstCaseGapSeconds)
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
	require.EqualValues(t, -1, gauge("keydb_replication_gap_seconds"), "no pass is done yet")
	p.put(t, testKeys(100)...)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return gauge("keydb_replication_gap_seconds") >= 0 },
		10*time.Second, 20*time.Millisecond, "the gap gauge never got a value")
	require.Positive(t, gauge("keydb_replication_bytes_total"))
	require.Positive(t, gauge("keydb_replication_budget_bytes_per_second"))

	p.shadows.set("")
	require.Eventually(t, func() bool { return gauge("keydb_replication_gap_seconds") == -1 },
		10*time.Second, 20*time.Millisecond, "a stopped copy has no gap")
	require.Zero(t, gauge("keydb_replication_budget_bytes_per_second"), "a stopped copy has no budget")
}

// TestReplicationMetricsOnOpenTelemetry uses the stats production runs on. An OpenTelemetry gauge panics when a store
// changes the type of its value, which stats.NOP and memstats accept.
func TestReplicationMetricsOnOpenTelemetry(t *testing.T) {
	conf := config.New()
	conf.Set("OpenTelemetry.enabled", true)
	conf.Set("OpenTelemetry.metrics.prometheus.enabled", true)
	conf.Set("RuntimeStats.enabled", false)
	registry := prometheus.NewRegistry()
	stat := stats.NewStats(conf, logger.NewFactory(conf), svcMetric.NewManager(),
		stats.WithServiceName(t.Name()), stats.WithPrometheusRegistry(registry, registry))
	require.NoError(t, stat.Start(context.Background(), stats.DefaultGoRoutineFactory))
	t.Cleanup(stat.Stop)

	gap := func() float64 {
		families, err := registry.Gather()
		if err != nil {
			return -100
		}
		for _, family := range families {
			if family.GetName() == "keydb_replication_gap_seconds" && len(family.GetMetric()) > 0 {
				return family.GetMetric()[0].GetGauge().GetValue()
			}
		}
		return -100 // never exported
	}

	p := newReplicationPair(t, fastReplication(nil), stat)
	require.Eventually(t, func() bool { return gap() == -1 },
		10*time.Second, 20*time.Millisecond, "no pass is done yet")
	p.put(t, testKeys(100)...)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return gap() >= 0 },
		10*time.Second, 20*time.Millisecond, "the gap gauge never got a value")
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

// An unchanged range must not start a load on the shadow, or every range of every pass costs a Load and a Sync
func TestReplicationUnchangedRangeLoadsNothing(t *testing.T) {
	for _, compress := range []bool{true, false} {
		t.Run("compress="+strconv.FormatBool(compress), func(t *testing.T) {
			ctx := context.Background()
			sourceAddress, shadowAddress := freeAddress(t), freeAddress(t)
			newConf := func() *config.Config {
				conf := newTestConf(t)
				conf.Set("BadgerDB.Dedup.Compress", compress)
				return conf
			}
			source := newTestService(ctx, t, Config{
				NodeID:            0,
				Addresses:         staticList(sourceAddress),
				AdvertisedAddress: sourceAddress,
				ShadowAddresses:   staticList(""),
			}, newConf())
			var counter *loadCountingCache
			shadow := newTestService(ctx, t, Config{
				NodeID:            0,
				Addresses:         staticList(sourceAddress),
				AdvertisedAddress: shadowAddress,
				ShadowAddresses:   staticList(shadowAddress),
				wrapCache: func(c Cache) Cache {
					counter = &loadCountingCache{Cache: c}
					return counter
				},
			}, newConf())
			serveTestService(t, shadow, shadowAddress)
			client := nodeClient(t, shadowAddress)

			keysByHashRange, err := source.GetKeysByHashRange(testKeys(100))
			require.NoError(t, err)
			require.NoError(t, source.cache.Put(keysByHashRange, time.Hour))
			pass := func() {
				for hashRange := range int64(4) {
					_, err := source.replication.sendRange(ctx, client, hashRange)
					require.NoError(t, err)
				}
			}

			pass()
			loads := counter.loads.Load()
			require.Positive(t, loads, "the first pass loads the data")
			pass()
			pass() // a marker that the idle pass moved back would load again here
			require.Equal(t, loads, counter.loads.Load(), "nothing changed, so nothing loads")
		})
	}
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
		ReconcileInterval:  20 * time.Millisecond,
	}
	if paused != nil {
		cfg.MaxBytesPerSecond = pausable(paused, DefaultReplicationMaxBytesPerSecond)
	}
	return cfg
}

// pausable is a byte budget ceiling of maxBytes, or 0, which pauses the copy, while paused is true
func pausable(paused *atomic.Bool, maxBytes int64) func() int64 {
	return func() int64 {
		if paused.Load() {
			return 0
		}
		return maxBytes
	}
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

// loadCountingCache counts the loads the service starts on its cache
type loadCountingCache struct {
	Cache
	loads atomic.Int64
}

func (c *loadCountingCache) LoadSnapshot(ctx context.Context, r io.Reader) error {
	c.loads.Add(1)
	return c.Cache.LoadSnapshot(ctx, r)
}
