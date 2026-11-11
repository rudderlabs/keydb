package node

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"

	pb "github.com/rudderlabs/keydb/proto"
)

// TestHandoverSendsTheRest shuts the active node down between two passes: from then on it refuses writes, and it
// sends what the passes haven't, ignoring the budget since it serves nothing anymore, so that the shadow holds every
// write the node acknowledged.
func TestHandoverSendsTheRest(t *testing.T) {
	var budget atomic.Int64
	budget.Store(DefaultReplicationMaxBytesPerSecond)
	p := newReplicationPair(t, ReplicationConfig{
		MaxBytesPerSecond:  budget.Load,
		PauseBetweenPasses: time.Hour, // no pass after the first one
		ReconcileInterval:  20 * time.Millisecond,
	}, stats.NOP)
	p.put(t, "first")
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().PassesCompleted >= 1 },
		10*time.Second, 10*time.Millisecond)
	budget.Store(1) // a pass would take days at this budget
	rest := testKeys(1000)
	p.put(t, rest...)
	require.False(t, p.onShadow(t, rest...), "no pass may have sent them yet")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.True(t, returnsWithin(5*time.Second, func() { p.source.Handover(ctx) }),
		"the handover must ignore the budget")
	require.True(t, p.onShadow(t, append(rest, "first")...), "the shadow must hold every write the node acknowledged")

	resp, err := p.source.Put(context.Background(), &pb.PutRequest{Keys: []string{"late"}, TtlSeconds: 60})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_NOT_READY, resp.ErrorCode, "a node handing its index over must refuse writes")
}

// TestHandoverSkippedWhilePaused checks that a pause, which stops the copy, stops the handover too
func TestHandoverSkippedWhilePaused(t *testing.T) {
	var paused atomic.Bool
	p := newReplicationPair(t, fastReplication(&paused), stats.NOP)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().PassesCompleted >= 1 },
		10*time.Second, 10*time.Millisecond)
	paused.Store(true)
	time.Sleep(200 * time.Millisecond) // a range already open reads the cache as it was when it opened
	p.put(t, "late")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p.source.Handover(ctx)
	require.False(t, p.onShadow(t, "late"))
}

// TestPromotedShadowWaitsForItsSource promotes a shadow while the node it was filled from still runs, as when the
// shadow's configmap refreshes first: that node may still hand its index over, so the promoted node refuses writes
// and takes that node's streams until it is gone, then serves.
func TestPromotedShadowWaitsForItsSource(t *testing.T) {
	cases := []struct {
		name string
		// host is the host of the source's advertised address
		host string
	}{
		// a terminating pod's name stops resolving once it leaves the headless Service, though it still hands over
		{"the source's name doesn't resolve", "keydb-0-0.keydb-headless.handover.invalid"},
		// the address its stream comes from is then its advertised address
		{"the source advertises its IP", "127.0.0.1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sourceListen, shadowAddress := freeAddress(t), freeAddress(t)
			_, port, err := net.SplitHostPort(sourceListen)
			require.NoError(t, err)
			sourceAddress := net.JoinHostPort(tc.host, port)
			source := newTestService(ctx, t, Config{ // its configmap hasn't refreshed
				NodeID:            0,
				Addresses:         staticList(sourceAddress),
				AdvertisedAddress: sourceAddress,
				ShadowAddresses:   staticList(shadowAddress),
				Replication:       fastReplication(nil),
			}, newTestConf(t))
			sourceServer := serveTestService(t, source, sourceListen)
			actives, shadows := &shadowList{}, &shadowList{}
			actives.set(sourceAddress)
			shadows.set(shadowAddress)
			shadow := newTestService(ctx, t, Config{
				NodeID:            0,
				Addresses:         actives.load,
				AdvertisedAddress: shadowAddress,
				ShadowAddresses:   shadows.load,
				Replication:       fastReplication(nil),
			}, newTestConf(t))
			serveTestService(t, shadow, shadowAddress)
			require.Equal(t, pb.ErrorCode_NO_ERROR, put(ctx, source, "before"))
			require.Eventually(t, func() bool { return existsOn(t, shadow, "before") }, 10*time.Second,
				10*time.Millisecond)

			actives.set(shadowAddress)
			shadows.set("")
			// longer than a check of the source plus the pause between two
			require.Never(t, func() bool { return put(ctx, shadow, "early") != pb.ErrorCode_NOT_READY }, 3*time.Second,
				50*time.Millisecond, "the promoted node must not serve while its source runs")
			require.Equal(t, pb.ErrorCode_NO_ERROR, put(ctx, source, "while-waiting"))
			require.Eventually(t, func() bool { return existsOn(t, shadow, "while-waiting") }, 10*time.Second,
				10*time.Millisecond, "the promoted node must take its source's streams while the source runs")

			sourceServer.Stop()
			require.Eventually(t, func() bool { return put(ctx, shadow, "after") == pb.ErrorCode_NO_ERROR },
				10*time.Second, 50*time.Millisecond, "the promoted node must serve once its source is gone")
		})
	}
}

// TestShadowRestartedPromotedWaitsForItsSource restarts a shadow that comes back with PR2's config, which promotes
// it, while the node it was filled from still runs: that node may still hand its index over, so the restarted node
// waits for it as if it had never restarted, then serves.
func TestShadowRestartedPromotedWaitsForItsSource(t *testing.T) {
	ctx := context.Background()
	sourceAddress, shadowAddress := freeAddress(t), freeAddress(t)
	source := newTestService(ctx, t, Config{ // its configmap hasn't refreshed
		NodeID:            0,
		Addresses:         staticList(sourceAddress),
		AdvertisedAddress: sourceAddress,
		ShadowAddresses:   staticList(shadowAddress),
		Replication:       fastReplication(nil),
	}, newTestConf(t))
	sourceServer := serveTestService(t, source, sourceAddress)
	shadowConf := newTestConf(t)
	shadow := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         staticList(sourceAddress),
		AdvertisedAddress: shadowAddress,
		ShadowAddresses:   staticList(shadowAddress),
		Replication:       fastReplication(nil),
	}, shadowConf)
	shadowServer := serveTestService(t, shadow, shadowAddress)
	require.Equal(t, pb.ErrorCode_NO_ERROR, put(ctx, source, "before"))
	require.Eventually(t, func() bool { return existsOn(t, shadow, "before") }, 10*time.Second, 10*time.Millisecond)

	shadowServer.Stop()
	shadow.Close()
	restarted := newTestService(ctx, t, Config{ // on the same disk, with PR2's config
		NodeID:            0,
		Addresses:         staticList(shadowAddress),
		AdvertisedAddress: shadowAddress,
		ShadowAddresses:   staticList(""),
		Replication:       fastReplication(nil),
	}, shadowConf)
	serveTestService(t, restarted, shadowAddress)
	require.Never(t, func() bool { return put(ctx, restarted, "early") != pb.ErrorCode_NOT_READY }, 3*time.Second,
		50*time.Millisecond, "the restarted node must not serve while its source runs")
	require.Equal(t, pb.ErrorCode_NO_ERROR, put(ctx, source, "while-waiting"))
	require.Eventually(t, func() bool { return existsOn(t, restarted, "while-waiting") }, 10*time.Second,
		10*time.Millisecond, "the restarted node must take its source's streams while the source runs")

	sourceServer.Stop()
	require.Eventually(t, func() bool { return put(ctx, restarted, "after") == pb.ErrorCode_NO_ERROR },
		10*time.Second, 50*time.Millisecond, "the restarted node must serve once its source is gone")
}

// TestShadowRestartedPromotedServesWithoutHandover restarts a promoted shadow whose source is already gone: no
// handover comes, and the node serves once restoredPredecessorWait has passed.
func TestShadowRestartedPromotedServesWithoutHandover(t *testing.T) {
	ctx := context.Background()
	sourceAddress, shadowAddress := freeAddress(t), freeAddress(t)
	source := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         staticList(sourceAddress),
		AdvertisedAddress: sourceAddress,
		ShadowAddresses:   staticList(shadowAddress),
		Replication:       fastReplication(nil),
	}, newTestConf(t))
	sourceServer := serveTestService(t, source, sourceAddress)
	shadowConf := newTestConf(t)
	shadow := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         staticList(sourceAddress),
		AdvertisedAddress: shadowAddress,
		ShadowAddresses:   staticList(shadowAddress),
		Replication:       fastReplication(nil),
	}, shadowConf)
	shadowServer := serveTestService(t, shadow, shadowAddress)
	require.Equal(t, pb.ErrorCode_NO_ERROR, put(ctx, source, "before"))
	require.Eventually(t, func() bool { return existsOn(t, shadow, "before") }, 10*time.Second, 10*time.Millisecond)

	sourceServer.Stop()
	source.Close()
	shadowServer.Stop()
	shadow.Close()
	start := time.Now()
	restarted := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         staticList(shadowAddress),
		AdvertisedAddress: shadowAddress,
		ShadowAddresses:   staticList(""),
		Replication:       fastReplication(nil),
	}, shadowConf)
	serveTestService(t, restarted, shadowAddress)
	require.Equal(t, pb.ErrorCode_NOT_READY, put(ctx, restarted, "early"), "a handover may still come")
	require.Eventually(t, func() bool { return put(ctx, restarted, "after") == pb.ErrorCode_NO_ERROR },
		restoredPredecessorWait+10*time.Second, 100*time.Millisecond, "the restarted node must serve in the end")
	require.GreaterOrEqual(t, time.Since(start), restoredPredecessorWait)
	require.True(t, existsOn(t, restarted, "before"))
}

// put writes key on s and returns the error code it answers with, INTERNAL_ERROR when the call fails
func put(ctx context.Context, s *Service, key string) pb.ErrorCode {
	resp, err := s.Put(ctx, &pb.PutRequest{Keys: []string{key}, TtlSeconds: 60})
	if err != nil {
		return pb.ErrorCode_INTERNAL_ERROR
	}
	return resp.ErrorCode
}
