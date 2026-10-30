package node

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	miniokit "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/hash"
	keydbth "github.com/rudderlabs/keydb/internal/testhelper"
	pb "github.com/rudderlabs/keydb/proto"
)

// These tests move a node onto a new disk through a shadow, under client load, and cover the clients around a move.
// The nodes are real services over real gRPC with Badger on disk and MinIO in a container, like the other node tests.
// Each node has its own view of the configuration, which tests change one node at a time to reproduce the kubelet
// refreshing the configmap of each pod at a different moment.

func TestMoveHappyPath(t *testing.T) {
	for _, order := range []switchOrder{oldNodeStopsFirst, newNodeRefreshesFirst, oldNodeRefreshesFirst} {
		t.Run("order "+strconv.Itoa(int(order)), func(t *testing.T) {
			c := newMoveCluster(t, fastMoveReplication())
			node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
			c.setTopology([]string{node0.address, node1.address}, nil)
			c.start(node0)
			c.start(node1)

			cl := c.newClient()
			l := startLoad(t, cl)
			require.Eventually(t, func() bool { return l.ackedCount() > 20 }, 10*time.Second, 10*time.Millisecond)

			lastPassStart, stopped := c.move(t, node0, node0b, cl, order)

			// the client keeps writing through the switch and lands on the new node
			acked := l.ackedCount()
			require.Eventually(t, func() bool { return l.ackedCount() > acked+20 }, 30*time.Second,
				10*time.Millisecond, "writes never resumed after the switch")
			l.finish()

			require.Empty(t, l.errors(), "the client must not surface errors during a move")
			require.Less(t, l.maxStall, 10*time.Second, "the switch stalled the client for too long")
			require.True(t, hasKeys(t, node0b, l.keysAckedBetween(time.Time{}, lastPassStart)),
				"keys acknowledged before the last completed pass must be on the new node")
			require.True(t, hasKeys(t, node0b, l.keysStartedAfter(stopped)),
				"keys written after the old node stopped must be on the new node")
			require.True(t, hasKeys(t, node1, l.keysAckedBetween(time.Time{}, time.Now())),
				"the node that didn't move keeps all its keys")
		})
	}
}

func TestMoveTwiceBackAndForth(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	slotA, node1 := c.addNode(0), c.addNode(1)
	c.setTopology([]string{slotA.address, node1.address}, nil)
	c.start(slotA)
	c.start(node1)

	cl := c.newClient()
	l := startLoad(t, cl)
	require.Eventually(t, func() bool { return l.ackedCount() > 20 }, 10*time.Second, 10*time.Millisecond)

	// first move: keydb-0 → keydb-0-b
	slotB := c.addNode(0)
	firstLastPass, _ := c.move(t, slotA, slotB, cl, oldNodeStopsFirst)
	acked := l.ackedCount()
	require.Eventually(t, func() bool { return l.ackedCount() > acked+20 }, 30*time.Second, 10*time.Millisecond)
	betweenMoves := time.Now()
	require.Eventually(t, func() bool { return l.ackedCount() > acked+60 }, 30*time.Second, 10*time.Millisecond)

	// second move: keydb-0-b → keydb-0 again, on a fresh disk
	slotAAgain := c.addNode(0)
	secondLastPass, _ := c.move(t, slotB, slotAAgain, cl, newNodeRefreshesFirst)
	acked = l.ackedCount()
	require.Eventually(t, func() bool { return l.ackedCount() > acked+20 }, 30*time.Second, 10*time.Millisecond)
	l.finish()

	require.Empty(t, l.errors())
	require.True(t, hasKeys(t, slotAAgain, l.keysAckedBetween(time.Time{}, firstLastPass)),
		"keys from before the first move survive both moves")
	require.True(t, hasKeys(t, slotAAgain, l.keysAckedBetween(betweenMoves, secondLastPass)),
		"keys written between the moves survive the second move")
	require.Equal(t, 2, cl.ClusterSize())

	// the client is back on slot a's address: node 0's keys are served from there
	finalKey := c.keyOf(0, "final")
	require.NoError(t, cl.Put(context.Background(), []string{finalKey}, time.Hour))
	require.True(t, hasKeys(t, slotAAgain, []string{finalKey}))
	exists, err := cl.Get(context.Background(), l.keysAckedBetween(time.Time{}, firstLastPass))
	require.NoError(t, err)
	for _, e := range exists {
		require.True(t, e)
	}
}

func TestMoveRestartMidReplication(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
	c.setTopology([]string{node0.address, node1.address}, nil)
	c.start(node0)
	c.start(node1)
	cl := c.newClient()
	keys := testKeys(2000)
	require.NoError(t, cl.Put(context.Background(), keys, time.Hour))

	c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
	c.start(node0b)
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond)

	// the serving node restarts on the same disk
	c.stop(node0)
	c.start(node0)
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().State == "running" }, 10*time.Second,
		10*time.Millisecond, "replication resumes on boot")
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond, "readiness must be reported again once caught up")
	status := node0.service.ReplicationStatus()
	require.Less(t, status.LastPassBytes, 100*bytesize.KB, "after a restart the passes are incremental")
	require.True(t, hasKeys(t, node0b, keys))
}

func TestMoveIdleClient(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
	c.setTopology([]string{node0.address, node1.address}, nil)
	c.start(node0)
	c.start(node1)

	active := c.newClient()
	idle := c.newClient()
	keys := testKeys(200)
	require.NoError(t, idle.Put(context.Background(), keys, time.Hour)) // discovers, then goes quiet

	c.move(t, node0, node0b, active, oldNodeStopsFirst)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exists, err := idle.Get(ctx, keys)
	require.NoError(t, err, "an idle client must find the new node once the old one is gone")
	for _, e := range exists {
		require.True(t, e)
	}
}

func TestMoveStaleListsDuringRefresh(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
	c.setTopology([]string{node0.address, node1.address}, nil)
	c.start(node0)
	c.start(node1)
	cl := c.newClient()
	keys := testKeys(200)
	require.NoError(t, cl.Put(context.Background(), keys, time.Hour))

	c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
	c.start(node0b)
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond)

	// the old node is gone, but neither the shadow nor node 1 picked up the new config yet: both still hand out
	// the old list, so the client is sent back to the dead address until they refresh
	c.stop(node0)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := cl.Get(ctx, keys)
		done <- err
	}()
	time.Sleep(time.Second)
	require.Empty(t, done, "the request cannot succeed before the configs refresh")

	after := []string{node0b.address, node1.address}
	c.setTopology(after, []string{"", ""}, node0b)
	c.setTopology(after, []string{"", ""}, node1)
	select {
	case err := <-done:
		require.NoError(t, err, "the client must converge once the configs refresh")
	case <-time.After(30 * time.Second):
		t.Fatal("the client never converged")
	}
}

func TestMoveCPUBounds(t *testing.T) {
	t.Run("byte budget holds", func(t *testing.T) {
		replication := fastMoveReplication()
		replication.MaxBytesPerSecond = func() int64 { return bytesize.MB }
		c := newMoveCluster(t, replication)
		node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
		c.setTopology([]string{node0.address, node1.address}, nil)
		c.start(node0)
		c.start(node1)
		// about 2.5 MiB to send: well above the one second of budget the limiter starts with
		keysByHashRange, err := c.hash.GetKeysByHashRange(ownedBy(c.hash, 0, testKeys(240_000)), 0)
		require.NoError(t, err)
		require.NoError(t, node0.service.cache.Put(keysByHashRange, time.Hour))

		c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
		c.start(node0b)
		require.Eventually(t, func() bool { return node0.service.ReplicationStatus().PassesCompleted >= 1 },
			60*time.Second, 20*time.Millisecond)
		status := node0.service.ReplicationStatus()
		duration, err := time.ParseDuration(status.LastPassDuration)
		require.NoError(t, err)
		// the limiter starts with a second of budget, every byte past it waits for the budget
		require.Greater(t, status.LastPassBytes, 2*bytesize.MB, "enough data to measure the budget")
		minDuration := time.Duration(float64(status.LastPassBytes-bytesize.MB) / float64(bytesize.MB) *
			float64(time.Second))
		require.GreaterOrEqual(t, duration, minDuration-50*time.Millisecond,
			"a budget of 1 MiB/s must slow the first pass down")
	})

	t.Run("brake engages and releases", func(t *testing.T) {
		signals := &switchableSignals{}
		replication := fastMoveReplication()
		replication.Signals = signals
		c := newMoveCluster(t, replication)
		node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
		c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
		c.start(node0)
		c.start(node1)
		c.start(node0b)
		require.Eventually(t, func() bool { return node0.service.ReplicationStatus().State == "running" },
			10*time.Second, 10*time.Millisecond)

		signals.loaded.Store(true)
		require.Eventually(t, func() bool {
			status := node0.service.ReplicationStatus()
			return status.BrakeEngaged && strings.Contains(status.BrakeReason, "scheduler latency")
		}, 10*time.Second, 10*time.Millisecond, "the brake never engaged")
		signals.loaded.Store(false)
		require.Eventually(t, func() bool { return !node0.service.ReplicationStatus().BrakeEngaged },
			30*time.Second, 10*time.Millisecond, "the brake never released")
	})

	t.Run("manual pause and resume", func(t *testing.T) {
		var paused atomic.Bool
		paused.Store(true)
		replication := fastMoveReplication()
		replication.Paused = paused.Load
		c := newMoveCluster(t, replication)
		node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
		c.setTopology([]string{node0.address, node1.address}, nil)
		c.start(node0)
		c.start(node1)
		cl := c.newClient()
		keys := testKeys(500)
		require.NoError(t, cl.Put(context.Background(), keys, time.Hour))

		c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
		c.start(node0b)
		time.Sleep(500 * time.Millisecond)
		status := node0.service.ReplicationStatus()
		require.Zero(t, status.PassesCompleted, "a paused copy makes no progress")
		require.Zero(t, status.BudgetBytesPerSecond)
		require.Contains(t, status.BrakeReason, "paused")

		paused.Store(false)
		require.Eventually(t, func() bool { return node0.service.ReplicationStatus().PassesCompleted >= 1 },
			30*time.Second, 20*time.Millisecond, "the copy never resumed")
		require.True(t, hasKeys(t, node0b, keys))
	})
}

func TestMoveLeftoverDisk(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	slotA, node1 := c.addNode(0), c.addNode(1)
	c.setTopology([]string{slotA.address, node1.address}, nil)
	c.start(slotA)
	c.start(node1)
	cl := c.newClient()
	require.NoError(t, cl.Put(context.Background(), testKeys(200), time.Hour))

	slotB := c.addNode(0)
	c.move(t, slotA, slotB, cl, oldNodeStopsFirst)

	// moving back onto slot a, but Kubernetes didn't delete its old disk
	leftover := c.addNode(0)
	leftover.dir = slotA.dir
	c.setTopology([]string{slotB.address, node1.address}, []string{leftover.address, ""})
	c.start(leftover)
	require.Eventually(t, func() bool {
		return strings.Contains(slotB.service.ReplicationStatus().LastError, "leftover")
	}, 30*time.Second, 20*time.Millisecond, "the shadow must refuse to merge into a leftover disk")
	require.False(t, slotB.service.ReplicationStatus().Ready)
}

func TestMoveLeftoverDiskReusedSlot(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	slotA, node1 := c.addNode(0), c.addNode(1)
	c.setTopology([]string{slotA.address, node1.address}, nil)
	c.start(slotA)
	c.start(node1)
	cl := c.newClient()
	require.NoError(t, cl.Put(context.Background(), testKeys(200), time.Hour))

	// a → b, then b → a: slot b's pod goes away, but suppose Kubernetes didn't delete its disk
	slotB := c.addNode(0)
	c.move(t, slotA, slotB, cl, oldNodeStopsFirst)
	slotAAgain := c.addNodeAt(0, slotA.address, t.TempDir())
	c.move(t, slotB, slotAAgain, cl, oldNodeStopsFirst)

	// a → b again: slot b's pod comes back with the same address and its leftover disk
	leftover := c.addNodeAt(0, slotB.address, slotB.dir)
	c.setTopology([]string{slotAAgain.address, node1.address}, []string{leftover.address, ""})
	c.start(leftover)
	require.Eventually(t, func() bool {
		return strings.Contains(slotAAgain.service.ReplicationStatus().LastError, "leftover")
	}, 30*time.Second, 20*time.Millisecond, "the shadow must refuse a leftover disk, even on a reused slot")
	require.False(t, slotAAgain.service.ReplicationStatus().Ready)
}

// TestMoveOneNodeCluster moves the only node of a cluster: once the old node is gone, the shadow is the only pod
// left to answer discovery, and until its config refreshes it hands out the old list.
func TestMoveOneNodeCluster(t *testing.T) {
	for _, order := range []switchOrder{oldNodeStopsFirst, newNodeRefreshesFirst, oldNodeRefreshesFirst} {
		t.Run("order "+strconv.Itoa(int(order)), func(t *testing.T) {
			c := newMoveClusterOf(t, 1, fastMoveReplication())
			node0, node0b := c.addNode(0), c.addNode(0)
			c.setTopology([]string{node0.address}, nil)
			c.start(node0)

			cl := c.newClient()
			l := startLoad(t, cl)
			require.Eventually(t, func() bool { return l.ackedCount() > 20 }, 10*time.Second, 10*time.Millisecond)

			lastPassStart, stopped := c.move(t, node0, node0b, cl, order)

			acked := l.ackedCount()
			require.Eventually(t, func() bool { return l.ackedCount() > acked+20 }, 30*time.Second,
				10*time.Millisecond, "writes never resumed after the switch")
			l.finish()

			require.Empty(t, l.errors(), "the client must not surface errors during a move")
			require.Less(t, l.maxStall, 10*time.Second, "the switch stalled the client for too long")
			require.True(t, hasKeys(t, node0b, l.keysAckedBetween(time.Time{}, lastPassStart)),
				"keys acknowledged before the last completed pass must be on the new node")
			require.True(t, hasKeys(t, node0b, l.keysStartedAfter(stopped)),
				"keys written after the old node stopped must be on the new node")
			require.Equal(t, 1, cl.ClusterSize())
		})
	}

	t.Run("the shadow refreshes late", func(t *testing.T) {
		c := newMoveClusterOf(t, 1, fastMoveReplication())
		node0, node0b := c.addNode(0), c.addNode(0)
		c.setTopology([]string{node0.address}, nil)
		c.start(node0)
		cl := c.newClient()
		keys := testKeys(200)
		require.NoError(t, cl.Put(context.Background(), keys, time.Hour))

		c.setTopology([]string{node0.address}, []string{node0b.address})
		c.start(node0b)
		require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
			20*time.Millisecond)

		// the old node is gone and the shadow, the only pod left, still hands out the old list
		c.stop(node0)
		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := cl.Get(ctx, keys)
			done <- err
		}()
		time.Sleep(time.Second)
		require.Empty(t, done, "the request cannot succeed before the shadow's config refreshes")

		c.setTopology([]string{node0b.address}, []string{""}, node0b)
		refreshed := time.Now()
		select {
		case err := <-done:
			require.NoError(t, err, "the client must converge once the shadow's config refreshes")
		case <-time.After(30 * time.Second):
			t.Fatal("the client never converged")
		}
		require.Less(t, time.Since(refreshed), 2*time.Second,
			"once the shadow is promoted, a backoff and a discovery are all it takes")
	})
}

// TestMoveMiddleNodeOfThree moves node 1 of a three-node cluster: the shadow list has an empty entry on each side of
// the shadow, and two nodes keep serving through the switch.
func TestMoveMiddleNodeOfThree(t *testing.T) {
	c := newMoveClusterOf(t, 3, fastMoveReplication())
	node0, node1, node2, node1b := c.addNode(0), c.addNode(1), c.addNode(2), c.addNode(1)
	c.setTopology([]string{node0.address, node1.address, node2.address}, nil)
	c.start(node0)
	c.start(node1)
	c.start(node2)

	cl := c.newClient()
	l := startLoad(t, cl)
	require.Eventually(t, func() bool { return l.ackedCount() > 20 }, 10*time.Second, 10*time.Millisecond)

	lastPassStart, stopped := c.move(t, node1, node1b, cl, newNodeRefreshesFirst)

	acked := l.ackedCount()
	require.Eventually(t, func() bool { return l.ackedCount() > acked+20 }, 30*time.Second, 10*time.Millisecond,
		"writes never resumed after the switch")
	l.finish()

	require.Empty(t, l.errors(), "the client must not surface errors during a move")
	require.Less(t, l.maxStall, 10*time.Second, "the switch stalled the client for too long")
	require.True(t, hasKeys(t, node1b, l.keysAckedBetween(time.Time{}, lastPassStart)),
		"keys acknowledged before the last completed pass must be on the new node")
	require.True(t, hasKeys(t, node1b, l.keysStartedAfter(stopped)),
		"keys written after the old node stopped must be on the new node")
	all := l.keysAckedBetween(time.Time{}, time.Now())
	require.True(t, hasKeys(t, node0, all), "the nodes that didn't move keep all their keys")
	require.True(t, hasKeys(t, node2, all), "the nodes that didn't move keep all their keys")
	require.Equal(t, "idle", node0.service.ReplicationStatus().State, "a node without a shadow copies nothing")
	require.Equal(t, "idle", node2.service.ReplicationStatus().State, "a node without a shadow copies nothing")
	require.Equal(t, 3, cl.ClusterSize())
}

// TestMoveClientCreatedDuringMove starts a client, as a processor does, while a shadow exists: the name resolves to
// the shadow too, so the client's first list has three entries for a two-node cluster, and the first answers correct
// it without a write ever landing on the shadow.
func TestMoveClientCreatedDuringMove(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
	c.setTopology([]string{node0.address, node1.address}, nil)
	c.start(node0)
	c.start(node1)
	c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
	c.start(node0b)
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond)

	cl := c.newClient()
	require.Equal(t, 3, cl.ClusterSize(), "the first list holds every pod behind the name, the shadow included")

	keys := testKeys(200)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, cl.Put(ctx, keys, time.Hour))
	require.Equal(t, 2, cl.ClusterSize())
	require.True(t, hasKeys(t, node0, keys))
	require.True(t, hasKeys(t, node1, keys))
	exists, err := cl.Get(ctx, keys)
	require.NoError(t, err)
	for _, e := range exists {
		require.True(t, e)
	}
}

// TestClientCreatedBeforeAnyNode creates a client while no node is ready, as a processor does when it starts during a
// keydb restart: NewClient returns at once, and the first request waits for the nodes.
func TestClientCreatedBeforeAnyNode(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, node1 := c.addNode(0), c.addNode(1)
	c.setTopology([]string{node0.address, node1.address}, nil)

	cl := c.newClient()
	require.Zero(t, cl.ClusterSize(), "the name resolves to no node yet")

	keys := testKeys(200)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		done <- cl.Put(ctx, keys, time.Hour)
	}()
	time.Sleep(500 * time.Millisecond)
	require.Empty(t, done, "the request cannot succeed before a node is ready")

	c.start(node0)
	c.start(node1)
	select {
	case err := <-done:
		require.NoError(t, err, "the request must go through once the nodes are ready")
	case <-time.After(30 * time.Second):
		t.Fatal("the request never went through")
	}
	require.Equal(t, 2, cl.ClusterSize())
	require.True(t, hasKeys(t, node0, keys))
	require.True(t, hasKeys(t, node1, keys))
}

// TestMovePromotionMidReceive promotes the shadow while part of a range is loaded and the rest hasn't arrived. The
// receive ends when the old node's SIGTERM cuts the stream, or at the next chunk: the shadow then drops the partial
// range and serves what the completed passes copied.
func TestMovePromotionMidReceive(t *testing.T) {
	cases := []struct {
		name string
		// cut ends the paused receive
		cut func(c *moveCluster, from *moveNode, paused *atomic.Bool)
	}{
		{"the old node stops", func(c *moveCluster, from *moveNode, _ *atomic.Bool) { c.stop(from) }},
		{"the next chunk arrives", func(_ *moveCluster, _ *moveNode, paused *atomic.Bool) { paused.Store(false) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var paused atomic.Bool
			replication := fastMoveReplication()
			replication.ChunkSize = 4 * int(bytesize.KB) // every range spans many chunks
			replication.MaxBytesPerSecond = func() int64 { return 256 * bytesize.KB }
			replication.Paused = paused.Load
			c := newMoveCluster(t, replication)
			node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
			c.setTopology([]string{node0.address, node1.address}, nil)
			c.start(node0)
			c.start(node1)
			cl := c.newClient()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			before := testKeys(200)
			require.NoError(t, cl.Put(ctx, before, time.Hour))

			c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
			c.start(node0b)
			require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
				20*time.Millisecond)

			// a big write gives every range of the next pass several batches, which the budget charges one at a time
			big := make([]string, 300_000)
			for i := range big {
				big[i] = "big-" + strconv.Itoa(i)
			}
			keysByHashRange, err := c.hash.GetKeysByHashRange(ownedBy(c.hash, 0, big), 0)
			require.NoError(t, err)
			require.NoError(t, node0.service.cache.Put(keysByHashRange, time.Hour))
			pauseMidRange(t, node0b, &paused)

			// PR2 reaches the shadow while it holds part of a range
			after := []string{node0b.address, node1.address}
			c.setTopology(after, []string{"", ""}, node0b)
			cut := time.Now()
			tc.cut(c, node0, &paused)
			require.Eventually(t, func() bool { return !node0b.service.receiving.Load() }, time.Second,
				time.Millisecond, "the promoted shadow must drop the partial range")
			require.Less(t, time.Since(cut), time.Second)

			c.stop(node0)
			c.setTopology(after, []string{"", ""}, node1)
			exists, err := cl.Get(ctx, before)
			require.NoError(t, err, "the promoted shadow must serve")
			for _, e := range exists {
				require.True(t, e, "keys copied by the completed passes must be on the new node")
			}
			afterPromotion := c.keyOf(0, "after-promotion")
			require.NoError(t, cl.Put(ctx, []string{afterPromotion}, time.Hour))
			require.True(t, hasKeys(t, node0b, []string{afterPromotion}))
		})
	}
}

// TestMoveOldNodeStopsAnswering has the old node stop answering at the switch instead of going away, like a pod that
// froze or a network partition that drops its packets. Nothing fails fast, so only the client's call timeout moves
// the request off it, long before the connection's keepalive would give up (10s plus 2s).
func TestMoveOldNodeStopsAnswering(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, proxy := c.addNodeBehindProxy(0)
	node1, node0b := c.addNode(1), c.addNode(0)
	c.setTopology([]string{node0.address, node1.address}, nil)
	c.start(node0)
	c.start(node1)
	cl := c.newClient(func(conf *client.Config) { conf.CallTimeout = 500 * time.Millisecond })
	keys := testKeys(200)
	require.NoError(t, cl.Put(context.Background(), keys, time.Hour)) // connects to node 0 through the proxy

	c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
	c.start(node0b)
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond)

	// PR2, but the old node stops answering instead of going away
	proxy.pause()
	after := []string{node0b.address, node1.address}
	c.setTopology(after, []string{"", ""}, node0b)
	c.setTopology(after, []string{"", ""}, node1)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exists, err := cl.Get(ctx, keys)
	require.NoError(t, err)
	for _, e := range exists {
		require.True(t, e)
	}
	require.Less(t, time.Since(start), 5*time.Second, "the call timeout must move the request off the frozen node")
}

const moveTestHashRanges = int64(8)

// topology is a node's view of nodeAddresses and shadowNodeAddresses
type topology struct {
	active, shadows []string
}

type moveNode struct {
	id      int64
	address string
	// listen is where the node listens when it isn't its address, behind proxy
	listen  string
	proxy   *pausableProxy
	dir     string
	hash    *hash.Hash
	view    atomic.Pointer[topology]
	service *Service
	server  *grpc.Server
	cancel  context.CancelFunc
	running atomic.Bool
}

type moveCluster struct {
	t           *testing.T
	storage     cloudStorage
	replication ReplicationConfig
	// hash is how the cluster spreads keys across its nodes
	hash *hash.Hash

	mu    sync.Mutex
	nodes []*moveNode
}

// newMoveCluster returns a two-node cluster, which the tests start
func newMoveCluster(t *testing.T, replication ReplicationConfig) *moveCluster {
	t.Helper()
	return newMoveClusterOf(t, 2, replication)
}

// newMoveClusterOf returns a cluster of the given size, which the tests start
func newMoveClusterOf(t *testing.T, size int64, replication ReplicationConfig) *moveCluster {
	t.Helper()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute
	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)
	return &moveCluster{
		t:           t,
		storage:     keydbth.GetCloudStorage(t, config.New(), minioContainer),
		replication: replication,
		hash:        hash.New(size, moveTestHashRanges),
	}
}

// addNode creates a node for the given index on a fresh disk without starting it
func (c *moveCluster) addNode(id int64) *moveNode {
	c.t.Helper()
	return c.addNodeAt(id, freeAddress(c.t), c.t.TempDir())
}

// addNodeAt creates a node for the given index with the given address and disk, like a pod of a slot that already
// existed, without starting it
func (c *moveCluster) addNodeAt(id int64, address, dir string) *moveNode {
	c.t.Helper()
	n := &moveNode{id: id, address: address, dir: dir, hash: c.hash}
	n.view.Store(&topology{})
	c.mu.Lock()
	c.nodes = append(c.nodes, n)
	c.mu.Unlock()
	return n
}

// start opens the node's disk and serves it
func (c *moveCluster) start(n *moveNode) {
	c.t.Helper()
	conf := config.New()
	conf.Set("BadgerDB.Dedup.Path", n.dir)
	conf.Set("BadgerDB.Dedup.NopLogger", true)
	ctx, cancel := context.WithCancel(context.Background())
	service, err := NewService(ctx, Config{
		NodeID:            n.id,
		TotalHashRanges:   moveTestHashRanges,
		BackupFolderName:  defaultBackupFolderName,
		Addresses:         func() []string { return n.view.Load().active },
		ShadowAddresses:   func() []string { return n.view.Load().shadows },
		AdvertisedAddress: n.address,
		Replication:       c.replication,
	}, c.storage, conf, stats.NOP, logger.NOP)
	if err != nil {
		cancel()
	}
	require.NoError(c.t, err)
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)
	listen := n.address
	if n.listen != "" {
		listen = n.listen
	}
	lis, err := net.Listen("tcp", listen)
	require.NoError(c.t, err)
	go func() { _ = server.Serve(lis) }()
	n.service, n.server, n.cancel = service, server, cancel
	n.running.Store(true)
	c.t.Cleanup(func() { c.stop(n) })
}

// stop shuts the node down the way the node's main does on SIGTERM: in-flight calls are cancelled, then the
// service and its disk are closed
func (c *moveCluster) stop(n *moveNode) {
	if !n.running.CompareAndSwap(true, false) {
		return
	}
	if n.proxy != nil {
		n.proxy.close() // the server would wait for handshakes the paused proxy holds
	}
	n.server.Stop()
	n.cancel()
	n.service.Close()
}

// setTopology changes the view of the given nodes, or of every node when none is given
func (c *moveCluster) setTopology(active, shadows []string, nodes ...*moveNode) {
	if len(nodes) == 0 {
		c.mu.Lock()
		nodes = append(nodes, c.nodes...)
		c.mu.Unlock()
	}
	for _, n := range nodes {
		n.view.Store(&topology{active: active, shadows: shadows})
	}
}

// moveTestHeadless is the name of the headless Service the clients are given. Nothing can dial it, so a data request
// sent to it would fail.
const moveTestHeadless = "keydb-headless.move.invalid:50051"

// resolveHeadless stands in for the DNS of the headless Service: its name resolves to every running node, the
// shadows included, the way it resolves to every ready pod
func (c *moveCluster) resolveHeadless(_ context.Context, address string) ([]string, error) {
	if address != moveTestHeadless {
		return nil, fmt.Errorf("unknown name %q", address)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var running []string
	for _, n := range c.nodes {
		if n.running.Load() {
			running = append(running, n.address)
		}
	}
	return running, nil
}

// newClient returns a client configured like the processors: with the headless Service as its only address. adjust
// can change the configuration first.
func (c *moveCluster) newClient(adjust ...func(*client.Config)) *client.Client {
	c.t.Helper()
	conf := client.Config{
		Addresses:       []string{moveTestHeadless},
		TotalHashRanges: moveTestHashRanges,
		RetryPolicy: client.RetryPolicy{
			InitialInterval: 20 * time.Millisecond,
			Multiplier:      1.5,
			MaxInterval:     300 * time.Millisecond,
		},
	}
	for _, a := range adjust {
		a(&conf)
	}
	cl, err := client.NewClient(c.t.Context(), conf, logger.NOP, client.WithAddressResolver(c.resolveHeadless))
	require.NoError(c.t, err)
	c.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// ownedBy returns the keys that belong to the given node when h spreads them
func ownedBy(h *hash.Hash, nodeID int64, keys []string) []string {
	var owned []string
	for _, k := range keys {
		if h.GetNodeNumber(k) == nodeID {
			owned = append(owned, k)
		}
	}
	return owned
}

// keyOf returns a key with the given prefix that belongs to the given node
func (c *moveCluster) keyOf(nodeID int64, prefix string) string {
	for i := 0; ; i++ {
		if k := prefix + "-" + strconv.Itoa(i); c.hash.GetNodeNumber(k) == nodeID {
			return k
		}
	}
}

// hasKeys returns true when every key among keys that belongs to the node exists on the node's disk
func hasKeys(t *testing.T, n *moveNode, keys []string) bool {
	t.Helper()
	owned := ownedBy(n.hash, n.id, keys)
	if len(owned) == 0 {
		return true
	}
	keysByHashRange, indexes, err := n.hash.GetKeysByHashRangeWithIndexes(owned, n.id)
	require.NoError(t, err)
	exists, err := n.service.cache.Get(keysByHashRange, indexes)
	require.NoError(t, err)
	for _, e := range exists {
		if !e {
			return false
		}
	}
	return true
}

// load writes batches of unique keys through the client until stopped, recording when each batch was acknowledged
type load struct {
	cl *client.Client

	mu        sync.Mutex
	acked     []ackedBatch
	errs      []error
	maxStall  time.Duration
	cancel    context.CancelFunc
	stopped   chan struct{}
	batchSize int
}

type ackedBatch struct {
	started time.Time
	at      time.Time
	keys    []string
}

func startLoad(t *testing.T, cl *client.Client) *load {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	l := &load{cl: cl, cancel: cancel, stopped: make(chan struct{}), batchSize: 20}
	go func() {
		defer close(l.stopped)
		for i := 0; ctx.Err() == nil; i++ {
			keys := make([]string, l.batchSize)
			for j := range keys {
				keys[j] = "load-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			}
			start := time.Now()
			putCtx, putCancel := context.WithTimeout(ctx, 30*time.Second) // a stuck client is an error, not a hang
			err := cl.Put(putCtx, keys, time.Hour)
			putCancel()
			took := time.Since(start)
			if ctx.Err() != nil {
				return // stopped while the request was in flight
			}
			l.mu.Lock()
			if err != nil {
				l.errs = append(l.errs, err)
			} else {
				l.acked = append(l.acked, ackedBatch{started: start, at: time.Now(), keys: keys})
			}
			l.maxStall = max(l.maxStall, took)
			l.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	t.Cleanup(l.finish)
	return l
}

func (l *load) finish() {
	l.cancel()
	<-l.stopped
}

func (l *load) errors() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.errs
}

// keysAckedBetween returns the keys acknowledged in [from, to)
func (l *load) keysAckedBetween(from, to time.Time) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var keys []string
	for _, b := range l.acked {
		if !b.at.Before(from) && b.at.Before(to) {
			keys = append(keys, b.keys...)
		}
	}
	return keys
}

// keysStartedAfter returns the keys of the acknowledged batches whose request started at or after from. A batch in
// flight when the old node stopped may have been written there and acknowledged late, which is the accepted tail.
func (l *load) keysStartedAfter(from time.Time) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var keys []string
	for _, b := range l.acked {
		if !b.started.Before(from) {
			keys = append(keys, b.keys...)
		}
	}
	return keys
}

func (l *load) ackedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.acked)
}

func fastMoveReplication() ReplicationConfig {
	return ReplicationConfig{
		PauseBetweenPasses: 20 * time.Millisecond,
		BrakeInterval:      50 * time.Millisecond,
		ReconcileInterval:  20 * time.Millisecond,
		Signals:            healthySignals{},
	}
}

type switchOrder int

const (
	// oldNodeStopsFirst: the old node gets SIGTERM before any pod picks up the new config
	oldNodeStopsFirst switchOrder = iota
	// newNodeRefreshesFirst: the shadow picks up the new config before the old node gets SIGTERM
	newNodeRefreshesFirst
	// oldNodeRefreshesFirst: the old node picks up the new config a while before it gets SIGTERM
	oldNodeRefreshesFirst
)

// move moves a node from the from node onto the to node, which must be a fresh node for the same index, the way PR1
// and PR2 would, while the other running nodes keep serving. It returns the start of the old node's last completed
// pass and when the old node stopped acknowledging writes.
func (c *moveCluster) move(
	t *testing.T, from, to *moveNode, cl *client.Client, order switchOrder,
) (lastPassStart, stopped time.Time) {
	t.Helper()
	others := c.runningExcept(from)

	// PR1: the shadow appears, and the active node starts filling it
	active := from.view.Load().active
	shadows := make([]string, len(active))
	shadows[from.id] = to.address
	c.setTopology(active, shadows)
	c.start(to)
	require.Eventually(t, func() bool { return from.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond, "the active node never became ready to switch")
	// a key written after readiness reaches the shadow too, syncing goes on after the signal
	afterReady := c.keyOf(from.id, "after-ready-"+to.address)
	require.NoError(t, cl.Put(context.Background(), []string{afterReady}, time.Hour))
	passes := from.service.ReplicationStatus().PassesCompleted
	require.Eventually(t, func() bool { return from.service.ReplicationStatus().PassesCompleted >= passes+2 },
		10*time.Second, 10*time.Millisecond)

	// PR2: the shadow becomes active and the old node is removed
	after := slices.Clone(active)
	after[from.id] = to.address
	noShadows := make([]string, len(active))
	refresh := func(nodes ...*moveNode) {
		for _, n := range nodes {
			c.setTopology(after, noShadows, n)
		}
	}
	lastPassStart = from.service.ReplicationStatus().LastPassStartedAt
	switch order {
	case oldNodeStopsFirst:
		c.stop(from)
		stopped = time.Now()
		refresh(to)
		refresh(others...)
	case newNodeRefreshesFirst:
		refresh(to)
		c.stop(from)
		stopped = time.Now()
		refresh(others...)
	case oldNodeRefreshesFirst:
		refresh(from)
		stopped = time.Now()
		time.Sleep(500 * time.Millisecond) // the client keeps sending the node's keys to the old node meanwhile
		c.stop(from)
		refresh(to)
		refresh(others...)
	}
	require.True(t, hasKeys(t, to, []string{afterReady}),
		"a key written after readiness must reach the shadow")
	return lastPassStart, stopped
}

// runningExcept returns the running nodes other than the given ones
func (c *moveCluster) runningExcept(except ...*moveNode) []*moveNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	var running []*moveNode
	for _, n := range c.nodes {
		if n.running.Load() && !slices.Contains(except, n) {
			running = append(running, n)
		}
	}
	return running
}

// switchableSignals reports load when told to
type switchableSignals struct{ loaded atomic.Bool }

func (s *switchableSignals) Sample() BrakeSample {
	if s.loaded.Load() {
		return BrakeSample{SchedLatencyP99: time.Second}
	}
	return BrakeSample{}
}

// pauseMidRange pauses the copy while the shadow is receiving a range, so that the range's stream stays open with part
// of it loaded. A pause that lands between two ranges is lifted and tried again.
func pauseMidRange(t *testing.T, shadow *moveNode, paused *atomic.Bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		paused.Store(false)
		for start := time.Now(); !shadow.service.receiving.Load(); time.Sleep(time.Millisecond) {
			if time.Since(start) > 5*time.Second {
				return false
			}
		}
		paused.Store(true)
		time.Sleep(500 * time.Millisecond) // longer than a paused budget waits before checking again
		return shadow.service.receiving.Load()
	}, time.Minute, 10*time.Millisecond, "the copy never paused in the middle of a range")
}

// addNodeBehindProxy creates a node for the given index whose address is a proxy in front of it, without starting it
func (c *moveCluster) addNodeBehindProxy(id int64) (*moveNode, *pausableProxy) {
	c.t.Helper()
	n := c.addNode(id)
	n.listen = freeAddress(c.t)
	n.proxy = startPausableProxy(c.t, n.address, n.listen)
	return n, n.proxy
}

// pausableProxy forwards TCP connections to a node, like rudder-go-kit's tcpproxy, which can only forward or close
// them. Once paused it forwards nothing and closes nothing, the way a frozen pod or a network partition that drops
// packets looks to a client.
type pausableProxy struct {
	listener net.Listener
	target   string
	paused   atomic.Bool
	closed   atomic.Bool

	mu        sync.Mutex
	conns     []net.Conn
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func startPausableProxy(t *testing.T, address, target string) *pausableProxy {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	p := &pausableProxy{listener: listener, target: target}
	p.wg.Go(p.accept)
	t.Cleanup(p.close)
	return p
}

func (p *pausableProxy) pause() { p.paused.Store(true) }

func (p *pausableProxy) accept() {
	for {
		in, err := p.listener.Accept()
		if err != nil {
			return // closed
		}
		out, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = in.Close() // the node isn't listening yet
			continue
		}
		p.mu.Lock()
		if p.closed.Load() { // accepted while closing
			p.mu.Unlock()
			_ = in.Close()
			_ = out.Close()
			return
		}
		p.conns = append(p.conns, in, out)
		p.mu.Unlock()
		p.wg.Go(func() { p.pipe(in, out) })
		p.wg.Go(func() { p.pipe(out, in) })
	}
}

// pipe copies src to dst, holding on to what it read while the proxy is paused
func (p *pausableProxy) pipe(src, dst net.Conn) {
	defer func() {
		_ = src.Close()
		_ = dst.Close()
	}()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		for p.paused.Load() && !p.closed.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *pausableProxy) close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed.Store(true)
		conns := p.conns
		p.mu.Unlock()
		_ = p.listener.Close()
		for _, conn := range conns {
			_ = conn.Close()
		}
		p.wg.Wait()
	})
}
