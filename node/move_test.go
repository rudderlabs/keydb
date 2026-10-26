package node

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

// These tests move node 0 of a two-node cluster onto a new disk through a shadow, under client load. The nodes are
// real services over real gRPC with Badger on disk and MinIO in a container, like the other node tests. Each node
// has its own view of the configuration, which tests change one node at a time to reproduce the kubelet refreshing
// the configmap of each pod at a different moment.

const moveTestHashRanges = int64(8)

// topology is a node's view of nodeAddresses and shadowNodeAddresses
type topology struct {
	active, shadows []string
}

type moveNode struct {
	id      int64
	address string
	dir     string
	view    atomic.Pointer[topology]
	service *Service
	server  *grpc.Server
	cancel  context.CancelFunc
	running bool
}

type moveCluster struct {
	t           *testing.T
	storage     cloudStorage
	replication ReplicationConfig

	mu    sync.Mutex
	nodes []*moveNode
}

func newMoveCluster(t *testing.T, replication ReplicationConfig) *moveCluster {
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
	}
}

// addNode creates a node for the given index on a fresh disk without starting it
func (c *moveCluster) addNode(id int64) *moveNode {
	c.t.Helper()
	n := &moveNode{id: id, address: freeAddress(c.t), dir: c.t.TempDir()}
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
	lis, err := net.Listen("tcp", n.address)
	require.NoError(c.t, err)
	go func() { _ = server.Serve(lis) }()
	n.service, n.server, n.cancel, n.running = service, server, cancel, true
	c.t.Cleanup(func() { c.stop(n) })
}

// stop shuts the node down the way the node's main does on SIGTERM: in-flight calls are cancelled, then the
// service and its disk are closed
func (c *moveCluster) stop(n *moveNode) {
	if !n.running {
		return
	}
	n.running = false
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

func (c *moveCluster) newClient(seeds ...string) *client.Client {
	c.t.Helper()
	cl, err := client.NewClient(client.Config{
		Addresses:           seeds,
		TotalHashRanges:     moveTestHashRanges,
		DeadNodeLookupAfter: 200 * time.Millisecond,
		RetryPolicy: client.RetryPolicy{
			InitialInterval: 20 * time.Millisecond,
			Multiplier:      1.5,
			MaxInterval:     300 * time.Millisecond,
		},
	}, logger.NOP)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// moveTestHash is how the two-node cluster spreads keys across nodes
var moveTestHash = hash.New(2, moveTestHashRanges)

// ownedBy returns the keys that belong to the given node of the two-node cluster
func ownedBy(nodeID int64, keys []string) []string {
	var owned []string
	for _, k := range keys {
		if moveTestHash.GetNodeNumber(k) == nodeID {
			owned = append(owned, k)
		}
	}
	return owned
}

// keyOf returns a key with the given prefix that belongs to the given node
func keyOf(nodeID int64, prefix string) string {
	for i := 0; ; i++ {
		if k := prefix + "-" + strconv.Itoa(i); moveTestHash.GetNodeNumber(k) == nodeID {
			return k
		}
	}
}

// hasKeys returns true when every key among keys that belongs to the node exists on the node's disk
func hasKeys(t *testing.T, n *moveNode, keys []string) bool {
	t.Helper()
	owned := ownedBy(n.id, keys)
	if len(owned) == 0 {
		return true
	}
	keysByHashRange, indexes, err := moveTestHash.GetKeysByHashRangeWithIndexes(owned, n.id)
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
	at   time.Time
	keys []string
}

func startLoad(t *testing.T, cl *client.Client, prefix string) *load {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	l := &load{cl: cl, cancel: cancel, stopped: make(chan struct{}), batchSize: 20}
	go func() {
		defer close(l.stopped)
		for i := 0; ctx.Err() == nil; i++ {
			keys := make([]string, l.batchSize)
			for j := range keys {
				keys[j] = prefix + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
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
				l.acked = append(l.acked, ackedBatch{at: time.Now(), keys: keys})
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
)

// move moves node 0 from the from node onto the to node, which must be a fresh node for the same index, the
// way PR1 and PR2 would. It returns the start of the old node's last completed pass and when the old node stopped.
func (c *moveCluster) move(
	t *testing.T, from, to, other *moveNode, cl *client.Client, order switchOrder,
) (lastPassStart, stopped time.Time) {
	t.Helper()

	// PR1: the shadow appears, and the active node starts filling it
	before := topology{active: []string{from.address, other.address}, shadows: []string{to.address, ""}}
	c.setTopology(before.active, before.shadows)
	c.start(to)
	require.Eventually(t, func() bool { return from.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond, "the active node never became ready to switch")
	// a key written after readiness reaches the shadow too, syncing goes on after the signal
	afterReady := keyOf(from.id, "after-ready-"+to.address)
	require.NoError(t, cl.Put(context.Background(), []string{afterReady}, time.Hour))
	passes := from.service.ReplicationStatus().PassesCompleted
	require.Eventually(t, func() bool { return from.service.ReplicationStatus().PassesCompleted >= passes+2 },
		10*time.Second, 10*time.Millisecond)

	// PR2: the shadow becomes active and the old node is removed
	after := topology{active: []string{to.address, other.address}, shadows: []string{"", ""}}
	lastPassStart = from.service.ReplicationStatus().LastPassStartedAt
	switch order {
	case oldNodeStopsFirst:
		c.stop(from)
		stopped = time.Now()
		c.setTopology(after.active, after.shadows, to)
		c.setTopology(after.active, after.shadows, other)
	case newNodeRefreshesFirst:
		c.setTopology(after.active, after.shadows, to)
		c.stop(from)
		stopped = time.Now()
		c.setTopology(after.active, after.shadows, other)
	}
	require.True(t, hasKeys(t, to, []string{afterReady}),
		"a key written after readiness must reach the shadow")
	return lastPassStart, stopped
}

func TestMoveHappyPath(t *testing.T) {
	for _, order := range []switchOrder{oldNodeStopsFirst, newNodeRefreshesFirst} {
		t.Run("order "+strconv.Itoa(int(order)), func(t *testing.T) {
			c := newMoveCluster(t, fastMoveReplication())
			node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
			c.setTopology([]string{node0.address, node1.address}, nil)
			c.start(node0)
			c.start(node1)

			cl := c.newClient(node1.address)
			l := startLoad(t, cl, "load")
			require.Eventually(t, func() bool { return l.ackedCount() > 20 }, 10*time.Second, 10*time.Millisecond)

			lastPassStart, stopped := c.move(t, node0, node0b, node1, cl, order)

			// the client keeps writing through the switch and lands on the new node
			acked := l.ackedCount()
			require.Eventually(t, func() bool { return l.ackedCount() > acked+20 }, 30*time.Second,
				10*time.Millisecond, "writes never resumed after the switch")
			l.finish()

			require.Empty(t, l.errors(), "the client must not surface errors during a move")
			require.Less(t, l.maxStall, 10*time.Second, "the switch stalled the client for too long")
			require.True(t, hasKeys(t, node0b, l.keysAckedBetween(time.Time{}, lastPassStart)),
				"keys acknowledged before the last completed pass must be on the new node")
			require.True(t, hasKeys(t, node0b, l.keysAckedBetween(stopped, time.Now())),
				"keys acknowledged after the old node stopped, retried in-flight ones too, must be on the new node")
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

	cl := c.newClient(node1.address)
	l := startLoad(t, cl, "load")
	require.Eventually(t, func() bool { return l.ackedCount() > 20 }, 10*time.Second, 10*time.Millisecond)

	// first move: keydb-0 → keydb-0-b
	slotB := c.addNode(0)
	firstLastPass, _ := c.move(t, slotA, slotB, node1, cl, oldNodeStopsFirst)
	acked := l.ackedCount()
	require.Eventually(t, func() bool { return l.ackedCount() > acked+20 }, 30*time.Second, 10*time.Millisecond)
	betweenMoves := time.Now()
	require.Eventually(t, func() bool { return l.ackedCount() > acked+60 }, 30*time.Second, 10*time.Millisecond)

	// second move: keydb-0-b → keydb-0 again, on a fresh disk
	slotAAgain := c.addNode(0)
	secondLastPass, _ := c.move(t, slotB, slotAAgain, node1, cl, newNodeRefreshesFirst)
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
	finalKey := keyOf(0, "final")
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
	cl := c.newClient(node0.address)
	keys := testKeys(2000)
	require.NoError(t, cl.Put(context.Background(), keys, time.Hour))

	c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
	c.start(node0b)
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond)

	// the serving node restarts on the same disk
	c.stop(node0)
	c.start(node0)
	require.Equal(t, "running", node0.service.ReplicationStatus().State, "replication resumes on boot")
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

	active := c.newClient(node1.address)
	idle := c.newClient(node1.address)
	keys := testKeys(200)
	require.NoError(t, idle.Put(context.Background(), keys, time.Hour)) // discovers, then goes quiet

	c.move(t, node0, node0b, node1, active, oldNodeStopsFirst)

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
	cl := c.newClient(node1.address)
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

// roundRobinProxy stands in for the keydb Service: every connection goes to the next backend
type roundRobinProxy struct {
	address  string
	backends []string
	next     atomic.Int64
}

func startRoundRobinProxy(t *testing.T, backends ...string) *roundRobinProxy {
	t.Helper()
	p := &roundRobinProxy{address: freeAddress(t), backends: backends}
	lis, err := net.Listen("tcp", p.address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			backend := p.backends[int(p.next.Add(1)-1)%len(p.backends)]
			go func() {
				defer func() { _ = conn.Close() }()
				upstream, err := net.Dial("tcp", backend)
				if err != nil {
					return
				}
				defer func() { _ = upstream.Close() }()
				go func() { _, _ = io.Copy(upstream, conn) }()
				_, _ = io.Copy(conn, upstream)
			}()
		}
	}()
	return p
}

func TestMoveSeedThroughService(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
	c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
	c.start(node0)
	c.start(node1)
	c.start(node0b)

	// the Service routes the first connection to the shadow
	proxy := startRoundRobinProxy(t, node0b.address, node0.address, node1.address)
	cl := c.newClient(proxy.address)
	keys := testKeys(100)
	require.NoError(t, cl.Put(context.Background(), keys, time.Hour))
	exists, err := cl.Get(context.Background(), keys)
	require.NoError(t, err)
	for _, e := range exists {
		require.True(t, e)
	}
	require.Equal(t, 2, cl.ClusterSize())
	require.Positive(t, proxy.next.Load(), "discovery must have gone through the Service")
}

func TestMoveOldClients(t *testing.T) {
	c := newMoveCluster(t, fastMoveReplication())
	node0, node1, node0b := c.addNode(0), c.addNode(1), c.addNode(0)
	active := []string{node0.address, node1.address}
	c.setTopology(active, []string{node0b.address, ""})
	c.start(node0)
	c.start(node1)
	c.start(node0b)
	require.Eventually(t, func() bool { return node0.service.ReplicationStatus().Ready }, 30*time.Second,
		20*time.Millisecond)

	// a client that predates node moves sends no features header
	rawClient := func(address string) pb.NodeServiceClient {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return pb.NewNodeServiceClient(conn)
	}
	_, err := rawClient(node0.address).Get(context.Background(), &pb.GetRequest{Keys: []string{"k"}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !node0.service.ReplicationStatus().Ready }, 10*time.Second,
		20*time.Millisecond)
	require.Contains(t, strings.Join(node0.service.ReplicationStatus().NotReadyReasons, ","), "old clients")

	// a shadow answers such a client with the real cluster size and the full list, a retryable error
	resp, err := rawClient(node0b.address).Get(context.Background(), &pb.GetRequest{Keys: []string{"k"}})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_NOT_READY, resp.ErrorCode)
	require.EqualValues(t, 2, resp.ClusterSize)
	require.Equal(t, active, resp.NodesAddresses)
}

// switchableSignals reports load when told to
type switchableSignals struct{ loaded atomic.Bool }

func (s *switchableSignals) Sample() BrakeSample {
	if s.loaded.Load() {
		return BrakeSample{SchedLatencyP99: time.Second}
	}
	return BrakeSample{}
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
		// about 3.5 MiB to send: well above the one second of budget the limiter starts with
		keysByHashRange, err := moveTestHash.GetKeysByHashRange(ownedBy(0, testKeys(240_000)), 0)
		require.NoError(t, err)
		require.NoError(t, node0.service.cache.Put(keysByHashRange, time.Hour))

		c.setTopology([]string{node0.address, node1.address}, []string{node0b.address, ""})
		c.start(node0b)
		require.Eventually(t, func() bool { return node0.service.ReplicationStatus().PassesCompleted >= 1 },
			60*time.Second, 20*time.Millisecond)
		status := node0.service.ReplicationStatus()
		duration, err := time.ParseDuration(status.LastPassDuration)
		require.NoError(t, err)
		require.Greater(t, duration, 1500*time.Millisecond, "a budget of 1 MiB/s must slow the first pass down")
		require.Greater(t, status.LastPassBytes, int64(0))
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
		cl := c.newClient(node0.address)
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
	cl := c.newClient(node1.address)
	require.NoError(t, cl.Put(context.Background(), testKeys(200), time.Hour))

	slotB := c.addNode(0)
	c.move(t, slotA, slotB, node1, cl, oldNodeStopsFirst)

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
