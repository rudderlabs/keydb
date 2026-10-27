package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"

	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/proto"
)

const discoveryTestHashRanges = 128

// mockNode is a mock keydb node whose behaviour tests can change while it runs
type mockNode struct {
	address string
	gets    atomic.Int64
	puts    atomic.Int64
	// peers records the client side address of every connection that sent a Get
	peers sync.Map
	// list is the address list the node answers with, cluster size included
	list atomic.Pointer[[]string]
	// notReady makes Get and Put answer NOT_READY with the node's list
	notReady atomic.Bool
	// stop shuts the node down, after which its address refuses connections
	stop func()
}

func (n *mockNode) setList(list ...string) { n.list.Store(&list) }

func (n *mockNode) currentList() []string { return *n.list.Load() }

func freeMockAddress(t *testing.T) string {
	t.Helper()
	port, err := testhelper.GetFreePort()
	require.NoError(t, err)
	return fmt.Sprintf("localhost:%d", port)
}

// newMockNode starts a mock node that answers Get with all keys existing, Put with success, and GetNodeInfo with
// its list
func newMockNode(t *testing.T, address string) *mockNode {
	t.Helper()
	n := &mockNode{address: address}
	n.setList(address)
	server := &mockNodeServiceServer{
		getFunc: func(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
			n.gets.Add(1)
			if p, ok := peer.FromContext(ctx); ok {
				n.peers.Store(p.Addr.String(), struct{}{})
			}
			list := n.currentList()
			resp := &proto.GetResponse{ClusterSize: int64(len(list)), NodesAddresses: list}
			if n.notReady.Load() {
				resp.ErrorCode = proto.ErrorCode_NOT_READY
				return resp, nil
			}
			resp.Exists = make([]bool, len(req.Keys))
			for i := range resp.Exists {
				resp.Exists[i] = true
			}
			return resp, nil
		},
		putFunc: func(_ context.Context, _ *proto.PutRequest) (*proto.PutResponse, error) {
			n.puts.Add(1)
			list := n.currentList()
			resp := &proto.PutResponse{ClusterSize: int64(len(list)), NodesAddresses: list}
			if n.notReady.Load() {
				resp.ErrorCode = proto.ErrorCode_NOT_READY
				return resp, nil
			}
			resp.Success = true
			return resp, nil
		},
		getNodeInfoFunc: func(context.Context, *proto.GetNodeInfoRequest) (*proto.GetNodeInfoResponse, error) {
			list := n.currentList()
			return &proto.GetNodeInfoResponse{ClusterSize: int64(len(list)), NodesAddresses: list}, nil
		},
	}
	n.stop = sync.OnceFunc(startMockServer(t, server, address))
	t.Cleanup(n.stop)
	return n
}

func newDiscoveryClient(t *testing.T, seeds ...string) *Client {
	t.Helper()
	return newTestClient(t, nil, seeds...)
}

// newTestClient creates a client with short timings, which adjust can change before the client is created
func newTestClient(t *testing.T, adjust func(*Config), seeds ...string) *Client {
	t.Helper()
	conf := Config{
		Addresses:           seeds,
		TotalHashRanges:     discoveryTestHashRanges,
		DeadNodeLookupAfter: 200 * time.Millisecond,
		RetryPolicy: RetryPolicy{
			InitialInterval: 20 * time.Millisecond,
			Multiplier:      1.5,
			MaxInterval:     200 * time.Millisecond,
		},
	}
	if adjust != nil {
		adjust(&conf)
	}
	c, err := NewClient(conf, logger.NOP, WithStats(stats.NOP))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// keysFor returns n keys that the hash sends to the given node
func keysFor(nodeID, clusterSize int64, n int) []string {
	h := hash.New(clusterSize, discoveryTestHashRanges)
	var keys []string
	for i := 0; len(keys) < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if h.GetNodeNumber(key) == nodeID {
			keys = append(keys, key)
		}
	}
	return keys
}

func TestDiscoveryFromSeed(t *testing.T) {
	addresses := []string{freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)}
	nodes := make([]*mockNode, len(addresses))
	for i, address := range addresses {
		nodes[i] = newMockNode(t, address)
		nodes[i].setList(addresses...)
	}

	c := newDiscoveryClient(t, addresses[1]) // only one seed, which is not node 0
	var keys []string
	for nodeID := range int64(3) {
		keys = append(keys, keysFor(nodeID, 3, 5)...)
	}
	exists, err := c.Get(context.Background(), keys)
	require.NoError(t, err)
	for _, e := range exists {
		require.True(t, e)
	}
	require.Equal(t, 3, c.ClusterSize())
	for i, n := range nodes {
		require.Positive(t, n.gets.Load(), "node %d got no request", i)
	}
}

func TestAdoptListFromFailedResponse(t *testing.T) {
	a0, old1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, old1)
	oldNode1 := newMockNode(t, old1)
	oldNode1.setList(a0, new1) // it knows it was replaced
	oldNode1.notReady.Store(true)
	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)

	c := newDiscoveryClient(t, a0)
	exists, err := c.Get(context.Background(), keysFor(1, 2, 3))
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true}, exists)
	require.Positive(t, newNode1.gets.Load(), "the client never moved to the new node")
}

func TestNotReadyRotatesConnection(t *testing.T) {
	a0, a1 := freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, a1)
	node1 := newMockNode(t, a1)
	node1.setList(a0, a1)
	node1.notReady.Store(true)

	c := newDiscoveryClient(t, a0)
	go func() {
		time.Sleep(300 * time.Millisecond)
		node1.notReady.Store(false)
	}()
	_, err := c.Get(context.Background(), keysFor(1, 2, 1))
	require.NoError(t, err)

	c.mu.RLock()
	next := c.pools[1].next.Load()
	c.mu.RUnlock()
	require.GreaterOrEqual(t, next, uint32(2), "every attempt must take the next pooled connection")
}

func TestDeadNodeLookup(t *testing.T) {
	a0, dead1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, dead1) // what the client discovers
	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)

	c := newDiscoveryClient(t, a0)
	require.NoError(t, c.ensureDiscovered(context.Background()))
	node0.setList(a0, new1) // the dead node was replaced

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := c.Get(ctx, keysFor(1, 2, 2))
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, exists)
	require.Positive(t, newNode1.gets.Load())
}

func TestConflictingListsNoHotLoop(t *testing.T) {
	a0, x1, y1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, x1)
	nodeX := newMockNode(t, x1)
	nodeX.setList(a0, y1)
	nodeX.notReady.Store(true)
	nodeY := newMockNode(t, y1)
	nodeY.setList(a0, x1)
	nodeY.notReady.Store(true)

	c := newDiscoveryClient(t, a0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.Get(ctx, keysFor(1, 2, 1))
	require.Error(t, err)
	require.Less(t, nodeX.gets.Load()+nodeY.gets.Load(), int64(60),
		"nodes handing out each other's list must not make the client spin")
}

func TestNoLockAcrossBackoff(t *testing.T) {
	x0, y0, a1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	nodeX := newMockNode(t, x0)
	nodeX.setList(y0, a1)
	nodeX.notReady.Store(true)
	nodeY := newMockNode(t, y0)
	nodeY.setList(x0, a1)
	nodeY.notReady.Store(true)
	node1 := newMockNode(t, a1)
	node1.setList(x0, a1)

	c := newDiscoveryClient(t, a1)
	require.NoError(t, c.ensureDiscovered(context.Background()))

	// node 0 keeps refusing and swapping lists, which takes the client's write lock over and over
	ctx := t.Context()
	go func() { _, _ = c.Get(ctx, keysFor(0, 2, 1)) }()
	time.Sleep(200 * time.Millisecond)

	done := make(chan error, 1)
	go func() {
		_, err := c.Get(context.Background(), keysFor(1, 2, 1))
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("a request to node 1 was blocked by node 0 backing off")
	}
}

func TestCallNodeAfterConcurrentShrink(t *testing.T) {
	addresses := []string{freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)}
	for _, address := range addresses {
		newMockNode(t, address).setList(addresses...)
	}
	c := newDiscoveryClient(t, addresses[0])
	require.NoError(t, c.ensureDiscovered(context.Background()))

	// another request learnt that the cluster shrank to 2 nodes while this one was routed to node 2
	_, err := c.applyAddresses(addresses[:2])
	require.NoError(t, err)
	_, err = callNode(context.Background(), c, 2, 3, "get", stats.NOP.NewStat("retries", stats.CountType),
		func(ctx context.Context, client proto.NodeServiceClient) (*proto.GetResponse, error) {
			return client.Get(ctx, &proto.GetRequest{Keys: []string{"k"}})
		},
		func(*proto.GetResponse) bool { return true },
	)
	changed, ok := errors.AsType[*errClusterSizeChanged](err)
	require.True(t, ok, "the request must be re-routed, not failed: %v", err)
	require.Equal(t, addresses[:2], changed.nodesAddresses)
}

func TestCloseStopsInFlightRetries(t *testing.T) {
	a0 := freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.notReady.Store(true) // the request keeps retrying

	c := newDiscoveryClient(t, a0)
	done := make(chan error, 1)
	go func() {
		_, err := c.Get(context.Background(), []string{"k"})
		done <- err
	}()
	require.Eventually(t, func() bool { return node0.gets.Load() > 0 }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, c.Close())
	select {
	case err := <-done:
		require.ErrorIs(t, err, errClientClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("the request kept retrying after Close")
	}
	c.mu.RLock()
	pools := len(c.pools)
	c.mu.RUnlock()
	require.Zero(t, pools, "a request must not reopen connections after Close")
}

func TestDeadNodeLookupAcrossShortRequests(t *testing.T) {
	a0, dead1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, dead1)
	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)

	c := newDiscoveryClient(t, a0) // DeadNodeLookupAfter is 200ms
	require.NoError(t, c.ensureDiscovered(context.Background()))
	node0.setList(a0, new1)

	// every request gives up well before DeadNodeLookupAfter, the failures must add up across requests
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		_, err := c.Get(ctx, keysFor(1, 2, 1))
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "short requests never moved off the dead node")
	require.Positive(t, newNode1.gets.Load())
}

func TestDiscoveryWaitersHonourContext(t *testing.T) {
	unreachable := freeMockAddress(t) // nothing listens here
	c := newDiscoveryClient(t, unreachable)

	leaderCtx := t.Context()
	go func() { _ = c.ensureDiscovered(leaderCtx) }() // keeps retrying discovery
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.ensureDiscovered(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second, "a waiter must give up when its own context ends")
}
