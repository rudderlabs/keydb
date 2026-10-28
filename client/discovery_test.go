package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
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

func newDiscoveryClient(t *testing.T, addresses ...string) *Client {
	t.Helper()
	return newTestClient(t, nil, addresses...)
}

// newTestClient creates a client with short timings, which adjust can change before the client is created
func newTestClient(t *testing.T, adjust func(*Config), addresses ...string) *Client {
	t.Helper()
	return newTestClientWith(t, adjust, addresses)
}

// newHeadlessClient creates a client with short timings that discovers the cluster through h
func newHeadlessClient(t *testing.T, h *headless) *Client {
	t.Helper()
	return newTestClientWith(t, nil, []string{headlessName}, WithAddressResolver(h.resolve))
}

func newTestClientWith(t *testing.T, adjust func(*Config), addresses []string, opts ...Opts) *Client {
	t.Helper()
	conf := Config{
		Addresses:       addresses,
		TotalHashRanges: discoveryTestHashRanges,
		RetryPolicy: RetryPolicy{
			InitialInterval: 20 * time.Millisecond,
			Multiplier:      1.5,
			MaxInterval:     200 * time.Millisecond,
		},
	}
	if adjust != nil {
		adjust(&conf)
	}
	c, err := NewClient(conf, logger.NOP, append([]Opts{WithStats(stats.NOP)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// headlessName is the address of the headless Service that headless stands in for. Nothing can dial it, so a data
// request sent to it would fail.
const headlessName = "keydb-headless.test.invalid:50051"

// headless stands in for a headless Service: its name resolves to the pods the test sets
type headless struct {
	mu      sync.Mutex
	pods    []string
	lookups atomic.Int64
}

func newHeadless(pods ...string) *headless { return &headless{pods: pods} }

func (h *headless) set(pods ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pods = pods
}

func (h *headless) resolve(_ context.Context, address string) ([]string, error) {
	h.lookups.Add(1)
	if address != headlessName {
		return nil, fmt.Errorf("unknown name %q", address)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.pods), nil
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

func TestDiscoveryFromOneNode(t *testing.T) {
	addresses := []string{freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)}
	nodes := make([]*mockNode, len(addresses))
	for i, address := range addresses {
		nodes[i] = newMockNode(t, address)
		nodes[i].setList(addresses...)
	}

	c := newDiscoveryClient(t, addresses[1]) // only one address, which is not node 0
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
	require.NoError(t, c.ensureDiscovered(context.Background()))
	c.mu.RLock()
	pool0, oldPool1 := c.pools[0], c.pools[1]
	c.mu.RUnlock()

	exists, err := c.Get(context.Background(), keysFor(1, 2, 3))
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true}, exists)
	require.Positive(t, newNode1.gets.Load(), "the client never moved to the new node")
	c.mu.RLock()
	newPool0, newPool1 := c.pools[0], c.pools[1]
	c.mu.RUnlock()
	require.Same(t, pool0, newPool0, "the connections to a node whose address didn't change must be kept")
	require.NotSame(t, oldPool1, newPool1)
}

func TestDataNeverThroughTheDiscoveryAddress(t *testing.T) {
	s, a0 := freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)    // a single node, whose list has as many entries as the configured addresses
	discovery := newMockNode(t, s) // answers with the cluster's list, without being one of its nodes
	discovery.setList(a0)

	c := newDiscoveryClient(t, s)
	keys := []string{"k1", "k2"}
	_, err := c.Get(context.Background(), keys)
	require.NoError(t, err)
	require.NoError(t, c.Put(context.Background(), keys, time.Hour))
	require.Zero(t, discovery.gets.Load()+discovery.puts.Load(),
		"data requests must go to the nodes the list names, never to the configured address")
	require.Positive(t, node0.gets.Load())
	require.Positive(t, node0.puts.Load())
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

	// every pooled connection is a TCP connection of its own, which the node sees from its own address
	attempts := node1.gets.Load()
	require.GreaterOrEqual(t, attempts, int64(2))
	connections := 0
	node1.peers.Range(func(any, any) bool {
		connections++
		return true
	})
	require.EqualValues(t, min(attempts, DefaultConnectionPoolSize), connections,
		"every attempt must take the next pooled connection")
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

	// with rudder-server's 30s MaxInterval, a lock held across the backoff would freeze other requests for longer
	c := newTestClient(t, func(conf *Config) {
		conf.RetryPolicy = RetryPolicy{InitialInterval: 4 * time.Second, Multiplier: 1.5, MaxInterval: 4 * time.Second}
	}, a1)
	require.NoError(t, c.ensureDiscovered(context.Background()))

	// node 0 keeps refusing and swapping lists, which takes the client's write lock over and over, and backs off
	// for at least 2s (notReadyMaxRetryDelay) after each attempt
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

func TestShortRequestsMoveOffADeadNode(t *testing.T) {
	a0, dead1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, dead1)
	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)

	c := newDiscoveryClient(t, a0)
	require.NoError(t, c.ensureDiscovered(context.Background()))
	node0.setList(a0, new1)

	// every request gives up well before the refresh interval
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

func TestNotReadyRetryDelayIsCapped(t *testing.T) {
	a0 := freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.notReady.Store(true) // a shadow about to be promoted

	// with rudder-server's 30s MaxInterval, a request that kept retrying sleeps for up to 45s before the next try
	c := newTestClient(t, func(conf *Config) {
		conf.RetryPolicy = RetryPolicy{
			InitialInterval: 20 * time.Second, Multiplier: 1.5, MaxInterval: 30 * time.Second,
		}
	}, a0)
	require.NoError(t, c.ensureDiscovered(context.Background()))

	var readyAt atomic.Pointer[time.Time]
	time.AfterFunc(1500*time.Millisecond, func() {
		now := time.Now()
		readyAt.Store(&now)
		node0.notReady.Store(false)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := c.Get(ctx, []string{"k"})
	require.NoError(t, err)
	require.Less(t, time.Since(*readyAt.Load()), 3*time.Second,
		"a request must be served within notReadyMaxRetryDelay of the node turning ready")
}

func TestDiscoveryRetriesAreCounted(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)
	c, err := NewClient(Config{
		Addresses:   []string{freeMockAddress(t)}, // nothing listens there
		RetryPolicy: RetryPolicy{InitialInterval: 10 * time.Millisecond, MaxInterval: 20 * time.Millisecond},
	}, logger.NOP, WithStats(statsStore))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.ensureDiscovered(ctx), context.DeadlineExceeded)
	retries := statsStore.Get("keydb_client_discovery_retries_total", nil)
	require.NotNil(t, retries, "a stuck discovery must show in the client's metrics")
	require.Greater(t, retries.LastValue(), float64(1))
}

func TestDiscoveryThroughHeadlessService(t *testing.T) {
	a0, a1 := freeMockAddress(t), freeMockAddress(t)
	nodes := []*mockNode{newMockNode(t, a0), newMockNode(t, a1)}
	for _, n := range nodes {
		n.setList(a0, a1)
	}

	c := newHeadlessClient(t, newHeadless(a1, a0))
	keys := append(keysFor(0, 2, 3), keysFor(1, 2, 3)...)
	exists, err := c.Get(context.Background(), keys)
	require.NoError(t, err)
	require.NotContains(t, exists, false)
	require.NoError(t, c.Put(context.Background(), keys, time.Hour))
	require.Equal(t, 2, c.ClusterSize())
	for i, n := range nodes {
		require.Positive(t, n.gets.Load(), "node %d got no request", i)
		require.Positive(t, n.puts.Load(), "node %d got no request", i)
	}
}

// TestRefreshResolvesAndAsksTheNextPod moves node 1 while node 0 hasn't reloaded its config: only the new pod knows
// the new list. The client has to resolve the name again to find the new pod, and take turns across refreshes so
// that the stale pod doesn't answer every time.
func TestRefreshResolvesAndAsksTheNextPod(t *testing.T) {
	a0, old1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t) // nothing listens on old1
	node0 := newMockNode(t, a0)
	node0.setList(a0, old1)
	h := newHeadless(a0)
	c := newHeadlessClient(t, h)
	require.NoError(t, c.ensureDiscovered(context.Background()))

	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)
	h.set(a0, new1) // the old pod is gone, the new one is ready

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := c.Get(ctx, keysFor(1, 2, 2))
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, exists)
	require.Positive(t, newNode1.gets.Load())
}

func TestRefreshesAreSpacedOut(t *testing.T) {
	a0, dead1 := freeMockAddress(t), freeMockAddress(t) // nothing listens on dead1, and nobody knows a replacement
	node0 := newMockNode(t, a0)
	node0.setList(a0, dead1)
	h := newHeadless(a0)
	c := newHeadlessClient(t, h)
	require.NoError(t, c.ensureDiscovered(context.Background()))
	before := h.lookups.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	for range 20 { // every request fails on the dead node over and over
		wg.Go(func() { _, _ = c.Get(ctx, keysFor(1, 2, 1)) })
	}
	wg.Wait()
	refreshes := h.lookups.Load() - before
	require.Positive(t, refreshes, "a node that doesn't answer must make the client ask for the list")
	require.LessOrEqual(t, refreshes, int64(3), "requests failing together must share refreshes, a second apart")
}

// TestMoveSelfHeals moves node 0 the way PR2 does when node 1 reloads its config before the promoted shadow: the
// old pod is gone, and the shadow answers NOT_READY with the old list until its own config refreshes. The client
// may go back and forth meanwhile, and is served shortly after the shadow is promoted.
func TestMoveSelfHeals(t *testing.T) {
	old0, new0, a1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t) // nothing listens on old0
	node1 := newMockNode(t, a1)
	node1.setList(old0, a1)
	h := newHeadless(a1)
	c := newHeadlessClient(t, h)
	require.NoError(t, c.ensureDiscovered(context.Background()))

	shadow := newMockNode(t, new0)
	shadow.setList(old0, a1)
	shadow.notReady.Store(true)
	node1.setList(new0, a1)
	h.set(new0, a1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.Get(ctx, keysFor(0, 2, 2))
		done <- err
	}()
	require.Eventually(t, func() bool { return shadow.gets.Load() > 0 }, 10*time.Second, time.Millisecond,
		"the client never reached the shadow")
	time.Sleep(time.Second) // the shadow keeps answering with the old list

	promoted := time.Now()
	shadow.setList(new0, a1)
	shadow.notReady.Store(false)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the request never completed after the shadow was promoted")
	}
	require.Less(t, time.Since(promoted), 3*time.Second,
		"once every pod has the new config, a refresh and a retry are all it takes")
}

func TestResolveAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resolved, err := resolveAddress(ctx, "127.0.0.1:1234")
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1:1234"}, resolved, "an IP address is used as it is")

	resolved, err = resolveAddress(ctx, ":1234")
	require.NoError(t, err)
	require.Equal(t, []string{":1234"}, resolved, "gRPC dials an empty host as the local machine")

	resolved, err = resolveAddress(ctx, "localhost:1234")
	require.NoError(t, err)
	require.Contains(t, resolved, "127.0.0.1:1234", "a name gives every address it resolves to, with the port")

	_, err = resolveAddress(ctx, "keydb.test.invalid:1234")
	require.Error(t, err)
	_, err = resolveAddress(ctx, "no-port")
	require.Error(t, err)
}
