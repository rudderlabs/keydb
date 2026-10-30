package client

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

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

// owns tells whether the node owns every key under its list, as a real node checks. A node that isn't in its own
// list, such as one reached under another name, owns every key.
func (n *mockNode) owns(list, keys []string) bool {
	nodeID := slices.Index(list, n.address)
	if nodeID < 0 {
		return true
	}
	h := hash.New(int64(len(list)), discoveryTestHashRanges)
	for _, key := range keys {
		if h.GetNodeNumber(key) != int64(nodeID) {
			return false
		}
	}
	return true
}

func freeMockAddress(t *testing.T) string {
	t.Helper()
	port, err := testhelper.GetFreePort()
	require.NoError(t, err)
	return fmt.Sprintf("localhost:%d", port)
}

// newMockNode starts a mock node that answers Get with all keys existing, Put with success, and GetNodeInfo with
// its list. Like a real node, it answers WRONG_NODE for keys it doesn't own.
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
			if !n.owns(list, req.Keys) {
				resp.ErrorCode = proto.ErrorCode_WRONG_NODE
				return resp, nil
			}
			resp.Exists = make([]bool, len(req.Keys))
			for i := range resp.Exists {
				resp.Exists[i] = true
			}
			return resp, nil
		},
		putFunc: func(_ context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
			n.puts.Add(1)
			list := n.currentList()
			resp := &proto.PutResponse{ClusterSize: int64(len(list)), NodesAddresses: list}
			if n.notReady.Load() {
				resp.ErrorCode = proto.ErrorCode_NOT_READY
				return resp, nil
			}
			if !n.owns(list, req.Keys) {
				resp.ErrorCode = proto.ErrorCode_WRONG_NODE
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

// testConfig returns a client configuration with short timings
func testConfig(addresses ...string) Config {
	return Config{
		Addresses:       addresses,
		TotalHashRanges: discoveryTestHashRanges,
		RetryPolicy: RetryPolicy{
			InitialInterval: 20 * time.Millisecond,
			Multiplier:      1.5,
			MaxInterval:     200 * time.Millisecond,
		},
	}
}

// asIs resolves every address to itself, so that a client's first list is its configured addresses
func asIs(_ context.Context, address string) ([]string, error) { return []string{address}, nil }

func newTestClientWith(t *testing.T, adjust func(*Config), addresses []string, opts ...Opts) *Client {
	t.Helper()
	conf := testConfig(addresses...)
	if adjust != nil {
		adjust(&conf)
	}
	opts = append([]Opts{WithStats(stats.NOP), WithAddressResolver(asIs)}, opts...)
	c, err := NewClient(t.Context(), conf, logger.NOP, opts...)
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

func TestNotReadyMakesTheClientDiscover(t *testing.T) {
	a0, old1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, new1)
	oldNode1 := newMockNode(t, old1)
	oldNode1.setList(a0, new1) // it knows it was replaced
	oldNode1.notReady.Store(true)
	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)

	// with a 10s backoff, only a re-route at once meets the deadline
	c := newTestClient(t, func(conf *Config) {
		conf.RetryPolicy = RetryPolicy{
			InitialInterval: 10 * time.Second, Multiplier: 1.5, MaxInterval: 10 * time.Second,
		}
	}, a0, old1) // the list before node 1 moved
	c.mu.RLock()
	pool0, oldPool1 := c.pools[0], c.pools[1]
	c.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	exists, err := c.Get(ctx, keysFor(1, 2, 3))
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true}, exists)
	require.Positive(t, newNode1.gets.Load(), "the client never moved to the new node")
	c.mu.RLock()
	newPool0, newPool1 := c.pools[0], c.pools[1]
	c.mu.RUnlock()
	require.Same(t, pool0, newPool0, "the connections to a node whose address didn't change must be kept")
	require.NotSame(t, oldPool1, newPool1)
}

// TestWrongNodeMakesTheClientDiscover has the name resolve to as many pods as there are nodes, in the wrong order:
// only the WRONG_NODE answers show that the list is wrong.
func TestWrongNodeMakesTheClientDiscover(t *testing.T) {
	a0, a1 := freeMockAddress(t), freeMockAddress(t)
	for _, address := range []string{a0, a1} {
		newMockNode(t, address).setList(a0, a1)
	}
	c := newHeadlessClient(t, newHeadless(a1, a0))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exists, err := c.Get(ctx, append(keysFor(0, 2, 2), keysFor(1, 2, 2)...))
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, true}, exists)
}

// startStuckNode starts a pod that accepts calls but never answers them, like a node whose handlers wait behind a lock
func startStuckNode(t *testing.T) string {
	t.Helper()
	address := freeMockAddress(t)
	t.Cleanup(startMockServer(t, &mockNodeServiceServer{
		getFunc: func(ctx context.Context, _ *proto.GetRequest) (*proto.GetResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		getNodeInfoFunc: func(ctx context.Context, _ *proto.GetNodeInfoRequest) (*proto.GetNodeInfoResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}, address))
	return address
}

func TestCallTimeout(t *testing.T) {
	t.Run("makes the client discover", func(t *testing.T) {
		stuck, a0 := startStuckNode(t), freeMockAddress(t)
		newMockNode(t, a0)
		h := newHeadless(stuck) // the client's first list is the stuck pod
		c := newTestClientWith(t, func(conf *Config) { conf.CallTimeout = 200 * time.Millisecond },
			[]string{headlessName}, WithAddressResolver(h.resolve),
		)
		h.set(stuck, a0)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		exists, err := c.Get(ctx, []string{"k"})
		require.NoError(t, err)
		require.Equal(t, []bool{true}, exists)
		require.Equal(t, 1, c.ClusterSize())
	})

	t.Run("the request's own deadline doesn't make the client discover", func(t *testing.T) {
		h := newHeadless(startStuckNode(t))
		c := newHeadlessClient(t, h) // the default CallTimeout outlasts the request
		lookups := h.lookups.Load()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := c.Get(ctx, []string{"k"})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, lookups, h.lookups.Load(), "a request that ran out of time must not discover")
	})
}

func TestNotReadyRotatesConnection(t *testing.T) {
	a0, a1 := freeMockAddress(t), freeMockAddress(t)
	node0 := newMockNode(t, a0)
	node0.setList(a0, a1)
	node1 := newMockNode(t, a1)
	node1.setList(a0, a1)
	node1.notReady.Store(true)

	c := newDiscoveryClient(t, a0, a1)
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
	a0, dead1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t) // nothing listens on dead1
	node0 := newMockNode(t, a0)
	node0.setList(a0, new1) // the dead node was replaced
	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)

	c := newDiscoveryClient(t, a0, dead1)

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

	c := newDiscoveryClient(t, a0, x1, y1) // discovery hears from any of them first
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.Get(ctx, keysFor(1, 2, 1))
	require.Error(t, err)
	require.Less(t, nodeX.gets.Load()+nodeY.gets.Load(), int64(60),
		"nodes handing out different lists must not make the client spin")
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
	}, x0, a1)

	// node 0 keeps refusing, and discovery keeps swapping lists, which takes the client's write lock over and over.
	// The request backs off for at least 2s (notReadyMaxRetryDelay) after each attempt.
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
	c := newDiscoveryClient(t, addresses...)

	// another request learnt that the cluster shrank to 2 nodes while this one was routed to node 2
	_, err := c.applyAddresses(addresses[:2])
	require.NoError(t, err)
	_, err = callNode(context.Background(), c, 2, 3, "get", stats.NOP.NewStat("retries", stats.CountType),
		func(ctx context.Context, client proto.NodeServiceClient) (*proto.GetResponse, error) {
			return client.Get(ctx, &proto.GetRequest{Keys: []string{"k"}})
		},
		func(*proto.GetResponse) bool { return true },
	)
	require.ErrorIs(t, err, errClusterChanged, "the request must be re-routed, not failed")
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
	a0, dead1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t) // nothing listens on dead1
	node0 := newMockNode(t, a0)
	node0.setList(a0, new1)
	newNode1 := newMockNode(t, new1)
	newNode1.setList(a0, new1)

	c := newDiscoveryClient(t, a0, dead1)

	// every request gives up after 80ms: the first failure must be enough to move off the dead node
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		_, err := c.Get(ctx, keysFor(1, 2, 1))
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "short requests never moved off the dead node")
	require.Positive(t, newNode1.gets.Load())
}

// TestNewClientDoesNotWaitForTheNodes has the name resolve to no pod when the client is created, as a headless Service
// does while no keydb pod is ready: NewClient returns at once, and the first request waits for a pod instead.
func TestNewClientDoesNotWaitForTheNodes(t *testing.T) {
	h := newHeadless() // no pod is ready yet
	c := newHeadlessClient(t, h)
	require.Zero(t, c.ClusterSize(), "the client must start without a node list")

	done := make(chan error, 1)
	go func() {
		_, err := c.Get(t.Context(), []string{"k"})
		done <- err
	}()
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the request returned before a pod was ready: %v", err)
	default:
	}

	a0 := freeMockAddress(t)
	newMockNode(t, a0)
	h.set(a0)
	select {
	case err := <-done:
		require.NoError(t, err)
		require.Equal(t, 1, c.ClusterSize())
	case <-time.After(5 * time.Second):
		t.Fatal("the request kept waiting after a pod was ready")
	}
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

func TestRequestWithoutNodes(t *testing.T) {
	t.Run("gives up with the context", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)
		c := newTestClientWith(t, nil, []string{headlessName},
			WithStats(statsStore), WithAddressResolver(newHeadless().resolve), // the name never resolves to a pod
		)
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err = c.Get(ctx, []string{"k"})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(start), 2*time.Second, "the request must give up when its context ends")
		retries := statsStore.Get("keydb_client_discovery_retries_total", nil)
		require.NotNil(t, retries, "a name that doesn't resolve must show in the client's metrics")
		require.Greater(t, retries.LastValue(), float64(1))
	})

	t.Run("fails at once without retries", func(t *testing.T) {
		c := newTestClientWith(t, func(conf *Config) { conf.RetryPolicy.Disabled = true }, []string{headlessName},
			WithAddressResolver(newHeadless().resolve),
		)
		err := c.Put(t.Context(), []string{"k"}, time.Minute)
		require.ErrorContains(t, err, "no node behind")
	})

	t.Run("stops after Close", func(t *testing.T) {
		c := newHeadlessClient(t, newHeadless())
		require.NoError(t, c.Close())
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err := c.Get(ctx, []string{"k"})
		require.ErrorIs(t, err, errClientClosed)
	})
}

// TestDiscoveryThroughHeadlessService has the name resolve to the pods in no particular order, a shadow included: the
// first requests go to the wrong pods, whose answers make the client discover the node list.
func TestDiscoveryThroughHeadlessService(t *testing.T) {
	a0, a1, s0 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
	nodes := []*mockNode{newMockNode(t, a0), newMockNode(t, a1)}
	for _, n := range nodes {
		n.setList(a0, a1)
	}
	shadow := newMockNode(t, s0)
	shadow.setList(a0, a1)
	shadow.notReady.Store(true)

	c := newHeadlessClient(t, newHeadless(a1, s0, a0))
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

// TestDiscoveryResolvesTheNameAgain moves node 1 while node 0 never reloads its config: only the new pod knows the
// new list. The client has to resolve the name again to find the new pod.
func TestDiscoveryResolvesTheNameAgain(t *testing.T) {
	a0, old1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t) // nothing listens on old1
	node0 := newMockNode(t, a0)
	node0.setList(a0, old1)
	h := newHeadless(a0)
	c := newHeadlessClient(t, h)

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

// TestDiscoveryAsksEveryPodAtOnce has two pods that never answer: asking one pod after the other would wait
// getNodeInfoTimeout for at least one of them.
func TestDiscoveryAsksEveryPodAtOnce(t *testing.T) {
	a0 := freeMockAddress(t)
	newMockNode(t, a0)
	c := newHeadlessClient(t, newHeadless(startSilentListener(t), startSilentListener(t), a0))
	start := time.Now()
	addresses, err := c.discover(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{a0}, addresses)
	require.Less(t, time.Since(start), getNodeInfoTimeout/2, "a pod that doesn't answer must not hold up the others")
}

func TestNoDiscoveryOnOtherErrors(t *testing.T) {
	a0 := freeMockAddress(t)
	var gets atomic.Int64
	t.Cleanup(startMockServer(t, &mockNodeServiceServer{
		getFunc: func(_ context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
			resp := &proto.GetResponse{ClusterSize: 1, NodesAddresses: []string{a0}}
			switch gets.Add(1) {
			case 1:
				return nil, status.Error(codes.Internal, "failing")
			case 2:
				resp.ErrorCode = proto.ErrorCode_SCALING
			case 3:
				resp.ErrorCode = proto.ErrorCode_INTERNAL_ERROR
			default:
				resp.Exists = make([]bool, len(req.Keys))
			}
			return resp, nil
		},
	}, a0))
	h := newHeadless(a0)
	c := newHeadlessClient(t, h)
	lookups := h.lookups.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.Get(ctx, []string{"k"})
	require.NoError(t, err)
	require.EqualValues(t, 4, gets.Load())
	require.Equal(t, lookups, h.lookups.Load(), "only a wrong node list or an unreachable node calls for discovery")
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
		"once every pod has the new config, a discovery and a retry are all it takes")
}

func TestDefaultResolveAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resolved, err := defaultResolveAddress(ctx, "127.0.0.1:1234")
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1:1234"}, resolved, "an IP address is used as it is")

	resolved, err = defaultResolveAddress(ctx, ":1234")
	require.NoError(t, err)
	require.Equal(t, []string{":1234"}, resolved, "gRPC dials an empty host as the local machine")

	resolved, err = defaultResolveAddress(ctx, "localhost:1234")
	require.NoError(t, err)
	require.Contains(t, resolved, "127.0.0.1:1234", "a name gives every address it resolves to, with the port")

	_, err = defaultResolveAddress(ctx, "keydb.test.invalid:1234")
	require.Error(t, err)
	_, err = defaultResolveAddress(ctx, "no-port")
	require.Error(t, err)
}
