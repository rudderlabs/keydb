package client

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"

	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/proto"
)

func TestClient_Get(t *testing.T) {
	t.Run("successful get", func(t *testing.T) {
		clusterSize := 3

		client, closer := createTestClient(t, int64(clusterSize))
		defer closer()
		keys := []string{"key1", "key2", "key3"}

		exists, err := client.Get(context.Background(), keys)
		require.NoError(t, err)
		require.Len(t, exists, len(keys))
		for _, e := range exists {
			require.True(t, e)
		}
	})

	t.Run("get with error", func(t *testing.T) {
		httpPort, err := testhelper.GetFreePort()
		require.NoError(t, err)
		address := fmt.Sprintf(":%d", httpPort)
		// Create a client with a server that returns an error
		errorServer := &mockNodeServiceServer{
			getFunc: func(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
				return nil, fmt.Errorf("simulated error")
			},
			clusterSize:    1,
			nodesAddresses: []string{address},
		}

		closer := startMockServer(t, errorServer, address)
		defer closer()

		errorClient, closer := createTestClientWithServers(t, []string{address})
		defer closer()

		keys := []string{"key1"}
		_, err = errorClient.Get(context.Background(), keys)
		require.Error(t, err)
	})

	t.Run("some true some false", func(t *testing.T) {
		reqResp := map[string]bool{
			"key1": true,
			"key2": false,
			"key3": true,
			"key4": false,
			"key5": true,
			"key6": false,
		}

		httpPort1, err := testhelper.GetFreePort()
		require.NoError(t, err)
		address1 := fmt.Sprintf(":%d", httpPort1)
		httpPort2, err := testhelper.GetFreePort()
		require.NoError(t, err)
		address2 := fmt.Sprintf(":%d", httpPort2)
		httpPort3, err := testhelper.GetFreePort()
		require.NoError(t, err)
		address3 := fmt.Sprintf(":%d", httpPort3)
		// Create a client with a server that returns an error
		server1 := &mockNodeServiceServer{
			getFunc: func(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
				exists := make([]bool, len(req.Keys))
				for i := range req.Keys {
					exists[i] = reqResp[req.Keys[i]]
				}
				return &proto.GetResponse{
					Exists:         exists,
					ErrorCode:      proto.ErrorCode_NO_ERROR,
					ClusterSize:    3,
					NodesAddresses: []string{address1, address2, address3},
				}, nil
			},
			clusterSize:    3,
			nodesAddresses: []string{address1, address2, address3},
		}
		server2 := &mockNodeServiceServer{
			getFunc: func(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
				exists := make([]bool, len(req.Keys))
				for i := range req.Keys {
					exists[i] = reqResp[req.Keys[i]]
				}
				return &proto.GetResponse{
					Exists:         exists,
					ErrorCode:      proto.ErrorCode_NO_ERROR,
					ClusterSize:    3,
					NodesAddresses: []string{address1, address2, address3},
				}, nil
			},
			clusterSize:    3,
			nodesAddresses: []string{address1, address2, address3},
		}
		server3 := &mockNodeServiceServer{
			getFunc: func(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
				exists := make([]bool, len(req.Keys))
				for i := range req.Keys {
					exists[i] = reqResp[req.Keys[i]]
				}
				return &proto.GetResponse{
					Exists:         exists,
					ErrorCode:      proto.ErrorCode_NO_ERROR,
					ClusterSize:    3,
					NodesAddresses: []string{address1, address2, address3},
				}, nil
			},
			clusterSize:    3,
			nodesAddresses: []string{address1, address2, address3},
		}

		closer := startMockServer(t, server1, address1)
		defer closer()

		closer = startMockServer(t, server2, address2)
		defer closer()

		closer = startMockServer(t, server3, address3)
		defer closer()

		client, closer := createTestClientWithServers(t, []string{address1, address2, address3})
		defer closer()

		keys := []string{"key1", "key2", "key3", "key4", "key5", "key6"}
		exists, err := client.Get(context.Background(), keys)
		require.NoError(t, err)
		require.Len(t, exists, len(keys))
		for i, e := range exists {
			require.Equal(t, reqResp[keys[i]], e)
		}
	})
}

func TestClient_Put(t *testing.T) {
	t.Run("successful put", func(t *testing.T) {
		client, closer := createTestClient(t, 3)
		defer closer()
		keys := []string{"key1", "key2", "key3"}
		ttl := time.Hour

		err := client.Put(context.Background(), keys, ttl)
		require.NoError(t, err)
	})

	t.Run("put with error", func(t *testing.T) {
		httpPort, err := testhelper.GetFreePort()
		require.NoError(t, err)
		address := fmt.Sprintf(":%d", httpPort)
		addresses := []string{address}

		// Create a client with a server that returns an error
		errorServer := &mockNodeServiceServer{
			putFunc: func(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
				return nil, fmt.Errorf("simulated error")
			},
			clusterSize:    int64(len(addresses)),
			nodesAddresses: addresses,
		}

		closer := startMockServer(t, errorServer, address)
		defer closer()

		errorClient, closer := createTestClientWithServers(t, []string{address})
		defer closer()

		keys := []string{"key1"}
		err = errorClient.Put(context.Background(), keys, time.Hour)
		require.Error(t, err)
	})
}

// mockNodeServiceServer implements the NodeServiceServer interface for testing
type mockNodeServiceServer struct {
	proto.UnimplementedNodeServiceServer
	getFunc         func(context.Context, *proto.GetRequest) (*proto.GetResponse, error)
	putFunc         func(context.Context, *proto.PutRequest) (*proto.PutResponse, error)
	getNodeInfoFunc func(context.Context, *proto.GetNodeInfoRequest) (*proto.GetNodeInfoResponse, error)
	clusterSize     int64
	nodesAddresses  []string
}

func (m *mockNodeServiceServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, req)
	}

	// Default implementation
	exists := make([]bool, len(req.Keys))
	for i := range req.Keys {
		exists[i] = true // Default to all keys existing
	}

	return &proto.GetResponse{
		Exists:         exists,
		ErrorCode:      proto.ErrorCode_NO_ERROR,
		ClusterSize:    m.clusterSize,
		NodesAddresses: m.nodesAddresses,
	}, nil
}

func (m *mockNodeServiceServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if m.putFunc != nil {
		return m.putFunc(ctx, req)
	}

	// Default implementation
	return &proto.PutResponse{
		Success:        true,
		ErrorCode:      proto.ErrorCode_NO_ERROR,
		ClusterSize:    m.clusterSize,
		NodesAddresses: m.nodesAddresses,
	}, nil
}

func (m *mockNodeServiceServer) GetNodeInfo(ctx context.Context, req *proto.GetNodeInfoRequest) (
	*proto.GetNodeInfoResponse, error,
) {
	if m.getNodeInfoFunc != nil {
		return m.getNodeInfoFunc(ctx, req)
	}
	return &proto.GetNodeInfoResponse{ClusterSize: m.clusterSize, NodesAddresses: m.nodesAddresses}, nil
}

func (m *mockNodeServiceServer) CreateSnapshots(ctx context.Context, req *proto.CreateSnapshotsRequest) (
	*proto.CreateSnapshotsResponse, error,
) {
	return &proto.CreateSnapshotsResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) LoadSnapshots(ctx context.Context, req *proto.LoadSnapshotsRequest) (
	*proto.LoadSnapshotsResponse, error,
) {
	return &proto.LoadSnapshotsResponse{Success: true}, nil
}

func startMockServer(t *testing.T, server *mockNodeServiceServer, address string) func() {
	t.Helper()

	// Create a TCP listener on the specified address.
	lis, err := net.Listen("tcp", address)
	require.NoError(t, err)

	s := grpc.NewServer()
	proto.RegisterNodeServiceServer(s, server)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Errorf("Server exited with error: %v", err)
		}
	}()

	return func() {
		s.Stop()
		_ = lis.Close()
	}
}

func createTestClientWithServers(t *testing.T, addresses []string) (*Client, func()) {
	t.Helper()

	// Create client with custom dialer for bufconn
	client, err := NewClient(t.Context(), Config{
		Addresses:       addresses,
		TotalHashRanges: 128,
		RetryPolicy:     RetryPolicy{Disabled: true},
	}, logger.NOP, WithStats(stats.NOP))

	require.NoError(t, err)

	// Override connection pools with bufconn connections
	client.mu.Lock()
	// Close existing pools
	for i := range client.pools {
		_ = client.pools[i].close()
	}

	// Create new pools with bufconn connections
	dialOpts := client.getGrpcDialOptions()
	for i, addr := range addresses {
		pool, err := newConnectionPool(addr, client.config.ConnectionPoolSize, dialOpts...)
		require.NoError(t, err)
		client.pools[i] = pool
	}
	client.mu.Unlock()

	return client, func() {
		_ = client.Close()
	}
}

func createTestClient(t *testing.T, clusterSize int64) (*Client, func()) {
	t.Helper()

	addresses := make([]string, clusterSize)
	for i := range addresses {
		httpPort, err := testhelper.GetFreePort()
		require.NoError(t, err)
		address := fmt.Sprintf(":%d", httpPort)
		addresses[i] = address
	}

	// Create mock servers
	servers := make([]*mockNodeServiceServer, clusterSize)
	for i := range addresses {
		servers[i] = &mockNodeServiceServer{
			clusterSize:    clusterSize,
			nodesAddresses: addresses,
		}
	}

	// Start mock servers
	closers := make([]func(), len(addresses))
	for i, server := range servers {
		closer := startMockServer(t, server, addresses[i])
		closers[i] = closer
	}

	client, closer := createTestClientWithServers(t, addresses)

	return client, func() {
		_ = client.Close()
		closer()
	}
}

// sizeFlipKeys returns keys that a 2-node cluster sends to node 1 and a 3-node cluster sends to node 2
func sizeFlipKeys(n int) []string {
	h2, h3 := hash.New(2, discoveryTestHashRanges), hash.New(3, discoveryTestHashRanges)
	var keys []string
	for i := 0; len(keys) < n; i++ {
		key := "flip-" + strconv.Itoa(i)
		if h2.GetNodeNumber(key) == 1 && h3.GetNodeNumber(key) == 2 {
			keys = append(keys, key)
		}
	}
	return keys
}

func TestClusterSizeDisagreement(t *testing.T) {
	requests := map[string]func(ctx context.Context, c *Client, keys []string) error{
		"get": func(ctx context.Context, c *Client, keys []string) error {
			_, err := c.Get(ctx, keys)
			return err
		},
		"put": func(ctx context.Context, c *Client, keys []string) error {
			return c.Put(ctx, keys, time.Hour)
		},
	}
	for method, request := range requests {
		t.Run(method, func(t *testing.T) {
			a0, a1, a2 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
			node0 := newMockNode(t, a0)
			node0.setList(a0, a1)
			node1 := newMockNode(t, a1)
			node1.setList(a0, a1, a2) // node 1 already sees a third node, as during a scale
			node2 := newMockNode(t, a2)
			node2.setList(a0, a1) // node 2 doesn't yet
			calls := func() int64 {
				return node1.gets.Load() + node1.puts.Load() + node2.gets.Load() + node2.puts.Load()
			}

			c := newDiscoveryClient(t, a0, a1) // discovery hears from either node first

			// the keys go to node 1 with 2 nodes and to node 2 with 3 nodes, so the request flips between the two
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- request(ctx, c, sizeFlipKeys(3)) }()

			time.Sleep(2 * time.Second)
			flips := calls()
			stack := make([]byte, 1<<20)
			stack = stack[:runtime.Stack(stack, true)]
			depth := strings.Count(string(stack), "client.(*Client)."+method+"(")

			node1.setList(a0, a1) // the nodes agree again
			select {
			case err := <-done:
				require.NoError(t, err, "the request must complete once the nodes agree")
			case <-time.After(10 * time.Second):
				t.Fatal("the request never completed after the nodes agreed")
			}
			// with a 20ms initial backoff growing to 200ms, 2s leave room for about 25 re-routes
			require.Less(t, flips, int64(60), "every re-route after the first must wait for the backoff")
			require.LessOrEqual(t, depth, 1, "a re-route must not add stack frames")
		})
	}
}
