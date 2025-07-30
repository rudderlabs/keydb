package client

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestClient_Get(t *testing.T) {
	t.Run("successful get", func(t *testing.T) {
		clusterSize := 3

		client, closer := createTestClient(t, uint32(clusterSize))
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
			clusterSize:    uint32(len(addresses)),
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
	getFunc           func(context.Context, *proto.GetRequest) (*proto.GetResponse, error)
	putFunc           func(context.Context, *proto.PutRequest) (*proto.PutResponse, error)
	getNodeInfoFunc   func(context.Context, *proto.GetNodeInfoRequest) (*proto.GetNodeInfoResponse, error)
	scaleFunc         func(context.Context, *proto.ScaleRequest) (*proto.ScaleResponse, error)
	scaleCompleteFunc func(context.Context, *proto.ScaleCompleteRequest) (*proto.ScaleCompleteResponse, error)
	createSnapFunc    func(context.Context, *proto.CreateSnapshotsRequest) (*proto.CreateSnapshotsResponse, error)
	loadSnapFunc      func(context.Context, *proto.LoadSnapshotsRequest) (*proto.LoadSnapshotsResponse, error)
	clusterSize       uint32
	nodesAddresses    []string
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

	return &proto.GetNodeInfoResponse{}, nil
}

func (m *mockNodeServiceServer) Scale(ctx context.Context, req *proto.ScaleRequest) (*proto.ScaleResponse, error) {
	if m.scaleFunc != nil {
		return m.scaleFunc(ctx, req)
	}

	return &proto.ScaleResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) ScaleComplete(ctx context.Context, req *proto.ScaleCompleteRequest) (
	*proto.ScaleCompleteResponse, error,
) {
	if m.scaleCompleteFunc != nil {
		return m.scaleCompleteFunc(ctx, req)
	}

	return &proto.ScaleCompleteResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) CreateSnapshots(ctx context.Context, req *proto.CreateSnapshotsRequest) (
	*proto.CreateSnapshotsResponse, error,
) {
	if m.createSnapFunc != nil {
		return m.createSnapFunc(ctx, req)
	}

	return &proto.CreateSnapshotsResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) LoadSnapshots(ctx context.Context, req *proto.LoadSnapshotsRequest) (
	*proto.LoadSnapshotsResponse, error,
) {
	if m.loadSnapFunc != nil {
		return m.loadSnapFunc(ctx, req)
	}

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
	client, err := NewClient(Config{
		Addresses:       addresses,
		TotalHashRanges: 128,
		RetryCount:      1,
		RetryDelay:      time.Millisecond,
	}, logger.NOP, WithStats(stats.NOP))

	require.NoError(t, err)

	// Override connections with bufconn connections
	client.mu.Lock()
	for i := range client.connections {
		_ = client.connections[i].Close()
	}

	for i, addr := range addresses {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)

		client.connections[i] = conn
		client.clients[i] = proto.NewNodeServiceClient(conn)
	}
	client.mu.Unlock()

	return client, func() {
		_ = client.Close()
	}
}

func createTestClient(t *testing.T, clusterSize uint32) (*Client, func()) {
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
