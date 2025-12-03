package scaler

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestSendSnapshot_Success(t *testing.T) {
	var (
		receivedReq *pb.SendSnapshotRequest
		mu          sync.Mutex
	)

	addr, cleanup := startMockNodeServiceWithOpts(t, mockNodeServiceOpts{
		sendSnapshotFunc: func(ctx context.Context, req *pb.SendSnapshotRequest) (*pb.SendSnapshotResponse, error) {
			mu.Lock()
			receivedReq = req
			require.EqualValues(t, 5, req.HashRange)
			require.Equal(t, "destination:50051", req.DestinationAddress)
			mu.Unlock()
			return &pb.SendSnapshotResponse{Success: true, NodeId: 0}, nil
		},
	})
	defer cleanup()

	client, err := NewClient(Config{
		Addresses:       []string{addr},
		TotalHashRanges: 16,
		RetryPolicy: RetryPolicy{
			InitialInterval: time.Millisecond,
			Multiplier:      1.0,
			MaxInterval:     time.Millisecond,
		},
	}, logger.NOP)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	err = client.SendSnapshot(context.Background(), 0, "destination:50051", 5)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, receivedReq)
	require.Equal(t, int64(5), receivedReq.HashRange)
	require.Equal(t, "destination:50051", receivedReq.DestinationAddress)
}

func TestSendSnapshot_Failure(t *testing.T) {
	addr, cleanup := startMockNodeServiceWithOpts(t, mockNodeServiceOpts{
		sendSnapshotFunc: func(ctx context.Context, req *pb.SendSnapshotRequest) (*pb.SendSnapshotResponse, error) {
			return &pb.SendSnapshotResponse{
				Success:      false,
				ErrorMessage: "destination unreachable",
				NodeId:       0,
			}, nil
		},
	})
	defer cleanup()

	client, err := NewClient(Config{
		Addresses:       []string{addr},
		TotalHashRanges: 16,
		RetryPolicy: RetryPolicy{
			Disabled: true, // Disable retries to fail fast
		},
	}, logger.NOP)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	err = client.SendSnapshot(context.Background(), 0, "destination:50051", 5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "destination unreachable")
}

func TestSendSnapshot_RetryOnError(t *testing.T) {
	var (
		callCount int
		mu        sync.Mutex
	)

	addr, cleanup := startMockNodeServiceWithOpts(t, mockNodeServiceOpts{
		sendSnapshotFunc: func(ctx context.Context, req *pb.SendSnapshotRequest) (*pb.SendSnapshotResponse, error) {
			mu.Lock()
			callCount++
			count := callCount
			mu.Unlock()

			if count < 3 {
				return &pb.SendSnapshotResponse{
					Success:      false,
					ErrorMessage: "temporary failure",
					NodeId:       0,
				}, nil
			}
			return &pb.SendSnapshotResponse{Success: true, NodeId: 0}, nil
		},
	})
	defer cleanup()

	client, err := NewClient(Config{
		Addresses:       []string{addr},
		TotalHashRanges: 16,
		RetryPolicy: RetryPolicy{
			InitialInterval: time.Millisecond,
			Multiplier:      1.0,
			MaxInterval:     time.Millisecond,
			MaxElapsedTime:  time.Second,
		},
	}, logger.NOP)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	err = client.SendSnapshot(context.Background(), 0, "destination:50051", 5)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 3, callCount)
}

func TestGetNodeAddress(t *testing.T) {
	client := &Client{
		config: Config{
			Addresses: []string{"node0:50051", "node1:50051", "node2:50051"},
		},
		logger: logger.NOP,
	}

	// Test valid node IDs
	addr, err := client.GetNodeAddress(0)
	require.NoError(t, err)
	require.Equal(t, "node0:50051", addr)

	addr, err = client.GetNodeAddress(2)
	require.NoError(t, err)
	require.Equal(t, "node2:50051", addr)

	// Test invalid node IDs
	_, err = client.GetNodeAddress(-1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")

	_, err = client.GetNodeAddress(3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
}

// mockNodeServiceServer is a mock implementation of the NodeServiceServer
type mockNodeServiceServer struct {
	pb.UnimplementedNodeServiceServer
	getNodeInfoFunc  func(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error)
	sendSnapshotFunc func(ctx context.Context, req *pb.SendSnapshotRequest) (*pb.SendSnapshotResponse, error)
}

func (m *mockNodeServiceServer) GetNodeInfo(
	ctx context.Context, req *pb.GetNodeInfoRequest,
) (*pb.GetNodeInfoResponse, error) {
	if m.getNodeInfoFunc != nil {
		return m.getNodeInfoFunc(ctx, req)
	}
	return &pb.GetNodeInfoResponse{NodeId: req.NodeId}, nil
}

func (m *mockNodeServiceServer) SendSnapshot(
	ctx context.Context, req *pb.SendSnapshotRequest,
) (*pb.SendSnapshotResponse, error) {
	if m.sendSnapshotFunc != nil {
		return m.sendSnapshotFunc(ctx, req)
	}
	return &pb.SendSnapshotResponse{Success: true, NodeId: 0}, nil
}

// mockNodeServiceOpts configures the mock node service
type mockNodeServiceOpts struct {
	getNodeInfoFunc  func(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error)
	sendSnapshotFunc func(ctx context.Context, req *pb.SendSnapshotRequest) (*pb.SendSnapshotResponse, error)
}

// startMockNodeServiceWithOpts starts a mock gRPC server with full options
func startMockNodeServiceWithOpts(t *testing.T, opts mockNodeServiceOpts) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	mockServer := &mockNodeServiceServer{
		getNodeInfoFunc:  opts.getNodeInfoFunc,
		sendSnapshotFunc: opts.sendSnapshotFunc,
	}
	pb.RegisterNodeServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	return lis.Addr().String(), func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	}
}
