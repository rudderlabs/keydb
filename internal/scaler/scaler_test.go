package scaler

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestExecuteScalingWithRollback_Success(t *testing.T) {
	// Setup
	scalerClient := &Client{
		logger: logger.NOP,
	}

	// Test successful operation
	err := scalerClient.ExecuteScalingWithRollback(ScaleUp,
		[]string{"node1", "node2"},
		[]string{"node1", "node2", "node3"},
		func() error {
			return nil // Simulate successful operation
		},
	)

	// Assertions
	require.NoError(t, err)
	lastOp := scalerClient.GetLastOperation()
	require.NotNil(t, lastOp)
	require.Equal(t, ScaleUp, lastOp.Type)
	require.Equal(t, Completed, lastOp.Status)
}

func TestExecuteScalingWithRollback_FailureWithRollback(t *testing.T) {
	// Counters for tracking calls
	var (
		getNodeInfoCalls int
		mu               sync.Mutex
	)

	// Setup mock gRPC server
	addr, cleanup := startMockNodeService(t,
		func(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
			mu.Lock()
			getNodeInfoCalls++
			mu.Unlock()
			return &pb.GetNodeInfoResponse{NodeId: req.NodeId}, nil
		},
	)
	defer cleanup()

	// Setup test client
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

	// Test failed operation that triggers rollback
	testErr := errors.New("test error")
	err = client.ExecuteScalingWithRollback(ScaleUp, []string{addr}, []string{"node1", "node2"}, func() error {
		return testErr // Simulate operation failure
	})

	// Assertions
	require.Error(t, err)
	require.Contains(t, err.Error(), "scaling failed and rolled back")
	lastOp := client.GetLastOperation()
	require.NotNil(t, lastOp)
	require.Equal(t, ScaleUp, lastOp.Type)
	require.Equal(t, RolledBack, lastOp.Status)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, getNodeInfoCalls)
}

func TestRecordAndGetOperation(t *testing.T) {
	scalerClient := &Client{
		logger: logger.NOP,
	}

	// Record an operation
	scalerClient.RecordOperation(ScaleDown, 4, 2,
		[]string{"node1", "node2", "node3", "node4"}, []string{"node1", "node2"})

	// Retrieve and verify
	lastOp := scalerClient.GetLastOperation()
	require.NotNil(t, lastOp)
	require.Equal(t, ScaleDown, lastOp.Type)
	require.Equal(t, int64(4), lastOp.OldClusterSize)
	require.Equal(t, int64(2), lastOp.NewClusterSize)
	require.Equal(t, []string{"node1", "node2", "node3", "node4"}, lastOp.OldAddresses)
	require.Equal(t, []string{"node1", "node2"}, lastOp.NewAddresses)
	require.Equal(t, InProgress, lastOp.Status)
}

func TestUpdateOperationStatus(t *testing.T) {
	scalerClient := &Client{
		logger: logger.NOP,
	}

	// Record an operation
	scalerClient.RecordOperation(AutoHealing, 3, 3,
		[]string{"node1", "node2", "node3"}, []string{"node1", "node2", "node3"})

	// Update status
	scalerClient.UpdateOperationStatus(Failed)

	// Verify
	lastOp := scalerClient.GetLastOperation()
	require.Equal(t, Failed, lastOp.Status)
}

func TestOperationRecordingTable(t *testing.T) {
	tests := []struct {
		name           string
		opType         ScalingOperationType
		oldClusterSize int64
		newClusterSize int64
		oldAddresses   []string
		newAddresses   []string
		expectedType   ScalingOperationType
	}{
		{
			name:           "scale up",
			opType:         ScaleUp,
			oldClusterSize: 2,
			newClusterSize: 3,
			oldAddresses:   []string{"node1", "node2"},
			newAddresses:   []string{"node1", "node2", "node3"},
			expectedType:   ScaleUp,
		},
		{
			name:           "scale down",
			opType:         ScaleDown,
			oldClusterSize: 3,
			newClusterSize: 2,
			oldAddresses:   []string{"node1", "node2", "node3"},
			newAddresses:   []string{"node1", "node2"},
			expectedType:   ScaleDown,
		},
		{
			name:           "auto healing",
			opType:         AutoHealing,
			oldClusterSize: 2,
			newClusterSize: 2,
			oldAddresses:   []string{"node1", "node2"},
			newAddresses:   []string{"node1", "node2"},
			expectedType:   AutoHealing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scalerClient := &Client{
				logger: logger.NOP,
			}

			scalerClient.RecordOperation(tt.opType, tt.oldClusterSize, tt.newClusterSize,
				tt.oldAddresses, tt.newAddresses)
			lastOp := scalerClient.GetLastOperation()

			require.Equal(t, tt.expectedType, lastOp.Type)
			require.Equal(t, tt.oldClusterSize, lastOp.OldClusterSize)
			require.Equal(t, tt.newClusterSize, lastOp.NewClusterSize)
			require.Equal(t, tt.oldAddresses, lastOp.OldAddresses)
			require.Equal(t, tt.newAddresses, lastOp.NewAddresses)
		})
	}
}

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

// startMockNodeService starts a mock gRPC server
func startMockNodeService(t *testing.T,
	getNodeInfoFunc func(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error),
) (string, func()) {
	return startMockNodeServiceWithOpts(t, mockNodeServiceOpts{
		getNodeInfoFunc: getNodeInfoFunc,
	})
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
