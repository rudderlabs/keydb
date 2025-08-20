package operator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"

	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// mockNodeServiceServer is a mock implementation of the NodeServiceServer
type mockNodeServiceServer struct {
	pb.UnimplementedNodeServiceServer
	nodeInfo          *pb.GetNodeInfoResponse
	scaleFunc         func(ctx context.Context, req *pb.ScaleRequest) (*pb.ScaleResponse, error)
	scaleCompleteFunc func(ctx context.Context, req *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error)
}

func (m *mockNodeServiceServer) Scale(ctx context.Context, req *pb.ScaleRequest) (*pb.ScaleResponse, error) {
	if m.scaleFunc != nil {
		return m.scaleFunc(ctx, req)
	}
	return &pb.ScaleResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) ScaleComplete(ctx context.Context, req *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error) {
	if m.scaleCompleteFunc != nil {
		return m.scaleCompleteFunc(ctx, req)
	}
	return &pb.ScaleCompleteResponse{Success: true}, nil
}

// startMockNodeService starts a mock gRPC server
func startMockNodeService(t *testing.T, scaleFunc func(context.Context, *pb.ScaleRequest) (*pb.ScaleResponse, error), scaleCompleteFunc func(context.Context, *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error)) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	mockServer := &mockNodeServiceServer{
		scaleFunc:         scaleFunc,
		scaleCompleteFunc: scaleCompleteFunc,
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

func TestExecuteScalingWithRollback_Success(t *testing.T) {
	// Setup
	operatorClient := &Client{
		logger: logger.NOP,
	}

	// Test successful operation
	err := operatorClient.ExecuteScalingWithRollback(ScaleUp, 2, 3, []string{"node1", "node2"}, []string{"node1", "node2", "node3"}, func() error {
		return nil // Simulate successful operation
	})

	// Assertions
	require.NoError(t, err)
	lastOp := operatorClient.GetLastOperation()
	require.NotNil(t, lastOp)
	require.Equal(t, ScaleUp, lastOp.Type)
	require.Equal(t, Completed, lastOp.Status)
}

func TestExecuteScalingWithRollback_FailureWithRollback(t *testing.T) {
	// Counters for tracking calls
	var (
		scaleCalls         int
		scaleCompleteCalls int
		mu                 sync.Mutex
	)

	// Setup mock gRPC server
	addr, cleanup := startMockNodeService(t,
		func(ctx context.Context, req *pb.ScaleRequest) (*pb.ScaleResponse, error) {
			mu.Lock()
			scaleCalls++
			mu.Unlock()
			return &pb.ScaleResponse{Success: true}, nil
		},
		func(ctx context.Context, req *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error) {
			mu.Lock()
			scaleCompleteCalls++
			mu.Unlock()
			return &pb.ScaleCompleteResponse{Success: true}, nil
		},
	)
	defer cleanup()

	// Setup test client
	client, err := NewClient(Config{
		Addresses:       []string{addr},
		RetryCount:      3,
		RetryDelay:      100 * time.Millisecond,
		TotalHashRanges: 16,
	}, logger.NOP)
	require.NoError(t, err)

	// Test failed operation that triggers rollback
	testErr := errors.New("test error")
	err = client.ExecuteScalingWithRollback(ScaleUp, 1, 3, []string{addr}, []string{"node1", "node2"}, func() error {
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
	require.Equal(t, 1, scaleCalls)
	require.Equal(t, 2, scaleCompleteCalls)
}

func TestExecuteScalingWithRollback_RollbackFailure(t *testing.T) {
	// Setup mock gRPC server that fails on Scale
	addr, cleanup := startMockNodeService(t,
		func(ctx context.Context, req *pb.ScaleRequest) (*pb.ScaleResponse, error) {
			return nil, errors.New("scale error")
		},
		func(ctx context.Context, req *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error) {
			return &pb.ScaleCompleteResponse{Success: true}, nil
		},
	)
	defer cleanup()

	// Setup test client
	testClient, err := NewClient(Config{
		Addresses:       []string{addr},
		RetryCount:      3,
		RetryDelay:      100 * time.Millisecond,
		TotalHashRanges: 16,
	}, logger.NOP)
	require.NoError(t, err)

	// Test failed operation that also fails to rollback
	testErr := errors.New("test error")
	err = testClient.ExecuteScalingWithRollback(ScaleUp, 2, 3, []string{addr, addr}, []string{"node1", "node2", "node3"}, func() error {
		return testErr // Simulate operation failure
	})

	// Assertions
	require.Error(t, err)
	require.Contains(t, err.Error(), "scaling failed")
	require.Contains(t, err.Error(), "rollback failed")
	lastOp := testClient.GetLastOperation()
	require.NotNil(t, lastOp)
	require.Equal(t, ScaleUp, lastOp.Type)
	require.Equal(t, Failed, lastOp.Status)
}

func TestRecordAndGetOperation(t *testing.T) {
	operatorClient := &Client{
		logger: logger.NOP,
	}

	// Record an operation
	operatorClient.RecordOperation(ScaleDown, 4, 2, []string{"node1", "node2", "node3", "node4"}, []string{"node1", "node2"})

	// Retrieve and verify
	lastOp := operatorClient.GetLastOperation()
	require.NotNil(t, lastOp)
	require.Equal(t, ScaleDown, lastOp.Type)
	require.Equal(t, uint32(4), lastOp.OldClusterSize)
	require.Equal(t, uint32(2), lastOp.NewClusterSize)
	require.Equal(t, []string{"node1", "node2", "node3", "node4"}, lastOp.OldAddresses)
	require.Equal(t, []string{"node1", "node2"}, lastOp.NewAddresses)
	require.Equal(t, InProgress, lastOp.Status)
}

func TestUpdateOperationStatus(t *testing.T) {
	operatorClient := &Client{
		logger: logger.NOP,
	}

	// Record an operation
	operatorClient.RecordOperation(AutoHealing, 3, 3, []string{"node1", "node2", "node3"}, []string{"node1", "node2", "node3"})

	// Update status
	operatorClient.UpdateOperationStatus(Failed)

	// Verify
	lastOp := operatorClient.GetLastOperation()
	require.Equal(t, Failed, lastOp.Status)
}

func TestOperationRecordingTable(t *testing.T) {
	tests := []struct {
		name           string
		opType         ScalingOperationType
		oldClusterSize uint32
		newClusterSize uint32
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
			operatorClient := &Client{
				logger: logger.NOP,
			}

			operatorClient.RecordOperation(tt.opType, tt.oldClusterSize, tt.newClusterSize, tt.oldAddresses, tt.newAddresses)
			lastOp := operatorClient.GetLastOperation()

			require.Equal(t, tt.expectedType, lastOp.Type)
			require.Equal(t, tt.oldClusterSize, lastOp.OldClusterSize)
			require.Equal(t, tt.newClusterSize, lastOp.NewClusterSize)
			require.Equal(t, tt.oldAddresses, lastOp.OldAddresses)
			require.Equal(t, tt.newAddresses, lastOp.NewAddresses)
		})
	}
}

func TestConcurrentOperations(t *testing.T) {
	operatorClient := &Client{
		logger: logger.NOP,
	}

	// Run multiple operations concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			opType := ScalingOperationType(fmt.Sprintf("test_op_%d", i))
			operatorClient.RecordOperation(opType, 2, 3, []string{"node1", "node2"}, []string{"node1", "node2", "node3"})
			time.Sleep(time.Millisecond * 10) // Simulate some work
			operatorClient.UpdateOperationStatus(Completed)
		}(i)
	}

	wg.Wait()

	// Verify that we have an operation (the last one)
	lastOp := operatorClient.GetLastOperation()
	require.NotNil(t, lastOp)
}
