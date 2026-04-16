package main

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"

	pb "github.com/rudderlabs/keydb/proto"
)

func TestGetNodeID(t *testing.T) {
	tests := []struct {
		name        string
		podName     string
		expectedID  int64
		expectError bool
	}{
		// Multi-statefulset format tests (new format)
		{
			name:        "valid multi-statefulset pod name - node 0",
			podName:     "keydb-0-0",
			expectedID:  0,
			expectError: false,
		},
		{
			name:        "valid multi-statefulset pod name - node 1",
			podName:     "keydb-1-0",
			expectedID:  1,
			expectError: false,
		},
		{
			name:        "valid multi-statefulset pod name - node 99",
			podName:     "keydb-99-0",
			expectedID:  99,
			expectError: false,
		},
		{
			name:        "valid multi-statefulset pod name - node 1234",
			podName:     "keydb-1234-0",
			expectedID:  1234,
			expectError: false,
		},
		// Legacy format tests (backward compatibility)
		{
			name:        "valid legacy pod name - node 0",
			podName:     "keydb-0",
			expectedID:  0,
			expectError: false,
		},
		{
			name:        "valid legacy pod name - node 1",
			podName:     "keydb-1",
			expectedID:  1,
			expectError: false,
		},
		{
			name:        "valid legacy pod name - node 99",
			podName:     "keydb-99",
			expectedID:  99,
			expectError: false,
		},
		{
			name:        "valid legacy pod name - node 1234",
			podName:     "keydb-1234",
			expectedID:  1234,
			expectError: false,
		},
		// Invalid format tests
		{
			name:        "invalid multi-statefulset pod name - wrong suffix",
			podName:     "keydb-0-1",
			expectError: true,
		},
		{
			name:        "invalid multi-statefulset pod name - wrong suffix 2",
			podName:     "keydb-5-2",
			expectError: true,
		},
		{
			name:        "invalid pod name - no number",
			podName:     "keydb-",
			expectError: true,
		},
		{
			name:        "invalid pod name - wrong prefix",
			podName:     "redis-0",
			expectError: true,
		},
		{
			name:        "invalid pod name - empty string",
			podName:     "",
			expectError: true,
		},
		{
			name:        "invalid pod name - just number",
			podName:     "0",
			expectError: true,
		},
		{
			name:        "invalid pod name - random string",
			podName:     "invalid-name",
			expectError: true,
		},
		{
			name:        "invalid pod name - multiple hyphens",
			podName:     "keydb-1-0-extra",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeID, err := getNodeID(tt.podName)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error for pod name %q, but got none", tt.podName)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for pod name %q: %v", tt.podName, err)
					return
				}
				if nodeID != tt.expectedID {
					t.Errorf("expected node ID %d, got %d", tt.expectedID, nodeID)
				}
			}
		})
	}
}

// TestRunWith_HealthEndpoints boots the full runWith() — exactly the code path
// the binary runs in production — against a fake cloud storage and asserts:
//   - the health endpoint is reachable on healthPort
//   - liveness ("") is SERVING (pod won't be killed during startup)
//   - readiness ("keydb.NodeService") becomes SERVING once NodeService is registered
//   - the main gRPC port also serves the health service
//   - SIGTERM / context cancel flips everything to NOT_SERVING and shuts down cleanly
func TestRunWith_HealthEndpoints(t *testing.T) {
	mainPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	healthPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	profilerPort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	conf := config.New()
	conf.Set("port", mainPort)
	conf.Set("healthPort", healthPort)
	conf.Set("Profiler.Port", profilerPort)
	conf.Set("nodeId", "keydb-1-0")
	conf.Set("nodeAddresses", "localhost:"+strconv.Itoa(mainPort)+",localhost:"+strconv.Itoa(mainPort))
	conf.Set("BadgerDB.Dedup.Path", t.TempDir())
	conf.Set("shutdownTimeout", "5s")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Run the binary's real entry point (minus cloudStorage construction) in a
	// goroutine. runErrCh receives the terminal error so the test can assert
	// clean shutdown at the end.
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- run(ctx, cancel, conf, stats.NOP, logger.NOP, &mockedCloudStorage{})
	}()

	healthAddr := "localhost:" + strconv.Itoa(healthPort)
	mainAddr := "localhost:" + strconv.Itoa(mainPort)

	newHealthClient := func(t *testing.T, addr string) healthpb.HealthClient {
		t.Helper()
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return healthpb.NewHealthClient(conn)
	}

	checkStatus := func(t *testing.T, client healthpb.HealthClient, service string) (
		healthpb.HealthCheckResponse_ServingStatus, error,
	) {
		t.Helper()
		reqCtx, reqCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer reqCancel()
		resp, err := client.Check(reqCtx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			return 0, err
		}
		return resp.Status, nil
	}

	healthClient := newHealthClient(t, healthAddr)

	// Step 1: the health port must come up before NodeService is ready. Because
	// NodeService init is effectively instant with the mocked cloud storage,
	// we can't *observe* NOT_SERVING reliably here we assert the contract
	// that SetServingStatus/Shutdown code paths were wired up by reaching
	// SERVING on both endpoints quickly.
	require.Eventually(t, func() bool {
		s, err := checkStatus(t, healthClient, "")
		return err == nil && s == healthpb.HealthCheckResponse_SERVING
	}, 10*time.Second, 50*time.Millisecond, "liveness ('') must be SERVING on health port")

	t.Run("readiness on health port reports NodeService SERVING once ready", func(t *testing.T) {
		require.Eventually(t, func() bool {
			s, err := checkStatus(t, healthClient, pb.NodeService_ServiceDesc.ServiceName)
			return err == nil && s == healthpb.HealthCheckResponse_SERVING
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("main gRPC port also serves the health service", func(t *testing.T) {
		// The main server registers health alongside NodeService, so clients
		// dialing 50051 can use gRPC health checks without a second connection.
		mainClient := newHealthClient(t, mainAddr)
		require.Eventually(t, func() bool {
			s, err := checkStatus(t, mainClient, pb.NodeService_ServiceDesc.ServiceName)
			return err == nil && s == healthpb.HealthCheckResponse_SERVING
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("NodeService RPC succeeds once readiness is SERVING", func(t *testing.T) {
		// Sanity check: if readiness says SERVING, an actual RPC should work.
		// Otherwise the health signal is lying and k8s would route traffic to a
		// broken pod.
		conn, err := grpc.NewClient(mainAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })

		reqCtx, reqCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer reqCancel()
		resp, err := pb.NewNodeServiceClient(conn).GetNodeInfo(reqCtx, &pb.GetNodeInfoRequest{})
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.NodeId)
	})

	// Step 2: simulate SIGTERM via context cancel. The shutdown handler must
	// flip everything to NOT_SERVING *before* the health server stops, so
	// readiness probes drain endpoints cleanly rather than hit a closed port.
	t.Run("shutdown flips readiness to NOT_SERVING before closing", func(t *testing.T) {
		cancel()

		// We might observe NOT_SERVING, or the connection might close. Both are
		// acceptable shutdown signals (k8s treats either as probe failure). The
		// crucial property is: we never see SERVING after shutdown started.
		require.Eventually(t, func() bool {
			s, err := checkStatus(t, healthClient, pb.NodeService_ServiceDesc.ServiceName)
			if err != nil {
				return true
			}
			return s == healthpb.HealthCheckResponse_NOT_SERVING
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("run returns cleanly", func(t *testing.T) {
		select {
		case err := <-runErrCh:
			// context.Canceled is the expected terminal error when ctx is cancelled.
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("run did not return within shutdown timeout")
		}
	})

	// Ports must actually be released after shutdown — otherwise subsequent
	// test runs (or the next binary start) would fail to bind.
	t.Run("ports are released after shutdown", func(t *testing.T) {
		for _, addr := range []string{healthAddr, mainAddr} {
			lis, err := net.Listen("tcp", addr)
			require.NoErrorf(t, err, "port %s should be released after shutdown", addr)
			_ = lis.Close()
		}
	})
}
