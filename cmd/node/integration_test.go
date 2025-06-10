package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rudderlabs/keydb/internal/client"
	"github.com/rudderlabs/keydb/internal/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// TODO fix test, it shouldn't use listeners but freeport with NewClient

const (
	bufSize = 1024 * 1024
	testTTL = 3600 // 1 hour in seconds
)

// startTestNode starts a test node with the given configuration
func startTestNode(t *testing.T, nodeID, clusterSize, totalHashRanges uint32) (*grpc.Server, *bufconn.Listener, func()) {
	t.Helper()

	// TODO use minio for testing snapshots

	// Create the node service
	config := node.Config{
		NodeID:           nodeID,
		ClusterSize:      clusterSize,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}

	service, err := node.NewService(context.TODO(), config, nil, logger.NOP)
	if err != nil {
		t.Fatalf("Failed to create node service: %v", err)
	}

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	// Create a bufconn listener
	lis := bufconn.Listen(bufSize)

	// Start the server
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Errorf("Failed to serve: %v", err)
		}
	}()

	// Return a cleanup function
	cleanup := func() {
		server.Stop()
	}

	return server, lis, cleanup
}

// createTestClient creates a test client that connects to the given listeners
func createTestClient(t *testing.T, listeners []*bufconn.Listener, totalHashRanges uint32) *client.Client {
	t.Helper()

	// Create addresses for each node
	addresses := make([]string, len(listeners))
	for i := range listeners {
		addresses[i] = fmt.Sprintf("node-%d", i)
	}

	// Create the client
	config := client.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
		RetryCount:      3,
		RetryDelay:      100 * time.Millisecond,
	}

	// Create a client with custom dialers
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	return c
}

// TestSingleNode tests a single node
func TestSingleNode(t *testing.T) {
	// Start a single node
	_, lis, cleanup := startTestNode(t, 0, 1, 128)
	defer cleanup()

	// Create a client
	c := createTestClient(t, []*bufconn.Listener{lis}, 128)
	defer c.Close()

	// Test context
	ctx := context.Background()

	// Test Put
	items := []*pb.KeyWithTTL{
		{Key: "key1", TtlSeconds: testTTL},
		{Key: "key2", TtlSeconds: testTTL},
		{Key: "key3", TtlSeconds: testTTL},
	}
	if err := c.Put(ctx, items); err != nil {
		t.Fatalf("Failed to put items: %v", err)
	}

	// Test Get
	keys := []string{"key1", "key2", "key3", "key4"}
	exists, err := c.Get(ctx, keys)
	if err != nil {
		t.Fatalf("Failed to get keys: %v", err)
	}

	// Verify results
	expected := []bool{true, true, true, false}
	for i, key := range keys {
		if exists[i] != expected[i] {
			t.Errorf("Unexpected result for key %s: got %v, want %v", key, exists[i], expected[i])
		}
	}

	// Test GetNodeInfo
	info, err := c.GetNodeInfo(ctx, 0)
	if err != nil {
		t.Fatalf("Failed to get node info: %v", err)
	}

	if info.NodeId != 0 {
		t.Errorf("Unexpected node ID: got %d, want 0", info.NodeId)
	}
	if info.ClusterSize != 1 {
		t.Errorf("Unexpected cluster size: got %d, want 1", info.ClusterSize)
	}
	if info.KeysCount != 3 {
		t.Errorf("Unexpected keys count: got %d, want 3", info.KeysCount)
	}

	// Test CreateSnapshot
	resp, err := c.CreateSnapshot(ctx, 0)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if !resp.Success {
		t.Errorf("CreateSnapshot failed: %s", resp.ErrorMessage)
	}
}

// TestScaleUp tests scaling up from 1 to 3 nodes
func TestScaleUp(t *testing.T) {
	// Start with a single node
	_, lis1, cleanup1 := startTestNode(t, 0, 1, 128)
	defer cleanup1()

	// Create a client
	c := createTestClient(t, []*bufconn.Listener{lis1}, 128)
	defer c.Close()

	// Test context
	ctx := context.Background()

	// Put some data
	items := []*pb.KeyWithTTL{
		{Key: "key1", TtlSeconds: testTTL},
		{Key: "key2", TtlSeconds: testTTL},
		{Key: "key3", TtlSeconds: testTTL},
		{Key: "key4", TtlSeconds: testTTL},
		{Key: "key5", TtlSeconds: testTTL},
	}
	if err := c.Put(ctx, items); err != nil {
		t.Fatalf("Failed to put items: %v", err)
	}

	// Verify data
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	exists, err := c.Get(ctx, keys)
	if err != nil {
		t.Fatalf("Failed to get keys: %v", err)
	}

	for i, key := range keys {
		if !exists[i] {
			t.Errorf("Key %s should exist", key)
		}
	}

	// Start two more nodes
	_, lis2, cleanup2 := startTestNode(t, 1, 3, 128)
	defer cleanup2()
	_, lis3, cleanup3 := startTestNode(t, 2, 3, 128)
	defer cleanup3()

	// Create a new client with all three nodes
	c2 := createTestClient(t, []*bufconn.Listener{lis1, lis2, lis3}, 128)
	defer c2.Close()

	// Scale up the cluster
	resp, err := c2.Scale(ctx, 0, 3)
	if err != nil {
		t.Fatalf("Failed to scale up: %v", err)
	}

	if !resp.Success {
		t.Errorf("Scale up failed: %s", resp.ErrorMessage)
	}
	if resp.PreviousClusterSize != 1 {
		t.Errorf("Unexpected previous cluster size: got %d, want 1", resp.PreviousClusterSize)
	}
	if resp.NewClusterSize != 3 {
		t.Errorf("Unexpected new cluster size: got %d, want 3", resp.NewClusterSize)
	}

	// Scale up the other nodes
	_, err = c2.Scale(ctx, 1, 3)
	if err != nil {
		t.Fatalf("Failed to scale up node 1: %v", err)
	}
	_, err = c2.Scale(ctx, 2, 3)
	if err != nil {
		t.Fatalf("Failed to scale up node 2: %v", err)
	}

	// Notify all nodes that scaling is complete
	for i := uint32(0); i < 3; i++ {
		scResp, err := c2.ScaleComplete(ctx, i)
		if err != nil {
			t.Fatalf("Failed to complete scaling on node %d: %v", i, err)
		}
		if !scResp.Success {
			t.Errorf("ScaleComplete failed on node %d", i)
		}
	}

	// Verify data after scaling
	exists, err = c2.Get(ctx, keys)
	if err != nil {
		t.Fatalf("Failed to get keys after scaling: %v", err)
	}

	for i, key := range keys {
		if !exists[i] {
			t.Errorf("Key %s should exist after scaling", key)
		}
	}

	// Put more data
	newItems := []*pb.KeyWithTTL{
		{Key: "key6", TtlSeconds: testTTL},
		{Key: "key7", TtlSeconds: testTTL},
		{Key: "key8", TtlSeconds: testTTL},
	}
	if err := c2.Put(ctx, newItems); err != nil {
		t.Fatalf("Failed to put items after scaling: %v", err)
	}

	// Verify all data
	allKeys := append(keys, []string{"key6", "key7", "key8"}...)
	exists, err = c2.Get(ctx, allKeys)
	if err != nil {
		t.Fatalf("Failed to get all keys: %v", err)
	}

	for i, key := range allKeys {
		if !exists[i] {
			t.Errorf("Key %s should exist", key)
		}
	}

	// Check node info for all nodes
	for i := uint32(0); i < 3; i++ {
		info, err := c2.GetNodeInfo(ctx, i)
		if err != nil {
			t.Fatalf("Failed to get node info for node %d: %v", i, err)
		}

		if info.NodeId != i {
			t.Errorf("Unexpected node ID: got %d, want %d", info.NodeId, i)
		}
		if info.ClusterSize != 3 {
			t.Errorf("Unexpected cluster size: got %d, want 3", info.ClusterSize)
		}
	}
}

// TestScaleDown tests scaling down from 3 to 2 nodes
func TestScaleDown(t *testing.T) {
	// Start with three nodes
	_, lis1, cleanup1 := startTestNode(t, 0, 3, 128)
	defer cleanup1()
	_, lis2, cleanup2 := startTestNode(t, 1, 3, 128)
	defer cleanup2()
	_, lis3, cleanup3 := startTestNode(t, 2, 3, 128)
	defer cleanup3()

	// Create a client
	c := createTestClient(t, []*bufconn.Listener{lis1, lis2, lis3}, 128)
	defer c.Close()

	// Test context
	ctx := context.Background()

	// Put some data
	items := []*pb.KeyWithTTL{
		{Key: "key1", TtlSeconds: testTTL},
		{Key: "key2", TtlSeconds: testTTL},
		{Key: "key3", TtlSeconds: testTTL},
		{Key: "key4", TtlSeconds: testTTL},
		{Key: "key5", TtlSeconds: testTTL},
	}
	if err := c.Put(ctx, items); err != nil {
		t.Fatalf("Failed to put items: %v", err)
	}

	// Verify data
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	exists, err := c.Get(ctx, keys)
	if err != nil {
		t.Fatalf("Failed to get keys: %v", err)
	}

	for i, key := range keys {
		if !exists[i] {
			t.Errorf("Key %s should exist", key)
		}
	}

	// Scale down to 2 nodes
	resp, err := c.Scale(ctx, 0, 2)
	if err != nil {
		t.Fatalf("Failed to scale down: %v", err)
	}

	if !resp.Success {
		t.Errorf("Scale down failed: %s", resp.ErrorMessage)
	}
	if resp.PreviousClusterSize != 3 {
		t.Errorf("Unexpected previous cluster size: got %d, want 3", resp.PreviousClusterSize)
	}
	if resp.NewClusterSize != 2 {
		t.Errorf("Unexpected new cluster size: got %d, want 2", resp.NewClusterSize)
	}

	// Scale down the other nodes
	_, err = c.Scale(ctx, 1, 2)
	if err != nil {
		t.Fatalf("Failed to scale down node 1: %v", err)
	}

	// Notify all nodes that scaling is complete
	for i := uint32(0); i < 2; i++ {
		scResp, err := c.ScaleComplete(ctx, i)
		if err != nil {
			t.Fatalf("Failed to complete scaling on node %d: %v", i, err)
		}
		if !scResp.Success {
			t.Errorf("ScaleComplete failed on node %d", i)
		}
	}

	// Verify data after scaling down
	exists, err = c.Get(ctx, keys)
	if err != nil {
		t.Fatalf("Failed to get keys after scaling down: %v", err)
	}

	for i, key := range keys {
		if !exists[i] {
			t.Errorf("Key %s should exist after scaling down", key)
		}
	}

	// Put more data
	newItems := []*pb.KeyWithTTL{
		{Key: "key6", TtlSeconds: testTTL},
		{Key: "key7", TtlSeconds: testTTL},
	}
	if err := c.Put(ctx, newItems); err != nil {
		t.Fatalf("Failed to put items after scaling down: %v", err)
	}

	// Verify all data
	allKeys := append(keys, []string{"key6", "key7"}...)
	exists, err = c.Get(ctx, allKeys)
	if err != nil {
		t.Fatalf("Failed to get all keys: %v", err)
	}

	for i, key := range allKeys {
		if !exists[i] {
			t.Errorf("Key %s should exist", key)
		}
	}

	// Check node info for the remaining nodes
	for i := uint32(0); i < 2; i++ {
		info, err := c.GetNodeInfo(ctx, i)
		if err != nil {
			t.Fatalf("Failed to get node info for node %d: %v", i, err)
		}

		if info.NodeId != i {
			t.Errorf("Unexpected node ID: got %d, want %d", info.NodeId, i)
		}
		if info.ClusterSize != 2 {
			t.Errorf("Unexpected cluster size: got %d, want 2", info.ClusterSize)
		}
	}
}
