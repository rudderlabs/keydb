package node

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rudderlabs/keydb/internal/client"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/rudderlabs/keydb/proto"
)

const (
	bufSize = 1024 * 1024
	testTTL = 3600 // 1 hour in seconds
)

func TestGetPut(t *testing.T) {
	snapshotDir := t.TempDir()

	// Create the node service
	config := Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  128,
		SnapshotDir:      snapshotDir,
		SnapshotInterval: 60, // 60 seconds
	}

	service, err := NewService(config)
	require.NoError(t, err)

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	// Create a bufconn listener
	lis := bufconn.Listen(bufSize)

	// Start the server
	go func() {
		require.NoError(t, server.Serve(lis))
	}()
	defer server.Stop()

	// Create a client
	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial() // TODO what about the context
	}

	clientConfig := client.Config{
		Addresses:       []string{"node-0"},
		TotalHashRanges: 128,
		RetryCount:      3,
		RetryDelay:      100 * time.Millisecond,
	}

	c, err := client.NewClientWithDialers(clientConfig, []func(context.Context, string) (net.Conn, error){dialer})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close())
	}()

	// Test context
	ctx := context.Background()

	// Test Put
	items := []*pb.KeyWithTTL{
		{Key: "key1", TtlSeconds: testTTL},
		{Key: "key2", TtlSeconds: testTTL},
		{Key: "key3", TtlSeconds: testTTL},
	}
	require.NoError(t, c.Put(ctx, items))

	keys := []string{"key1", "key2", "key3", "key4"}
	exists, err := c.Get(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)
}
