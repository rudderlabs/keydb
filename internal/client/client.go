package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

// TODO lots of things can be done in parallel

// Config holds the configuration for a client
type Config struct {
	// Addresses is a list of node addresses (host:port)
	Addresses []string

	// TotalHashRanges is the total number of hash ranges
	TotalHashRanges uint32

	// RetryCount is the number of times to retry a request
	RetryCount int

	// RetryDelay is the delay between retries
	RetryDelay time.Duration
}

// Client is a client for the KeyDB service
type Client struct {
	config Config

	// clusterSize is the number of nodes in the cluster
	clusterSize uint32

	// connections is a map of node index to connection
	connections map[int]*grpc.ClientConn

	// clients is a map of node index to client
	clients map[int]pb.NodeServiceClient

	// mu protects connections, clients and clusterSize
	mu sync.RWMutex
}

// NewClient creates a new KeyDB client
func NewClient(config Config) (*Client, error) {
	if len(config.Addresses) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	if config.TotalHashRanges == 0 {
		config.TotalHashRanges = 128
	}

	if config.RetryCount == 0 {
		config.RetryCount = 3
	}

	if config.RetryDelay == 0 {
		config.RetryDelay = 100 * time.Millisecond
	}

	client := &Client{
		config:      config,
		connections: make(map[int]*grpc.ClientConn),
		clients:     make(map[int]pb.NodeServiceClient),
		clusterSize: uint32(len(config.Addresses)),
	}

	// Connect to all nodes
	for i, addr := range config.Addresses {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "tcp", addr)
			}),
		)
		if err != nil {
			// Close all connections on error
			_ = client.Close()
			return nil, fmt.Errorf("failed to connect to node %d at %s: %w", i, addr, err)
		}

		client.connections[i] = conn
		client.clients[i] = pb.NewNodeServiceClient(conn)
	}

	return client, nil
}

// Close closes all connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for i, conn := range c.connections {
		if err := conn.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close connection to node %d: %w", i, err)
		}
	}

	c.connections = make(map[int]*grpc.ClientConn)
	c.clients = make(map[int]pb.NodeServiceClient)
	c.clusterSize = 0

	return lastErr
}

// Get retrieves values for multiple keys
func (c *Client) Get(ctx context.Context, keys []string) ([]bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Group keys by node
	// TODO the hashing can be done by a few routines that push into a channel, then a single routine reads from the ch
	// and populates the slice. It might be overkilling for a few keys but we could do it for big batches if we see
	// that the hashing takes time - TODO add metrics to measure latency of hashing and requests
	keysByNode := make(map[uint32][]string)
	for _, key := range keys {
		_, nodeID := hash.GetNodeNumber(key, c.clusterSize, c.config.TotalHashRanges)
		keysByNode[nodeID] = append(keysByNode[nodeID], key)
	}

	// Create a map to store results
	results := make(map[string]bool)
	resultsMu := sync.Mutex{}

	group, ctx := kitsync.NewEagerGroup(ctx, len(keysByNode))
	for nodeID, nodeKeys := range keysByNode {
		group.Go(func() error {
			// Get the client for this node
			client, ok := c.clients[int(nodeID)]
			if !ok {
				return fmt.Errorf("no client for node %d", nodeID)
			}

			// Create the request
			req := &pb.GetRequest{Keys: nodeKeys}

			// Send the request with retries
			var err error
			var resp *pb.GetResponse

			for i := 0; i <= c.config.RetryCount; i++ {
				resp, err = client.Get(ctx, req)
				if err == nil && resp != nil {
					if resp.ErrorCode == pb.ErrorCode_NO_ERROR {
						break // for internal errors, scaling or wrong code we retry
					}
				}

				// TODO add some logging and metrics here

				// If this is the last retry, return the error
				if i == c.config.RetryCount {
					return fmt.Errorf("failed to get keys from node %d: %w", nodeID, err)
				}

				// Wait before retrying
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(c.config.RetryDelay):
				}
			}

			// Store results
			resultsMu.Lock()
			for i, key := range nodeKeys {
				results[key] = resp.Exists[i]
			}
			resultsMu.Unlock()
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	// Convert results map to slice in the same order as input keys
	exists := make([]bool, len(keys))
	for i, key := range keys {
		exists[i] = results[key]
	}

	return exists, nil
}

// Put stores multiple key-value pairs with TTL
func (c *Client) Put(ctx context.Context, items []*pb.KeyWithTTL) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Group items by node
	// TODO the hashing can be done by a few routines that push into a channel, then a single routine reads from the ch
	// and populates the slice. It might be overkilling for a few keys but we could do it for big batches if we see
	// that the hashing takes time - TODO add metrics to measure latency of hashing and requests
	itemsByNode := make(map[uint32][]*pb.KeyWithTTL)
	for _, item := range items {
		_, nodeID := hash.GetNodeNumber(item.Key, c.clusterSize, c.config.TotalHashRanges)
		itemsByNode[nodeID] = append(itemsByNode[nodeID], item)
	}

	// Send requests to each node
	for nodeID, nodeItems := range itemsByNode {
		// Get the client for this node
		client, ok := c.clients[int(nodeID)]
		if !ok {
			return fmt.Errorf("no client for node %d", nodeID)
		}

		// Create the request
		req := &pb.PutRequest{
			Items: nodeItems,
		}

		// Send the request with retries
		var resp *pb.PutResponse
		var err error

		for i := 0; i <= c.config.RetryCount; i++ {
			resp, err = client.Put(ctx, req)
			if err == nil {
				break
			}

			// If this is the last retry, return the error
			if i == c.config.RetryCount {
				return fmt.Errorf("failed to put items to node %d: %w", nodeID, err)
			}

			// Wait before retrying
			time.Sleep(c.config.RetryDelay)
		}

		// Check for errors in the response
		if !resp.Success || resp.ErrorCode != pb.ErrorCode_NO_ERROR {
			// Handle wrong node error
			if resp.ErrorCode == pb.ErrorCode_WRONG_NODE {
				// Update cluster size and retry
				c.clusterSize = resp.ClusterSize
				return c.Put(ctx, items)
			}

			// Handle scaling error
			if resp.ErrorCode == pb.ErrorCode_SCALING {
				// Wait and retry
				time.Sleep(c.config.RetryDelay)
				return c.Put(ctx, items)
			}

			return fmt.Errorf("error from node %d: %v", nodeID, resp.ErrorCode)
		}
	}

	return nil
}

// GetNodeInfo returns information about a node
func (c *Client) GetNodeInfo(ctx context.Context, nodeID uint32) (*pb.GetNodeInfoResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		return nil, fmt.Errorf("no client for node %d", nodeID)
	}

	// Create the request
	req := &pb.GetNodeInfoRequest{
		NodeId: nodeID,
	}

	// Send the request with retries
	var resp *pb.GetNodeInfoResponse
	var err error

	for i := 0; i <= c.config.RetryCount; i++ {
		resp, err = client.GetNodeInfo(ctx, req)
		if err == nil {
			break
		}

		// If this is the last retry, return the error
		if i == c.config.RetryCount {
			return nil, fmt.Errorf("failed to get node info from node %d: %w", nodeID, err)
		}

		// Wait before retrying
		time.Sleep(c.config.RetryDelay)
	}

	return resp, nil
}

// CreateSnapshot forces the creation of snapshots on a node
func (c *Client) CreateSnapshot(ctx context.Context, nodeID uint32) (*pb.CreateSnapshotResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		return nil, fmt.Errorf("no client for node %d", nodeID)
	}

	// Create the request
	req := &pb.CreateSnapshotRequest{}

	// Send the request with retries
	var resp *pb.CreateSnapshotResponse
	var err error

	for i := 0; i <= c.config.RetryCount; i++ {
		resp, err = client.CreateSnapshot(ctx, req)
		if err == nil {
			break
		}

		// If this is the last retry, return the error
		if i == c.config.RetryCount {
			return nil, fmt.Errorf("failed to create snapshot on node %d: %w", nodeID, err)
		}

		// Wait before retrying
		time.Sleep(c.config.RetryDelay)
	}

	return resp, nil
}

// Scale changes the number of nodes in the cluster
func (c *Client) Scale(ctx context.Context, nodeID, newClusterSize uint32) (*pb.ScaleResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		return nil, fmt.Errorf("no client for node %d", nodeID)
	}

	// Create the request
	req := &pb.ScaleRequest{
		NewClusterSize: newClusterSize,
	}

	// Send the request with retries
	var resp *pb.ScaleResponse
	var err error

	for i := 0; i <= c.config.RetryCount; i++ {
		resp, err = client.Scale(ctx, req)
		if err == nil {
			break
		}

		// If this is the last retry, return the error
		if i == c.config.RetryCount {
			return nil, fmt.Errorf("failed to scale cluster from node %d: %w", nodeID, err)
		}

		// Wait before retrying
		time.Sleep(c.config.RetryDelay)
	}

	// Update cluster size
	if resp.Success {
		c.clusterSize = resp.NewClusterSize
	}

	return resp, nil
}

// ScaleComplete notifies a node that the scaling operation is complete
func (c *Client) ScaleComplete(ctx context.Context, nodeID uint32) (*pb.ScaleCompleteResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		return nil, fmt.Errorf("no client for node %d", nodeID)
	}

	// Create the request
	req := &pb.ScaleCompleteRequest{}

	// Send the request with retries
	var resp *pb.ScaleCompleteResponse
	var err error

	for i := 0; i <= c.config.RetryCount; i++ {
		resp, err = client.ScaleComplete(ctx, req)
		if err == nil {
			break
		}

		// If this is the last retry, return the error
		if i == c.config.RetryCount {
			return nil, fmt.Errorf("failed to complete scale operation on node %d: %w", nodeID, err)
		}

		// Wait before retrying
		time.Sleep(c.config.RetryDelay)
	}

	return resp, nil
}
