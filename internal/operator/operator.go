package operator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

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

	logger logger.Logger
}

type Opts func(*Client)

// NewClient creates a new KeyDB client
func NewClient(config Config, log logger.Logger, opts ...Opts) (*Client, error) {
	if len(config.Addresses) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	if config.TotalHashRanges == 0 {
		return nil, fmt.Errorf("total hash ranges must be greater than 0")
	}

	if config.RetryCount == 0 {
		return nil, fmt.Errorf("retry count must be greater than 0")
	}

	if config.RetryDelay == 0 {
		return nil, fmt.Errorf("retry delay must be greater than 0")
	}

	client := &Client{
		config:      config,
		connections: make(map[int]*grpc.ClientConn),
		clients:     make(map[int]pb.NodeServiceClient),
		clusterSize: uint32(len(config.Addresses)),
		logger:      log,
	}

	for _, opt := range opts {
		opt(client)
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

func (c *Client) ClusterSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int(c.clusterSize)
}

// GetNodeInfo returns information about a node
func (c *Client) GetNodeInfo(ctx context.Context, nodeID uint32) (*pb.GetNodeInfoResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		// this should never happen unless clusterSize is updated and the c.clients map isn't
		// or if there is a bug in the hashing function
		return nil, fmt.Errorf("no client for node %d", nodeID)
	}

	// Create the request
	req := &pb.GetNodeInfoRequest{NodeId: nodeID}

	// Send the request with retries
	var err error
	var resp *pb.GetNodeInfoResponse
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
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.config.RetryDelay):
		}
	}

	return resp, nil
}

// CreateSnapshots forces the creation of snapshots on a node
// WARNING: This method is meant to be used ONLY by an Operator!!!
func (c *Client) CreateSnapshots(ctx context.Context, nodeID uint32, fullSync bool, hashRanges ...uint32) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		// this should never happen unless clusterSize is updated and the c.clients map isn't
		// or if there is a bug in the hashing function
		return fmt.Errorf("no client for node %d", nodeID)
	}

	req := &pb.CreateSnapshotsRequest{
		HashRange: hashRanges,
		FullSync:  fullSync,
	}

	var err error
	var resp *pb.CreateSnapshotsResponse
	for i := 0; i <= c.config.RetryCount; i++ {
		resp, err = client.CreateSnapshots(ctx, req)
		if err == nil {
			break
		}

		// If this is the last retry, return the error
		if i == c.config.RetryCount {
			return fmt.Errorf("failed to create snapshot on node %d: %w", nodeID, err)
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.config.RetryDelay):
		}
	}

	if !resp.Success {
		if err == nil {
			err = errors.New(resp.ErrorMessage)
		}
		return fmt.Errorf("failed to create snapshot on node %d: %w", nodeID, err)
	}

	return nil
}

// LoadSnapshots forces all nodes to load snapshots from cloud storage
// This method is meant to be used by an Operator process only!
func (c *Client) LoadSnapshots(ctx context.Context, nodeID uint32, hashRanges ...uint32) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		// this should never happen unless clusterSize is updated and the c.clients map isn't
		// or if there is a bug in the hashing function
		return fmt.Errorf("no client for node %d", nodeID)
	}

	req := &pb.LoadSnapshotsRequest{
		HashRange: hashRanges,
	}

	var err error
	var resp *pb.LoadSnapshotsResponse
	for i := 0; i <= c.config.RetryCount; i++ {
		resp, err = client.LoadSnapshots(ctx, req)
		if err == nil && resp != nil && resp.Success {
			break
		}

		// If this is the last retry, return the error
		if i == c.config.RetryCount {
			errMsg := "unknown error"
			if err != nil {
				errMsg = err.Error()
			} else if resp != nil {
				errMsg = resp.ErrorMessage
			}
			return fmt.Errorf("failed to load snapshots on node %d: %s", nodeID, errMsg)
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.config.RetryDelay):
		}
	}

	return nil
}

// Scale changes the number of nodes in the cluster
func (c *Client) Scale(ctx context.Context, nodeIDs []uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(nodeIDs) == 0 {
		return fmt.Errorf("at least one node ID must be provided")
	}

	clients := make([]pb.NodeServiceClient, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		client, ok := c.clients[int(id)]
		if !ok {
			return fmt.Errorf("no client for node %d, update cluster size first", id)
		}
		clients = append(clients, client)
	}

	// Send ScaleRequest to all nodes
	group, ctx := kitsync.NewEagerGroup(ctx, len(clients))
	for nodeID, client := range clients {
		group.Go(func() error {
			req := &pb.ScaleRequest{
				NodesAddresses: c.config.Addresses,
			}

			var err error
			var resp *pb.ScaleResponse
			for i := 0; i <= c.config.RetryCount; i++ {
				resp, err = client.Scale(ctx, req)
				if err == nil && resp != nil && resp.Success {
					break
				}

				// If this is the last retry, save the error
				if i == c.config.RetryCount {
					errMsg := "unknown error"
					if err != nil {
						errMsg = err.Error()
					} else if resp != nil {
						errMsg = resp.ErrorMessage
					}
					return fmt.Errorf("failed to scale node %d: %s", nodeID, errMsg)
				}

				// Wait before retrying
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(c.config.RetryDelay):
				}
			}

			return nil
		})
	}

	err := group.Wait()
	if err != nil {
		return err
	}

	return nil
}

// ScaleComplete notifies a node that the scaling operation is complete
func (c *Client) ScaleComplete(ctx context.Context, nodeIDs []uint32) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(nodeIDs) == 0 {
		return fmt.Errorf("at least one node ID must be provided")
	}

	clients := make([]pb.NodeServiceClient, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		client, ok := c.clients[int(id)]
		if !ok {
			return fmt.Errorf("no client for node %d, update cluster size first", id)
		}
		clients = append(clients, client)
	}

	group, ctx := kitsync.NewEagerGroup(ctx, len(clients))
	for nodeID, client := range clients {
		group.Go(func() error {
			req := &pb.ScaleCompleteRequest{}

			// Send the request with retries
			var err error
			var resp *pb.ScaleCompleteResponse
			for i := 0; i <= c.config.RetryCount; i++ {
				resp, err = client.ScaleComplete(ctx, req)
				if err == nil && resp != nil && resp.Success {
					break
				}

				// If this is the last retry, return the error
				if i == c.config.RetryCount {
					return fmt.Errorf("failed to complete scale operation on node %d: %w", nodeID, err)
				}

				// Wait before retrying
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(c.config.RetryDelay):
				}
			}

			return nil
		})
	}

	return group.Wait()
}

// UpdateClusterData updates the cluster size in a race-condition safe manner.
// It takes a new cluster size and the current keys being processed.
// It returns a slice of keys that need to be fetched again.
func (c *Client) UpdateClusterData(nodesAddresses ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger.Infon("Updating to new cluster size",
		logger.NewIntField("oldClusterSize", int64(c.clusterSize)),
		logger.NewIntField("newClusterSize", int64(uint32(len(nodesAddresses)))),
		logger.NewStringField("nodesAddresses", strings.Join(nodesAddresses, ",")),
	)

	// Close all clients and connections
	for i := range c.connections {
		_ = c.connections[i].Close()
		delete(c.connections, i)
		delete(c.clients, i)
	}

	for i, addr := range nodesAddresses {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "tcp", addr)
			}),
		)
		if err != nil {
			return fmt.Errorf("failed to connect to node %d at %s: %w", i, addr, err)
		}

		c.connections[i] = conn
		c.clients[i] = pb.NewNodeServiceClient(conn)
	}

	c.config.Addresses = nodesAddresses
	c.clusterSize = uint32(len(nodesAddresses))

	return nil
}
