package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

var (
	DefaultRetryCount             = 3
	DefaultRetryDelay             = 1 * time.Second
	DefaultTotalHashRanges uint32 = 128
)

type errClusterSizeChanged struct {
	nodesAddresses []string
}

func (e *errClusterSizeChanged) Error() string { return "cluster size changed" }

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

// NewClient creates a new KeyDB client
func NewClient(config Config, log logger.Logger) (*Client, error) {
	if len(config.Addresses) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	if config.TotalHashRanges == 0 {
		config.TotalHashRanges = DefaultTotalHashRanges
	}

	if config.RetryCount == 0 {
		config.RetryCount = DefaultRetryCount
	}

	if config.RetryDelay == 0 {
		config.RetryDelay = DefaultRetryDelay
	}

	client := &Client{
		config:      config,
		connections: make(map[int]*grpc.ClientConn),
		clients:     make(map[int]pb.NodeServiceClient),
		clusterSize: uint32(len(config.Addresses)),
		logger:      log,
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

// Get retrieves values for multiple keys
func (c *Client) Get(ctx context.Context, keys []string) ([]bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a map to store results
	results := make(map[string]bool)
	resultsMu := sync.Mutex{}

	return c.get(ctx, keys, results, &resultsMu)
}

func (c *Client) get(
	ctx context.Context, keys []string, results map[string]bool, resultsMu *sync.Mutex,
) (
	[]bool, error,
) {
	// Group keys by node
	keysByNode := make(map[uint32][]string)
	for _, key := range keys {
		if _, alreadyFetched := results[key]; alreadyFetched {
			continue
		}
		_, nodeID := hash.GetNodeNumber(key, c.clusterSize, c.config.TotalHashRanges)
		keysByNode[nodeID] = append(keysByNode[nodeID], key)
	}

	hasClusterSizeChanged := atomic.Value{}

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, gCtx := kitsync.NewEagerGroup(cancellableCtx, len(keysByNode))

	for nodeID, nodeKeys := range keysByNode {
		group.Go(func() error {
			// Get the client for this node
			client, ok := c.clients[int(nodeID)]
			if !ok {
				// this should never happen unless clusterSize is updated and the c.clients map isn't
				// or if there is a bug in the hashing function
				cancel()
				return fmt.Errorf("no client for node %d", nodeID)
			}

			// Create the request
			req := &pb.GetRequest{Keys: nodeKeys}

			// Send the request with retries
			var err error
			var resp *pb.GetResponse

			for i := 0; i <= c.config.RetryCount; i++ {
				resp, err = client.Get(gCtx, req)
				if err == nil && resp != nil {
					if resp.ErrorCode == pb.ErrorCode_NO_ERROR {
						break // for internal errors, scaling or wrong code we retry
					}
				}

				// TODO add some logging and metrics here

				if resp != nil && c.clusterSize != resp.ClusterSize {
					hasClusterSizeChanged.Store(resp.NodesAddresses)
					cancel()
					return &errClusterSizeChanged{nodesAddresses: resp.NodesAddresses}
				}

				// If this is the last retry, return the error
				if i == c.config.RetryCount {
					cancel()
					return fmt.Errorf("failed to get keys from node %d: no retries left", nodeID)
				}

				// Wait before retrying
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case <-time.After(c.config.RetryDelay):
				}
			}

			if c.clusterSize != resp.ClusterSize {
				hasClusterSizeChanged.Store(resp.NodesAddresses)
				cancel()
				return &errClusterSizeChanged{nodesAddresses: resp.NodesAddresses}
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
		nodesAddresses, ok := hasClusterSizeChanged.Load().([]string)
		if ok && len(nodesAddresses) > 0 {
			if err = c.updateClusterSize(nodesAddresses); err != nil {
				return nil, fmt.Errorf("failed to update cluster size: %w", err)
			}
			return c.get(ctx, keys, results, resultsMu)
		}
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
func (c *Client) Put(ctx context.Context, keys []string, ttl time.Duration) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.put(ctx, keys, ttl)
}

func (c *Client) put(ctx context.Context, keys []string, ttl time.Duration) error {
	// Group items by node
	itemsByNode := make(map[uint32][]string)
	for _, key := range keys {
		_, nodeID := hash.GetNodeNumber(key, c.clusterSize, c.config.TotalHashRanges)
		itemsByNode[nodeID] = append(itemsByNode[nodeID], key)
	}

	hasClusterSizeChanged := atomic.Value{}

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, gCtx := kitsync.NewEagerGroup(cancellableCtx, len(itemsByNode))
	for nodeID, nodeItems := range itemsByNode {
		group.Go(func() error {
			// Get the client for this node
			client, ok := c.clients[int(nodeID)]
			if !ok {
				// this should never happen unless clusterSize is updated and the c.clients map isn't
				// or if there is a bug in the hashing function
				cancel()
				return fmt.Errorf("no client for node %d", nodeID)
			}

			// Create the request
			req := &pb.PutRequest{Keys: nodeItems, TtlSeconds: uint64(ttl.Seconds())}

			// Send the request with retries
			var err error
			var resp *pb.PutResponse
			for i := 0; i <= c.config.RetryCount; i++ {
				resp, err = client.Put(gCtx, req)
				if err == nil && resp != nil {
					if resp.Success && resp.ErrorCode == pb.ErrorCode_NO_ERROR {
						break // for internal errors, scaling or wrong code we retry
					}
				}

				// TODO add some logging and metrics here

				if resp != nil && c.clusterSize != resp.ClusterSize {
					hasClusterSizeChanged.Store(resp.NodesAddresses)
					cancel()
					return &errClusterSizeChanged{nodesAddresses: resp.NodesAddresses}
				}

				// If this is the last retry, return the error
				if i == c.config.RetryCount {
					cancel()
					return fmt.Errorf("failed to get keys from node %d: no retries left", nodeID)
				}

				// Wait before retrying
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case <-time.After(c.config.RetryDelay):
				}
			}

			if c.clusterSize != resp.ClusterSize {
				hasClusterSizeChanged.Store(resp.NodesAddresses)
				cancel()
				return &errClusterSizeChanged{nodesAddresses: resp.NodesAddresses}
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		nodesAddresses, ok := hasClusterSizeChanged.Load().([]string)
		if ok && len(nodesAddresses) > 0 {
			if err = c.updateClusterSize(nodesAddresses); err != nil {
				return fmt.Errorf("failed to update cluster size: %w", err)
			}
			return c.put(ctx, keys, ttl)
		}
		return err
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

	if c.clusterSize != resp.ClusterSize {
		if err = c.updateClusterSize(resp.NodesAddresses); err != nil {
			return nil, fmt.Errorf("failed to update cluster size: %w", err)
		}
	}

	return resp, nil
}

// CreateSnapshots forces the creation of snapshots on a node
// This method is meant to be used by an Operator process only!
func (c *Client) CreateSnapshots(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group, ctx := kitsync.NewEagerGroup(ctx, len(c.clients))
	for nodeID, client := range c.clients {
		group.Go(func() error {
			req := &pb.CreateSnapshotsRequest{}

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
				return fmt.Errorf("failed to create snapshot on node %d: %w", nodeID, err)
			}

			return nil
		})
	}

	return group.Wait()
}

// LoadSnapshots forces all nodes to load snapshots from cloud storage
// This method is meant to be used by an Operator process only!
func (c *Client) LoadSnapshots(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group, ctx := kitsync.NewEagerGroup(ctx, len(c.clients))
	for nodeID, client := range c.clients {
		group.Go(func() error {
			req := &pb.LoadSnapshotsRequest{}

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
		})
	}

	return group.Wait()
}

// Scale changes the number of nodes in the cluster
// This method is meant to be used by an Operator process only!
func (c *Client) Scale(ctx context.Context, addresses ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	newClusterSize := uint32(len(addresses))
	if newClusterSize == c.clusterSize {
		return nil // No change needed
	}

	// Handle case when newClusterSize is bigger
	if newClusterSize > c.clusterSize {
		// Establish new connections to the new nodes
		for i := int(c.clusterSize); i < int(newClusterSize); i++ {
			addr := addresses[i]
			conn, err := grpc.NewClient(addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					var dialer net.Dialer
					return dialer.DialContext(ctx, "tcp", addr)
				}),
			)
			if err != nil {
				// Close any new connections we've made so far
				for j := int(c.clusterSize); j < i; j++ {
					if conn, ok := c.connections[j]; ok {
						_ = conn.Close()
						delete(c.connections, j)
						delete(c.clients, j)
					}
				}
				return fmt.Errorf("failed to connect to node %d at %s: %w", i, addr, err)
			}

			c.connections[i] = conn
			c.clients[i] = pb.NewNodeServiceClient(conn)
		}
	} else if newClusterSize < c.clusterSize {
		// Handle case when newClusterSize is smaller
		// Close unnecessary connections
		for i := int(newClusterSize); i < int(c.clusterSize); i++ {
			if conn, ok := c.connections[i]; ok {
				_ = conn.Close() // Ignore errors during close
				delete(c.connections, i)
				delete(c.clients, i)
			}
		}
	}

	// Send ScaleRequest to all nodes
	group, ctx := kitsync.NewEagerGroup(ctx, len(c.clients))
	for nodeID, client := range c.clients {
		group.Go(func() error {
			req := &pb.ScaleRequest{
				NewClusterSize: newClusterSize,
				NodesAddresses: addresses,
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

	c.config.Addresses = addresses
	c.clusterSize = newClusterSize

	return nil
}

// ScaleComplete notifies a node that the scaling operation is complete
// This method is meant to be used by an Operator process only!
func (c *Client) ScaleComplete(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group, ctx := kitsync.NewEagerGroup(ctx, len(c.clients))
	for nodeID, client := range c.clients {
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

// updateClusterSize updates the cluster size in a race-condition safe manner.
// It takes a new cluster size and the current keys being processed.
// It returns a slice of keys that need to be fetched again.
func (c *Client) updateClusterSize(nodesAddresses []string) error {
	// Release read lock and acquire write lock
	c.mu.RUnlock()
	c.mu.Lock()
	defer func() {
		// Release write lock and reacquire read lock
		c.mu.Unlock()
		c.mu.RLock()
	}()

	newClusterSize := uint32(len(nodesAddresses))

	// Check if cluster size has already been updated by someone else
	if c.clusterSize == newClusterSize {
		return nil // No need to update or refetch
	}

	c.logger.Infon("Detected new cluster size",
		logger.NewIntField("oldClusterSize", int64(c.clusterSize)),
		logger.NewIntField("newClusterSize", int64(newClusterSize)),
		logger.NewStringField("nodesAddresses", fmt.Sprintf("%+v", nodesAddresses)),
	)

	c.config.Addresses = nodesAddresses
	oldClusterSize := c.clusterSize
	c.clusterSize = newClusterSize

	// If cluster is smaller, close connections that are not needed
	if newClusterSize < oldClusterSize {
		for i := int(newClusterSize); i < int(oldClusterSize); i++ {
			if conn, ok := c.connections[i]; ok {
				_ = conn.Close() // Ignore errors during close
				delete(c.connections, i)
				delete(c.clients, i)
			}
		}
	} else { // we get here if newClusterSize > oldClusterSize
		// The cluster is bigger, create new connections
		// But we can only do this if we have addresses for the new nodes
		for i := int(oldClusterSize); i < int(newClusterSize); i++ {
			addr := nodesAddresses[i]
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
	}

	return nil
}
