package scaler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	DefaultRetryPolicyInitialInterval = 1 * time.Second
	DefaultRetryPolicyMultiplier      = 1.5
	DefaultRetryPolicyMaxInterval     = 30 * time.Second
	DefaultMaxElapsedTime             = 10 * time.Minute
)

// RetryPolicy defines the retry policy configuration
type RetryPolicy struct {
	Disabled        bool          `json:"disabled"`
	InitialInterval time.Duration `json:"initial_interval"`
	Multiplier      float64       `json:"multiplier"`
	MaxInterval     time.Duration `json:"max_interval"`
	MaxElapsedTime  time.Duration `json:"max_elapsed_time"`
}

// Config holds the configuration for a client
type Config struct {
	// Addresses is a list of node addresses (host:port)
	Addresses []string

	// TotalHashRanges is the total number of hash ranges
	TotalHashRanges uint32

	// RetryPolicy defines the retry behavior for failed requests
	RetryPolicy RetryPolicy
}

type ScalingOperationType string

const (
	ScaleUp     ScalingOperationType = "scale_up"
	ScaleDown   ScalingOperationType = "scale_down"
	AutoHealing ScalingOperationType = "auto_healing"
)

type ScalingOperationStatus string

const (
	InProgress ScalingOperationStatus = "in_progress"
	Completed  ScalingOperationStatus = "completed"
	Failed     ScalingOperationStatus = "failed"
	RolledBack ScalingOperationStatus = "rolled_back"
)

// ScalingOperation represents the last scaling operation that can be rolled back
type ScalingOperation struct {
	Type           ScalingOperationType   `json:"type"`
	OldClusterSize uint32                 `json:"old_cluster_size"`
	NewClusterSize uint32                 `json:"new_cluster_size"`
	OldAddresses   []string               `json:"old_addresses"`
	NewAddresses   []string               `json:"new_addresses"`
	Status         ScalingOperationStatus `json:"status"` // "in_progress", "completed", "failed", "rolled_back"
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

	// lastOperation tracks the last scaling operation for potential rollback
	lastOperation *ScalingOperation

	// mu protects connections, clients, clusterSize and lastOperation
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

	// Set default retry policy values if not specified
	if config.RetryPolicy.InitialInterval == 0 {
		config.RetryPolicy.InitialInterval = DefaultRetryPolicyInitialInterval
	}
	if config.RetryPolicy.Multiplier == 0 {
		config.RetryPolicy.Multiplier = DefaultRetryPolicyMultiplier
	}
	if config.RetryPolicy.MaxInterval == 0 {
		config.RetryPolicy.MaxInterval = DefaultRetryPolicyMaxInterval
	}
	if config.RetryPolicy.MaxElapsedTime == 0 {
		config.RetryPolicy.MaxElapsedTime = DefaultMaxElapsedTime
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

func (c *Client) TotalHashRanges() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.TotalHashRanges
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
	var (
		err         error
		resp        *pb.GetNodeInfoResponse
		nextBackoff = c.getNextBackoffFunc()
	)
	for attempt := int64(1); ; attempt++ {
		resp, err = client.GetNodeInfo(ctx, req)
		if err == nil {
			break
		}

		retryDelay := nextBackoff()
		if c.config.RetryPolicy.Disabled || retryDelay == backoff.Stop {
			return nil, fmt.Errorf("failed to get node info from node %d: %w", nodeID, err)
		}

		c.logger.Warnn("Cannot get node info",
			logger.NewIntField("nodeID", int64(nodeID)),
			logger.NewIntField("attempt", attempt),
			logger.NewDurationField("retryDelay", retryDelay),
			obskit.Error(err))

		// Wait before retrying
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryDelay):
		}
	}

	return resp, nil
}

// CreateSnapshots forces the creation of snapshots on a node
// WARNING: This method is meant to be used ONLY by a Scaler!!!
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

	var (
		err         error
		resp        *pb.CreateSnapshotsResponse
		nextBackoff = c.getNextBackoffFunc()
	)
	for attempt := int64(1); ; attempt++ {
		resp, err = client.CreateSnapshots(ctx, req)
		if err == nil && resp != nil && resp.Success {
			break
		}

		retryDelay := nextBackoff()
		if c.config.RetryPolicy.Disabled || retryDelay == backoff.Stop {
			if err != nil {
				return fmt.Errorf("failed to create snapshot on node %d: %w", nodeID, err)
			}
			if resp != nil {
				return fmt.Errorf("failed to create snapshot on node %d: %s", nodeID, resp.ErrorMessage)
			}
			return fmt.Errorf("cannot create snapshots on node %d: both error and response are nil", nodeID)
		}

		if err != nil {
			c.logger.Warnn("Cannot create snapshots",
				logger.NewIntField("nodeID", int64(nodeID)),
				logger.NewIntField("attempt", attempt),
				logger.NewDurationField("retryDelay", retryDelay),
				obskit.Error(err))
		} else if resp != nil {
			c.logger.Warnn("Create snapshots unsuccessful",
				logger.NewIntField("nodeID", int64(nodeID)),
				logger.NewIntField("attempt", attempt),
				logger.NewBoolField("success", resp.Success),
				logger.NewDurationField("retryDelay", retryDelay),
				obskit.Error(errors.New(resp.ErrorMessage)))
		} else {
			return fmt.Errorf("cannot create snapshots on node %d: both error and response are nil", nodeID)
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}
	}

	return nil
}

// LoadSnapshots forces all nodes to load snapshots from cloud storage
// This method is meant to be used by a Scaler process only!
func (c *Client) LoadSnapshots(ctx context.Context, nodeID, maxConcurrency uint32, hashRanges ...uint32) error {
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
		HashRange:      hashRanges,
		MaxConcurrency: maxConcurrency,
	}

	var (
		err         error
		resp        *pb.LoadSnapshotsResponse
		nextBackoff = c.getNextBackoffFunc()
	)
	for attempt := int64(1); ; attempt++ {
		resp, err = client.LoadSnapshots(ctx, req)
		if err == nil && resp != nil && resp.Success {
			break
		}

		retryDelay := nextBackoff()
		if c.config.RetryPolicy.Disabled || retryDelay == backoff.Stop {
			if err != nil {
				return fmt.Errorf("failed to load snapshots on node %d: %w", nodeID, err)
			}
			if resp != nil {
				return fmt.Errorf("failed to load snapshots on node %d: %s", nodeID, resp.ErrorMessage)
			}
			return fmt.Errorf("cannot load snapshots on node %d: both error and response are nil", nodeID)
		}

		if err != nil {
			c.logger.Warnn("Cannot load snapshots",
				logger.NewIntField("nodeID", int64(nodeID)),
				logger.NewIntField("attempt", attempt),
				logger.NewDurationField("retryDelay", retryDelay),
				obskit.Error(err))
		} else if resp != nil {
			c.logger.Warnn("Load snapshots unsuccessful",
				logger.NewIntField("nodeID", int64(nodeID)),
				logger.NewIntField("attempt", attempt),
				logger.NewBoolField("success", resp.Success),
				logger.NewDurationField("retryDelay", retryDelay),
				obskit.Error(errors.New(resp.ErrorMessage)))
		} else {
			return fmt.Errorf("cannot load snapshots on node %d: both error and response are nil", nodeID)
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
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

			var (
				err         error
				resp        *pb.ScaleResponse
				nextBackoff = c.getNextBackoffFunc()
			)
			for attempt := int64(1); ; attempt++ {
				resp, err = client.Scale(ctx, req)
				if err == nil && resp != nil && resp.Success {
					break
				}

				if err == nil {
					if resp != nil {
						err = errors.New(resp.ErrorMessage)
					} else {
						err = errors.New("unknown error")
					}
				}

				retryDelay := nextBackoff()
				if c.config.RetryPolicy.Disabled || retryDelay == backoff.Stop {
					return fmt.Errorf("failed to scale node %d: %w", nodeID, err)
				}

				c.logger.Warnn("Cannot scale node",
					logger.NewIntField("nodeID", int64(nodeID)),
					logger.NewIntField("attempt", attempt),
					logger.NewDurationField("retryDelay", retryDelay),
					obskit.Error(err))

				// Wait before retrying
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retryDelay):
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
			var (
				err         error
				resp        *pb.ScaleCompleteResponse
				nextBackoff = c.getNextBackoffFunc()
			)
			for attempt := int64(1); ; attempt++ {
				resp, err = client.ScaleComplete(ctx, req)
				if err == nil && resp != nil && resp.Success {
					break
				}

				if err == nil {
					if resp != nil {
						err = errors.New("unsuccessful response from nodes")
					} else {
						err = errors.New("unknown error")
					}
				}

				retryDelay := nextBackoff()
				if c.config.RetryPolicy.Disabled || retryDelay == backoff.Stop {
					return fmt.Errorf("failed to complete scale on node %d: %w", nodeID, err)
				}

				c.logger.Warnn("Cannot complete scale operation",
					logger.NewIntField("nodeID", int64(nodeID)),
					logger.NewIntField("attempt", attempt),
					logger.NewDurationField("retryDelay", retryDelay),
					obskit.Error(err))

				// Wait before retrying
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retryDelay):
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

// RecordOperation records the last scaling operation for potential rollback
func (c *Client) RecordOperation(
	opType ScalingOperationType,
	oldClusterSize, newClusterSize uint32,
	oldAddresses, newAddresses []string,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastOperation = &ScalingOperation{
		Type:           opType,
		OldClusterSize: oldClusterSize,
		NewClusterSize: newClusterSize,
		OldAddresses:   append([]string{}, oldAddresses...),
		NewAddresses:   append([]string{}, newAddresses...),
		Status:         InProgress,
	}
}

// UpdateOperationStatus updates the status of the last scaling operation
func (c *Client) UpdateOperationStatus(status ScalingOperationStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lastOperation != nil {
		c.lastOperation.Status = status
	}
}

// GetLastOperation retrieves the last scaling operation
func (c *Client) GetLastOperation() *ScalingOperation {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastOperation
}

// ExecuteScalingWithRollback executes a scaling function with automatic rollback on failure
//
// This function records the scaling operation details, executes the provided scaling function,
// and automatically performs a rollback if the function returns an error. It ensures that
// the cluster state remains consistent even when scaling operations fail.
//
// The background context is used for rollback operations to ensure that even if the original
// request context is cancelled (e.g., client disconnects), the rollback can still complete
// and leave the cluster in a consistent state.
//
// Parameters:
//   - opType: The type of scaling operation (ScaleUp, ScaleDown, AutoHealing)
//   - oldAddresses: The node addresses before the operation
//   - newAddresses: The node addresses after the operation
//   - fn: The function that performs the actual scaling operation
//
// Returns:
//   - error: If the operation or rollback fails, or nil if successful
func (c *Client) ExecuteScalingWithRollback(opType ScalingOperationType,
	oldAddresses, newAddresses []string, fn func() error,
) error {
	// Record the operation
	c.RecordOperation(opType, uint32(len(oldAddresses)), uint32(len(newAddresses)), oldAddresses, newAddresses)

	// Execute the scaling function
	err := fn()
	if err != nil {
		c.UpdateOperationStatus(Failed)
		c.logger.Warnn("Scaling operation failed, initiating rollback",
			logger.NewStringField("operationType", string(opType)),
			obskit.Error(err))

		// Attempt rollback
		// background one is needed so that if a client disconnects or cancels the request
		// then we won't leave the cluster in a bad state
		rollbackErr := c.rollbackToOldConfiguration(context.Background(), c.GetLastOperation())
		if rollbackErr != nil {
			c.logger.Errorn("Rollback failed",
				logger.NewStringField("operationType", string(opType)),
				obskit.Error(rollbackErr))
			return fmt.Errorf("scaling failed: %v, rollback failed: %w", err, rollbackErr)
		}

		c.UpdateOperationStatus(RolledBack)
		c.logger.Infon("Scaling operation rolled back successfully",
			logger.NewStringField("operationType", string(opType)))
		return fmt.Errorf("scaling failed and rolled back: %w", err)
	}

	c.UpdateOperationStatus(Completed)
	c.logger.Infon("Scaling operation completed successfully",
		logger.NewStringField("operationType", string(opType)))
	return nil
}

// rollbackToOldConfiguration restores the cluster to its previous configuration
func (c *Client) rollbackToOldConfiguration(ctx context.Context, operation *ScalingOperation) error {
	if operation == nil {
		return fmt.Errorf("no operation provided for rollback")
	}

	c.logger.Infon("Rolling back operation",
		logger.NewStringField("operationType", string(operation.Type)))

	// Restore cluster to old configuration
	if err := c.UpdateClusterData(operation.OldAddresses...); err != nil {
		return fmt.Errorf("failed to restore cluster data: %w", err)
	}

	// Scale back to old cluster size
	oldNodeIDs := make([]uint32, operation.OldClusterSize)
	for i := uint32(0); i < operation.OldClusterSize; i++ {
		oldNodeIDs[i] = i
	}

	// stop any ongoing scaling
	if err := c.ScaleComplete(ctx, oldNodeIDs); err != nil {
		return fmt.Errorf("failed to complete scale back: %w", err)
	}

	// restore correct behavior
	if err := c.Scale(ctx, oldNodeIDs); err != nil {
		return fmt.Errorf("failed to scale back nodes: %w", err)
	}

	// notify all nodes that the scaling operation is complete
	if err := c.ScaleComplete(ctx, oldNodeIDs); err != nil {
		return fmt.Errorf("failed to complete scale back: %w", err)
	}

	return nil
}

func (c *Client) getNextBackoffFunc() func() time.Duration {
	bo := backoff.NewExponentialBackOff()
	bo.Multiplier = c.config.RetryPolicy.Multiplier
	bo.InitialInterval = c.config.RetryPolicy.InitialInterval
	bo.MaxInterval = c.config.RetryPolicy.MaxInterval

	start := time.Now()
	return func() time.Duration {
		if c.config.RetryPolicy.MaxElapsedTime > 0 && time.Since(start) > c.config.RetryPolicy.MaxElapsedTime {
			return backoff.Stop
		}
		return bo.NextBackOff()
	}
}
