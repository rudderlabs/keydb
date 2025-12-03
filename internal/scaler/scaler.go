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
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

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

	DefaultGrpcKeepAliveTime    = 10 * time.Second
	DefaultGrpcKeepAliveTimeout = 2 * time.Second

	DefaultGrpcBackoffBaseDelay  = 1 * time.Second
	DefaultGrpcBackoffMultiplier = 1.6
	DefaultGrpcBackoffJitter     = 0.2
	DefaultGrpcMaxDelay          = 2 * time.Minute
	DefaultGrpcMinConnectTimeout = 20 * time.Second

	DefaultClusterUpdateTimeout           = 10 * time.Second
	DefaultClusterUpdateConnCheckInterval = 50 * time.Millisecond
)

// RetryPolicy defines the retry policy configuration
type RetryPolicy struct {
	Disabled        bool          `json:"disabled"`
	InitialInterval time.Duration `json:"initial_interval"`
	Multiplier      float64       `json:"multiplier"`
	MaxInterval     time.Duration `json:"max_interval"`
	MaxElapsedTime  time.Duration `json:"max_elapsed_time"`
}

// GrpcConfig holds gRPC connection configuration
type GrpcConfig struct {
	// KeepAliveTime is the time after which a ping will be sent on the transport
	KeepAliveTime time.Duration `json:"keep_alive_time"`
	// KeepAliveTimeout is the time the client waits for a response to the keepalive ping
	KeepAliveTimeout time.Duration `json:"keep_alive_timeout"`
	// DisableKeepAlivePermitWithoutStream disables keepalive pings even when there are no active streams
	DisableKeepAlivePermitWithoutStream bool `json:"disable_keep_alive_permit_without_stream"`

	// BackoffBaseDelay is the initial backoff delay for connection attempts
	BackoffBaseDelay time.Duration `json:"backoff_base_delay"`
	// BackoffMultiplier is the multiplier for exponential backoff
	BackoffMultiplier float64 `json:"backoff_multiplier"`
	// BackoffJitter adds randomness to backoff delays
	BackoffJitter float64 `json:"backoff_jitter"`
	// BackoffMaxDelay is the maximum backoff delay
	BackoffMaxDelay time.Duration `json:"backoff_max_delay"`
	// MinConnectTimeout is the minimum timeout for connection attempts
	MinConnectTimeout time.Duration `json:"min_connect_timeout"`
}

// Config holds the configuration for a client
type Config struct {
	// Addresses is a list of node addresses (host:port)
	Addresses []string

	// TotalHashRanges is the total number of hash ranges
	TotalHashRanges int64

	// RetryPolicy defines the retry behavior for failed requests
	RetryPolicy RetryPolicy

	// GrpcConfig defines the gRPC connection configuration
	GrpcConfig GrpcConfig

	// ClusterUpdateTimeout is the timeout for UpdateClusterData operations
	ClusterUpdateTimeout time.Duration

	// ClusterUpdateConnCheckInterval is the interval for checking node connectivity during cluster updates
	ClusterUpdateConnCheckInterval time.Duration
}

// Client is a client for the KeyDB service
type Client struct {
	config Config

	// clusterSize is the number of nodes in the cluster
	clusterSize int64

	// connections is a map of node index to connection
	connections map[int]*grpc.ClientConn

	// clients is a map of node index to client
	clients map[int]pb.NodeServiceClient

	// mu protects connections, clients, and clusterSize
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

	// Set gRPC config defaults
	if config.GrpcConfig.KeepAliveTime == 0 {
		config.GrpcConfig.KeepAliveTime = DefaultGrpcKeepAliveTime
	}
	if config.GrpcConfig.KeepAliveTimeout == 0 {
		config.GrpcConfig.KeepAliveTimeout = DefaultGrpcKeepAliveTimeout
	}
	if config.GrpcConfig.BackoffBaseDelay == 0 {
		config.GrpcConfig.BackoffBaseDelay = DefaultGrpcBackoffBaseDelay
	}
	if config.GrpcConfig.BackoffMultiplier == 0 {
		config.GrpcConfig.BackoffMultiplier = DefaultGrpcBackoffMultiplier
	}
	if config.GrpcConfig.BackoffJitter == 0 {
		config.GrpcConfig.BackoffJitter = DefaultGrpcBackoffJitter
	}
	if config.GrpcConfig.BackoffMaxDelay == 0 {
		config.GrpcConfig.BackoffMaxDelay = DefaultGrpcMaxDelay
	}
	if config.GrpcConfig.MinConnectTimeout == 0 {
		config.GrpcConfig.MinConnectTimeout = DefaultGrpcMinConnectTimeout
	}
	if config.ClusterUpdateTimeout == 0 {
		config.ClusterUpdateTimeout = DefaultClusterUpdateTimeout
	}
	if config.ClusterUpdateConnCheckInterval == 0 {
		config.ClusterUpdateConnCheckInterval = DefaultClusterUpdateConnCheckInterval
	}

	client := &Client{
		config:      config,
		connections: make(map[int]*grpc.ClientConn),
		clients:     make(map[int]pb.NodeServiceClient),
		clusterSize: int64(len(config.Addresses)),
		logger:      log,
	}

	for _, opt := range opts {
		opt(client)
	}

	// Connect to all nodes
	for i, addr := range config.Addresses {
		conn, err := client.createConnection(addr)
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

func (c *Client) TotalHashRanges() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.TotalHashRanges
}

// GetNodeInfo returns information about a node
func (c *Client) GetNodeInfo(ctx context.Context, nodeID int64) (*pb.GetNodeInfoResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		// this should never happen unless clusterSize is updated and the c.clients map isn't
		// or if there is a bug in the hashing function
		return nil, fmt.Errorf("no client for node %d", nodeID)
	}
	conn, ok := c.connections[int(nodeID)]
	if !ok {
		return nil, fmt.Errorf("no connection for node %d", nodeID)
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
			logger.NewIntField("nodeID", nodeID),
			logger.NewIntField("attempt", attempt),
			logger.NewDurationField("retryDelay", retryDelay),
			logger.NewStringField("canonicalTarget", conn.CanonicalTarget()),
			logger.NewStringField("connState", conn.GetState().String()),
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
func (c *Client) CreateSnapshots(ctx context.Context, nodeID int64, fullSync bool, hashRanges ...int64) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		// this should never happen unless clusterSize is updated and the c.clients map isn't
		// or if there is a bug in the hashing function
		return fmt.Errorf("no client for node %d", nodeID)
	}
	conn, ok := c.connections[int(nodeID)]
	if !ok {
		return fmt.Errorf("no connection for node %d", nodeID)
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
				logger.NewIntField("nodeID", nodeID),
				logger.NewIntField("attempt", attempt),
				logger.NewDurationField("retryDelay", retryDelay),
				logger.NewStringField("canonicalTarget", conn.CanonicalTarget()),
				logger.NewStringField("connState", conn.GetState().String()),
				obskit.Error(err))
		} else if resp != nil {
			c.logger.Warnn("Create snapshots unsuccessful",
				logger.NewIntField("nodeID", nodeID),
				logger.NewIntField("attempt", attempt),
				logger.NewBoolField("success", resp.Success),
				logger.NewDurationField("retryDelay", retryDelay),
				logger.NewStringField("canonicalTarget", conn.CanonicalTarget()),
				logger.NewStringField("connState", conn.GetState().String()),
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
func (c *Client) LoadSnapshots(ctx context.Context, nodeID, maxConcurrency int64, hashRanges ...int64) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for this node
	client, ok := c.clients[int(nodeID)]
	if !ok {
		// this should never happen unless clusterSize is updated and the c.clients map isn't
		// or if there is a bug in the hashing function
		return fmt.Errorf("no client for node %d", nodeID)
	}
	conn, ok := c.connections[int(nodeID)]
	if !ok {
		return fmt.Errorf("no connection for node %d", nodeID)
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
				logger.NewIntField("nodeID", nodeID),
				logger.NewIntField("attempt", attempt),
				logger.NewDurationField("retryDelay", retryDelay),
				logger.NewStringField("canonicalTarget", conn.CanonicalTarget()),
				logger.NewStringField("connState", conn.GetState().String()),
				obskit.Error(err))
		} else if resp != nil {
			c.logger.Warnn("Load snapshots unsuccessful",
				logger.NewIntField("nodeID", nodeID),
				logger.NewIntField("attempt", attempt),
				logger.NewBoolField("success", resp.Success),
				logger.NewDurationField("retryDelay", retryDelay),
				logger.NewStringField("canonicalTarget", conn.CanonicalTarget()),
				logger.NewStringField("connState", conn.GetState().String()),
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

// SendSnapshot instructs a node to stream a hash range to a destination node.
// This method is meant to be used by a Scaler process only!
func (c *Client) SendSnapshot(
	ctx context.Context, sourceNodeID int64, destinationAddress string, hashRange int64,
) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get the client for the source node
	client, ok := c.clients[int(sourceNodeID)]
	if !ok {
		return fmt.Errorf("no client for node %d", sourceNodeID)
	}
	conn, ok := c.connections[int(sourceNodeID)]
	if !ok {
		return fmt.Errorf("no connection for node %d", sourceNodeID)
	}

	req := &pb.SendSnapshotRequest{
		HashRange:          hashRange,
		DestinationAddress: destinationAddress,
	}

	var (
		err         error
		resp        *pb.SendSnapshotResponse
		nextBackoff = c.getNextBackoffFunc()
	)
	for attempt := int64(1); ; attempt++ {
		resp, err = client.SendSnapshot(ctx, req)
		if err == nil && resp != nil && resp.Success {
			break
		}

		retryDelay := nextBackoff()
		if c.config.RetryPolicy.Disabled || retryDelay == backoff.Stop {
			if err != nil {
				return fmt.Errorf("sending snapshot from node %d to %s: %w", sourceNodeID, destinationAddress, err)
			}
			if resp != nil {
				return fmt.Errorf("sending snapshot from node %d to %s: %s",
					sourceNodeID, destinationAddress, resp.ErrorMessage,
				)
			}
			return fmt.Errorf("sending snapshot from node %d to %s: both error and response are nil",
				sourceNodeID, destinationAddress,
			)
		}

		loggerFields := func(fields ...logger.Field) []logger.Field {
			return append(fields,
				logger.NewIntField("sourceNodeID", sourceNodeID),
				logger.NewStringField("destinationAddress", destinationAddress),
				logger.NewIntField("hashRange", hashRange),
				logger.NewIntField("attempt", attempt),
				logger.NewDurationField("retryDelay", retryDelay),
				logger.NewStringField("canonicalTarget", conn.CanonicalTarget()),
				logger.NewStringField("connState", conn.GetState().String()),
			)
		}
		if err != nil {
			c.logger.Warnn("Cannot send snapshot", loggerFields(obskit.Error(err))...)
		} else if resp != nil {
			c.logger.Warnn("Send snapshot unsuccessful", loggerFields(
				logger.NewBoolField("success", resp.Success),
				obskit.Error(errors.New(resp.ErrorMessage)),
			)...)
		} else {
			return fmt.Errorf("sending snapshot from node %d to %s: both error and response are nil",
				sourceNodeID, destinationAddress,
			)
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

// GetNodeAddress returns the address of a node by its ID.
func (c *Client) GetNodeAddress(nodeID int64) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if nodeID < 0 || int(nodeID) >= len(c.config.Addresses) {
		return "", fmt.Errorf("node ID %d out of range (0-%d)", nodeID, len(c.config.Addresses)-1)
	}
	return c.config.Addresses[nodeID], nil
}

// UpdateClusterData updates the cluster size in a race-condition safe manner.
// It takes a new cluster size and the current keys being processed.
// It returns a slice of keys that need to be fetched again.
func (c *Client) UpdateClusterData(ctx context.Context, nodesAddresses ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger.Infon("Updating to new cluster size",
		logger.NewIntField("oldClusterSize", c.clusterSize),
		logger.NewIntField("newClusterSize", int64(len(nodesAddresses))),
		logger.NewStringField("nodesAddresses", strings.Join(nodesAddresses, ",")),
	)

	// Close all clients and connections
	for i := range c.connections {
		_ = c.connections[i].Close()
		delete(c.connections, i)
		delete(c.clients, i)
	}

	for i, addr := range nodesAddresses {
		conn, err := c.createConnection(addr)
		if err != nil {
			return fmt.Errorf("failed to connect to node %d at %s: %w", i, addr, err)
		}

		c.connections[i] = conn
		c.clients[i] = pb.NewNodeServiceClient(conn)
	}

	c.config.Addresses = nodesAddresses
	c.clusterSize = int64(len(nodesAddresses))

	ctx, cancel := context.WithTimeout(ctx, c.config.ClusterUpdateTimeout)
	defer cancel()

	group, gCtx := kitsync.NewEagerGroup(ctx, len(nodesAddresses))
	for i := range c.connections {
		group.Go(func() error {
			// use GetNodeInfo to force the clients to connect
			_, err := c.clients[i].GetNodeInfo(gCtx, &pb.GetNodeInfoRequest{NodeId: int64(i)})
			// we use the error only to return the routines early
			// the cluster update will be successful as long as there is connectivity
			return err
		})
	}

	// Checking connectivity since the gRPC client has its own backoff policy, the above group might not return on time
	// to honour the context within the given timeout.
	ticker := time.NewTicker(c.config.ClusterUpdateConnCheckInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ok := true
			for i, conn := range c.connections {
				if conn.GetState() != connectivity.Ready {
					ok = false
					c.logger.Warnn("Detected connectivity issue",
						logger.NewIntField("nodeId", int64(i)),
						logger.NewStringField("addr", conn.CanonicalTarget()),
						logger.NewStringField("connState", conn.GetState().String()),
					)
				}
			}
			if ok {
				return nil
			}
		}
	}
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

// createConnection creates a gRPC connection with proper keepalive and retry configuration
func (c *Client) createConnection(addr string) (*grpc.ClientConn, error) {
	// Configure keepalive parameters to detect dead connections
	kacp := keepalive.ClientParameters{
		Time:                c.config.GrpcConfig.KeepAliveTime,
		Timeout:             c.config.GrpcConfig.KeepAliveTimeout,
		PermitWithoutStream: !c.config.GrpcConfig.DisableKeepAlivePermitWithoutStream,
	}

	// Configure connection backoff parameters
	backoffConfig := grpcbackoff.Config{
		BaseDelay:  c.config.GrpcConfig.BackoffBaseDelay,
		Multiplier: c.config.GrpcConfig.BackoffMultiplier,
		Jitter:     c.config.GrpcConfig.BackoffJitter,
		MaxDelay:   c.config.GrpcConfig.BackoffMaxDelay,
	}

	// WARNING: for DNS related issues please refer to https://github.com/grpc/grpc/blob/master/doc/naming.md
	// Additionally make sure that you can resolve the address (e.g. via ping) from one pod to another.
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kacp),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoffConfig,
			MinConnectTimeout: c.config.GrpcConfig.MinConnectTimeout,
		}),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", addr)
		}),
	)
}
