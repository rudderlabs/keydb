package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	DefaultRetryPolicyInitialInterval        = 100 * time.Millisecond
	DefaultRetryPolicyMultiplier             = 1.5
	DefaultRetryPolicyMaxInterval            = 30 * time.Second
	DefaultTotalHashRanges            uint32 = 128

	DefaultGrpcKeepAliveTime    = 10 * time.Second
	DefaultGrpcKeepAliveTimeout = 2 * time.Second

	DefaultGrpcBackoffBaseDelay  = 1 * time.Second
	DefaultGrpcBackoffMultiplier = 1.6
	DefaultGrpcBackoffJitter     = 0.2
	DefaultGrpcMaxDelay          = 2 * time.Minute
	DefaultGrpcMinConnectTimeout = 20 * time.Second
)

type errClusterSizeChanged struct {
	nodesAddresses []string
}

func (e *errClusterSizeChanged) Error() string { return "cluster size changed" }

// RetryPolicy defines the retry policy configuration
type RetryPolicy struct {
	Disabled        bool
	InitialInterval time.Duration
	Multiplier      float64
	MaxInterval     time.Duration
}

// GrpcConfig holds gRPC connection configuration
type GrpcConfig struct {
	// KeepAliveTime is the time after which a ping will be sent on the transport
	KeepAliveTime time.Duration
	// KeepAliveTimeout is the time the client waits for a response to the keepalive ping
	KeepAliveTimeout time.Duration
	// DisableKeepAlivePermitWithoutStream disables keepalive pings even when there are no active streams
	DisableKeepAlivePermitWithoutStream bool

	// BackoffBaseDelay is the initial backoff delay for connection attempts
	BackoffBaseDelay time.Duration
	// BackoffMultiplier is the multiplier for exponential backoff
	BackoffMultiplier float64
	// BackoffJitter adds randomness to backoff delays
	BackoffJitter float64
	// BackoffMaxDelay is the maximum backoff delay
	BackoffMaxDelay time.Duration
	// MinConnectTimeout is the minimum timeout for connection attempts
	MinConnectTimeout time.Duration
}

// Config holds the configuration for a client
type Config struct {
	// Addresses is a list of node addresses (host:port)
	Addresses []string

	// TotalHashRanges is the total number of hash ranges
	TotalHashRanges uint32

	// RetryPolicy defines the retry behavior for failed requests
	RetryPolicy RetryPolicy

	// GrpcConfig defines the gRPC connection configuration
	GrpcConfig GrpcConfig
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

	stats stats.Stats

	metrics struct {
		getReqCount    stats.Counter
		getReqLatency  stats.Timer
		getReqFailures stats.Counter
		getReqRetries  stats.Counter
		getKeysQueried stats.Counter
		getKeysFound   stats.Counter

		putReqCount    stats.Counter
		putReqLatency  stats.Timer
		putReqFailures stats.Counter
		putReqRetries  stats.Counter
	}
}

type Opts func(*Client)

func WithStats(stats stats.Stats) Opts {
	return func(client *Client) {
		client.stats = stats
	}
}

// NewClient creates a new KeyDB client
func NewClient(config Config, log logger.Logger, opts ...Opts) (*Client, error) {
	if len(config.Addresses) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	if config.TotalHashRanges == 0 {
		config.TotalHashRanges = DefaultTotalHashRanges
	}

	if config.RetryPolicy.InitialInterval == 0 {
		config.RetryPolicy.InitialInterval = DefaultRetryPolicyInitialInterval
	}
	if config.RetryPolicy.Multiplier == 0 {
		config.RetryPolicy.Multiplier = DefaultRetryPolicyMultiplier
	}
	if config.RetryPolicy.MaxInterval == 0 {
		config.RetryPolicy.MaxInterval = DefaultRetryPolicyMaxInterval
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

	if client.stats == nil {
		client.stats = stats.NOP
	}

	client.initMetrics()

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

func (c *Client) initMetrics() {
	c.metrics.getReqCount = c.stats.NewTaggedStat("keydb_client_req_count_total", stats.CountType, stats.Tags{
		"method": "get",
	})
	c.metrics.getReqLatency = c.stats.NewTaggedStat("keydb_client_req_latency_seconds", stats.TimerType, stats.Tags{
		"method": "get",
	})
	c.metrics.getReqFailures = c.stats.NewTaggedStat("keydb_client_req_failures_total", stats.CountType, stats.Tags{
		"method": "get",
	})
	c.metrics.getReqRetries = c.stats.NewTaggedStat("keydb_client_req_retries_total", stats.CountType, stats.Tags{
		"method": "get",
	})
	c.metrics.getKeysQueried = c.stats.NewStat("keydb_client_get_keys_queried_total", stats.CountType)
	c.metrics.getKeysFound = c.stats.NewStat("keydb_client_get_keys_found_total", stats.CountType)

	c.metrics.putReqCount = c.stats.NewTaggedStat("keydb_client_req_count_total", stats.CountType, stats.Tags{
		"method": "put",
	})
	c.metrics.putReqLatency = c.stats.NewTaggedStat("keydb_client_req_latency_seconds", stats.TimerType, stats.Tags{
		"method": "put",
	})
	c.metrics.putReqFailures = c.stats.NewTaggedStat("keydb_client_req_failures_total", stats.CountType, stats.Tags{
		"method": "put",
	})
	c.metrics.putReqRetries = c.stats.NewTaggedStat("keydb_client_req_retries_total", stats.CountType, stats.Tags{
		"method": "put",
	})
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
	defer c.metrics.getReqLatency.RecordDuration()()
	c.metrics.getReqCount.Increment()
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a map to store results
	results := make(map[string]bool)
	resultsMu := sync.Mutex{}

	res, err := c.get(ctx, keys, results, &resultsMu)
	if err != nil {
		c.metrics.getReqFailures.Increment()
		return res, err
	}
	return res, nil
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
			var (
				err         error
				resp        *pb.GetResponse
				nextBackoff = c.getNextBackoffFunc()
			)
			for attempt := int64(1); ; attempt++ {
				// Increment retry counter (except for the first attempt)
				if attempt > 1 {
					c.metrics.getReqRetries.Increment()
				}
				resp, err = client.Get(gCtx, req)
				if resp != nil && c.clusterSize != resp.ClusterSize {
					hasClusterSizeChanged.Store(resp.NodesAddresses)
					cancel()
					return &errClusterSizeChanged{nodesAddresses: resp.NodesAddresses}
				}
				if err == nil && resp != nil {
					if resp.ErrorCode == pb.ErrorCode_NO_ERROR {
						break // for internal errors, scaling or wrong code we retry
					}
				}

				// If retry is disabled, return immediately after first attempt
				if c.config.RetryPolicy.Disabled {
					if err != nil {
						return fmt.Errorf("failed to get keys from node %d: %w",
							nodeID, err)
					}
					if resp != nil {
						return fmt.Errorf("failed to get keys from node %d with error code %s",
							nodeID, resp.ErrorCode.String())
					}
					return fmt.Errorf("critical: failed to get keys from node %d: "+
						"both error and response are nil", nodeID)
				}

				retryDelay := nextBackoff()

				if err != nil {
					c.logger.Errorn("get keys from node",
						logger.NewIntField("nodeId", int64(nodeID)),
						logger.NewIntField("attempt", attempt),
						logger.NewDurationField("retryDelay", retryDelay),
						obskit.Error(err))
				} else if resp != nil {
					c.logger.Warnn("get keys from node",
						logger.NewIntField("nodeId", int64(nodeID)),
						logger.NewIntField("attempt", attempt),
						logger.NewDurationField("retryDelay", retryDelay),
						obskit.Error(errors.New(resp.ErrorCode.String())))
				} else {
					cancel()
					return fmt.Errorf("critical: failed to get keys from node %d: "+
						"both error and response are nil", nodeID)
				}

				// Wait before retrying
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case <-time.After(retryDelay):
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
	existsCount := 0
	for i, key := range keys {
		exists[i] = results[key]
		if exists[i] {
			existsCount++
		}
	}

	c.metrics.getKeysQueried.Count(len(keys))
	c.metrics.getKeysFound.Count(existsCount)
	return exists, nil
}

// Put stores multiple key-value pairs with TTL
func (c *Client) Put(ctx context.Context, keys []string, ttl time.Duration) error {
	defer c.metrics.putReqLatency.RecordDuration()()
	c.metrics.putReqCount.Increment()
	c.mu.RLock()
	defer c.mu.RUnlock()

	err := c.put(ctx, keys, ttl)
	if err != nil {
		c.metrics.putReqFailures.Increment()
		return err
	}
	return nil
}

func (c *Client) put(ctx context.Context, keys []string, ttl time.Duration) error {
	// Group keys by node
	keysByNode := make(map[uint32][]string)
	for _, key := range keys {
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
			req := &pb.PutRequest{Keys: nodeKeys, TtlSeconds: uint64(ttl.Seconds())}

			// Send the request with retries
			var (
				err         error
				resp        *pb.PutResponse
				nextBackoff = c.getNextBackoffFunc()
			)
			for attempt := int64(1); ; attempt++ {
				// Increment retry counter (except for the first attempt)
				if attempt > 1 {
					c.metrics.putReqRetries.Increment()
				}
				resp, err = client.Put(gCtx, req)
				if resp != nil && c.clusterSize != resp.ClusterSize {
					hasClusterSizeChanged.Store(resp.NodesAddresses)
					cancel()
					return &errClusterSizeChanged{nodesAddresses: resp.NodesAddresses}
				}
				if err == nil && resp != nil {
					if resp.Success && resp.ErrorCode == pb.ErrorCode_NO_ERROR {
						break // for internal errors, scaling or wrong code we retry
					}
				}

				// If retry is disabled, return immediately after first attempt
				if c.config.RetryPolicy.Disabled {
					if err != nil {
						return fmt.Errorf("failed to put keys from node %d: %w",
							nodeID, err)
					}
					if resp != nil {
						return fmt.Errorf("failed to put keys from node %d with error code %s",
							nodeID, resp.ErrorCode.String())
					}
					return fmt.Errorf("critical: failed to put keys from node %d: "+
						"both error and response are nil", nodeID)
				}

				retryDelay := nextBackoff()

				if err != nil {
					c.logger.Errorn("put keys in node",
						logger.NewIntField("nodeID", int64(nodeID)),
						logger.NewIntField("attempt", attempt),
						logger.NewDurationField("retryDelay", retryDelay),
						obskit.Error(err))
				} else if resp != nil {
					c.logger.Warnn("put keys in node",
						logger.NewIntField("nodeID", int64(nodeID)),
						logger.NewIntField("attempt", attempt),
						logger.NewDurationField("retryDelay", retryDelay),
						obskit.Error(errors.New(resp.ErrorCode.String())))
				} else {
					cancel()
					return fmt.Errorf("critical: failed to put keys from node %d: "+
						"both error and response are nil", nodeID)
				}

				// Wait before retrying
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case <-time.After(retryDelay):
				}
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
			conn, err := c.createConnection(addr)
			if err != nil {
				return fmt.Errorf("failed to connect to node %d at %s: %w", i, addr, err)
			}

			c.connections[i] = conn
			c.clients[i] = pb.NewNodeServiceClient(conn)
		}
	}

	return nil
}

func (c *Client) getNextBackoffFunc() func() time.Duration {
	bo := backoff.NewExponentialBackOff()
	bo.Multiplier = c.config.RetryPolicy.Multiplier
	bo.InitialInterval = c.config.RetryPolicy.InitialInterval
	bo.MaxInterval = c.config.RetryPolicy.MaxInterval

	return func() time.Duration {
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
