package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
)

const (
	DefaultRetryPolicyInitialInterval       = 100 * time.Millisecond
	DefaultRetryPolicyMultiplier            = 1.5
	DefaultRetryPolicyMaxInterval           = 30 * time.Second
	DefaultTotalHashRanges            int64 = 271

	DefaultGrpcKeepAliveTime    = 10 * time.Second
	DefaultGrpcKeepAliveTimeout = 2 * time.Second

	DefaultGrpcBackoffBaseDelay  = 1 * time.Second
	DefaultGrpcBackoffMultiplier = 1.6
	DefaultGrpcBackoffJitter     = 0.2
	DefaultGrpcMaxDelay          = 2 * time.Minute
	DefaultGrpcMinConnectTimeout = 20 * time.Second

	DefaultConnectionPoolSize = 10

	DefaultDeadNodeLookupAfter = 5 * time.Second
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
	TotalHashRanges int64

	// ConnectionPoolSize is the number of connections per node (0 means use default)
	ConnectionPoolSize int

	// RetryPolicy defines the retry behavior for failed requests
	RetryPolicy RetryPolicy

	// GrpcConfig defines the gRPC connection configuration
	GrpcConfig GrpcConfig

	// DeadNodeLookupAfter is how long a node may fail without answering before the client asks another node for
	// the current list of addresses (0 means use default)
	DeadNodeLookupAfter time.Duration
}

// Client is a client for the KeyDB service
type Client struct {
	config Config

	// clusterSize is the number of nodes in the cluster
	clusterSize int64

	// hash is the hash instance used for consistent hashing
	hash *hash.Hash

	// pools is a map of node index to gRPC connection pool
	pools map[int]*connectionPool

	// mu protects pools, clusterSize, hash, config.Addresses and metrics.connectionOps
	mu sync.RWMutex

	// seeds are the configured addresses, used to discover the cluster and to look it up again
	seeds []string
	// discovered is true once the client learnt the cluster's addresses
	discovered atomic.Bool
	// discoverMu protects discovering, which is closed when the discovery in progress ends
	discoverMu  sync.Mutex
	discovering chan struct{}
	// closed is true once Close was called, requests still retrying then stop
	closed atomic.Bool

	// abandoned is, for each node a dead-node lookup moved, the address it was moved away from. It is protected by mu.
	abandoned map[int]abandonedAddress

	// failMu protects failingSince, when each node started failing without answering, across requests, and
	// clearedAt, when each node last answered or got a new address
	failMu       sync.Mutex
	failingSince map[int64]time.Time
	clearedAt    map[int64]time.Time

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
		connectionOps  map[int]map[int]stats.Counter
	}
}

// abandonedAddress is an address a dead-node lookup moved a node away from
type abandonedAddress struct {
	// address is the address the node was moved away from, movedTo the one it was moved to
	address, movedTo string
	// until is when the client forgets it
	until time.Time
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

	// Set connection pool size default
	if config.ConnectionPoolSize == 0 {
		config.ConnectionPoolSize = DefaultConnectionPoolSize
	}

	if config.DeadNodeLookupAfter == 0 {
		config.DeadNodeLookupAfter = DefaultDeadNodeLookupAfter
	}
	config.Addresses = slices.Clone(config.Addresses)

	clusterSize := int64(len(config.Addresses))
	client := &Client{
		seeds:       slices.Clone(config.Addresses),
		config:      config,
		pools:       make(map[int]*connectionPool),
		abandoned:   make(map[int]abandonedAddress),
		clusterSize: clusterSize,
		hash:        hash.New(clusterSize, config.TotalHashRanges),
		logger:      log,
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.stats == nil {
		client.stats = stats.NOP
	}

	client.initMetrics()
	client.initConnectionMetrics(config)

	// Create connection pools for all nodes
	dialOpts := client.getGrpcDialOptions()
	for i, addr := range config.Addresses {
		pool, err := newConnectionPool(addr, config.ConnectionPoolSize, dialOpts...)
		if err != nil {
			// Close all pools on error
			_ = client.Close()
			return nil, fmt.Errorf("creating gRPC connection pool for node %d at %s: %w", i, addr, err)
		}

		client.pools[i] = pool
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

func (c *Client) initConnectionMetrics(config Config) {
	c.metrics.connectionOps = make(map[int]map[int]stats.Counter)
	for i := range config.Addresses {
		c.metrics.connectionOps[i] = make(map[int]stats.Counter)
		for j := range config.ConnectionPoolSize {
			c.metrics.connectionOps[i][j] = c.stats.NewTaggedStat(
				"keydb_client_connection_ops_total", stats.CountType, stats.Tags{
					"node": strconv.Itoa(i),
					"conn": strconv.Itoa(j),
				},
			)
		}
	}
}

// Close will terminate all connection pools
func (c *Client) Close() error {
	c.closed.Store(true)
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for i, pool := range c.pools {
		if err := pool.close(); err != nil {
			lastErr = fmt.Errorf("closing connection pool for node %d: %w", i, err)
		}
	}

	c.pools = make(map[int]*connectionPool)
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

	if err := c.ensureDiscovered(ctx); err != nil {
		c.metrics.getReqFailures.Increment()
		return nil, err
	}

	// Create a map to store results
	results := make(map[string]bool)
	resultsMu := sync.Mutex{}

	err := c.routeUntilStable(ctx, func() error { return c.get(ctx, keys, results, &resultsMu) })
	if err != nil {
		c.metrics.getReqFailures.Increment()
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

// routeUntilStable sends a request with the client's current routing until it no longer fails because the cluster
// size changed. The first re-route goes at once, every later one waits for the request's backoff first, so that
// nodes that disagree on the cluster size don't make the request spin.
func (c *Client) routeUntilStable(ctx context.Context, send func() error) error {
	var nextBackoff func() time.Duration
	for {
		err := send()
		changed, ok := errors.AsType[*errClusterSizeChanged](err)
		if !ok {
			return err
		}
		if _, err := c.applyAddresses(changed.nodesAddresses); err != nil {
			return fmt.Errorf("updating cluster size: %w", err)
		}
		if nextBackoff == nil {
			nextBackoff = c.getNextBackoffFunc()
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextBackoff()):
		}
	}
}

// get sends the keys that have no result yet to their nodes, routed with the client's current hash.
// It returns errClusterSizeChanged when a node reports another cluster size.
func (c *Client) get(ctx context.Context, keys []string, results map[string]bool, resultsMu *sync.Mutex) error {
	h, clusterSize := c.routing()

	// Group keys by node
	keysByNode := make(map[int64][]string)
	for _, key := range keys {
		if _, alreadyFetched := results[key]; alreadyFetched {
			continue
		}
		nodeID := h.GetNodeNumber(key)
		keysByNode[nodeID] = append(keysByNode[nodeID], key)
	}

	hasClusterSizeChanged := atomic.Value{}

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, gCtx := kitsync.NewEagerGroup(cancellableCtx, len(keysByNode))

	for nodeID, nodeKeys := range keysByNode {
		group.Go(func() error {
			req := &pb.GetRequest{Keys: nodeKeys}
			resp, err := callNode(gCtx, c, nodeID, clusterSize, "get", c.metrics.getReqRetries,
				func(ctx context.Context, client pb.NodeServiceClient) (*pb.GetResponse, error) {
					return client.Get(ctx, req)
				},
				func(resp *pb.GetResponse) bool { return resp.ErrorCode == pb.ErrorCode_NO_ERROR },
			)
			if err != nil {
				if changed, ok := errors.AsType[*errClusterSizeChanged](err); ok {
					hasClusterSizeChanged.Store(changed.nodesAddresses)
					cancel()
				}
				return err
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
		if nodesAddresses, ok := hasClusterSizeChanged.Load().([]string); ok && len(nodesAddresses) > 0 {
			return &errClusterSizeChanged{nodesAddresses: nodesAddresses}
		}
		return err
	}
	return nil
}

// Put stores multiple key-value pairs with TTL
func (c *Client) Put(ctx context.Context, keys []string, ttl time.Duration) error {
	defer c.metrics.putReqLatency.RecordDuration()()
	c.metrics.putReqCount.Increment()

	if err := c.ensureDiscovered(ctx); err != nil {
		c.metrics.putReqFailures.Increment()
		return err
	}

	err := c.routeUntilStable(ctx, func() error { return c.put(ctx, keys, ttl) })
	if err != nil {
		c.metrics.putReqFailures.Increment()
		return err
	}
	return nil
}

// put sends the keys to their nodes, routed with the client's current hash.
// It returns errClusterSizeChanged when a node reports another cluster size.
func (c *Client) put(ctx context.Context, keys []string, ttl time.Duration) error {
	h, clusterSize := c.routing()

	// Group keys by node
	keysByNode := make(map[int64][]string)
	for _, key := range keys {
		nodeID := h.GetNodeNumber(key)
		keysByNode[nodeID] = append(keysByNode[nodeID], key)
	}

	hasClusterSizeChanged := atomic.Value{}

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, gCtx := kitsync.NewEagerGroup(cancellableCtx, len(keysByNode))
	for nodeID, nodeKeys := range keysByNode {
		group.Go(func() error {
			req := &pb.PutRequest{Keys: nodeKeys, TtlSeconds: int64(ttl.Seconds())}
			_, err := callNode(gCtx, c, nodeID, clusterSize, "put", c.metrics.putReqRetries,
				func(ctx context.Context, client pb.NodeServiceClient) (*pb.PutResponse, error) {
					return client.Put(ctx, req)
				},
				func(resp *pb.PutResponse) bool { return resp.Success && resp.ErrorCode == pb.ErrorCode_NO_ERROR },
			)
			if err != nil {
				if changed, ok := errors.AsType[*errClusterSizeChanged](err); ok {
					hasClusterSizeChanged.Store(changed.nodesAddresses)
					cancel()
				}
				return err
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		if nodesAddresses, ok := hasClusterSizeChanged.Load().([]string); ok && len(nodesAddresses) > 0 {
			return &errClusterSizeChanged{nodesAddresses: nodesAddresses}
		}
		return err
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

// getGrpcDialOptions returns the gRPC dial options for creating connections
func (c *Client) getGrpcDialOptions() []grpc.DialOption {
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
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// keydb does not publish service config over DNS. Left enabled, the dns resolver additionally looks up
		// TXT _grpc_config.<host> on every channel and withholds the resolved addresses until that lookup
		// returns, so a slow or throttled DNS server stalls connection setup even once the host has resolved.
		grpc.WithDisableServiceConfig(),
		grpc.WithKeepaliveParams(kacp),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoffConfig,
			MinConnectTimeout: c.config.GrpcConfig.MinConnectTimeout,
		}),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", addr)
		}),
	}
}

// connectionPool manages multiple gRPC connections for round-robin load balancing
type connectionPool struct {
	connections []*grpc.ClientConn
	clients     []pb.NodeServiceClient
	next        atomic.Uint32
}

// newConnectionPool creates a new connection pool with the specified number of connections
func newConnectionPool(addr string, size int, dialOpts ...grpc.DialOption) (*connectionPool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("pool size must be positive, got %d", size)
	}

	pool := &connectionPool{
		connections: make([]*grpc.ClientConn, size),
		clients:     make([]pb.NodeServiceClient, size),
	}

	for i := range size {
		conn, err := grpc.NewClient(addr, dialOpts...)
		if err != nil {
			// Close any connections we've already created
			for j := range i {
				_ = pool.connections[j].Close()
			}
			return nil, fmt.Errorf("creating connection %d/%d: %w", i+1, size, err)
		}
		pool.connections[i] = conn
		pool.clients[i] = pb.NewNodeServiceClient(conn)
	}

	return pool, nil
}

// getConn returns the next connection in round-robin fashion
func (p *connectionPool) getClient() (pb.NodeServiceClient, *grpc.ClientConn, int) {
	n := p.next.Add(1)
	i := (int(n) - 1) % len(p.connections)
	return p.clients[i], p.connections[i], i
}

// close closes all connections in the pool
func (p *connectionPool) close() error {
	var lastErr error
	for i, conn := range p.connections {
		if err := conn.Close(); err != nil {
			lastErr = fmt.Errorf("closing connection %d/%d: %w", i+1, len(p.connections), err)
		}
	}
	// the slices are kept: a request that picked this pool before it was replaced fails and retries on the new one
	return lastErr
}
