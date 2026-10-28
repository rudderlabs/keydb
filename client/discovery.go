package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
)

const (
	// addressesTimeout bounds each call made to learn the cluster's addresses
	addressesTimeout = 5 * time.Second

	// notReadyMaxRetryDelay caps the wait before retrying a node that answered NOT_READY. A shadow becomes ACTIVE
	// as soon as its kubelet refreshes the config and a NOT_READY answer is cheap, so this bounds how long a request
	// that kept retrying waits once the node is promoted, instead of a full backoff of up to 1.5×MaxInterval.
	notReadyMaxRetryDelay = 2 * time.Second

	// refreshInterval is the least time between two refreshes of the node list after a node didn't answer. A node
	// that is down fails every request at once, and they don't all need to ask.
	refreshInterval = time.Second
)

// errClientClosed is returned by requests made or still retrying after Close
var errClientClosed = errors.New("keydb client closed")

// nodeResponse is what Get and Put responses have in common
type nodeResponse interface {
	GetClusterSize() int64
	GetNodesAddresses() []string
	GetErrorCode() pb.ErrorCode
}

// ensureDiscovered learns the cluster's addresses from the nodes behind the discovery addresses the first time it is
// called. Data requests never go to a discovery address: a headless Service's name resolves to every pod, while each
// key must reach the node that owns it.
// One caller discovers while the others wait for it, each giving up when its own context ends.
func (c *Client) ensureDiscovered(ctx context.Context) error {
	for {
		if c.closed.Load() {
			return errClientClosed
		}
		if c.discovered.Load() {
			return nil
		}
		c.discoverMu.Lock()
		if c.discovered.Load() {
			c.discoverMu.Unlock()
			return nil
		}
		if running := c.discovering; running != nil {
			c.discoverMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-running:
				continue // it either succeeded, or failed and this caller tries in turn
			}
		}
		running := make(chan struct{})
		c.discovering = running
		c.discoverMu.Unlock()

		err := c.discoverAndApply(ctx)

		c.discoverMu.Lock()
		c.discovering = nil
		c.discoverMu.Unlock()
		close(running)
		return err
	}
}

func (c *Client) discoverAndApply(ctx context.Context) error {
	addresses, err := c.discover(ctx)
	if err != nil {
		return err
	}
	if _, err := c.applyAddresses(addresses); err != nil {
		return fmt.Errorf("applying discovered addresses: %w", err)
	}
	c.discovered.Store(true)
	return nil
}

// discover asks the nodes behind the discovery addresses for the cluster's addresses (see askAnyNode). It retries with
// backoff unless retries are disabled, counting every failed attempt it retries in
// keydb_client_discovery_retries_total. When the nodes answer without a list, as nodes that predate discovery may,
// the discovery addresses are the list.
func (c *Client) discover(ctx context.Context) ([]string, error) {
	nextBackoff := c.getNextBackoffFunc()
	for {
		addresses, answered, err := c.askAnyNode(ctx)
		if err == nil {
			return addresses, nil
		}
		if answered {
			// A node also answers an empty list when every node of the cluster is marked degraded. Using the
			// discovery addresses as the list is then harmless: the first data response carries the cluster's size
			// and addresses, and the client adopts them before it routes the keys again.
			return slices.Clone(c.discoveryAddresses), nil
		}
		if c.config.RetryPolicy.Disabled {
			return nil, fmt.Errorf("discovering the cluster through %s: %w",
				strings.Join(c.discoveryAddresses, ","), err)
		}
		if c.closed.Load() {
			return nil, errClientClosed
		}
		c.metrics.discoveryRetries.Increment()
		delay := nextBackoff()
		c.logger.Warnn("Discovering the cluster",
			logger.NewStringField("addresses", strings.Join(c.discoveryAddresses, ",")),
			logger.NewDurationField("retryDelay", delay),
			obskit.Error(err),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

// refreshAddresses asks the nodes behind the discovery addresses for the cluster's addresses after a node didn't
// answer. Requests that fail at the same time share one refresh, and refreshes are at least refreshInterval apart.
// It returns nil when it didn't refresh or no node answered with a list.
func (c *Client) refreshAddresses(ctx context.Context) []string {
	if !c.refreshMu.TryLock() {
		return nil // another request is refreshing, this one retries with what it finds
	}
	defer c.refreshMu.Unlock()
	if time.Since(c.lastRefresh) < refreshInterval {
		return nil
	}
	c.lastRefresh = time.Now()
	addresses, _, err := c.askAnyNode(ctx)
	if err != nil {
		c.logger.Warnn("Refreshing the node addresses", obskit.Error(err))
		return nil
	}
	return addresses
}

// askAnyNode resolves the discovery addresses and asks the nodes behind them for the cluster's addresses, one after
// the other, starting one node further than the previous call. It returns the first list a node answers with. A
// node that hasn't reloaded its config yet may answer with an old list: the next refresh starts from another node.
// answered is true when a node answered without a list.
func (c *Client) askAnyNode(ctx context.Context) (addresses []string, answered bool, err error) {
	nodes := c.resolveDiscoveryAddresses(ctx)
	if len(nodes) == 0 {
		return nil, false, fmt.Errorf("no node behind %s", strings.Join(c.discoveryAddresses, ","))
	}
	start := int(c.nextNode.Add(1) % uint64(len(nodes)))
	for i := range nodes {
		addresses, askErr := c.askForAddresses(ctx, nodes[(start+i)%len(nodes)])
		switch {
		case askErr == nil && len(addresses) > 0:
			return addresses, false, nil
		case askErr == nil || status.Code(askErr) == codes.Unimplemented:
			answered = true
		default:
			err = askErr
		}
	}
	if err == nil {
		err = errors.New("no node answered with a list")
	}
	return nil, answered, err
}

// resolveDiscoveryAddresses returns the addresses of the nodes behind the discovery addresses, skipping the ones
// that don't resolve.
func (c *Client) resolveDiscoveryAddresses(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(ctx, addressesTimeout)
	defer cancel()
	var nodes []string
	for _, address := range c.discoveryAddresses {
		resolved, err := c.resolve(ctx, address)
		if err != nil {
			c.logger.Warnn("Resolving a discovery address",
				logger.NewStringField("address", address), obskit.Error(err),
			)
			continue
		}
		nodes = append(nodes, resolved...)
	}
	return nodes
}

// resolveAddress looks the host of address up in DNS and returns every address it resolves to, with the port: every
// ready pod for a headless Service. An IP address is returned as it is.
func resolveAddress(ctx context.Context, address string) ([]string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("splitting %q into host and port: %w", address, err)
	}
	if host == "" {
		return []string{address}, nil // gRPC dials it as the local machine
	}
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("looking up %q: %w", host, err)
	}
	resolved := make([]string, 0, len(ips))
	for _, ip := range ips {
		resolved = append(resolved, net.JoinHostPort(ip, port))
	}
	return resolved, nil
}

// askForAddresses asks the node at address for the cluster's addresses on a fresh connection.
func (c *Client) askForAddresses(ctx context.Context, address string) ([]string, error) {
	conn, err := grpc.NewClient(address, c.getGrpcDialOptions()...)
	if err != nil {
		return nil, fmt.Errorf("creating gRPC client for %s: %w", address, err)
	}
	defer func() { _ = conn.Close() }()
	return askNodeForAddresses(ctx, pb.NewNodeServiceClient(conn))
}

func askNodeForAddresses(ctx context.Context, client pb.NodeServiceClient) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, addressesTimeout)
	defer cancel()
	info, err := client.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	if err != nil {
		return nil, err
	}
	return info.NodesAddresses, nil
}

// applyAddresses adopts a new list of node addresses. Only the connection pools whose address changed are
// replaced. It returns true when the cluster size changed, in which case keys must be hashed again.
func (c *Client) applyAddresses(addresses []string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return false, errClientClosed // never reopen connections after Close
	}
	if slices.Equal(addresses, c.config.Addresses) && len(c.pools) == len(addresses) {
		return false, nil
	}

	dialOpts := c.getGrpcDialOptions()
	for i, addr := range addresses {
		if i < len(c.config.Addresses) && c.config.Addresses[i] == addr {
			if _, ok := c.pools[i]; ok {
				continue
			}
		}
		pool, err := newConnectionPool(addr, c.config.ConnectionPoolSize, dialOpts...)
		if err != nil {
			return false, fmt.Errorf("creating gRPC connection pool for node %d at %s: %w", i, addr, err)
		}
		if old, ok := c.pools[i]; ok {
			_ = old.close() // requests still using it fail and retry on the new pool
		}
		c.pools[i] = pool
		if c.metrics.connectionOps[i] == nil {
			c.metrics.connectionOps[i] = make(map[int]stats.Counter)
			for j := range c.config.ConnectionPoolSize {
				c.metrics.connectionOps[i][j] = c.stats.NewTaggedStat(
					"keydb_client_connection_ops_total", stats.CountType, stats.Tags{
						"node": strconv.Itoa(i),
						"conn": strconv.Itoa(j),
					},
				)
			}
		}
	}
	for i := len(addresses); i < len(c.config.Addresses) || i < len(c.pools); i++ {
		if pool, ok := c.pools[i]; ok {
			_ = pool.close()
			delete(c.pools, i)
			delete(c.metrics.connectionOps, i)
		}
	}

	oldClusterSize := c.clusterSize
	newClusterSize := int64(len(addresses))
	c.config.Addresses = slices.Clone(addresses)
	c.clusterSize = newClusterSize
	if newClusterSize != oldClusterSize {
		c.hash = hash.New(newClusterSize, c.config.TotalHashRanges)
	}

	c.logger.Infon("Adopted new node addresses",
		logger.NewIntField("oldClusterSize", oldClusterSize),
		logger.NewIntField("newClusterSize", newClusterSize),
		logger.NewStringField("nodesAddresses", strings.Join(addresses, ",")),
	)
	return newClusterSize != oldClusterSize, nil
}

// routing returns the hash and cluster size requests must use.
func (c *Client) routing() (*hash.Hash, int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hash, c.clusterSize
}

// nodeClient returns the next pooled connection to a node, counting the operation.
func (c *Client) nodeClient(nodeID int64) (pb.NodeServiceClient, *grpc.ClientConn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pool, ok := c.pools[int(nodeID)]
	if !ok {
		return nil, nil, false
	}
	client, conn, connID := pool.getClient()
	if counter, ok := c.metrics.connectionOps[int(nodeID)][connID]; ok {
		counter.Increment()
	}
	return client, conn, true
}

// addressesIfShrunk returns the current addresses when the cluster no longer has the given node, nil otherwise.
func (c *Client) addressesIfShrunk(nodeID int64) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if nodeID < c.clusterSize {
		return nil
	}
	return slices.Clone(c.config.Addresses)
}

// callNode sends a request to a node until it succeeds, the context is done, or the cluster size changes.
//   - A failed response carries the node's address list: when it differs from the client's, the client adopts it.
//   - When the node doesn't answer, the client refreshes the list from the nodes behind the discovery addresses.
//   - Every retry takes the next pooled connection and waits for the backoff, without holding the client's lock.
//     After a NOT_READY answer the wait is capped at notReadyMaxRetryDelay.
func callNode[R nodeResponse](
	ctx context.Context, c *Client, nodeID, clusterSize int64, method string, retries stats.Counter,
	call func(ctx context.Context, client pb.NodeServiceClient) (R, error),
	succeeded func(R) bool,
) (R, error) {
	var (
		zero        R
		nextBackoff = c.getNextBackoffFunc()
	)
	for attempt := int64(1); ; attempt++ {
		if c.closed.Load() {
			return zero, errClientClosed
		}
		if attempt > 1 {
			retries.Increment()
		}
		client, conn, ok := c.nodeClient(nodeID)
		if !ok {
			if addresses := c.addressesIfShrunk(nodeID); addresses != nil {
				// another request learnt that the cluster shrank: route the keys again
				return zero, &errClusterSizeChanged{nodesAddresses: addresses}
			}
			// this should never happen unless clusterSize is updated and the pools map isn't
			// or if there is a bug in the hashing function
			return zero, fmt.Errorf("no connection pool for node %d", nodeID)
		}

		resp, respErr := call(ctx, client)
		if respErr == nil {
			addresses := resp.GetNodesAddresses()
			if resp.GetClusterSize() != clusterSize && len(addresses) > 0 {
				return zero, &errClusterSizeChanged{nodesAddresses: addresses}
			}
			if succeeded(resp) {
				return resp, nil
			}
			if len(addresses) > 0 {
				sizeChanged, err := c.applyAddresses(addresses)
				if err != nil {
					return zero, err
				}
				if sizeChanged {
					return zero, &errClusterSizeChanged{nodesAddresses: addresses}
				}
			}
		} else if ctx.Err() == nil {
			// the node may be gone, for example replaced by another pod
			if addresses := c.refreshAddresses(ctx); addresses != nil {
				sizeChanged, err := c.applyAddresses(addresses)
				if err != nil {
					return zero, err
				}
				if sizeChanged {
					return zero, &errClusterSizeChanged{nodesAddresses: addresses}
				}
			}
		}

		// If retry is disabled, return immediately after first attempt
		if c.config.RetryPolicy.Disabled {
			if respErr != nil {
				return zero, fmt.Errorf("%s keys from node %d: %w", method, nodeID, respErr)
			}
			return zero, fmt.Errorf("%s keys from node %d: error code %s", method, nodeID, resp.GetErrorCode().String())
		}

		retryDelay := nextBackoff()
		if respErr == nil && resp.GetErrorCode() == pb.ErrorCode_NOT_READY {
			retryDelay = min(retryDelay, notReadyMaxRetryDelay)
		}
		fields := []logger.Field{
			logger.NewIntField("nodeId", nodeID),
			logger.NewIntField("attempt", attempt),
			logger.NewDurationField("retryDelay", retryDelay),
			logger.NewStringField("canonicalTarget", conn.CanonicalTarget()),
			logger.NewStringField("connState", conn.GetState().String()),
		}
		if respErr != nil {
			c.logger.Errorn(method+" keys from node", append(fields, obskit.Error(respErr))...)
		} else {
			c.logger.Warnn(method+" keys from node",
				append(fields, obskit.Error(errors.New(resp.GetErrorCode().String())))...)
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(retryDelay):
		}
	}
}
