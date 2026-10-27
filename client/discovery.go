package client

import (
	"context"
	"errors"
	"fmt"
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
)

// errClientClosed is returned by requests made or still retrying after Close
var errClientClosed = errors.New("keydb client closed")

// nodeResponse is what Get and Put responses have in common
type nodeResponse interface {
	GetClusterSize() int64
	GetNodesAddresses() []string
	GetErrorCode() pb.ErrorCode
}

// ensureDiscovered learns the cluster's addresses through the seeds the first time it is called.
// Data requests never go through a seed: with a Service as seed, they would reach random nodes.
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

// discover asks the seeds for the cluster's addresses, opening a fresh connection for every attempt so that a
// seed behind a Service spreads the attempts across nodes. It retries with backoff unless retries are disabled.
// When the seeds answer without a list, as nodes that predate discovery may, the seeds are the list.
func (c *Client) discover(ctx context.Context) ([]string, error) {
	nextBackoff := c.getNextBackoffFunc()
	for {
		var lastErr error
		answered := false
		for _, seed := range c.seeds {
			addresses, err := c.askForAddresses(ctx, seed)
			if err == nil && len(addresses) > 0 {
				return addresses, nil
			}
			if err == nil || status.Code(err) == codes.Unimplemented {
				answered = true
				continue
			}
			lastErr = err
		}
		if answered {
			return c.seeds, nil
		}
		if c.config.RetryPolicy.Disabled {
			return nil, fmt.Errorf("discovering the cluster through %s: %w", strings.Join(c.seeds, ","), lastErr)
		}
		if c.closed.Load() {
			return nil, errClientClosed
		}
		delay := nextBackoff()
		c.logger.Warnn("Discovering the cluster",
			logger.NewStringField("seeds", strings.Join(c.seeds, ",")),
			logger.NewDurationField("retryDelay", delay),
			obskit.Error(lastErr),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
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

// lookupAddresses asks the seeds, then the nodes other than failingNode, for the cluster's current addresses.
// It is used when a node stops answering, for example because it was replaced by another one.
// An answer equal to the client's own list doesn't end the lookup: a source that hasn't refreshed its config yet,
// such as a pod reached through the Service, would hide the sources that did. The client's own list is returned
// only when every source that answered returned it.
func (c *Client) lookupAddresses(ctx context.Context, failingNode int64) ([]string, error) {
	c.mu.RLock()
	current := slices.Clone(c.config.Addresses)
	c.mu.RUnlock()

	var (
		lastErr  error
		answered bool // a source answered with the client's own list
	)
	differs := func(addresses []string, err error) bool {
		switch {
		case err != nil:
			lastErr = err
		case slices.Equal(addresses, current):
			answered = true
		case len(addresses) > 0:
			return true
		}
		return false
	}
	for _, seed := range c.seeds {
		if addresses, err := c.askForAddresses(ctx, seed); differs(addresses, err) {
			return addresses, nil
		}
	}

	c.mu.RLock()
	others := make([]*connectionPool, 0, len(c.pools))
	for i, pool := range c.pools {
		if int64(i) != failingNode {
			others = append(others, pool)
		}
	}
	c.mu.RUnlock()
	for _, pool := range others {
		client, _, _ := pool.getClient()
		if addresses, err := askNodeForAddresses(ctx, client); differs(addresses, err) {
			return addresses, nil
		}
	}
	if answered {
		return current, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no node returned any address")
	}
	return nil, lastErr
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
		c.clearFailure(int64(i)) // a new address starts with a clean slate
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

// noteFailure records that a node failed without answering in an attempt that started at attemptStart. It returns
// true when the node has been failing for DeadNodeLookupAfter, across requests, so that the caller looks the
// addresses up again. The time counts from the start of the first failing attempt: against an address that drops
// packets one attempt already blocks for MinConnectTimeout. It never counts from before the node last answered or
// got a new address, so an attempt still blocked on a replaced address doesn't count against the new one.
func (c *Client) noteFailure(nodeID int64, attemptStart time.Time) bool {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	if c.failingSince == nil {
		c.failingSince = make(map[int64]time.Time)
	}
	now := time.Now()
	since, ok := c.failingSince[nodeID]
	if !ok {
		since = attemptStart
		if cleared, ok := c.clearedAt[nodeID]; ok && cleared.After(since) {
			since = cleared
		}
		c.failingSince[nodeID] = since
	}
	if now.Sub(since) < c.config.DeadNodeLookupAfter {
		return false
	}
	c.failingSince[nodeID] = now // the next lookup waits for another DeadNodeLookupAfter
	return true
}

// clearFailure forgets that a node was failing, once it answers or its address changes.
func (c *Client) clearFailure(nodeID int64) {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	delete(c.failingSince, nodeID)
	if c.clearedAt == nil {
		c.clearedAt = make(map[int64]time.Time)
	}
	c.clearedAt[nodeID] = time.Now()
}

// callNode sends a request to a node until it succeeds, the context is done, or the cluster size changes.
//   - A failed response carries the node's address list: when it differs from the client's, the client adopts it.
//   - When the node doesn't answer for DeadNodeLookupAfter, the client asks other nodes for the current list.
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

		attemptStart := time.Now()
		resp, respErr := call(ctx, client)
		if respErr == nil {
			addresses := resp.GetNodesAddresses()
			if resp.GetClusterSize() != clusterSize && len(addresses) > 0 {
				return zero, &errClusterSizeChanged{nodesAddresses: addresses}
			}
			c.clearFailure(nodeID)
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
			if c.noteFailure(nodeID, attemptStart) {
				if addresses, err := c.lookupAddresses(ctx, nodeID); err == nil {
					sizeChanged, err := c.applyAddresses(addresses)
					if err != nil {
						return zero, err
					}
					if sizeChanged {
						return zero, &errClusterSizeChanged{nodesAddresses: addresses}
					}
				}
			}
		}

		// If retry is disabled, return immediately after first attempt
		if c.config.RetryPolicy.Disabled {
			if respErr != nil {
				return zero, fmt.Errorf("failed to %s keys from node %d: %w", method, nodeID, respErr)
			}
			return zero, fmt.Errorf("failed to %s keys from node %d with error code %s",
				method, nodeID, resp.GetErrorCode().String())
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
