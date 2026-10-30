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
	// resolveAddressTimeout bounds the DNS lookups of config.Addresses
	resolveAddressTimeout = 5 * time.Second
	// getNodeInfoTimeout bounds each GetNodeInfo call that asks a node for the cluster's addresses
	getNodeInfoTimeout = 5 * time.Second

	// notReadyMaxRetryDelay caps the wait before retrying a node that answered NOT_READY. A shadow becomes ACTIVE
	// as soon as its kubelet refreshes the config and a NOT_READY answer is cheap, so this bounds how long a request
	// that kept retrying waits once the node is promoted, instead of a full backoff of up to 1.5×MaxInterval.
	notReadyMaxRetryDelay = 2 * time.Second
)

var (
	// errClientClosed is returned by requests made or still retrying after Close
	errClientClosed = errors.New("keydb client closed")
	// errClusterChanged is returned by a call to a node after the client adopted another node list: the request's
	// keys must be routed again
	errClusterChanged = errors.New("cluster changed")
)

// nodeResponse is what Get and Put responses have in common
type nodeResponse interface {
	GetClusterSize() int64
	GetErrorCode() pb.ErrorCode
}

// waitForNodes finds the nodes for a request made while the client has no node list, because its addresses resolved
// to no node when it was created. It asks the nodes behind them for the list, retrying with backoff until one answers
// or ctx ends, unless retries are disabled. Every failed attempt it retries counts in
// keydb_client_discovery_retries_total.
func (c *Client) waitForNodes(ctx context.Context) error {
	nextBackoff := c.getNextBackoffFunc()
	for {
		if c.closed.Load() {
			return errClientClosed
		}
		addresses, err := c.discover(ctx)
		if err == nil {
			_, err = c.applyAddresses(addresses)
			return err
		}
		if c.config.RetryPolicy.Disabled {
			return err
		}
		c.metrics.discoveryRetries.Increment()
		delay := nextBackoff()
		c.logger.Warnn("Discovering the node addresses",
			logger.NewDurationField("retryDelay", delay), obskit.Error(err),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// discover resolves config.Addresses and asks every node behind them for the cluster's addresses at once. It
// returns the first list a node answers with. A node that hasn't reloaded its config yet may answer first with an
// old list: requests then fail again, and the next discovery may hear from another node first.
func (c *Client) discover(ctx context.Context) ([]string, error) {
	nodes := c.resolveNodes(ctx)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no node behind %s", strings.Join(c.config.Addresses, ","))
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // stops the calls still running once a node answered

	type answer struct {
		addresses []string
		err       error
	}
	answers := make(chan answer, len(nodes))
	for _, node := range nodes {
		go func() {
			addresses, err := c.askForAddresses(ctx, node)
			answers <- answer{addresses, err}
		}()
	}
	errs := make([]error, 0, len(nodes))
	for range nodes {
		a := <-answers
		if a.err == nil {
			return a.addresses, nil
		}
		errs = append(errs, a.err)
	}
	return nil, errors.Join(errs...)
}

// resolveNodes returns the addresses of the nodes behind config.Addresses, skipping the ones that don't resolve.
func (c *Client) resolveNodes(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(ctx, resolveAddressTimeout)
	defer cancel()
	var nodes []string
	for _, address := range c.config.Addresses {
		resolved, err := c.resolve(ctx, address)
		if err != nil {
			c.logger.Warnn("Resolving a configured address",
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
	ctx, cancel := context.WithTimeout(ctx, getNodeInfoTimeout)
	defer cancel()
	info, err := pb.NewNodeServiceClient(conn).GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("asking %s for the node addresses: %w", address, err)
	}
	if len(info.NodesAddresses) == 0 {
		return nil, fmt.Errorf("%s answered with no node address", address)
	}
	return info.NodesAddresses, nil
}

// applyAddresses adopts a new list of node addresses. Only the connection pools whose address changed are
// replaced. It returns true when the list changed.
func (c *Client) applyAddresses(addresses []string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return false, errClientClosed // never reopen connections after Close
	}
	if slices.Equal(addresses, c.nodeAddresses) && len(c.pools) == len(addresses) {
		return false, nil
	}

	dialOpts := c.getGrpcDialOptions()
	for i, addr := range addresses {
		if i < len(c.nodeAddresses) && c.nodeAddresses[i] == addr {
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
	for i := len(addresses); i < len(c.nodeAddresses) || i < len(c.pools); i++ {
		if pool, ok := c.pools[i]; ok {
			_ = pool.close()
			delete(c.pools, i)
			delete(c.metrics.connectionOps, i)
		}
	}

	oldClusterSize := int64(len(c.nodeAddresses))
	newClusterSize := int64(len(addresses))
	c.nodeAddresses = slices.Clone(addresses)
	if newClusterSize != oldClusterSize {
		c.hash = hash.New(newClusterSize, c.config.TotalHashRanges)
	}

	c.logger.Infon("Adopted new node addresses",
		logger.NewIntField("oldClusterSize", oldClusterSize),
		logger.NewIntField("newClusterSize", newClusterSize),
		logger.NewStringField("nodesAddresses", strings.Join(addresses, ",")),
	)
	return true, nil
}

// routingInfo returns the hash and cluster size requests must use.
func (c *Client) routingInfo() (*hash.Hash, int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hash, int64(len(c.nodeAddresses))
}

// nodeClient returns the next pooled connection to a node, counting the operation. It returns errClusterChanged when
// the client adopted a list of another size since the request was routed with clusterSize.
func (c *Client) nodeClient(nodeID, clusterSize int64) (pb.NodeServiceClient, *grpc.ClientConn, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if int64(len(c.nodeAddresses)) != clusterSize {
		return nil, nil, errClusterChanged
	}
	pool, ok := c.pools[int(nodeID)]
	if !ok {
		// this should never happen unless clusterSize is updated and the pools map isn't
		// or if there is a bug in the hashing function
		return nil, nil, fmt.Errorf("no connection pool for node %d", nodeID)
	}
	client, conn, connID := pool.getClient()
	if counter, ok := c.metrics.connectionOps[int(nodeID)][connID]; ok {
		counter.Increment()
	}
	return client, conn, nil
}

// routeUntilStable sends a request with the client's current routing until it no longer fails because the client
// adopted another node list. A client without a node list waits for the nodes first (see waitForNodes). The first
// re-route goes at once, every later one waits for the request's backoff first, so that nodes that hand out different
// lists don't make the request spin.
func (c *Client) routeUntilStable(ctx context.Context, send func() error) error {
	if c.ClusterSize() == 0 {
		if err := c.waitForNodes(ctx); err != nil {
			return err
		}
	}
	var nextBackoff func() time.Duration
	for {
		err := send()
		if !errors.Is(err, errClusterChanged) {
			return err
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

// listIsWrong tells whether a node's answer shows that the client's node list is wrong: the node can't be reached,
// doesn't own the keys, isn't ready to serve them, or sees a cluster of another size.
func listIsWrong[R nodeResponse](resp R, err error, clusterSize int64) bool {
	if err != nil {
		return status.Code(err) == codes.Unavailable
	}
	switch resp.GetErrorCode() {
	case pb.ErrorCode_WRONG_NODE, pb.ErrorCode_NOT_READY:
		return true
	default:
		return resp.GetClusterSize() != clusterSize
	}
}

// callNode sends a request to a node until it succeeds, the context is done, or the client adopts another node list.
//   - When the answer shows that the client's node list is wrong (see listIsWrong), the client asks every node behind
//     config.Addresses for the list and adopts the first answer. A changed list re-routes the keys at once.
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
		client, conn, err := c.nodeClient(nodeID, clusterSize)
		if err != nil {
			return zero, err
		}

		resp, respErr := call(ctx, client)
		if respErr == nil && resp.GetClusterSize() == clusterSize && succeeded(resp) {
			return resp, nil
		}
		if listIsWrong(resp, respErr, clusterSize) {
			addresses, err := c.discover(ctx)
			if err != nil {
				c.logger.Warnn("Discovering the node addresses", obskit.Error(err))
			} else if changed, err := c.applyAddresses(addresses); err != nil {
				return zero, err
			} else if changed {
				return zero, errClusterChanged
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
