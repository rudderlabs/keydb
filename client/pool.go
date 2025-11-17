package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

var (
	// ErrPoolClosed is returned when trying to get a connection from a closed pool
	ErrPoolClosed = errors.New("connection pool is closed")
	// ErrPoolTimeout is returned when getting a connection from the pool times out
	ErrPoolTimeout = errors.New("timeout waiting for connection from pool")
)

// PooledConnection wraps a gRPC connection with metadata
type PooledConnection struct {
	conn   *grpc.ClientConn
	client pb.NodeServiceClient
	nodeID int
}

// ConnectionPool manages a pool of gRPC connections to a single node
type ConnectionPool struct {
	nodeID    int
	address   string
	size      int
	pool      chan *PooledConnection
	mu        sync.RWMutex
	closed    atomic.Bool
	connOpts  []grpc.DialOption
	log       logger.Logger
	createdAt atomic.Int64
}

// NewConnectionPool creates a new connection pool for a specific node
func NewConnectionPool(ctx context.Context, nodeID int, address string, size int, connOpts []grpc.DialOption, log logger.Logger) (*ConnectionPool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("pool size must be positive, got %d", size)
	}

	cp := &ConnectionPool{
		nodeID:   nodeID,
		address:  address,
		size:     size,
		pool:     make(chan *PooledConnection, size),
		connOpts: connOpts,
		log:      log,
	}

	// Pre-create all connections in the pool
	for i := 0; i < size; i++ {
		conn, client, err := cp.createConnection(ctx)
		if err != nil {
			// Close any connections we've already created
			cp.Close()
			return nil, fmt.Errorf("creating connection %d/%d for node %d: %w", i+1, size, nodeID, err)
		}

		cp.pool <- &PooledConnection{
			conn:   conn,
			client: client,
			nodeID: nodeID,
		}
	}

	cp.log.Infon("connection pool created",
		logger.NewIntField("nodeID", int64(nodeID)),
		logger.NewStringField("address", address),
		logger.NewIntField("poolSize", int64(size)),
	)

	return cp, nil
}

// createConnection creates a new gRPC connection and client
func (cp *ConnectionPool) createConnection(ctx context.Context) (*grpc.ClientConn, pb.NodeServiceClient, error) {
	conn, err := grpc.NewClient(cp.address, cp.connOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("creating gRPC client to %s: %w", cp.address, err)
	}

	client := pb.NewNodeServiceClient(conn)

	cp.createdAt.Add(1)

	return conn, client, nil
}

// Get acquires a connection from the pool
// Blocks until a connection is available or context is cancelled
func (cp *ConnectionPool) Get(ctx context.Context) (*PooledConnection, error) {
	if cp.closed.Load() {
		return nil, ErrPoolClosed
	}

	select {
	case conn := <-cp.pool:
		if conn == nil {
			return nil, ErrPoolClosed
		}
		return conn, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil, ErrPoolTimeout
		}
		return nil, ctx.Err()
	}
}

// Put returns a connection to the pool
func (cp *ConnectionPool) Put(conn *PooledConnection) error {
	if conn == nil {
		return errors.New("cannot return nil connection to pool")
	}

	if cp.closed.Load() {
		// Pool is closed, close this connection
		return conn.conn.Close()
	}

	select {
	case cp.pool <- conn:
		return nil
	default:
		// Pool is full (shouldn't happen in normal operation)
		// Close the connection to prevent resource leak
		cp.log.Warnn("pool is full, closing excess connection",
			logger.NewIntField("nodeID", int64(cp.nodeID)),
			logger.NewStringField("address", cp.address),
		)
		return conn.conn.Close()
	}
}

// Close closes all connections in the pool
func (cp *ConnectionPool) Close() error {
	if cp.closed.Swap(true) {
		// Already closed
		return nil
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	close(cp.pool)

	var errs []error
	for conn := range cp.pool {
		if err := conn.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing connection for node %d: %w", cp.nodeID, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pool: %v", errs)
	}

	cp.log.Infon("connection pool closed",
		logger.NewIntField("nodeID", int64(cp.nodeID)),
		logger.NewStringField("address", cp.address),
		logger.NewIntField("totalCreated", cp.createdAt.Load()),
	)

	return nil
}

// Size returns the configured size of the pool
func (cp *ConnectionPool) Size() int {
	return cp.size
}

// Available returns the number of connections currently available in the pool
func (cp *ConnectionPool) Available() int {
	return len(cp.pool)
}

// IsClosed returns whether the pool is closed
func (cp *ConnectionPool) IsClosed() bool {
	return cp.closed.Load()
}

// NodeID returns the node ID this pool is associated with
func (cp *ConnectionPool) NodeID() int {
	return cp.nodeID
}

// Address returns the address this pool connects to
func (cp *ConnectionPool) Address() string {
	return cp.address
}

// executeWithRetry executes a function with a pooled connection, handling retries
func (cp *ConnectionPool) executeWithRetry(ctx context.Context, operation string, fn func(*PooledConnection) error) error {
	pc, err := cp.Get(ctx)
	if err != nil {
		cp.log.Errorn("acquiring connection from pool",
			logger.NewIntField("nodeID", int64(cp.nodeID)),
			logger.NewStringField("operation", operation),
			obskit.Error(err),
		)
		return fmt.Errorf("acquiring connection from pool: %w", err)
	}
	defer func() {
		if putErr := cp.Put(pc); putErr != nil {
			cp.log.Warnn("returning connection to pool",
				logger.NewIntField("nodeID", int64(cp.nodeID)),
				obskit.Error(putErr),
			)
		}
	}()

	return fn(pc)
}
