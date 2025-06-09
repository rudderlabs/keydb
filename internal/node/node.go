package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/keydb/internal/cachettl"
	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

const (
	// DefaultTotalHashRanges is the default number of hash ranges
	DefaultTotalHashRanges = 128

	// DefaultSnapshotInterval is the default interval for creating snapshots (in seconds)
	DefaultSnapshotInterval = 60
)

// Config holds the configuration for a node
type Config struct {
	// NodeID is the ID of this node (0-based)
	NodeID uint32

	// ClusterSize is the total number of nodes in the cluster
	ClusterSize uint32

	// TotalHashRanges is the total number of hash ranges
	TotalHashRanges uint32

	// SnapshotInterval is the interval for creating snapshots (in seconds)
	SnapshotInterval int

	// SnapshotDir is the directory where snapshots are stored
	SnapshotDir string
}

// Service implements the NodeService gRPC service
type Service struct {
	pb.UnimplementedNodeServiceServer

	now func() time.Time

	config Config

	// caches is a map of hash range ID to cache
	caches map[uint32]*cachettl.Cache[string, bool]

	// mu protects the caches and scaling operations
	mu sync.RWMutex

	// scaling indicates if a scaling operation is in progress
	scaling bool

	// lastSnapshotTime is the timestamp of the last snapshot
	lastSnapshotTime time.Time
}

// NewService creates a new NodeService
func NewService(ctx context.Context, config Config) (*Service, error) {
	// Set defaults for unspecified config values
	if config.TotalHashRanges == 0 {
		config.TotalHashRanges = DefaultTotalHashRanges
	}

	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = DefaultSnapshotInterval
	}

	if config.SnapshotDir == "" {
		config.SnapshotDir = "snapshots"
	}

	// Create snapshot directory if it doesn't exist
	if err := os.MkdirAll(config.SnapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	service := &Service{
		now:    time.Now,
		config: config,
		caches: make(map[uint32]*cachettl.Cache[string, bool]),
	}

	// Initialize caches for all hash ranges this node handles
	if err := service.initCaches(ctx); err != nil {
		return nil, err
	}

	// Start background snapshot creation
	go service.snapshotLoop(ctx)

	return service, nil
}

// snapshotLoop periodically creates snapshots
func (s *Service) snapshotLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.config.SnapshotInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				if s.scaling {
					return
				}
				if s.now().Sub(s.lastSnapshotTime) < time.Duration(s.config.SnapshotInterval) { // TODO write a test for this
					// we created a snapshot already recently due to a scaling operation
					return
				}
				if err := s.createSnapshots(); err != nil {
					log.Printf("Error creating snapshots: %v", err)
				}
			}()
		}
	}
}

// initCaches initializes the caches for all hash ranges this node handles
func (s *Service) initCaches(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get hash ranges for this node
	ranges := hash.GetNodeHashRanges(s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)

	// Remove caches and key maps for ranges this node no longer handles
	for r := range s.caches {
		if _, shouldHandle := ranges[r]; !shouldHandle {
			delete(s.caches, r)
		}
	}

	// Create a cache for each hash range
	group, _ := kitsync.NewEagerGroup(ctx, len(ranges))
	for r := range ranges {
		// Check if we already have a cache for this range
		if _, exists := s.caches[r]; exists {
			continue
		}

		// Create a new cache with no TTL refresh
		cache := cachettl.New[string, bool](cachettl.WithNoRefreshTTL)
		s.caches[r] = cache

		group.Go(func() error {
			// Try to load snapshot for this range
			if err := s.loadSnapshot(ctx, r, cache); err != nil {
				return fmt.Errorf("failed to load snapshot for range %d: %w", r, err)
			}
			return nil
		})
	}

	return group.Wait()
}

// Get implements the Get RPC method
func (s *Service) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.scaling {
		return &pb.GetResponse{
			ErrorCode:   pb.ErrorCode_SCALING,
			ClusterSize: s.config.ClusterSize,
		}, nil
	}

	response := &pb.GetResponse{
		Exists:      make([]bool, len(req.Keys)),
		ClusterSize: s.config.ClusterSize,
	}

	for i, key := range req.Keys {
		// Determine which node should handle this key
		hashRange, nodeID := hash.GetNodeNumber(key, s.config.ClusterSize, s.config.TotalHashRanges)

		// Check if this node should handle this key
		if nodeID != s.config.NodeID {
			response.ErrorCode = pb.ErrorCode_WRONG_NODE
			return response, nil
		}

		// Get the cache for this hash range
		cache, exists := s.caches[hashRange]
		if !exists {
			// This should not happen if our hash functions are correct
			//return nil, status.Errorf(codes.Internal, "no cache for hash range %d", hashRange)
			response.ErrorCode = pb.ErrorCode_INTERNAL_ERROR
			return response, nil
		}

		// Check if the key exists in the cache
		response.Exists[i] = cache.Get(key)
	}

	return response, nil
}

// Put implements the Put RPC method
func (s *Service) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.scaling {
		return &pb.PutResponse{
			Success:     false,
			ErrorCode:   pb.ErrorCode_SCALING,
			ClusterSize: s.config.ClusterSize,
		}, nil
	}

	for _, item := range req.Items {
		// Determine which node should handle this key
		hashRange, nodeID := hash.GetNodeNumber(item.Key, s.config.ClusterSize, s.config.TotalHashRanges)

		// Check if this node should handle this key
		if nodeID != s.config.NodeID {
			return &pb.PutResponse{
				Success:     false,
				ErrorCode:   pb.ErrorCode_WRONG_NODE,
				ClusterSize: s.config.ClusterSize,
			}, nil
		}

		// Get the cache for this hash range
		cache, exists := s.caches[hashRange]
		if !exists {
			// This should not happen if our hash functions are correct
			return &pb.PutResponse{
				Success:     false,
				ErrorCode:   pb.ErrorCode_INTERNAL_ERROR,
				ClusterSize: s.config.ClusterSize,
			}, nil
		}

		// Store the key in the cache with the specified TTL
		ttl := time.Duration(item.TtlSeconds) * time.Second
		cache.Put(item.Key, true, ttl)
	}

	return &pb.PutResponse{
		Success:     true,
		ClusterSize: s.config.ClusterSize,
	}, nil
}

// GetNodeInfo implements the GetNodeInfo RPC method
func (s *Service) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If a specific node ID was requested, check if it matches this node
	if req.NodeId != 0 && req.NodeId != s.config.NodeID {
		return nil, status.Errorf(codes.InvalidArgument, "node ID mismatch: requested %d, this node is %d", req.NodeId, s.config.NodeID)
	}

	// Get hash ranges for this node
	ranges := hash.GetNodeHashRanges(s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)

	// Convert to proto hash ranges
	hashRanges := make([]*pb.HashRange, 0, len(ranges))
	for r := range ranges {
		hashRanges = append(hashRanges, &pb.HashRange{
			RangeId: r,
			Start:   r,
			End:     r + 1,
		})
	}

	// Count total keys
	var keysCount uint64
	for r := range ranges {
		cache := s.caches[r]
		keysCount += uint64(cache.Len())
	}

	return &pb.GetNodeInfoResponse{
		NodeId:                s.config.NodeID,
		ClusterSize:           s.config.ClusterSize,
		HashRanges:            hashRanges,
		KeysCount:             keysCount,
		LastSnapshotTimestamp: uint64(s.lastSnapshotTime.Unix()),
	}, nil
}

// CreateSnapshot implements the CreateSnapshot RPC method
func (s *Service) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.CreateSnapshotResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.scaling {
		return &pb.CreateSnapshotResponse{
			Success:      false,
			ErrorMessage: "scaling operation in progress",
			NodeId:       s.config.NodeID,
		}, nil
	}

	// Create snapshots for all hash ranges this node handles
	if err := s.createSnapshots(); err != nil {
		return &pb.CreateSnapshotResponse{
			Success:      false,
			ErrorMessage: err.Error(),
			NodeId:       s.config.NodeID,
		}, nil
	}

	return &pb.CreateSnapshotResponse{
		Success: true,
		NodeId:  s.config.NodeID,
	}, nil
}

// createSnapshots creates snapshots for all hash ranges this node handles
func (s *Service) createSnapshots() error {
	// Get hash ranges for this node
	ranges := hash.GetNodeHashRanges(s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)

	// Create a snapshot for each hash range
	group, _ := kitsync.NewEagerGroup(context.TODO(), len(ranges))
	for r := range ranges {
		group.Go(func() error {
			cache, exists := s.caches[r]
			if !exists {
				return fmt.Errorf("no cache for hash range %d", r)
			}

			if err := s.createSnapshot(r, cache); err != nil {
				return fmt.Errorf("failed to create snapshot for range %d: %w", r, err)
			}
			return nil
		})
	}

	s.lastSnapshotTime = time.Now()

	return group.Wait()
}

// createSnapshot creates a snapshot for a specific hash range
func (s *Service) createSnapshot(hashRange uint32, cache *cachettl.Cache[string, bool]) error {
	// Create snapshot file
	filename := "node_" + strconv.Itoa(int(s.config.NodeID)) + "_range_" + strconv.Itoa(int(hashRange)) + ".snapshot"
	snapshotPath := filepath.Join(s.config.SnapshotDir, filename)
	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file %q: %w", filename, err)
	}
	defer func() { _ = file.Close() }()

	return cachettl.CreateSnapshot(file, cache)
}

// Scale implements the Scale RPC method
func (s *Service) Scale(ctx context.Context, req *pb.ScaleRequest) (*pb.ScaleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.scaling {
		return &pb.ScaleResponse{ // TODO we could have auto-healing here easily by attempting the scaling anyway
			Success:             false,
			ErrorMessage:        "scaling operation already in progress",
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// Validate new cluster size
	if req.NewClusterSize == 0 {
		return &pb.ScaleResponse{
			Success:             false,
			ErrorMessage:        "new cluster size must be greater than 0",
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// If the cluster size is not changing, do nothing
	if req.NewClusterSize == s.config.ClusterSize {
		return &pb.ScaleResponse{
			Success:             true,
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// Set scaling flag
	s.scaling = true

	// Save the previous cluster size
	previousClusterSize := s.config.ClusterSize

	// Update cluster size
	s.config.ClusterSize = req.NewClusterSize

	// Reinitialize caches for the new cluster size
	// TODO check this ctx, a client cancellation from the Operator client might leave the node in a unwanted state
	if err := s.initCaches(ctx); err != nil {
		// Revert to previous cluster size on error
		s.config.ClusterSize = previousClusterSize
		s.scaling = false
		return &pb.ScaleResponse{
			Success:             false,
			ErrorMessage:        fmt.Sprintf("failed to initialize caches: %v", err),
			PreviousClusterSize: previousClusterSize,
			NewClusterSize:      previousClusterSize,
		}, nil
	}

	// Note: We don't clear the scaling flag here.
	// The scaling flag will be cleared when the ScaleComplete RPC is called.

	return &pb.ScaleResponse{
		Success:             true,
		PreviousClusterSize: previousClusterSize,
		NewClusterSize:      s.config.ClusterSize,
	}, nil
}

// ScaleComplete implements the ScaleComplete RPC method
func (s *Service) ScaleComplete(ctx context.Context, req *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// No need to check the s.scaling value, let's be optimistic and go ahead here for auto-healing purposes

	s.scaling = false

	return &pb.ScaleCompleteResponse{Success: true}, nil
}

// loadSnapshot loads a snapshot for a specific hash range
func (s *Service) loadSnapshot(ctx context.Context, hashRange uint32, cache *cachettl.Cache[string, bool]) error {
	// Check if snapshot file exists
	snapshotPath := filepath.Join(s.config.SnapshotDir, fmt.Sprintf("node_%d_range_%d.snapshot", s.config.NodeID, hashRange))
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		// No snapshot file, nothing to load
		return nil
	}

	// Open snapshot file
	file, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer func() { _ = file.Close() }()

	return cachettl.LoadSnapshot(file, cache)
}
