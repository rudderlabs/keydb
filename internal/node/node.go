package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/keydb/internal/cachettl"
	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
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
func NewService(config Config) (*Service, error) {
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
		config: config,
		caches: make(map[uint32]*cachettl.Cache[string, bool]),
	}

	// Initialize caches for all hash ranges this node handles
	if err := service.initCaches(); err != nil {
		return nil, err
	}

	// Start background snapshot creation
	// TODO this should be moved also it's missing a context
	go service.snapshotLoop()

	return service, nil
}

// initCaches initializes the caches for all hash ranges this node handles
func (s *Service) initCaches() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get hash ranges for this node
	ranges := hash.GetNodeHashRanges(s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)

	// Create a cache for each hash range
	for _, r := range ranges {
		// Check if we already have a cache for this range
		if _, exists := s.caches[r]; exists {
			continue
		}

		// Create a new cache with no TTL refresh
		cache := cachettl.New[string, bool](cachettl.WithNoRefreshTTL)
		s.caches[r] = cache

		// Try to load snapshot for this range
		if err := s.loadSnapshot(r); err != nil {
			// Continue anyway, we'll start with an empty cache
			log.Printf("Warning: failed to load snapshot for range %d: %v", r, err)
		}
	}

	// Remove caches and key maps for ranges this node no longer handles
	for r := range s.caches {
		shouldHandle := false
		for _, nodeRange := range ranges {
			if r == nodeRange {
				shouldHandle = true
				break
			}
		}

		if !shouldHandle {
			delete(s.caches, r)
		}
	}

	return nil
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
	for _, r := range ranges {
		hashRanges = append(hashRanges, &pb.HashRange{
			RangeId: r,
			Start:   r,
			End:     r + 1,
		})
	}

	// Count total keys
	var keysCount uint64
	for _, r := range ranges {
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

// Scale implements the Scale RPC method
func (s *Service) Scale(ctx context.Context, req *pb.ScaleRequest) (*pb.ScaleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.scaling {
		return &pb.ScaleResponse{
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

	// Create snapshots before scaling
	if err := s.createSnapshots(); err != nil {
		s.scaling = false
		return &pb.ScaleResponse{
			Success:             false,
			ErrorMessage:        fmt.Sprintf("failed to create snapshots: %v", err),
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// Save the previous cluster size
	previousClusterSize := s.config.ClusterSize

	// Update cluster size
	s.config.ClusterSize = req.NewClusterSize

	// Reinitialize caches for the new cluster size
	if err := s.initCaches(); err != nil {
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

	// Note: We don't clear the scaling flag here anymore.
	// The scaling flag will be cleared when the ScaleComplete RPC is called.

	return &pb.ScaleResponse{
		Success:             true,
		PreviousClusterSize: previousClusterSize,
		NewClusterSize:      s.config.ClusterSize,
	}, nil
}

// snapshotLoop periodically creates snapshots
func (s *Service) snapshotLoop() {
	ticker := time.NewTicker(time.Duration(s.config.SnapshotInterval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		if !s.scaling {
			if err := s.createSnapshots(); err != nil {
				log.Printf("Error creating snapshots: %v", err)
			}
		}
		s.mu.Unlock()
	}
}

// ScaleComplete implements the ScaleComplete RPC method
func (s *Service) ScaleComplete(ctx context.Context, req *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we're actually in scaling mode
	if !s.scaling {
		return &pb.ScaleCompleteResponse{
			Success: false,
		}, nil
	}

	// Clear the scaling flag
	s.scaling = false

	return &pb.ScaleCompleteResponse{
		Success: true,
	}, nil
}

// createSnapshots creates snapshots for all hash ranges this node handles
func (s *Service) createSnapshots() error {
	// Get hash ranges for this node
	ranges := hash.GetNodeHashRanges(s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)

	// Create a snapshot for each hash range
	for _, r := range ranges {
		cache, exists := s.caches[r]
		if !exists {
			return fmt.Errorf("no cache for hash range %d", r)
		}

		if err := s.createSnapshot(r, cache); err != nil {
			return fmt.Errorf("failed to create snapshot for range %d: %w", r, err)
		}
	}

	s.lastSnapshotTime = time.Now()
	return nil
}

// createSnapshot creates a snapshot for a specific hash range
func (s *Service) createSnapshot(hashRange uint32, cache *cachettl.Cache[string, bool]) error {
	// Create snapshot file
	// TODO avoid reflection for snapshotPath
	snapshotPath := filepath.Join(s.config.SnapshotDir, fmt.Sprintf("node_%d_range_%d.snapshot", s.config.NodeID, hashRange))
	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	// TODO fix createSnapshot

	//// Get all keys from our key map
	//keys := s.keyMap[hashRange]
	//
	//// Write each key to the snapshot file
	//for key := range keys {
	//	// In a real implementation, we would also save the TTL
	//	// For simplicity, we're just saving the key
	//	if _, err := fmt.Fprintln(file, key); err != nil {
	//		return fmt.Errorf("failed to write to snapshot file: %w", err)
	//	}
	//}

	return nil
}

// loadSnapshot loads a snapshot for a specific hash range
func (s *Service) loadSnapshot(hashRange uint32) error {
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
	defer file.Close()

	// Get the cache for this hash range
	cache, exists := s.caches[hashRange]
	if !exists {
		return fmt.Errorf("no cache for hash range %d", hashRange)
	}

	// In a real implementation, we would also load the TTL
	// For simplicity, we're using a default TTL of 1 hour
	ttl := 1 * time.Hour

	// Read each key from the snapshot file
	var key string
	for {
		_, err := fmt.Fscanln(file, &key)
		if err != nil {
			break // End of file or error
		}

		// Store the key in the cache
		cache.Put(key, true, ttl)

		// Also store the key in our key map
		//s.keyMap[hashRange][key] = struct{}{} // TODO fix
	}

	return nil
}
