package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	// DefaultTotalHashRanges is the default number of hash ranges
	DefaultTotalHashRanges = 128

	// DefaultSnapshotInterval is the default interval for creating snapshots (in seconds)
	DefaultSnapshotInterval = 60 * time.Second
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
	SnapshotInterval time.Duration

	// Addresses is a list of node addresses that this node will advertise to clients
	Addresses []string
}

// Service implements the NodeService gRPC service
type Service struct {
	pb.UnimplementedNodeServiceServer

	now func() time.Time

	config Config

	caches       map[uint32]Cache
	cacheFactory cacheFactory

	// mu protects the caches and scaling operations
	mu sync.RWMutex

	// scaling indicates if a scaling operation is in progress
	scaling bool

	// lastSnapshotTime is the timestamp of the last snapshot
	lastSnapshotTime time.Time

	waitGroup sync.WaitGroup

	storage cloudStorage

	logger logger.Logger
}

// Cache is an interface for a key-value store with TTL support
type Cache interface {
	// Get returns the values associated with the keys and an error if the operation failed
	Get(keys []string) ([]bool, error)

	// Put adds or updates elements inside the cache with the specified TTL and returns an error if the operation failed
	Put(keys []string, ttl time.Duration) error

	// Len returns the number of elements in the cache
	Len() int

	// String returns a string representation of the cache
	String() string

	// CreateSnapshot writes the cache contents to the provided writer
	CreateSnapshot(w io.Writer) error

	// LoadSnapshot reads the cache contents from the provided reader
	LoadSnapshot(r io.Reader) error

	// Close releases any resources associated with the cache and ensures proper cleanup. Returns an error if the operation fails.
	Close() error
}

type cacheFactory func(hashRange uint32) (Cache, error)

type cloudStorageReader interface {
	Download(ctx context.Context, output io.WriterAt, key string) error
}

type cloudStorageWriter interface {
	UploadReader(ctx context.Context, objName string, rdr io.Reader) (filemanager.UploadedFile, error)
}

type cloudStorage interface {
	cloudStorageReader
	cloudStorageWriter
}

// NewService creates a new NodeService
func NewService(
	ctx context.Context, config Config, cf cacheFactory, storage cloudStorage, log logger.Logger,
) (*Service, error) {
	// Set defaults for unspecified config values
	if config.TotalHashRanges == 0 {
		config.TotalHashRanges = DefaultTotalHashRanges
	}

	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = DefaultSnapshotInterval
	}

	service := &Service{
		now:          time.Now,
		config:       config,
		caches:       make(map[uint32]Cache),
		cacheFactory: cf,
		storage:      storage,
		logger: log.Withn(
			logger.NewIntField("nodeId", int64(config.NodeID)),
			logger.NewIntField("totalHashRanges", int64(config.TotalHashRanges)),
			logger.NewIntField("snapshotInterval", int64(config.SnapshotInterval.Seconds())),
		),
	}

	// Initialize caches for all hash ranges this node handles
	if err := service.initCaches(ctx); err != nil {
		return nil, err
	}

	// Start background snapshot creation
	service.waitGroup.Add(1)
	go service.snapshotLoop(ctx)

	return service, nil
}

func (s *Service) Close() {
	s.waitGroup.Wait()
	for _, cache := range s.caches {
		if err := cache.Close(); err != nil {
			s.logger.Errorn("Error closing cache", obskit.Error(err))
		}
	}
}

// snapshotLoop periodically creates snapshots
func (s *Service) snapshotLoop(ctx context.Context) {
	defer s.waitGroup.Done()

	ticker := time.NewTicker(s.config.SnapshotInterval)
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
				if s.now().Sub(s.lastSnapshotTime) < s.config.SnapshotInterval { // TODO write a test for this
					// we created a snapshot already recently due to a scaling operation
					return
				}
				if err := s.createSnapshots(ctx); err != nil {
					s.logger.Errorn("Error creating snapshots", obskit.Error(err))
				}
			}()
		}
	}
}

// initCaches initializes the caches for all hash ranges this node handles
func (s *Service) initCaches(ctx context.Context) error {
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
		cache, err := s.cacheFactory(r)
		if err != nil {
			return fmt.Errorf("failed to create cache for range %d: %w", r, err)
		}
		s.caches[r] = cache

		group.Go(func() error { // Try to load snapshot for this range
			if err := s.loadSnapshot(ctx, r, cache); err != nil {
				if errors.Is(err, filemanager.ErrKeyNotFound) {
					s.logger.Warnn("No cached snapshot for range", logger.NewIntField("range", int64(r)))
					return nil
				}
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

	response := &pb.GetResponse{
		ClusterSize:    s.config.ClusterSize,
		NodesAddresses: s.config.Addresses,
	}

	if s.scaling {
		response.ErrorCode = pb.ErrorCode_SCALING
		return response, nil
	}

	itemsByHashRange, indexes, err := hash.GetKeysByHashRangeWithIndexes(
		req.Keys, s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges,
	)
	if err != nil {
		if errors.Is(err, hash.ErrWrongNode) {
			response.ErrorCode = pb.ErrorCode_WRONG_NODE
			return response, nil
		}
		response.ErrorCode = pb.ErrorCode_INTERNAL_ERROR
		s.logger.Errorn("Error getting hashes for keys", obskit.Error(err))
		return response, nil
	}

	// Create a map to track the original indices of keys
	responseMu := sync.Mutex{}
	response.Exists = make([]bool, len(req.Keys))

	group, _ := kitsync.NewEagerGroup(ctx, len(itemsByHashRange))
	for hashRange, keys := range itemsByHashRange {
		group.Go(func() error {
			// Get the cache for this hash range
			cache, exists := s.caches[hashRange]
			if !exists {
				return fmt.Errorf("no cache for hash range %d", hashRange)
			}

			// Check if the keys exist in the cache
			existsValues, err := cache.Get(keys)
			if err != nil {
				return fmt.Errorf("failed to get keys from cache: %w", err)
			}

			// Update the response with the results
			responseMu.Lock()
			for i, key := range keys {
				response.Exists[indexes[key]] = existsValues[i]
			}
			responseMu.Unlock()

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		response.ErrorCode = pb.ErrorCode_INTERNAL_ERROR
		return response, nil
	}

	return response, nil
}

// Put implements the Put RPC method
func (s *Service) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := &pb.PutResponse{
		Success:        false,
		ClusterSize:    s.config.ClusterSize,
		NodesAddresses: s.config.Addresses,
	}

	if s.scaling {
		resp.ErrorCode = pb.ErrorCode_SCALING
		return resp, nil
	}

	// Group items by hash range
	itemsByHashRange, err := hash.GetKeysByHashRange(
		req.Keys, s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges,
	)
	if err != nil {
		if errors.Is(err, hash.ErrWrongNode) {
			resp.ErrorCode = pb.ErrorCode_WRONG_NODE
			return resp, nil
		}
		resp.ErrorCode = pb.ErrorCode_INTERNAL_ERROR
		s.logger.Errorn("Error getting hashes for keys", obskit.Error(err))
		return resp, nil
	}

	ttl := time.Duration(req.TtlSeconds) * time.Second
	group, _ := kitsync.NewEagerGroup(ctx, len(itemsByHashRange))
	for hashRange, keys := range itemsByHashRange {
		group.Go(func() error {
			// Get the cache for this hash range
			cache, exists := s.caches[hashRange]
			if !exists {
				return fmt.Errorf("no cache for hash range %d", hashRange)
			}

			// Store the keys in the cache with the specified TTL
			err := cache.Put(keys, ttl)
			if err != nil {
				return fmt.Errorf("failed to put keys into cache: %w", err)
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		resp.ErrorCode = pb.ErrorCode_INTERNAL_ERROR
		return resp, nil
	}

	resp.Success = true
	return resp, nil
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
		NodesAddresses:        s.config.Addresses,
		HashRanges:            hashRanges,
		KeysCount:             keysCount,
		LastSnapshotTimestamp: uint64(s.lastSnapshotTime.Unix()),
	}, nil
}

// CreateSnapshot implements the CreateSnapshot RPC method
// TODO this can be optimized a lot! For example we could snapshot on local disk every 10s and work only on the head
// and tail of the file (i.e. remove expired from head and append new entries).
// Then once a minute we can upload the whole file to S3.
// The file that we upload could be compressed, for example by using zstd with dictionaries.
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
	if err := s.createSnapshots(ctx); err != nil {
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
func (s *Service) createSnapshots(ctx context.Context) error {
	// Get hash ranges for this node
	ranges := hash.GetNodeHashRanges(s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)

	// Create a snapshot for each hash range
	group, ctx := kitsync.NewEagerGroup(context.TODO(), len(ranges))
	for r := range ranges {
		group.Go(func() error {
			cache, exists := s.caches[r]
			if !exists {
				return fmt.Errorf("no cache for hash range %d", r)
			}

			if err := s.createSnapshot(ctx, r, cache); err != nil {
				return fmt.Errorf("failed to create snapshot for range %d: %w", r, err)
			}
			return nil
		})
	}

	s.lastSnapshotTime = time.Now()

	return group.Wait()
}

// createSnapshot creates a snapshot for a specific hash range
func (s *Service) createSnapshot(ctx context.Context, hashRange uint32, cache Cache) error {
	// Create snapshot file
	filename := getSnapshotFilename(hashRange)

	buf := new(bytes.Buffer)
	err := cache.CreateSnapshot(buf)
	if err != nil {
		return fmt.Errorf("failed to create snapshot for range %d: %w", hashRange, err)
	}

	if len(buf.Bytes()) == 0 {
		return nil // no data to upload
	}

	_, err = s.storage.UploadReader(ctx, filename, buf)
	if err != nil {
		return fmt.Errorf("failed to upload snapshot file %q: %w", filename, err)
	}

	return nil
}

// Scale implements the Scale RPC method
func (s *Service) Scale(ctx context.Context, req *pb.ScaleRequest) (*pb.ScaleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log := s.logger.Withn(logger.NewIntField("newClusterSize", int64(req.NewClusterSize)))
	log.Debugn("Scale request received")

	if s.scaling {
		log.Debugn("Scaling operation already in progress")
		return &pb.ScaleResponse{ // TODO we could have auto-healing here easily by attempting the scaling anyway
			Success:             false,
			ErrorMessage:        "scaling operation already in progress",
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// Validate new cluster size
	if req.NewClusterSize == 0 {
		log.Debugn("New cluster size must be greater than 0")
		return &pb.ScaleResponse{
			Success:             false,
			ErrorMessage:        "new cluster size must be greater than 0",
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// If the cluster size is not changing, do nothing
	if req.NewClusterSize == s.config.ClusterSize {
		log.Debugn("Cluster size is already set to the requested value")
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
	s.config.Addresses = req.NodesAddresses

	// Reinitialize caches for the new cluster size
	// TODO check this ctx, a client cancellation from the Operator client might leave the node in a unwanted state
	if err := s.initCaches(ctx); err != nil { // Revert to previous cluster size on error
		log.Errorn("Failed to initialize caches", obskit.Error(err))
		s.config.ClusterSize = previousClusterSize
		s.scaling = false
		return &pb.ScaleResponse{
			Success:             false,
			ErrorMessage:        fmt.Sprintf("failed to initialize caches: %v", err),
			PreviousClusterSize: previousClusterSize,
			NewClusterSize:      previousClusterSize,
		}, nil
	}

	// WARNING: We don't clear the scaling flag here.
	// The scaling flag will be cleared when the ScaleComplete RPC is called.

	log.Infon("Scale complete")

	return &pb.ScaleResponse{
		Success:             true,
		PreviousClusterSize: previousClusterSize,
		NewClusterSize:      s.config.ClusterSize,
	}, nil
}

// ScaleComplete implements the ScaleComplete RPC method
func (s *Service) ScaleComplete(_ context.Context, _ *pb.ScaleCompleteRequest) (*pb.ScaleCompleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// No need to check the s.scaling value, let's be optimistic and go ahead here for auto-healing purposes
	s.scaling = false

	return &pb.ScaleCompleteResponse{Success: true}, nil
}

// loadSnapshot loads a snapshot for a specific hash range
func (s *Service) loadSnapshot(ctx context.Context, hashRange uint32, cache Cache) error {
	filename := getSnapshotFilename(hashRange)

	buf := aws.NewWriteAtBuffer([]byte{})
	err := s.storage.Download(ctx, buf, filename)
	if err != nil {
		return fmt.Errorf("failed to download snapshot file %q: %w", filename, err)
	}

	reader := bytes.NewReader(buf.Bytes())
	return cache.LoadSnapshot(reader)
}

func getSnapshotFilename(hashRange uint32) string {
	return "range_" + strconv.Itoa(int(hashRange)) + ".snapshot"
}
