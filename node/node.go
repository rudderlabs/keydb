package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
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
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	// DefaultTotalHashRanges is the default number of hash ranges
	DefaultTotalHashRanges = 128

	// DefaultSnapshotInterval is the default interval for creating snapshots (in seconds)
	DefaultSnapshotInterval = 24 * time.Hour

	// DefaultGarbageCollectionInterval defines how often garbage collection should happen per cache
	DefaultGarbageCollectionInterval = 5 * time.Minute

	// DefaultMaxFilesToList defines the default maximum number of files to list in a single operation, set to 1000.
	DefaultMaxFilesToList int64 = 1000
)

var snapshotFilenameRegex = regexp.MustCompile(`^hr_(\d+)_s_(\d+).snapshot$`)

// Config holds the configuration for a node
type Config struct {
	// NodeID is the ID of this node (0-based)
	NodeID uint32

	// ClusterSize is the total number of nodes in the cluster
	ClusterSize uint32

	// TotalHashRanges is the total number of hash ranges
	TotalHashRanges uint32

	// MaxFilesToList specifies the maximum number of files that can be listed in a single operation.
	MaxFilesToList int64

	// SnapshotInterval is the interval for creating snapshots (in seconds)
	SnapshotInterval time.Duration

	// GarbageCollectionInterval defines the duration between automatic GC operation per cache
	GarbageCollectionInterval time.Duration

	// Addresses is a list of node addresses that this node will advertise to clients
	Addresses []string
}

// Service implements the NodeService gRPC service
type Service struct {
	pb.UnimplementedNodeServiceServer

	config Config

	// mu protects the scaling operations
	mu      sync.RWMutex
	cache   Cache
	scaling bool

	now              func() time.Time
	lastSnapshotTime time.Time
	maxFilesToList   int64
	waitGroup        sync.WaitGroup
	storage          cloudStorage
	stats            stats.Stats
	logger           logger.Logger

	metrics struct {
		getKeysCounters     map[uint32]stats.Counter
		putKeysCounter      map[uint32]stats.Counter
		errScalingCounter   stats.Counter
		errWrongNodeCounter stats.Counter
		errInternalCounter  stats.Counter
		gcDuration          stats.Histogram
	}
}

// Cache is an interface for a key-value store with TTL support
type Cache interface {
	// Get returns the values associated with the keys and an error if the operation failed
	Get(keys []string) ([]bool, error)

	// Put adds or updates elements inside the cache with the specified TTL and returns an error if the operation failed
	Put(keys []string, ttl time.Duration) error

	// CreateSnapshots writes the cache contents to the provided writers
	// it returns a timestamp (version) indicating the version of last entry that is dumped
	CreateSnapshots(ctx context.Context, w map[uint32]io.Writer) (uint64, error)

	// LoadSnapshots reads the cache contents from the provided readers
	LoadSnapshots(ctx context.Context, r ...io.Reader) error

	// RunGarbageCollection is designed to do GC while the cache is online
	RunGarbageCollection()

	// Close releases any resources associated with the cache and ensures proper cleanup. Returns an error if the operation fails.
	Close() error
}

type cacheFactory func(hashRange uint32) (Cache, error)

type cloudStorageReader interface {
	Download(ctx context.Context, output io.WriterAt, key string) error
	ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) filemanager.ListSession
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
	ctx context.Context, config Config, cf cacheFactory, storage cloudStorage, stat stats.Stats, log logger.Logger,
) (*Service, error) {
	// Set defaults for unspecified config values
	if config.TotalHashRanges == 0 {
		config.TotalHashRanges = DefaultTotalHashRanges
	}
	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = DefaultSnapshotInterval
	}
	if config.GarbageCollectionInterval == 0 {
		config.GarbageCollectionInterval = DefaultGarbageCollectionInterval
	}
	if config.MaxFilesToList == 0 {
		config.MaxFilesToList = DefaultMaxFilesToList
	}

	service := &Service{
		now:            time.Now,
		config:         config,
		caches:         make(map[uint32]Cache),
		cacheFactory:   cf,
		storage:        storage,
		maxFilesToList: config.MaxFilesToList,
		stats:          stat,
		logger: log.Withn(
			logger.NewIntField("nodeId", int64(config.NodeID)),
			logger.NewIntField("totalHashRanges", int64(config.TotalHashRanges)),
			logger.NewStringField("snapshotInterval", config.SnapshotInterval.String()),
			logger.NewStringField("garbageCollectionInterval", config.GarbageCollectionInterval.String()),
			logger.NewIntField("maxFilesToList", config.MaxFilesToList),
		),
	}

	statsTags := stats.Tags{"nodeID": strconv.Itoa(int(config.NodeID))}
	service.metrics.errScalingCounter = stat.NewTaggedStat("keydb_err_scaling_count", stats.CountType, statsTags)
	service.metrics.errWrongNodeCounter = stat.NewTaggedStat("keydb_err_wrong_node_count", stats.CountType, statsTags)
	service.metrics.errInternalCounter = stat.NewTaggedStat("keydb_err_internal_count", stats.CountType, statsTags)
	service.metrics.getKeysCounters = make(map[uint32]stats.Counter)
	service.metrics.putKeysCounter = make(map[uint32]stats.Counter)
	service.metrics.gcDuration = stat.NewTaggedStat("keydb_gc_duration_seconds", stats.HistogramType, statsTags)

	// Initialize caches for all hash ranges this node handles
	if err := service.initCaches(ctx); err != nil {
		return nil, err
	}

	// Start background snapshot creation
	service.waitGroup.Add(1)
	go service.snapshotLoop(ctx)
	service.waitGroup.Add(1)
	go service.garbageCollection(ctx)

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
					s.logger.Debugn("Skipping snapshot due to scaling operation")
					return
				}
				if err := s.createSnapshots(ctx); err != nil {
					s.logger.Errorn("Error creating snapshots", obskit.Error(err))
				}
			}()
		}
	}
}

func (s *Service) garbageCollection(ctx context.Context) {
	defer s.waitGroup.Done()

	ticker := time.NewTicker(s.config.GarbageCollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				s.mu.Lock() // TODO this might affect scaling operations
				defer s.mu.Unlock()

				if s.scaling {
					s.logger.Warnn("Skipping garbage collection while scaling")
					return
				}

				start := time.Now()
				s.logger.Infon("Running garbage collection", logger.NewIntField("noOfCaches", int64(len(s.caches))))
				defer func() {
					elapsed := time.Since(start)
					s.logger.Infon("Garbage collection finished",
						logger.NewIntField("noOfCaches", int64(len(s.caches))),
						logger.NewStringField("duration", elapsed.String()),
					)
					s.metrics.gcDuration.Observe(elapsed.Seconds())
				}()

				for _, cache := range s.caches {
					// For now let's just run one GC at a time to avoid overwhelming the CPU
					cache.RunGarbageCollection()
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
			delete(s.metrics.getKeysCounters, r)
			delete(s.metrics.putKeysCounter, r)
		}
	}

	// List all files in the bucket
	filenamesPrefix := getSnapshotFilenamePrefix()
	list := s.storage.ListFilesWithPrefix(ctx, "", filenamesPrefix, s.maxFilesToList)
	files, err := list.Next()
	if err != nil {
		return fmt.Errorf("failed to list snapshot files: %w", err)
	}

	filesByHashRange := make(map[uint32]string, len(files))
	for _, file := range files {
		matches := snapshotFilenameRegex.FindStringSubmatch(file.Key)
		if len(matches) != 3 {
			continue
		}
		hashRange, err := strconv.Atoi(matches[1])
		if err != nil {
			s.logger.Warnn("Invalid snapshot filename", logger.NewStringField("filename", file.Key))
			continue
		}
		filesByHashRange[uint32(hashRange)] = file.Key
	}

	// Create a cache for each hash range
	group, gCtx := kitsync.NewEagerGroup(ctx, len(ranges))
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

		statsTags := stats.Tags{
			"nodeID":    strconv.Itoa(int(s.config.NodeID)),
			"hashRange": strconv.Itoa(int(r)),
		}
		s.metrics.getKeysCounters[r] = s.stats.NewTaggedStat("keydb_get_keys_count", stats.CountType, statsTags)
		s.metrics.putKeysCounter[r] = s.stats.NewTaggedStat("keydb_put_keys_count", stats.CountType, statsTags)

		snapshotFile := filesByHashRange[r]
		if snapshotFile == "" {
			continue
		}

		group.Go(func() error { // Try to load snapshot for this range
			if err := s.loadSnapshot(gCtx, cache, snapshotFile); err != nil {
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
		s.metrics.errScalingCounter.Increment()
		response.ErrorCode = pb.ErrorCode_SCALING
		return response, nil
	}

	itemsByHashRange, indexes, err := hash.GetKeysByHashRangeWithIndexes(
		req.Keys, s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges,
	)
	if err != nil {
		if errors.Is(err, hash.ErrWrongNode) {
			s.metrics.errWrongNodeCounter.Increment()
			response.ErrorCode = pb.ErrorCode_WRONG_NODE
			return response, nil
		}
		s.metrics.errInternalCounter.Increment()
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

			s.metrics.getKeysCounters[hashRange].Count(len(keys))

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
		s.metrics.errInternalCounter.Increment()
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
		s.metrics.errScalingCounter.Increment()
		resp.ErrorCode = pb.ErrorCode_SCALING
		return resp, nil
	}

	// Group items by hash range
	itemsByHashRange, err := hash.GetKeysByHashRange(
		req.Keys, s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges,
	)
	if err != nil {
		if errors.Is(err, hash.ErrWrongNode) {
			s.metrics.errWrongNodeCounter.Increment()
			resp.ErrorCode = pb.ErrorCode_WRONG_NODE
			return resp, nil
		}
		s.metrics.errInternalCounter.Increment()
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

			s.metrics.putKeysCounter[hashRange].Count(len(keys))

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		s.metrics.errInternalCounter.Increment()
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
	hashRanges := make([]uint32, 0, len(ranges))
	for r := range ranges {
		hashRanges = append(hashRanges, r)
	}

	return &pb.GetNodeInfoResponse{
		NodeId:                s.config.NodeID,
		ClusterSize:           s.config.ClusterSize,
		NodesAddresses:        s.config.Addresses,
		HashRanges:            hashRanges,
		LastSnapshotTimestamp: uint64(s.lastSnapshotTime.Unix()),
	}, nil
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

// CreateSnapshot implements the CreateSnapshot RPC method
// TODO should we trigger a Garbage Collection before taking a snapshot?
// TODO this can be optimized a lot! For example we could snapshot on local disk every 10s and work only on the head
// and tail of the file (i.e. remove expired from head and append new entries).
// Then once a minute we can upload the whole file to S3.
// The file that we upload could be compressed, for example by using zstd with dictionaries.
func (s *Service) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.CreateSnapshotResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

	buf := new(bytes.Buffer)
	since, err := cache.CreateSnapshot(buf)
	if err != nil {
		return fmt.Errorf("failed to create snapshot for range %d: %w", hashRange, err)
	}

	if len(buf.Bytes()) == 0 {
		return nil // no data to upload
	}

	filename := getSnapshotFilenamePrefix() + getSnapshotFilenamePostfix(hashRange, since)

	_, err = s.storage.UploadReader(ctx, filename, buf)
	if err != nil {
		return fmt.Errorf("failed to upload snapshot file %q: %w", filename, err)
	}

	return nil
}

// loadSnapshot loads a snapshot for a specific hash range
func (s *Service) loadSnapshot(ctx context.Context, cache Cache, filename string) error {
	buf := aws.NewWriteAtBuffer([]byte{})
	err := s.storage.Download(ctx, buf, filename)
	if err != nil {
		return fmt.Errorf("failed to download snapshot file %q: %w", filename, err)
	}

	reader := bytes.NewReader(buf.Bytes())
	if err := cache.LoadSnapshot(reader); err != nil {
		return fmt.Errorf("failed to load snapshot file %q: %w", filename, err)
	}

	return nil
}

func (s *Service) GetKeysByHashRange(keys []string) (map[uint32][]string, error) {
	return hash.GetKeysByHashRange(keys, s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)
}

func (s *Service) GetKeysByHashRangeWithIndexes(keys []string) (map[uint32][]string, map[string]int, error) {
	return hash.GetKeysByHashRangeWithIndexes(keys, s.config.NodeID, s.config.ClusterSize, s.config.TotalHashRanges)
}

func getSnapshotFilenamePrefix() string {
	return "hr_"
}

func getSnapshotFilenamePostfix(hashRange uint32, since uint64) string {
	return strconv.Itoa(int(hashRange)) + "_s_" + strconv.FormatUint(since, 10) + ".snapshot"
}
