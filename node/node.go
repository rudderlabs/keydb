package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/keydb/internal/cache/badger"
	"github.com/rudderlabs/keydb/internal/hash"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	// DefaultTotalHashRanges is the default number of hash ranges
	DefaultTotalHashRanges = 271

	// DefaultSnapshotInterval is the default interval for creating snapshots (in seconds)
	DefaultSnapshotInterval = 24 * time.Hour

	// DefaultGarbageCollectionInterval defines how often garbage collection should happen per cache
	DefaultGarbageCollectionInterval = 5 * time.Minute

	// DefaultMaxFilesToList defines the default maximum number of files to list in a single operation, set to 1000.
	DefaultMaxFilesToList int64 = 1000
)

// file format is hr_<hash_range>_s_<from_timestamp>_<to_timestamp>.snapshot
var snapshotFilenameRegex = regexp.MustCompile(`^.+/hr_(\d+)_s_(\d+)_(\d+).snapshot$`)

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

	// DegradedNodes is a list of nodes that are considered degraded and should not be used for reads and writes.
	DegradedNodes func() []bool

	// logTableStructureDuration defines the duration for which the table structure is logged
	LogTableStructureDuration time.Duration

	// backupFolderName is the name of the folder in the S3 bucket where snapshots are stored
	BackupFolderName string
}

// Service implements the NodeService gRPC service
type Service struct {
	pb.UnimplementedNodeServiceServer

	config Config

	// mu protects cache, scaling, lastSnapshotTime and hasher
	mu               sync.RWMutex
	cache            Cache
	scaling          bool
	lastSnapshotTime time.Time
	hasher           *hash.Hash

	now            func() time.Time
	maxFilesToList int64
	waitGroup      sync.WaitGroup
	storage        cloudStorage
	stats          stats.Stats
	logger         logger.Logger

	metrics struct {
		getKeysCounters             map[uint32]stats.Counter
		putKeysCounter              map[uint32]stats.Counter
		errScalingCounter           stats.Counter
		errWrongNodeCounter         stats.Counter
		errInternalCounter          stats.Counter
		gcDuration                  stats.Histogram
		getKeysHashingDuration      stats.Timer
		getFromCacheSuccessDuration stats.Timer
		getFromCacheFailDuration    stats.Timer
		putKeysHashingDuration      stats.Timer
		putInCacheSuccessDuration   stats.Timer
		putInCacheFailDuration      stats.Timer
		downloadSnapshotDuration    stats.Timer
		loadSnapshotToDiskDuration  stats.Timer
		uploadSnapshotDuration      stats.Timer
		createSnapshotsDuration     stats.Timer
	}
}

// Cache is an interface for a key-value store with TTL support
type Cache interface {
	// Get returns whether the keys exist and an error if the operation failed
	Get(keysByHashRange map[uint32][]string, indexes map[string]int) ([]bool, error)

	// Put adds or updates keys inside the cache with the specified TTL and returns an error if the operation failed
	Put(keysByHashRange map[uint32][]string, ttl time.Duration) error

	// CreateSnapshots writes the cache contents to the provided writers
	// it returns a timestamp (version) indicating the version of last entry that is dumped
	CreateSnapshots(
		ctx context.Context,
		writers map[uint32]io.Writer,
		since map[uint32]uint64,
	) (uint64, map[uint32]bool, error)

	// LoadSnapshots reads the cache contents from the provided readers
	LoadSnapshots(ctx context.Context, r ...io.Reader) error

	// RunGarbageCollection is designed to do GC while the cache is online
	RunGarbageCollection()

	// Close releases any resources associated with the cache and ensures proper cleanup.
	// Returns an error if the operation fails.
	Close() error

	// LevelsToString returns a string representation of the cache levels
	LevelsToString() string
}

type cloudStorageReader interface {
	Download(ctx context.Context, output io.WriterAt, key string, opts ...filemanager.DownloadOption) error
	ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) filemanager.ListSession
}

type cloudStorageWriter interface {
	UploadReader(ctx context.Context, objName string, rdr io.Reader) (filemanager.UploadedFile, error)
	Delete(ctx context.Context, keys []string) error
}

type cloudStorage interface {
	cloudStorageReader
	cloudStorageWriter
}

// NewService creates a new NodeService
func NewService(
	ctx context.Context, config Config, storage cloudStorage,
	kitConf *config.Config, stat stats.Stats, log logger.Logger,
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
		storage:        storage,
		maxFilesToList: config.MaxFilesToList,
		hasher:         hash.New(config.ClusterSize, config.TotalHashRanges),
		stats:          stat,
		logger: log.Withn(
			logger.NewIntField("nodeId", int64(config.NodeID)),
			logger.NewIntField("totalHashRanges", int64(config.TotalHashRanges)),
			logger.NewStringField("snapshotInterval", config.SnapshotInterval.String()),
			logger.NewStringField("garbageCollectionInterval", config.GarbageCollectionInterval.String()),
			logger.NewIntField("maxFilesToList", config.MaxFilesToList),
		),
	}

	var err error
	service.cache, err = badger.New(kitConf, log.Child("badger"))
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	statsTags := stats.Tags{"nodeID": strconv.Itoa(int(config.NodeID))}
	service.metrics.errScalingCounter = stat.NewTaggedStat("keydb_err_scaling_count", stats.CountType, statsTags)
	service.metrics.errWrongNodeCounter = stat.NewTaggedStat("keydb_err_wrong_node_count", stats.CountType, statsTags)
	service.metrics.errInternalCounter = stat.NewTaggedStat("keydb_err_internal_count", stats.CountType, statsTags)
	service.metrics.getKeysCounters = make(map[uint32]stats.Counter)
	service.metrics.putKeysCounter = make(map[uint32]stats.Counter)
	service.metrics.gcDuration = stat.NewTaggedStat("keydb_gc_duration_seconds", stats.HistogramType, statsTags)
	service.metrics.getKeysHashingDuration = stat.NewTaggedStat("keydb_keys_hashing_duration_seconds", stats.TimerType,
		stats.Tags{"method": "get"})
	service.metrics.getFromCacheSuccessDuration = stat.NewTaggedStat("keydb_grpc_cache_get_duration_seconds",
		stats.TimerType, stats.Tags{"success": "true"})
	service.metrics.getFromCacheFailDuration = stat.NewTaggedStat("keydb_grpc_cache_get_duration_seconds",
		stats.TimerType, stats.Tags{"success": "false"})
	service.metrics.putKeysHashingDuration = stat.NewTaggedStat("keydb_keys_hashing_duration_seconds",
		stats.TimerType, stats.Tags{"method": "put"})
	service.metrics.putInCacheSuccessDuration = stat.NewTaggedStat("keydb_grpc_cache_put_duration_seconds",
		stats.TimerType, stats.Tags{"success": "true"})
	service.metrics.putInCacheFailDuration = stat.NewTaggedStat("keydb_grpc_cache_put_duration_seconds",
		stats.TimerType, stats.Tags{"success": "false"})
	// NOTE: pay attention to the wording of the metrics below. if singular (i.e. snapshot instead of snapshots) then
	// it tracks an operation that handles a single snapshot. if plural (i.e. snapshots instead of snapshot) then it
	// tracks an operation that handles multiple snapshots (e.g. CreateSnapshots).
	service.metrics.downloadSnapshotDuration = stat.NewStat("keydb_download_snapshot_duration_seconds", stats.TimerType)
	service.metrics.loadSnapshotToDiskDuration = stat.NewStat(
		"keydb_load_snapshot_to_disk_duration_seconds", stats.TimerType,
	)
	service.metrics.uploadSnapshotDuration = stat.NewStat("keydb_upload_snapshot_duration_seconds", stats.TimerType)
	service.metrics.createSnapshotsDuration = stat.NewStat("keydb_create_snapshots_duration_seconds", stats.TimerType)

	// Initialize caches for all hash ranges this node handles
	if err := service.initCaches(ctx, false, 0); err != nil {
		return nil, err
	}

	// Start background snapshot creation
	// TODO restore automatic daily snapshot creation
	// service.waitGroup.Add(1)
	// go service.snapshotLoop(ctx)

	service.waitGroup.Add(1)
	go service.garbageCollection(ctx)

	service.waitGroup.Add(1)
	go service.logCacheLevels(ctx)

	return service, nil
}

func (s *Service) Close() {
	s.waitGroup.Wait()
	if err := s.cache.Close(); err != nil {
		s.logger.Errorn("Error closing cache", obskit.Error(err))
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
				s.logger.Infon("Running garbage collection")
				defer func() {
					elapsed := time.Since(start)
					s.logger.Infon("Garbage collection finished",
						logger.NewStringField("duration", elapsed.String()),
					)
					s.metrics.gcDuration.Observe(elapsed.Seconds())
				}()

				s.cache.RunGarbageCollection()
			}()
		}
	}
}

func (s *Service) logCacheLevels(ctx context.Context) {
	defer s.waitGroup.Done()

	s.logger.Infon("Cache levels",
		logger.NewStringField("levels", s.cache.LevelsToString()),
	)
	if s.config.LogTableStructureDuration == 0 {
		return
	}
	ticker := time.NewTicker(s.config.LogTableStructureDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.logger.Infon("Cache levels",
				logger.NewStringField("levels", s.cache.LevelsToString()),
			)
		}
	}
}

func (s *Service) getCurrentRanges() map[uint32]struct{} {
	return s.hasher.GetNodeHashRanges(s.config.NodeID)
}

// initCaches initializes the caches for all hash ranges this node handles
func (s *Service) initCaches(
	ctx context.Context, download bool, maxConcurrency uint32, selectedHashRanges ...uint32,
) error {
	currentRanges := s.getCurrentRanges() // gets the hash ranges for this node

	// Remove caches and key maps for ranges this node no longer handles
	for r := range s.metrics.getKeysCounters {
		if _, shouldHandle := currentRanges[r]; !shouldHandle {
			delete(s.metrics.getKeysCounters, r)
		}
	}
	for r := range s.metrics.putKeysCounter {
		if _, shouldHandle := currentRanges[r]; !shouldHandle {
			delete(s.metrics.putKeysCounter, r)
		}
	}

	for r := range currentRanges {
		statsTags := stats.Tags{
			"nodeID":    strconv.Itoa(int(s.config.NodeID)),
			"hashRange": strconv.Itoa(int(r)),
		}
		s.metrics.getKeysCounters[r] = s.stats.NewTaggedStat("keydb_get_keys_count", stats.CountType, statsTags)
		s.metrics.putKeysCounter[r] = s.stats.NewTaggedStat("keydb_put_keys_count", stats.CountType, statsTags)
	}

	if !download {
		s.logger.Infon("Downloading disabled, skipping caches initialization")
		return nil
	}

	totalFiles, filesByHashRange, err := s.listSnapshots(ctx, selectedHashRanges...)
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}

	if maxConcurrency == 0 {
		maxConcurrency = uint32(len(currentRanges))
	}

	var (
		loadCtx, loadCancel = context.WithCancel(ctx)
		loadDone            = make(chan error, 1)
		filesLoaded         int64
		group, gCtx         = kitsync.NewEagerGroup(ctx, 0)
		readers             = make(chan snapshotReader, maxConcurrency)
	)
	defer loadCancel()
	go func() {
		defer close(loadDone)
		for sn := range readers {
			s.logger.Infon("Loading downloaded snapshots",
				logger.NewIntField("range", int64(sn.hashRange)),
				logger.NewStringField("filename", sn.filename),
				logger.NewIntField("totalFiles", int64(totalFiles)),
				logger.NewFloatField("loadingPercentage",
					float64(filesLoaded)*100/float64(totalFiles),
				),
			)
			loadStart := time.Now()
			if err := s.cache.LoadSnapshots(loadCtx, sn.reader); err != nil {
				loadDone <- fmt.Errorf("failed to load snapshots: %w", err)
				return
			}
			s.metrics.loadSnapshotToDiskDuration.Since(loadStart)
			filesLoaded++
		}
	}()
	for r := range filesByHashRange {
		snapshotFiles := filesByHashRange[r]
		if len(snapshotFiles) == 0 {
			s.logger.Infon("Skipping range while initializing caches", logger.NewIntField("range", int64(r)))
			continue
		}

		for _, snapshotFile := range snapshotFiles {
			group.Go(func() error {
				s.logger.Infon("Starting download of snapshot file from cloud storage",
					logger.NewStringField("filename", snapshotFile.filename),
				)

				startDownload := time.Now()
				buf := manager.NewWriteAtBuffer([]byte{})
				err := s.storage.Download(gCtx, buf, snapshotFile.filename)
				if err != nil {
					if errors.Is(err, filemanager.ErrKeyNotFound) {
						s.logger.Warnn("No cached snapshot for range", logger.NewIntField("range", int64(r)))
						return nil
					}
					return fmt.Errorf("failed to download snapshot file %q: %w", snapshotFile, err)
				}
				s.metrics.downloadSnapshotDuration.Since(startDownload)

				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case readers <- snapshotReader{
					filename:  snapshotFile.filename,
					hashRange: r,
					reader:    bytes.NewReader(buf.Bytes()),
				}:
				}
				return nil
			})
		}
	}
	if err = group.Wait(); err != nil {
		close(readers)
		loadCancel()
		<-loadDone // ignoring this error, even if the load failed we want to report the error from the group
		return err
	}

	close(readers)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-loadDone:
		if err != nil {
			return err
		}
	}

	s.logger.Infon("Caches initialized successfully",
		logger.NewFloatField("loadingPercentage",
			float64(filesLoaded)*100/float64(totalFiles),
		),
	)

	return nil
}

func (s *Service) listSnapshots(ctx context.Context, selectedHashRanges ...uint32) (
	int,
	map[uint32][]snapshotFile,
	error,
) {
	// List all files in the bucket
	list := s.storage.ListFilesWithPrefix(ctx, "", s.getSnapshotFilenamePrefix(), s.maxFilesToList)
	files, err := list.Next()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to list snapshot files: %w", err)
	}
	if len(files) == 0 {
		s.logger.Infon("No snapshots found, skipping caches initialization")
		return 0, nil, nil
	}

	var selectedRangesMap map[uint32]struct{}
	if len(selectedHashRanges) > 0 {
		selectedRangesMap = make(map[uint32]struct{}, len(selectedHashRanges))
		for _, r := range selectedHashRanges {
			selectedRangesMap[r] = struct{}{}
		}
	} else { // no hash range was selected, download the data for all the ranges handled by this node
		selectedRangesMap = s.getCurrentRanges()
	}

	totalFiles := 0
	filesByHashRange := make(map[uint32][]snapshotFile, len(files))
	for _, file := range files {
		matches := snapshotFilenameRegex.FindStringSubmatch(file.Key)
		if len(matches) != 4 {
			continue
		}
		hashRangeInt, err := strconv.Atoi(matches[1])
		if err != nil {
			s.logger.Warnn("Invalid snapshot filename (hash range)", logger.NewStringField("filename", file.Key))
			continue
		}
		hashRange := uint32(hashRangeInt)
		if len(selectedHashRanges) > 0 {
			if _, shouldHandle := selectedRangesMap[hashRange]; !shouldHandle {
				s.logger.Warnn("Ignoring snapshot file for hash range since it was not selected")
				continue
			}
		}
		from, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			s.logger.Warnn("Invalid snapshot filename (from)", logger.NewStringField("filename", file.Key))
			continue
		}
		to, err := strconv.ParseUint(matches[3], 10, 64)
		if err != nil {
			s.logger.Warnn("Invalid snapshot filename (to)", logger.NewStringField("filename", file.Key))
			continue
		}
		if filesByHashRange[hashRange] == nil {
			filesByHashRange[hashRange] = make([]snapshotFile, 0, 1)
		}

		s.logger.Debugn("Found snapshot file",
			logger.NewIntField("hashRange", int64(hashRange)),
			logger.NewStringField("filename", file.Key),
			logger.NewIntField("from", int64(from)),
			logger.NewIntField("to", int64(to)),
		)
		filesByHashRange[hashRange] = append(filesByHashRange[hashRange], snapshotFile{
			filename:  file.Key,
			hashRange: hashRange,
			from:      from,
			to:        to,
		})
		totalFiles++
	}

	// Sort each slice by "from" and "to"
	for hashRange := range filesByHashRange {
		sort.Slice(filesByHashRange[hashRange], func(i, j int) bool {
			if filesByHashRange[hashRange][i].from != filesByHashRange[hashRange][j].from {
				return filesByHashRange[hashRange][i].from < filesByHashRange[hashRange][j].from
			}
			return filesByHashRange[hashRange][i].to < filesByHashRange[hashRange][j].to
		})
	}

	return totalFiles, filesByHashRange, nil
}

// Get implements the Get RPC method
func (s *Service) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
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

	// Group keys by hash range
	hashingStart := time.Now()
	keysByHashRange, indexes, err := s.hasher.GetKeysByHashRangeWithIndexes(req.Keys, s.config.NodeID)
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

	// Record metrics
	s.metrics.getKeysHashingDuration.Since(hashingStart)
	for hashRange, keys := range keysByHashRange {
		s.metrics.getKeysCounters[hashRange].Count(len(keys))
	}

	getFromCacheStart := time.Now()
	existsValues, err := s.cache.Get(keysByHashRange, indexes)
	if err != nil {
		s.metrics.errInternalCounter.Increment()
		s.metrics.getFromCacheFailDuration.Since(getFromCacheStart)
		response.ErrorCode = pb.ErrorCode_INTERNAL_ERROR
		return response, nil
	}
	s.metrics.getFromCacheSuccessDuration.Since(getFromCacheStart)
	response.Exists = existsValues

	return response, nil
}

// Put implements the Put RPC method
func (s *Service) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
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

	// Group keys by hash range
	hashingStart := time.Now()
	keysByHashRange, err := s.hasher.GetKeysByHashRange(req.Keys, s.config.NodeID)
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

	// Record metrics
	s.metrics.putKeysHashingDuration.Since(hashingStart)
	for hashRange, keys := range keysByHashRange {
		s.metrics.putKeysCounter[hashRange].Count(len(keys))
	}

	// Store the keys in the cache with the specified TTL
	putToCacheStart := time.Now()
	err = s.cache.Put(keysByHashRange, time.Duration(req.TtlSeconds)*time.Second)
	if err != nil {
		s.metrics.errInternalCounter.Increment()
		s.metrics.putInCacheFailDuration.Since(putToCacheStart)
		resp.ErrorCode = pb.ErrorCode_INTERNAL_ERROR
		return resp, nil
	}
	s.metrics.putInCacheSuccessDuration.Since(putToCacheStart)

	resp.Success = true
	return resp, nil
}

// GetNodeInfo implements the GetNodeInfo RPC method
func (s *Service) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If a specific node ID was requested, check if it matches this node
	if req.NodeId != 0 && req.NodeId != s.config.NodeID {
		return nil, status.Errorf(codes.InvalidArgument,
			"node ID mismatch: requested %d, this node is %d", req.NodeId, s.config.NodeID,
		)
	}

	// Get hash ranges for this node
	ranges := s.hasher.GetNodeHashRanges(s.config.NodeID)

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

	log := s.logger.Withn(logger.NewIntField("newClusterSize", int64(len(req.NodesAddresses))))
	log.Infon("Scale request received")

	if s.scaling {
		log.Infon("Scaling operation already in progress")
		return &pb.ScaleResponse{
			Success:             false,
			ErrorMessage:        "scaling operation already in progress",
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// Validate new cluster size
	if len(req.NodesAddresses) == 0 {
		log.Warnn("New cluster size must be greater than 0")
		return &pb.ScaleResponse{
			Success:             false,
			ErrorMessage:        "new cluster size must be greater than 0",
			PreviousClusterSize: s.config.ClusterSize,
			NewClusterSize:      s.config.ClusterSize,
		}, nil
	}

	// WARNING!
	// We don't check if the cluster size is already at the desired size to allow for auto-healing
	// and to force the node to load the desired snapshots from S3.

	// Set scaling flag
	s.scaling = true

	// Save the previous cluster size
	previousClusterSize := s.config.ClusterSize

	// Update cluster size
	s.config.ClusterSize = uint32(len(req.NodesAddresses))
	s.config.Addresses = req.NodesAddresses

	// Update hash instance
	s.hasher = hash.New(s.config.ClusterSize, s.config.TotalHashRanges)

	// Reinitialize caches for the new cluster size
	if err := s.initCaches(ctx, false, 0); err != nil { // Revert to previous cluster size on error
		log.Errorn("Failed to initialize caches", obskit.Error(err))
		s.config.ClusterSize = previousClusterSize
		s.hasher = hash.New(s.config.ClusterSize, s.config.TotalHashRanges)
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

	log.Infon("Scale phase 1 of 2 completed successfully",
		logger.NewIntField("previousClusterSize", int64(previousClusterSize)),
	)

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
	s.logger.Infon("Scale phase 2 of 2 completed successfully",
		logger.NewIntField("newClusterSize", int64(s.config.ClusterSize)),
	)

	return &pb.ScaleCompleteResponse{Success: true}, nil
}

// LoadSnapshots forces the node to load all snapshots from cloud storage
func (s *Service) LoadSnapshots(ctx context.Context, req *pb.LoadSnapshotsRequest) (*pb.LoadSnapshotsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Infon("Load snapshots request received")

	// Load snapshots for all hash ranges this node handles
	if err := s.initCaches(ctx, true, req.MaxConcurrency, req.HashRange...); err != nil {
		s.logger.Errorn("Failed to load snapshots", obskit.Error(err))
		return &pb.LoadSnapshotsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
			NodeId:       s.config.NodeID,
		}, nil
	}

	s.logger.Infon("Successfully loaded snapshots from cloud storage")

	return &pb.LoadSnapshotsResponse{
		Success: true,
		NodeId:  s.config.NodeID,
	}, nil
}

// CreateSnapshots implements the CreateSnapshots RPC method
func (s *Service) CreateSnapshots(
	ctx context.Context, req *pb.CreateSnapshotsRequest,
) (*pb.CreateSnapshotsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.logger.Infon("Create snapshots request received")

	if s.scaling {
		s.logger.Warnn("Skipping snapshots while scaling")
		return &pb.CreateSnapshotsResponse{
			Success:      false,
			ErrorMessage: "scaling operation in progress",
			NodeId:       s.config.NodeID,
		}, nil
	}

	// Create snapshots for all hash ranges this node handles
	if err := s.createSnapshots(ctx, req.FullSync, req.HashRange...); err != nil {
		s.logger.Errorn("Failed to create snapshots", obskit.Error(err))
		return &pb.CreateSnapshotsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
			NodeId:       s.config.NodeID,
		}, nil
	}

	return &pb.CreateSnapshotsResponse{
		Success: true,
		NodeId:  s.config.NodeID,
	}, nil
}

// createSnapshots creates snapshots for all hash ranges this node handles
func (s *Service) createSnapshots(ctx context.Context, fullSync bool, selectedHashRanges ...uint32) error {
	// Create a map of the hash ranges that we need to create as per request.
	// If the map ends up empty, then we create snapshots for all hash ranges.
	selected := make(map[uint32]struct{}, len(selectedHashRanges))
	for _, r := range selectedHashRanges {
		selected[r] = struct{}{}
	}

	// Get hash ranges for this node
	currentRanges := s.getCurrentRanges()
	var writers map[uint32]io.Writer
	if len(selected) > 0 {
		writers = make(map[uint32]io.Writer, len(selected))
		for r := range selected {
			if _, ok := currentRanges[r]; !ok {
				return fmt.Errorf("hash range %d not handled by this node", r)
			}
			writers[r] = bytes.NewBuffer([]byte{})
		}
	} else {
		writers = make(map[uint32]io.Writer, len(currentRanges))
		for r := range currentRanges {
			writers[r] = bytes.NewBuffer([]byte{})
		}
	}

	var (
		since    map[uint32]uint64
		sinceLog strings.Builder
	)
	if fullSync {
		since = make(map[uint32]uint64, len(currentRanges))
		i := 0
		for r := range currentRanges {
			if i != 0 {
				sinceLog.WriteString(",")
			}
			since[r] = 0
			sinceLog.WriteString(fmt.Sprintf("%d:%d", r, 0))
			i++
		}
	} else {
		_, filesByHashRange, err := s.listSnapshots(ctx, selectedHashRanges...)
		if err != nil {
			return fmt.Errorf("list snapshots: %w", err)
		}

		since = make(map[uint32]uint64, len(currentRanges))
		for r := range currentRanges {
			since[r] = 0
		}

		for hr, files := range filesByHashRange {
			for _, file := range files {
				if since[hr] < file.to {
					since[hr] = file.to
				}
			}
		}

		i := 0
		for hr, ss := range since {
			if i != 0 {
				sinceLog.WriteString(",")
			}
			sinceLog.WriteString(fmt.Sprintf("%d:%d", hr, ss))
			i++
		}
	}

	log := s.logger.Withn(
		logger.NewBoolField("fullSync", fullSync),
		logger.NewIntField("numHashRanges", int64(len(currentRanges))),
		logger.NewIntField("numWriters", int64(len(writers))),
		logger.NewStringField("since", sinceLog.String()))
	log.Infon("Creating snapshots")

	start := time.Now()
	newSince, hasData, err := s.cache.CreateSnapshots(ctx, writers, since)
	if err != nil {
		return fmt.Errorf("failed to create snapshots: %w", err)
	}
	s.metrics.createSnapshotsDuration.Since(start)

	log = log.Withn(logger.NewIntField("newSince", int64(newSince)))

	filesToBeDeletedByHashRange := make(map[uint32][]string)
	if fullSync {
		list := s.storage.ListFilesWithPrefix(ctx, "", s.getSnapshotFilenamePrefix(), s.maxFilesToList)
		files, err := list.Next()
		if err != nil {
			return fmt.Errorf("failed to list snapshot files: %w", err)
		}

		log.Infon("Got list of existing snapshot files", logger.NewIntField("numFiles", int64(len(files))))

		for _, file := range files {
			matches := snapshotFilenameRegex.FindStringSubmatch(file.Key)
			if len(matches) != 4 {
				continue
			}
			hashRangeInt, err := strconv.Atoi(matches[1])
			if err != nil {
				log.Warnn("Invalid snapshot filename (hash range)", logger.NewStringField("filename", file.Key))
				continue
			}
			hashRange := uint32(hashRangeInt)
			_, ok := writers[hashRange]
			if !ok {
				// We won't upload anything about this hash range, so we can skip it.
				continue
			}
			// If we're going to overwrite the file, we don't need to delete it.
			newFilename := s.getSnapshotFilenamePrefix() +
				getSnapshotFilenamePostfix(hashRange, since[hashRange], newSince)
			if newFilename != file.Key {
				filesToBeDeletedByHashRange[hashRange] = append(filesToBeDeletedByHashRange[hashRange], file.Key)
			}
		}
	}

	log.Infon("Uploading snapshots")

	for hashRange, w := range writers {
		buf, ok := w.(*bytes.Buffer)
		if !ok {
			return fmt.Errorf("invalid writer type %T for hash range: %d", w, hashRange)
		}
		if !hasData[hashRange] {
			log.Infon("No data to upload for hash range", logger.NewIntField("hashRange", int64(hashRange)))
			continue // no data to upload
		}

		filename := s.getSnapshotFilenamePrefix() + getSnapshotFilenamePostfix(hashRange, since[hashRange], newSince)
		log.Infon("Uploading snapshot file",
			logger.NewIntField("from", int64(since[hashRange])),
			logger.NewIntField("to", int64(newSince)),
			logger.NewIntField("hashRange", int64(hashRange)),
			logger.NewStringField("filename", filename),
		)

		startUpload := time.Now()
		_, err = s.storage.UploadReader(ctx, filename, buf)
		if err != nil {
			return fmt.Errorf("failed to upload snapshot file %q: %w", filename, err)
		}
		s.metrics.uploadSnapshotDuration.Since(startUpload)

		if fullSync {
			if len(filesToBeDeletedByHashRange[hashRange]) > 0 {
				// Clearing up old files that are incremental updates.
				// Since we've done a "full sync" they are not needed anymore and shouldn't be loaded next time.
				filesToBeDeleted := filesToBeDeletedByHashRange[hashRange]
				filesToBeDeletedLogField := logger.NewStringField("filenames", strings.Join(filesToBeDeleted, ","))

				log.Infon("Deleting old snapshot files",
					filesToBeDeletedLogField, logger.NewIntField("hashRange", int64(hashRange)),
				)

				err = s.storage.Delete(ctx, filesToBeDeleted)
				if err != nil {
					log.Errorn("Failed to delete old snapshot files",
						filesToBeDeletedLogField, logger.NewIntField("hashRange", int64(hashRange)), obskit.Error(err),
					)
					return fmt.Errorf("failed to delete old snapshot files: %w", err)
				}
			} else {
				log.Infon("No old snapshots files to be deleted",
					logger.NewIntField("hashRange", int64(hashRange)),
				)
			}
		}
	}

	s.lastSnapshotTime = time.Now()

	return nil
}

func (s *Service) GetKeysByHashRange(keys []string) (map[uint32][]string, error) {
	return s.hasher.GetKeysByHashRange(keys, s.config.NodeID)
}

func (s *Service) GetKeysByHashRangeWithIndexes(keys []string) (map[uint32][]string, map[string]int, error) {
	return s.hasher.GetKeysByHashRangeWithIndexes(keys, s.config.NodeID)
}

func (s *Service) getSnapshotFilenamePrefix() string {
	return path.Join(s.config.BackupFolderName, "hr_")
}

func getSnapshotFilenamePostfix(hashRange uint32, from, to uint64) string {
	return strconv.Itoa(int(hashRange)) +
		"_s_" + strconv.FormatUint(from, 10) + "_" + strconv.FormatUint(to, 10) +
		".snapshot"
}

type snapshotReader struct {
	filename  string
	hashRange uint32
	reader    io.Reader
}

type snapshotFile struct {
	filename  string
	hashRange uint32
	from, to  uint64
}
