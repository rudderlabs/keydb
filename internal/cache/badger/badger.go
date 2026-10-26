package badger

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/zstd"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"google.golang.org/protobuf/proto"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	defaultCompressionLevel = zstd.DefaultCompression

	// internalHashRange is the hash range the node reserves for its internal keys, like since markers
	internalHashRange = int64(-1)

	// maxTxnMarkerSize is the length of the largest commit timestamp, a uint64, as a decimal string.
	// Badger stores it as the value of each transaction finish marker.
	maxTxnMarkerSize = 20
)

var ErrSnapshotInProgress = errors.New("snapshotting already in progress")

// LevelInfo contains information about an LSM tree level
type LevelInfo struct {
	Level     int
	NumTables int
	Size      int64
	Score     float64
}

type Cache struct {
	cache                   *badger.DB
	conf                    *config.Config
	logger                  logger.Logger
	compress                bool
	compressionLevel        int
	numLevelZeroTablesStall int
	discardRatio            float64
	snapshotting            bool
	snapshottingLock        sync.Mutex
	debugMode               bool
	jitterEnabled           bool
	jitterDuration          time.Duration
}

func New(conf *config.Config, log logger.Logger) (*Cache, error) {
	path := conf.GetStringVar("/tmp/badger", "BadgerDB.Dedup.Path")
	numLevelZeroTablesStall := conf.GetIntVar(
		40, 1, "BadgerDB.Dedup.numLevelZeroTablesStall", "BadgerDB.numLevelZeroTablesStall",
	)
	opts := badger.DefaultOptions(path).
		WithCompression(options.None).
		WithNumVersionsToKeep(1).
		WithNumGoroutines(conf.GetIntVar(100, 1, "BadgerDB.Dedup.NumGoroutines")).
		WithBloomFalsePositive(conf.GetFloat64Var(0.000001, "BadgerDB.Dedup.BloomFalsePositive")).
		WithIndexCacheSize(conf.GetInt64Var(
			16*bytesize.MB, 1, "BadgerDB.Dedup.indexCacheSize", "BadgerDB.indexCacheSize",
		)).
		WithValueLogFileSize(conf.GetInt64Var(
			1*bytesize.MB, 1, "BadgerDB.Dedup.valueLogFileSize", "BadgerDB.valueLogFileSize",
		)).
		WithBlockSize(conf.GetIntVar(int(4*bytesize.KB), 1, "BadgerDB.Dedup.blockSize", "BadgerDB.blockSize")).
		WithMemTableSize(conf.GetInt64Var(64*bytesize.MB, 1, "BadgerDB.Dedup.memTableSize", "BadgerDB.memTableSize")).
		WithNumMemtables(conf.GetIntVar(5, 1, "BadgerDB.Dedup.numMemtable", "BadgerDB.numMemtable")).
		WithNumLevelZeroTables(conf.GetIntVar(
			10, 1, "BadgerDB.Dedup.numLevelZeroTables", "BadgerDB.numLevelZeroTables",
		)).
		WithNumLevelZeroTablesStall(numLevelZeroTablesStall).
		WithBaseTableSize(conf.GetInt64Var(
			16*bytesize.MB, 1, "BadgerDB.Dedup.baseTableSize", "BadgerDB.baseTableSize",
		)).
		WithBaseLevelSize(conf.GetInt64Var(1*bytesize.GB, 1, "BadgerDB.Dedup.baseLevelSize", "BadgerDB.baseLevelSize")).
		WithLevelSizeMultiplier(conf.GetIntVar(
			5, 1, "BadgerDB.Dedup.levelSizeMultiplier", "BadgerDB.levelSizeMultiplier",
		)).
		WithMaxLevels(conf.GetIntVar(7, 1, "BadgerDB.Dedup.maxLevels", "BadgerDB.maxLevels")).
		// Cannot have 1 compactor. Need at least 2
		WithNumCompactors(conf.GetIntVar(4, 1, "BadgerDB.Dedup.numCompactors", "BadgerDB.numCompactors")).
		// Every commit writes a transaction finish marker holding the commit timestamp as a decimal string
		// (up to 20 bytes). With a threshold at or below 20 the marker lands in the value log, where it is
		// never garbage collected and breaks WAL replay after an unclean shutdown.
		// See https://github.com/dgraph-io/badger/issues/2353
		// TODO: revisit this default once a badger release with a fix is out.
		WithValueThreshold(conf.GetInt64Var(
			1*bytesize.KB, 1, "BadgerDB.Dedup.valueThreshold", "BadgerDB.valueThreshold",
		)).
		WithSyncWrites(conf.GetBoolVar(false, "BadgerDB.Dedup.syncWrites", "BadgerDB.syncWrites")).
		WithBlockCacheSize(conf.GetInt64Var(0, 1, "BadgerDB.Dedup.blockCacheSize", "BadgerDB.blockCacheSize")).
		WithDetectConflicts(conf.GetBoolVar(false, "BadgerDB.Dedup.detectConflicts", "BadgerDB.detectConflicts"))

	if conf.GetBoolVar(false, "BadgerDB.Dedup.NopLogger") {
		opts = opts.WithLogger(loggerForBadger{logger.NOP})
	} else {
		opts = opts.WithLogger(loggerForBadger{log})
	}

	var (
		compressionLevel int
		compress         = conf.GetBoolVar(true, "BadgerDB.Dedup.Compress")
	)
	if compress {
		compressionLevel = conf.GetIntVar(defaultCompressionLevel, 1, "BadgerDB.Dedup.CompressionLevel")
		if compressionLevel < 1 || compressionLevel > 20 {
			log.Warnn("BadgerDB.Dedup.CompressionLevel must be >= 1 and <= 20",
				logger.NewIntField("level", int64(compressionLevel)),
			)
			compressionLevel = defaultCompressionLevel
		}
		log.Infon("BadgerDB.Dedup.Compress is enabled, using zstd-cgo compression for snapshots",
			logger.NewIntField("level", int64(compressionLevel)),
		)
	}

	if conf.GetBoolVar(false, "BadgerDB.Dedup.purgeValueLog") {
		if err := purgeValueLog(opts.ValueDir, opts.ValueThreshold, log); err != nil {
			return nil, fmt.Errorf("purging value log: %w", err)
		}
	}

	log.Infon("Starting BadgerDB", logger.NewStringField("path", path))

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Cache{
		cache:                   db,
		conf:                    conf,
		logger:                  log,
		compress:                compress,
		compressionLevel:        compressionLevel,
		numLevelZeroTablesStall: numLevelZeroTablesStall,
		discardRatio:            conf.GetFloat64Var(0.7, "BadgerDB.Dedup.DiscardRatio"),
		debugMode:               conf.GetBoolVar(false, "BadgerDB.DebugMode"),
		jitterEnabled:           conf.GetBoolVar(false, "cache.ttlJitter.enabled"),
		jitterDuration:          conf.GetDurationVar(1, time.Hour, "cache.ttlJitter.duration"),
	}, nil
}

// purgeValueLog deletes every value log file in dir except the one with the highest file id, which
// badger expects to find on open. It must run before the database is opened.
//
// keydb only stores nil and 8-byte values, which badger keeps in the LSM tree, so the value log holds
// nothing but transaction finish markers. A value pointing to a deleted file would read back empty:
// badger logs a missing value log file instead of returning an error.
// With a threshold at or below maxTxnMarkerSize badger keeps writing markers to the value log, so the
// purge is skipped.
func purgeValueLog(dir string, valueThreshold int64, log logger.Logger) error {
	if valueThreshold <= maxTxnMarkerSize {
		log.Warnn("Skipping value log purge, the value threshold is too small",
			logger.NewIntField("valueThreshold", valueThreshold),
			logger.NewIntField("minValueThreshold", maxTxnMarkerSize+1),
		)
		return nil
	}

	start := time.Now()
	files, err := valueLogFiles(dir)
	if err != nil {
		return err
	}
	if len(files) < 2 {
		return nil
	}
	purge := files[:len(files)-1]
	log.Infon("Purging value log", logger.NewIntField("files", int64(len(purge))))

	var purgedBytes int64
	for _, f := range purge {
		info, err := f.Info()
		if err != nil {
			return fmt.Errorf("getting size of %s: %w", f.Name(), err)
		}
		if err := os.Remove(filepath.Join(dir, f.Name())); err != nil {
			return fmt.Errorf("removing %s: %w", f.Name(), err)
		}
		purgedBytes += info.Size()
	}
	log.Infon("Purged value log",
		logger.NewIntField("files", int64(len(purge))),
		logger.NewIntField("bytes", purgedBytes),
		logger.NewDurationField("duration", time.Since(start)),
	)
	return nil
}

// valueLogFiles returns the value log files in dir, ordered by file id.
func valueLogFiles(dir string) ([]fs.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}
	type valueLogFile struct {
		fid   uint64
		entry fs.DirEntry
	}
	var vlogs []valueLogFile
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".vlog")
		if !ok || e.IsDir() {
			continue
		}
		fid, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			continue
		}
		vlogs = append(vlogs, valueLogFile{fid: fid, entry: e})
	}
	slices.SortFunc(vlogs, func(a, b valueLogFile) int { return cmp.Compare(a.fid, b.fid) })
	files := make([]fs.DirEntry, len(vlogs))
	for i, v := range vlogs {
		files[i] = v.entry
	}
	return files, nil
}

// Get returns the values associated with the keys and an error if the operation failed
func (c *Cache) Get(keysByHashRange map[int64][]string, indexes map[string]int) ([]bool, error) {
	results := make([]bool, len(indexes))

	err := c.cache.View(func(txn *badger.Txn) error {
		for hashRange, keys := range keysByHashRange {
			for _, key := range keys {
				_, err := txn.Get(getKey(key, hashRange))
				if err != nil {
					if errors.Is(err, badger.ErrKeyNotFound) {
						results[indexes[key]] = false
						continue
					}
					return fmt.Errorf("failed to get key %s: %w", key, err)
				}
				// Key exists, value is true
				results[indexes[key]] = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Put adds or updates elements inside the cache with the specified TTL and returns an error if the operation failed
func (c *Cache) Put(keysByHashRange map[int64][]string, ttl time.Duration) error {
	modifiedTTL := c.getTTL(ttl)

	bw := c.cache.NewWriteBatch()
	for hashRange, keys := range keysByHashRange {
		for _, key := range keys {
			cacheKey := getKey(key, hashRange)
			entry := badger.NewEntry(cacheKey, nil)
			if ttl > 0 {
				entry = entry.WithTTL(c.getTTL(modifiedTTL))
			}
			if err := bw.SetEntry(entry); err != nil {
				return fmt.Errorf("failed to put key %s: %w", cacheKey, err)
			}
		}
	}
	return bw.Flush()
}

// GetValue retrieves the value associated with a key.
// Returns the value, a boolean indicating if the key exists, and an error if the operation failed.
func (c *Cache) GetValue(key string, hashRange int64) ([]byte, bool, error) {
	var value []byte
	var found bool

	err := c.cache.View(func(txn *badger.Txn) error {
		item, err := txn.Get(getKey(key, hashRange))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				found = false
				return nil
			}
			return fmt.Errorf("getting key %s: %w", key, err)
		}

		found = true
		value, err = item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("copying value for key %s: %w", key, err)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	return value, found, nil
}

// PutValue stores a key-value pair with the specified TTL.
func (c *Cache) PutValue(key string, hashRange int64, value []byte, ttl time.Duration) error {
	modifiedTTL := c.getTTL(ttl)

	return c.cache.Update(func(txn *badger.Txn) error {
		cacheKey := getKey(key, hashRange)
		entry := badger.NewEntry(cacheKey, value)
		if ttl > 0 {
			entry = entry.WithTTL(modifiedTTL)
		}
		if err := txn.SetEntry(entry); err != nil {
			return fmt.Errorf("putting key %s: %w", key, err)
		}
		return nil
	})
}

func (c *Cache) getTTL(ttl time.Duration) time.Duration {
	if ttl > 0 && c.jitterEnabled && c.jitterDuration > 0 {
		jitter := time.Duration(rand.Int63n(int64(c.jitterDuration)))
		ttl += jitter
	}
	return ttl
}

// CreateSnapshots writes the cache contents to the provided writers
func (c *Cache) CreateSnapshots(
	ctx context.Context,
	writers map[int64]io.Writer,
	since map[int64]uint64,
) (
	uint64, map[int64]bool, error,
) {
	c.snapshottingLock.Lock()
	if c.snapshotting {
		c.snapshottingLock.Unlock()
		return 0, nil, ErrSnapshotInProgress
	}

	c.snapshotting = true
	c.snapshottingLock.Unlock()

	defer func() {
		c.snapshottingLock.Lock()
		c.snapshotting = false
		c.snapshottingLock.Unlock()
	}()

	hashRangesMap := make(map[int64]struct{})
	// We need to know which hash ranges are being written to, as buffers will have zstd footer in case compression is
	// enabled, so we can't use the buff.Len() to check if the writer has data
	hasData := make(map[int64]bool)
	for hashRange := range writers {
		hashRangesMap[hashRange] = struct{}{}
		hasData[hashRange] = false
	}

	lockedWriters := make(map[int64]*lockedWriter)
	for hr := range writers {
		lw := &lockedWriter{}
		if c.compress {
			zw := zstd.NewWriterLevel(writers[hr], c.compressionLevel)
			lw.writer = zw
			lw.closer = zw
		} else {
			lw.writer = writers[hr]
			if wc, ok := writers[hr].(io.Closer); ok {
				lw.closer = wc
			}
		}
		lockedWriters[hr] = lw
	}

	// Ensure writers are closed at the end
	defer func() {
		for _, lw := range lockedWriters {
			if lw.closer != nil {
				// this prevents race conditions with Badger's background goroutines
				lw.mu.Lock()
				_ = lw.closer.Close()
				lw.mu.Unlock()
			}
		}
	}()

	var maxSince uint64
	numGo := c.conf.GetIntVar(10, 1, "BadgerDB.Dedup.Snapshots.NumGoroutines")
	for hashRange, writer := range lockedWriters {
		stream, getState := c.createStream(numGo, hashRange, since[hashRange], writer)
		err := stream.Orchestrate(ctx)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to create snapshot: %w", err)
		}
		since, ok := getState()
		if since > maxSince {
			maxSince = since
		}
		if ok {
			hasData[hashRange] = true
		}
	}

	return maxSince, hasData, nil
}

func (c *Cache) createStream(numGo int, hashRange int64, since uint64, writer *lockedWriter) (
	*badger.Stream,
	func() (uint64, bool),
) {
	var (
		hasData    bool
		maxVersion uint64
	)
	stream := c.cache.NewStream()
	stream.NumGo = numGo
	stream.Prefix = []byte(getKeyPrefix(hashRange))
	stream.SinceTs = since
	stream.ChooseKey = func(item *badger.Item) (ok bool) {
		hasExpired := item.ExpiresAt() > 0 && item.ExpiresAt() <= uint64(time.Now().Unix())
		return !hasExpired
	}
	stream.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		var kvs []*pb.KV
		for _, kv := range list.Kv {
			if maxVersion < kv.Version {
				maxVersion = kv.Version
			}
			if !kv.StreamDone {
				// Extract the hash range from the key.
				// Key format is "hr<hashRange>:<actualKey>".
				parts := bytes.Split(kv.Key, []byte(":"))
				if len(parts) < 2 {
					c.logger.Warnn("Skipping malformed key", logger.NewStringField("key", string(kv.Key)))
					continue // Skip malformed keys
				}

				hashRangeStr := string(parts[0][2:]) // Remove "hr" prefix
				hr, err := strconv.ParseUint(hashRangeStr, 10, 32)
				if err != nil {
					c.logger.Warnn("Skipping key with invalid hash range",
						logger.NewIntField("expected", hashRange),
						logger.NewStringField("actual", hashRangeStr),
						logger.NewStringField("key", string(kv.Key)))
					continue // Skip keys with invalid hash range
				}

				if int64(hr) != hashRange {
					// if this happens stream.Prefix is not used correctly
					c.logger.Warnn("Skipping key different hash range",
						logger.NewIntField("expected", hashRange),
						logger.NewIntField("actual", int64(hr)),
						logger.NewStringField("key", string(kv.Key)))
					continue
				}

				kvs = append(kvs, kv)
				hasData = true
			}
		}

		if err := c.writeTo(&pb.KVList{Kv: kvs}, writer); err != nil {
			return fmt.Errorf("failed to write to hash range %d: %w", hashRange, err)
		}

		return nil
	}

	return stream, func() (uint64, bool) { return maxVersion, hasData }
}

// defaultStreamBatchSize is the size at which StreamSnapshot writes a batch of entries
const defaultStreamBatchSize = 256 << 10

// StreamOptions configures StreamSnapshot
type StreamOptions struct {
	// CompressionLevel is the zstd level used when compression is enabled, the configured level when zero
	CompressionLevel int
	// BatchSize is the size in bytes at which a batch of entries is written, 256 KiB when zero
	BatchSize int
	// Wait is called with the size of each batch before it is written, so callers can apply a byte budget.
	// An error stops the stream. Nil means no budget.
	Wait func(ctx context.Context, n int) error
}

// StreamSnapshot writes the entries of one hash range with a version above since to w, as LoadSnapshot reads them.
// Unlike CreateSnapshots it reads the range with a single iterator and writes small batches as it goes, so it never
// holds more than a batch in memory, and a pass over unchanged data allocates next to nothing.
// Expired keys are skipped. It returns the highest version written and whether anything was written.
func (c *Cache) StreamSnapshot(
	ctx context.Context, hashRange int64, since uint64, w io.Writer, opts StreamOptions,
) (uint64, bool, error) {
	c.snapshottingLock.Lock()
	if c.snapshotting {
		c.snapshottingLock.Unlock()
		return 0, false, ErrSnapshotInProgress
	}
	c.snapshotting = true
	c.snapshottingLock.Unlock()
	defer func() {
		c.snapshottingLock.Lock()
		c.snapshotting = false
		c.snapshottingLock.Unlock()
	}()

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = defaultStreamBatchSize
	}
	lw := &lockedWriter{writer: w}
	if c.compress {
		level := opts.CompressionLevel
		if level <= 0 {
			level = c.compressionLevel
		}
		zw := zstd.NewWriterLevel(w, level)
		lw.writer = zw
		lw.closer = zw
	}

	maxVersion, hasData, err := c.streamRange(ctx, hashRange, since, lw, batchSize, opts.Wait)
	if lw.closer != nil {
		// flushes the zstd frame, the caller's writer is left open
		if closeErr := lw.closer.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("closing compressed stream: %w", closeErr)
		}
	}
	if err != nil {
		return 0, false, fmt.Errorf("streaming hash range %d: %w", hashRange, err)
	}
	return maxVersion, hasData, nil
}

func (c *Cache) streamRange(
	ctx context.Context, hashRange int64, since uint64, w *lockedWriter, batchSize int,
	wait func(ctx context.Context, n int) error,
) (uint64, bool, error) {
	txn := c.cache.NewTransaction(false)
	defer txn.Discard()
	iterOpts := badger.DefaultIteratorOptions
	iterOpts.Prefix = []byte(getKeyPrefix(hashRange))
	iterOpts.SinceTs = since
	iterOpts.PrefetchValues = false
	it := txn.NewIterator(iterOpts)
	defer it.Close()

	var (
		maxVersion uint64
		hasData    bool
		batch      = &pb.KVList{}
		batchBytes int
		now        = uint64(time.Now().Unix())
	)
	flush := func() error {
		if len(batch.Kv) == 0 {
			return nil
		}
		if wait != nil {
			// the batch is written as its size (8 bytes) followed by the marshalled list
			if err := wait(ctx, 8+proto.Size(batch)); err != nil {
				return err
			}
		}
		if err := c.writeTo(batch, w); err != nil {
			return fmt.Errorf("writing a batch: %w", err)
		}
		batch, batchBytes = &pb.KVList{}, 0
		return nil
	}

	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		item := it.Item()
		if item.IsDeletedOrExpired() || (item.ExpiresAt() > 0 && item.ExpiresAt() <= now) {
			continue
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return 0, false, fmt.Errorf("reading value: %w", err)
		}
		kv := &pb.KV{
			Key:       item.KeyCopy(nil),
			Value:     value,
			Version:   item.Version(),
			ExpiresAt: item.ExpiresAt(),
			UserMeta:  []byte{item.UserMeta()},
		}
		batch.Kv = append(batch.Kv, kv)
		batchBytes += len(kv.Key) + len(kv.Value) + 24
		maxVersion = max(maxVersion, kv.Version)
		hasData = true
		if batchBytes >= batchSize {
			if err := flush(); err != nil {
				return 0, false, err
			}
		}
	}
	if err := flush(); err != nil {
		return 0, false, err
	}
	return maxVersion, hasData, nil
}

// LoadSnapshot reads the cache contents from the provided reader
func (c *Cache) LoadSnapshot(_ context.Context, reader io.Reader) error {
	// Use BadgerDB's built-in Load() function which properly handles transaction timestamps
	maxPendingWrites := c.conf.GetIntVar(256, 1, "BadgerDB.Dedup.Snapshots.MaxPendingWrites")

	// The Load() method is not race safe so we have to load data sequentially
	if c.compress {
		reader = zstd.NewReader(reader)
	}
	if err := c.cache.Load(reader, maxPendingWrites); err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	// Force a sync to ensure data is committed
	if err := c.cache.Sync(); err != nil {
		return fmt.Errorf("syncing after load: %w", err)
	}

	return nil
}

// HasData returns true when the cache holds at least one key outside the internal keys
func (c *Cache) HasData() (bool, error) {
	internalPrefix := []byte(getKeyPrefix(internalHashRange))
	var hasData bool
	err := c.cache.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte("hr")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if !bytes.HasPrefix(it.Item().Key(), internalPrefix) {
				hasData = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("iterating keys: %w", err)
	}
	return hasData, nil
}

// Levels returns information about all LSM tree levels and true if L0 tables count is high enough to stall
func (c *Cache) Levels() ([]LevelInfo, bool) {
	stall := false
	levels := c.cache.Levels()
	result := make([]LevelInfo, len(levels))
	for i, l := range levels {
		if l.Level == 0 && l.NumTables >= c.numLevelZeroTablesStall {
			stall = true
		}
		result[i] = LevelInfo{
			Level:     l.Level,
			NumTables: l.NumTables,
			Size:      l.Size,
			Score:     l.Score,
		}
	}
	return result, stall
}

func (c *Cache) LevelsToString() string {
	return c.cache.LevelsToString()
}

func (c *Cache) RunGarbageCollection() {
again: // see https://dgraph.io/docs/badger/get-started/#garbage-collection
	err := c.cache.RunValueLogGC(c.discardRatio)
	if err == nil {
		goto again
	}
}

func (c *Cache) Close() error {
	return c.cache.Close()
}

// Len returns the number of elements in the cache
// WARNING: this must be used in tests only TODO protect this with a testMode=false by default
func (c *Cache) Len() int {
	if !c.debugMode {
		panic("Len() is only available in debug mode")
	}

	count := 0
	err := c.cache.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		panic("failed to get cache length: " + err.Error())
	}
	return count
}

// String returns a string representation of the cache
// WARNING: this must be used in tests only TODO protect this with a testMode=false by default
func (c *Cache) String() string {
	if !c.debugMode {
		panic("String() is only available in debug mode")
	}

	sb := strings.Builder{}
	sb.WriteString("{")

	err := c.cache.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		first := true
		for it.Rewind(); it.Valid(); it.Next() {
			if !first {
				sb.WriteString(",")
			}
			first = false

			item := it.Item()
			key := string(item.Key())
			_, _ = fmt.Fprintf(&sb, "%s:true", key)
		}
		return nil
	})
	if err != nil {
		return fmt.Sprintf("{error:%v}", err)
	}

	sb.WriteString("}")
	return sb.String()
}

func (c *Cache) writeTo(list *pb.KVList, w *lockedWriter) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := binary.Write(w.writer, binary.LittleEndian, uint64(proto.Size(list)))
	if err != nil {
		return err
	}

	var buf []byte
	buf, err = proto.Marshal(list)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(buf)
	return err
}

func getKeyPrefix(hashRange int64) string {
	return "hr" + strconv.Itoa(int(hashRange)) + ":"
}

func getKey(key string, hashRange int64) []byte {
	return []byte(getKeyPrefix(hashRange) + key)
}

// lockedWriter is used to create synchronized writers for each hash range
type lockedWriter struct {
	mu     sync.Mutex
	writer io.Writer
	closer io.Closer // for zstd writers
}

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...any) {
	l.Warnf(fmt, args...)
}
