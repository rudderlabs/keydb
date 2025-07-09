package badger

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"google.golang.org/protobuf/proto"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

var ErrSnapshotInProgress = errors.New("snapshotting already in progress")

type hasher interface {
	GetKeysByHashRange(keys []string) (
		map[uint32][]string, // itemsByHashRange
		error,
	)
	GetKeysByHashRangeWithIndexes(keys []string) (
		map[uint32][]string, // itemsByHashRange
		map[string]int, // indexes
		error,
	)
}

type Cache struct {
	hasher           hasher
	cache            *badger.DB
	conf             *config.Config
	logger           logger.Logger
	compress         bool
	discardRatio     float64
	snapshotSince    uint64
	snapshotting     bool
	snapshottingLock sync.Mutex
	debugMode        bool
}

func New(h hasher, conf *config.Config, log logger.Logger) (*Cache, error) {
	path := conf.GetString("BadgerDB.Dedup.Path", "/tmp/badger")
	opts := badger.DefaultOptions(path).
		WithCompression(options.None).
		WithNumVersionsToKeep(1).
		WithNumGoroutines(conf.GetInt("BadgerDB.Dedup.NumGoroutines", 128/3)).
		WithBloomFalsePositive(conf.GetFloat64("BadgerDB.Dedup.BloomFalsePositive", 0.000001)).
		WithIndexCacheSize(conf.GetInt64Var(16*bytesize.MB, 1, "BadgerDB.Dedup.indexCacheSize", "BadgerDB.indexCacheSize")).
		WithValueLogFileSize(conf.GetInt64Var(1*bytesize.MB, 1, "BadgerDB.Dedup.valueLogFileSize", "BadgerDB.valueLogFileSize")).
		WithBlockSize(conf.GetIntVar(int(4*bytesize.KB), 1, "BadgerDB.Dedup.blockSize", "BadgerDB.blockSize")).
		WithMemTableSize(conf.GetInt64Var(20*bytesize.MB, 1, "BadgerDB.Dedup.memTableSize", "BadgerDB.memTableSize")).
		WithNumMemtables(conf.GetIntVar(5, 1, "BadgerDB.Dedup.numMemtable", "BadgerDB.numMemtable")).
		WithNumLevelZeroTables(conf.GetIntVar(5, 1, "BadgerDB.Dedup.numLevelZeroTables", "BadgerDB.numLevelZeroTables")).
		WithNumLevelZeroTablesStall(conf.GetIntVar(10, 1, "BadgerDB.Dedup.numLevelZeroTablesStall", "BadgerDB.numLevelZeroTablesStall")).
		WithBaseTableSize(conf.GetInt64Var(1*bytesize.MB, 1, "BadgerDB.Dedup.baseTableSize", "BadgerDB.baseTableSize")).
		WithBaseLevelSize(conf.GetInt64Var(5*bytesize.MB, 1, "BadgerDB.Dedup.baseLevelSize", "BadgerDB.baseLevelSize")).
		WithLevelSizeMultiplier(conf.GetIntVar(10, 1, "BadgerDB.Dedup.levelSizeMultiplier", "BadgerDB.levelSizeMultiplier")).
		WithMaxLevels(conf.GetIntVar(7, 1, "BadgerDB.Dedup.maxLevels", "BadgerDB.maxLevels")).
		WithNumCompactors(conf.GetIntVar(4, 1, "BadgerDB.Dedup.numCompactors", "BadgerDB.numCompactors")).
		WithValueThreshold(conf.GetInt64Var(10*bytesize.B, 1, "BadgerDB.Dedup.valueThreshold", "BadgerDB.valueThreshold")).
		WithSyncWrites(conf.GetBoolVar(false, "BadgerDB.Dedup.syncWrites", "BadgerDB.syncWrites")).
		WithBlockCacheSize(conf.GetInt64Var(0, 1, "BadgerDB.Dedup.blockCacheSize", "BadgerDB.blockCacheSize")).
		WithDetectConflicts(conf.GetBoolVar(false, "BadgerDB.Dedup.detectConflicts", "BadgerDB.detectConflicts")).
		WithLogger(loggerForBadger{log})

	compress := conf.GetBool("BadgerDB.Dedup.Compress", true)
	if compress {
		log.Infon("BadgerDB.Dedup.Compress is enabled, using gzip compression for snapshots")
	}

	log.Infon("Starting BadgerDB", logger.NewStringField("path", path))

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Cache{
		hasher:       h,
		cache:        db,
		conf:         conf,
		logger:       log,
		compress:     compress,
		discardRatio: conf.GetFloat64("BadgerDB.Dedup.DiscardRatio", 0.7),
		debugMode:    conf.GetBool("BadgerDB.DebugMode", false),
	}, nil
}

// Get returns the values associated with the keys and an error if the operation failed
func (c *Cache) Get(keys []string) ([]bool, error) {
	itemsByHashRange, indexes, err := c.hasher.GetKeysByHashRangeWithIndexes(keys)
	if err != nil {
		return nil, fmt.Errorf("cache get keys: %w", err)
	}

	results := make([]bool, len(keys))

	err = c.cache.View(func(txn *badger.Txn) error {
		for hashRange, keys := range itemsByHashRange {
			for _, key := range keys {
				_, err := txn.Get(c.getKey(key, hashRange))
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
func (c *Cache) Put(keys []string, ttl time.Duration) error {
	itemsByHashRange, err := c.hasher.GetKeysByHashRange(keys)
	if err != nil {
		return fmt.Errorf("cache put keys: %w", err)
	}

	err = c.cache.Update(func(txn *badger.Txn) error {
		for hashRange, keys := range itemsByHashRange {
			for _, key := range keys {
				entry := badger.NewEntry(c.getKey(key, hashRange), []byte{})
				if ttl > 0 {
					entry = entry.WithTTL(ttl)
				}
				if err := txn.SetEntry(entry); err != nil {
					return fmt.Errorf("failed to put key %s: %w", key, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// CreateSnapshots writes the cache contents to the provided writers
func (c *Cache) CreateSnapshots(ctx context.Context, w map[uint32]io.Writer) (uint64, error) {
	c.snapshottingLock.Lock()
	if c.snapshotting {
		c.snapshottingLock.Unlock()
		return 0, ErrSnapshotInProgress
	}

	since := c.snapshotSince
	c.snapshotting = true
	c.snapshottingLock.Unlock()

	// TODO gzip should be configurable, maybe we want to use another algorithm with a different compression level
	if c.compress {
		for hashRange, writer := range w {
			w[hashRange] = gzip.NewWriter(writer)
		}
		defer func() {
			for _, writer := range w {
				err := writer.(*gzip.Writer).Close()
				if err != nil {
					c.logger.Errorn("failed to close gzip writer", obskit.Error(err))
				}
			}
		}()
	}

	hashRangesMap := make(map[uint32]struct{})
	for hashRange := range w {
		hashRangesMap[hashRange] = struct{}{}
	}

	var (
		maxVersion     uint64
		keysToDelete   = make([][]byte, 0)
		keysToDeleteMu = sync.Mutex{}
	)
	stream := c.cache.NewStream()
	stream.NumGo = c.conf.GetInt("BadgerDB.Dedup.Snapshots.NumGoroutines", 10)
	stream.Prefix = []byte("hr")
	stream.SinceTs = since
	stream.ChooseKey = func(item *badger.Item) (ok bool) {
		hasExpired := item.ExpiresAt() > 0 && item.ExpiresAt() <= uint64(time.Now().Unix())
		if !hasExpired {
			parts := bytes.Split(item.Key(), []byte(":"))
			hashRange, _ := strconv.ParseUint(string(parts[0][2:]), 10, 32)
			_, ok = hashRangesMap[uint32(hashRange)]
		}
		if hasExpired || !ok {
			keysToDeleteMu.Lock()
			keysToDelete = append(keysToDelete, item.Key())
			keysToDeleteMu.Unlock()
			return false
		}
		return true
	}
	stream.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		// Group KV pairs by hash range
		kvsByHashRange := make(map[uint32][]*pb.KV)

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
				hashRange, err := strconv.ParseUint(hashRangeStr, 10, 32)
				if err != nil {
					c.logger.Warnn("Skipping key with invalid hash range", logger.NewStringField("key", string(kv.Key)))
					continue // Skip keys with invalid hash range
				}

				hashRange32 := uint32(hashRange)

				// Only include if we have a writer for this hash range
				if _, exists := w[hashRange32]; exists {
					kvsByHashRange[hashRange32] = append(kvsByHashRange[hashRange32], kv)
				}
			}
		}

		// Write to appropriate writers by hash range
		for hashRange, kvs := range kvsByHashRange {
			writer := w[hashRange]

			// Create a new KVList for this hash range
			rangeList := &pb.KVList{Kv: kvs}

			if err := writeTo(rangeList, writer); err != nil {
				return fmt.Errorf("failed to write to hash range %d: %w", hashRange, err)
			}
		}

		return nil
	}

	err := stream.Orchestrate(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to create snapshot: %w", err)
	}

	c.snapshottingLock.Lock()
	c.snapshotSince = maxVersion
	c.snapshotting = false
	c.snapshottingLock.Unlock()

	if len(keysToDelete) > 0 {
		batchSize := c.conf.GetInt("BadgerDB.Dedup.Snapshots.DeleteBatchSize", 1000)
		keysToDeleteBatch := make([][]byte, 0, batchSize)
		deleteKeys := func() error {
			err := c.cache.Update(func(txn *badger.Txn) error {
				for _, key := range keysToDeleteBatch {
					return txn.Delete(key)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to delete keys: %w", err)
			}
			keysToDeleteBatch = make([][]byte, 0, batchSize)
			return nil
		}
		for _, key := range keysToDelete {
			keysToDeleteBatch = append(keysToDeleteBatch, key)
			if len(keysToDeleteBatch) == batchSize {
				if err := deleteKeys(); err != nil {
					return 0, fmt.Errorf("failed to delete keys: %w", err)
				}
			}
		}
		if len(keysToDeleteBatch) > 0 {
			if err := deleteKeys(); err != nil {
				return 0, fmt.Errorf("failed to delete keys: %w", err)
			}
		}
	}

	return since, nil
}

// LoadSnapshots reads the cache contents from the provided readers
func (c *Cache) LoadSnapshots(r ...io.Reader) error {
	// TODO gzip should be configurable, maybe we want to use another algorithm with a different compression level
	if c.compress {
		var err error
		for i, rdr := range r {
			r[i], err = gzip.NewReader(rdr)
			if err != nil {
				return fmt.Errorf("failed to create gzip reader: %w", err)
			}
		}
		defer func() {
			for _, rdr := range r {
				err := rdr.(*gzip.Reader).Close()
				if err != nil {
					c.logger.Errorn("failed to close gzip reader", obskit.Error(err))
				}
			}
		}()
	}

	// TODO implement custom load with KVLoader

	return nil
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
			sb.WriteString(fmt.Sprintf("%s:true", key))
		}
		return nil
	})
	if err != nil {
		return fmt.Sprintf("{error:%v}", err)
	}

	sb.WriteString("}")
	return sb.String()
}

func (c *Cache) getKey(key string, hashRange uint32) []byte {
	return []byte("hr" + strconv.Itoa(int(hashRange)) + ":" + key)
}

func writeTo(list *pb.KVList, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(proto.Size(list))); err != nil {
		return err
	}
	buf, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...any) {
	l.Warnf(fmt, args...)
}
