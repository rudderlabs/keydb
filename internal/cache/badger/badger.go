package badger

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

var ErrSnapshotInProgress = errors.New("snapshotting already in progress")

type Cache struct {
	cache            *badger.DB
	compress         bool
	snapshotSince    uint64
	snapshotting     bool
	snapshottingLock sync.Mutex
}

func Factory(conf *config.Config, log logger.Logger) func(hashRange uint32) (*Cache, error) {
	return func(hashRange uint32) (*Cache, error) {
		badgerPath := conf.GetString("BadgerDB.Dedup.Path", "/tmp/badger")
		badgerPath = path.Join(badgerPath, fmt.Sprintf("%d", hashRange))
		if err := os.MkdirAll(badgerPath, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create badger directory for hash range %d: %w", hashRange, err)
		}
		if conf.GetBool("BadgerDB.TestMode", false) {
			// TODO use this to enable usage of String() string and Len() methods
		}
		badgerCache, err := New(badgerPath, conf, log)
		if err != nil {
			return nil, fmt.Errorf("failed to create cache factory: %w", err)
		}
		return badgerCache, nil
	}
}

func New(path string, conf *config.Config, log logger.Logger) (*Cache, error) {
	opts := badger.DefaultOptions(path).
		WithCompression(options.None).
		WithNumGoroutines(1).
		WithNumVersionsToKeep(1).
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

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Cache{
		cache:    db,
		compress: compress,
	}, nil
}

// Get returns the values associated with the keys and an error if the operation failed
func (c *Cache) Get(keys []string) ([]bool, error) {
	results := make([]bool, len(keys))

	err := c.cache.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			_, err := txn.Get([]byte(key))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					results[i] = false
					continue
				}
				return fmt.Errorf("failed to get key %s: %w", key, err)
			}
			// Key exists, value is true
			results[i] = true
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
	err := c.cache.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			entry := badger.NewEntry([]byte(key), []byte{})
			if ttl > 0 {
				entry = entry.WithTTL(ttl)
			}
			if err := txn.SetEntry(entry); err != nil {
				return fmt.Errorf("failed to put key %s: %w", key, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Len returns the number of elements in the cache
// WARNING: this must be used in tests only TODO protect this with a testMode=false by default
func (c *Cache) Len() int {
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

// CreateSnapshot writes the cache contents to the provided writer
func (c *Cache) CreateSnapshot(w io.Writer) (uint64, error) {
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
		w = gzip.NewWriter(w)
		defer func() { _ = w.(*gzip.Writer).Close() }()
	}

	newSince, err := c.cache.Backup(w, since)
	if err != nil {
		return 0, fmt.Errorf("failed to create snapshot: %w", err)
	}

	c.snapshottingLock.Lock()
	c.snapshotSince = newSince
	c.snapshotting = false
	c.snapshottingLock.Unlock()

	return since, nil
}

// LoadSnapshot reads the cache contents from the provided reader
func (c *Cache) LoadSnapshot(r io.Reader) error {
	// TODO gzip should be configurable, maybe we want to use another algorithm with a different compression level
	if c.compress {
		var err error
		r, err = gzip.NewReader(r)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer func() { _ = r.(*gzip.Reader).Close() }()
	}
	return c.cache.Load(r, 16)
}

func (c *Cache) Close() error {
	return c.cache.Close()
}

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...any) {
	l.Warnf(fmt, args...)
}
