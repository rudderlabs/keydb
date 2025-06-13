package badger

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Cache struct {
	cache *badger.DB
}

func New(path string) (*Cache, error) {
	opts := badger.DefaultOptions(path) // Open a badger database on disk
	opts.WithBloomFalsePositive(0)      // Setting this to 0 disables the bloom filter completely.
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Cache{cache: db}, nil
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
// WARNING: this must be used in tests only
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
func (c *Cache) CreateSnapshot(w io.Writer) error {
	_, err := c.cache.Backup(w, 0)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot reads the cache contents from the provided reader
func (c *Cache) LoadSnapshot(r io.Reader) error {
	return c.cache.Load(r, 16)
}
