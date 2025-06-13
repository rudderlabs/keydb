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

// Get returns the value associated with the key and an error if the operation failed
func (c *Cache) Get(key string) (bool, error) {
	var result bool
	err := c.cache.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				result = false
				return nil
			}
			return err
		}
		// Key exists, value is true
		result = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	return result, nil
}

// Put adds or updates an element inside the cache with the specified TTL and returns an error if the operation failed
func (c *Cache) Put(key string, _ bool, ttl time.Duration) error {
	err := c.cache.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), []byte{})
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
	if err != nil {
		return fmt.Errorf("failed to put key %s: %w", key, err)
	}
	return nil
}

// Len returns the number of elements in the cache
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
		return 0 // TODO we should handle the error
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
