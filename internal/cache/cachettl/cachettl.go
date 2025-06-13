package cachettl

import (
	"io"
	"time"

	"github.com/rudderlabs/keydb/internal/cachettl"
)

// TODO this memory implementation might be useless after all since badger also supports InMemory mode, also
// keeping everything in memory won't really be doable in production

// Cache is an in-memory implementation of the cache interface using the cachettl package
type Cache struct {
	cache *cachettl.Cache[string, bool]
}

// New creates a new in-memory cache
func New() *Cache {
	return &Cache{
		cache: cachettl.New[string, bool](cachettl.WithNoRefreshTTL),
	}
}

// Get returns the values associated with the keys and an error if the operation failed
func (c *Cache) Get(keys []string) ([]bool, error) {
	results := make([]bool, len(keys))
	for i, key := range keys {
		results[i] = c.cache.Get(key)
	}
	return results, nil
}

// Put adds or updates elements inside the cache with the specified TTL and returns an error if the operation failed
func (c *Cache) Put(keys []string, ttl time.Duration) error {
	for _, key := range keys {
		c.cache.Put(key, true, ttl)
	}
	return nil
}

// Len returns the number of elements in the cache
func (c *Cache) Len() int {
	return c.cache.Len()
}

// String returns a string representation of the cache
func (c *Cache) String() string {
	return c.cache.String()
}

// CreateSnapshot writes the cache contents to the provided writer
func (c *Cache) CreateSnapshot(w io.Writer) error {
	return cachettl.CreateSnapshot(w, c.cache)
}

// LoadSnapshot reads the cache contents from the provided reader
func (c *Cache) LoadSnapshot(r io.Reader) error {
	return cachettl.LoadSnapshot(r, c.cache)
}
