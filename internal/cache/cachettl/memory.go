package cachettl

import (
	"io"
	"time"

	"github.com/rudderlabs/keydb/internal/cachettl"
)

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

// Get returns the value associated with the key
func (c *Cache) Get(key string) bool {
	return c.cache.Get(key)
}

// Put adds or updates an element inside the cache with the specified TTL
func (c *Cache) Put(key string, value bool, ttl time.Duration) {
	c.cache.Put(key, value, ttl)
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
