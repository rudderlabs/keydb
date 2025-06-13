package badger

import (
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Cache struct {
	cache *badger.DB
}

func New() *Cache {
	// TODO implement
	return nil
}

// Get returns the value associated with the key
func (c *Cache) Get(key string) bool {
	return false // TODO implement
}

// Put adds or updates an element inside the cache with the specified TTL
func (c *Cache) Put(key string, value bool, ttl time.Duration) {
	// TODO implement
}

// Len returns the number of elements in the cache
func (c *Cache) Len() int {
	return 0 // TODO implement
}

// String returns a string representation of the cache
func (c *Cache) String() string { return "not supported" }

// CreateSnapshot writes the cache contents to the provided writer
func (c *Cache) CreateSnapshot(w io.Writer) error {
	return nil // TODO implement
}

// LoadSnapshot reads the cache contents from the provided reader
func (c *Cache) LoadSnapshot(r io.Reader) error {
	return nil // TODO implement
}
