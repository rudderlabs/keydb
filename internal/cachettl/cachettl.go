package cachettl

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Cache is a double-linked list sorted by expiration time (ascending order)
// the root (head) node is the node with the lowest expiration time
// the tail node (end) is the node with the highest expiration time
// Cleanups are done on Get() calls so if Get() is never invoked then Nodes stay in-memory.
type Cache[K comparable, V any] struct {
	root *node[K, V]
	mu   sync.RWMutex
	m    map[K]*node[K, V]

	config    cacheConfig
	onEvicted func(key K, value V)
}

type node[K comparable, V any] struct {
	key        K
	value      V
	prev       *node[K, V]
	next       *node[K, V]
	ttl        time.Duration
	expiration time.Time
}

func (n *node[K, V]) remove() {
	n.prev.next = n.next
	n.next.prev = n.prev
}

// New returns a new Cache.
func New[K comparable, V any](opts ...Opt) *Cache[K, V] {
	c := &Cache[K, V]{
		config: cacheConfig{
			now:        time.Now,
			refreshTTL: true,
		},
		root: &node[K, V]{},
		m:    make(map[K]*node[K, V]),
	}
	for _, opt := range opts {
		opt(&c.config)
	}
	return c
}

func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

func (c *Cache[K, V]) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sb := strings.Builder{}
	sb.WriteString("{")
	for n := c.root.next; n != nil && n != c.root; n = n.next {
		sb.WriteString(fmt.Sprintf("%v:%v", n.key, n.value))
		if n.next != nil && n.next != c.root {
			sb.WriteString(",")
		}
	}
	sb.WriteString("}")
	return sb.String()
}

// Get returns the value associated with the key or nil otherwise.
// Additionally, Get() will refresh the TTL by default and cleanup expired nodes.
func (c *Cache[K, V]) Get(key K) (zero V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	defer func() { // remove expired nodes
		cn := c.root.next // start from head since we're sorting by expiration with the highest expiration at the tail
		for cn != nil && cn != c.root {
			if c.config.now().After(cn.expiration) {
				cn.remove()             // removes a node from the linked list (leaves the map untouched)
				delete(c.m, cn.key)     // remove node from map too
				if c.onEvicted != nil { // call the OnEvicted callback if it's set
					c.onEvicted(cn.key, cn.value)
				}
			} else { // there is nothing else to clean up, no need to iterate further
				break
			}
			cn = cn.next
		}
	}()

	if n, ok := c.m[key]; ok && n.expiration.After(c.config.now()) {
		if c.config.refreshTTL {
			n.remove()
			n.expiration = c.config.now().Add(n.ttl) // refresh TTL
			c.add(n)
		}
		return n.value
	}
	return zero
}

// Put adds or updates an element inside the Cache.
// The Cache will be sorted with the node with the highest expiration at the tail.
func (c *Cache[K, V]) Put(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.put(key, value, ttl)
}

func (c *Cache[K, V]) put(key K, value V, ttl time.Duration) {
	now := c.config.now()

	n, ok := c.m[key]
	if !ok {
		n = &node[K, V]{
			key: key, value: value, ttl: ttl, expiration: now.Add(ttl),
		}
		c.m[key] = n
	} else {
		n.value = value
		n.expiration = now.Add(ttl)
	}

	if c.root.next == nil { // first node insertion
		c.root.next = n
		c.root.prev = n
		n.prev = c.root
		n.next = c.root
		return
	}

	if ok { // removes a node from the linked list (leaves the map untouched)
		n.remove()
	}

	c.add(n)
}

func (c *Cache[K, V]) OnEvicted(onEvicted func(key K, value V)) {
	c.onEvicted = onEvicted
}

func (c *Cache[K, V]) add(n *node[K, V]) {
	cn := c.root.prev // tail
	for cn != nil {   // iterate from tail to root because we have expiring nodes towards the tail
		if n.expiration.After(cn.expiration) || n.expiration.Equal(cn.expiration) {
			// insert node after cn
			save := cn.next
			cn.next = n
			n.prev = cn
			n.next = save
			save.prev = n
			break
		}
		cn = cn.prev
	}
}

// slice is used for debugging purposes only
func (c *Cache[K, V]) slice() (s []V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cn := c.root.next
	for cn != nil && cn != c.root {
		s = append(s, cn.value)
		cn = cn.next
	}
	return
}

// TODO using CreateSnapshot and LoadSnapshot without generics for now to simplify the prototype

// CreateSnapshot writes one line per key (without the value) with its expiration time in ms, one line per key
// e.g.
// key1:1749455912571\n
// key2:1749455912573\n
func CreateSnapshot(w io.Writer, c *Cache[string, bool]) error {
	c.mu.Lock()
	cn := c.root.next // start from head since we're sorting by expiration with the highest expiration at the tail
	for cn != nil && cn != c.root {
		if c.config.now().After(cn.expiration) {
			cn.remove()             // removes a node from the linked list (leaves the map untouched)
			delete(c.m, cn.key)     // remove node from map too
			if c.onEvicted != nil { // call the OnEvicted callback if it's set
				c.onEvicted(cn.key, cn.value)
			}
		} else {
			break
		}
		cn = cn.next
	}
	c.mu.Unlock()

	c.mu.RLock()
	defer c.mu.RUnlock()
	for cn != nil && cn != c.root {
		if c.config.now().After(cn.expiration) { // checking again since we had to release and re-get the lock
			// let's not remove these, we'll remove them at the next snapshot, now we want to focus on performance
			// meaning we just operate with a read lock now
			cn = cn.next
			continue
		}
		_, err := w.Write([]byte(cn.key + ":" + strconv.FormatInt(cn.expiration.UnixMilli(), 10) + "\n"))
		if err != nil {
			return fmt.Errorf("failed to write snapshot: %w", err)
		}
		cn = cn.next
	}

	return nil
}

// LoadSnapshot reads snapshot data from an io.Reader and populates the cache.
// The format of the snapshot is one line per key in the format "key:ttl\n"
// where ttl is in milliseconds.
// Lines with expired TTLs are skipped.
func LoadSnapshot(r io.Reader, c *Cache[string, bool]) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		// Split the line into key and ttl
		parts := strings.Split(scanner.Text(), ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid format: %s", scanner.Text())
		}

		unixMilli, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid expiration time: %s", parts[1])
		}

		t := time.UnixMilli(unixMilli)

		// Skip expired entries
		if c.config.now().After(t) {
			continue
		}

		// Add the key to the cache
		c.put(parts[0], true, t.Sub(c.config.now()))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
