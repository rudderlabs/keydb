package hash

import (
	"fmt"
	"hash/fnv"
	"strconv"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

var ErrWrongNode = fmt.Errorf("wrong node")

const (
	DefaultReplicationFactor = 20
	DefaultLoad              = 1.25
)

type Hash struct {
	clusterSize     int64
	totalHashRanges int64
	consistent      *consistent.Consistent
	hasher          consistent.Hasher
}

type Option func(*consistent.Config) // TODO use functional options with *config.Config

func WithReplicationFactor(factor int) Option {
	return func(cfg *consistent.Config) { cfg.ReplicationFactor = factor }
}

func WithLoad(load float64) Option {
	return func(cfg *consistent.Config) { cfg.Load = load }
}

func WithFnvHasher() Option {
	return func(cfg *consistent.Config) { cfg.Hasher = fnvHasher{} }
}

func New(clusterSize, totalHashRanges int64, opts ...Option) *Hash {
	if clusterSize == 0 {
		panic("clusterSize must be greater than 0")
	}
	if totalHashRanges < clusterSize {
		panic("totalHashRanges must be greater than or equal to clusterSize")
	}

	cfg := consistent.Config{
		PartitionCount:    int(totalHashRanges),
		ReplicationFactor: DefaultReplicationFactor,
		Load:              DefaultLoad,
		Hasher:            xxhashHasher{},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	c := consistent.New(nil, cfg)
	for i := int64(0); i < clusterSize; i++ {
		c.Add(member(strconv.FormatInt(i, 10)))
	}

	return &Hash{
		clusterSize:     clusterSize,
		totalHashRanges: totalHashRanges,
		consistent:      c,
		hasher:          cfg.Hasher,
	}
}

func (h *Hash) ClusterSize() int64 {
	return h.clusterSize
}

func (h *Hash) TotalHashRanges() int64 {
	return h.totalHashRanges
}

func (h *Hash) GetNodeNumber(key string) int64 {
	m := h.consistent.LocateKey([]byte(key))
	nodeID, err := strconv.ParseInt(m.String(), 10, 32)
	if err != nil {
		panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
	}
	return nodeID
}

func (h *Hash) GetKeysByHashRange(keys []string, nodeID int64) (
	map[int64][]string,
	error,
) {
	m := make(map[int64][]string)
	for _, key := range keys {
		bk := []byte(key)
		mem := h.consistent.LocateKey(bk)
		keyNodeID, err := strconv.ParseInt(mem.String(), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if nodeID != keyNodeID {
			hashRange := h.getHashRangeBytes(bk)
			return nil, fmt.Errorf("hashRange %d not for node %d: %w", hashRange, nodeID, ErrWrongNode)
		}

		hashRange := h.getHashRangeBytes(bk)
		m[hashRange] = append(m[hashRange], key)
	}

	return m, nil
}

func (h *Hash) GetKeysByHashRangeWithIndexes(keys []string, nodeID int64) (
	map[int64][]string,
	map[string]int,
	error,
) {
	m := make(map[int64][]string)
	indexes := make(map[string]int, len(keys))
	for i, key := range keys {
		bk := []byte(key)
		mem := h.consistent.LocateKey(bk)
		keyNodeID, err := strconv.ParseInt(mem.String(), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if nodeID != keyNodeID {
			hashRange := h.getHashRangeBytes(bk)
			return nil, nil, fmt.Errorf("hashRange %d not for node %d: %w", hashRange, nodeID, ErrWrongNode)
		}

		hashRange := h.getHashRangeBytes(bk)
		m[hashRange] = append(m[hashRange], key)
		indexes[key] = i
	}

	return m, indexes, nil
}

func (h *Hash) GetNodeHashRanges(nodeID int64) map[int64]struct{} {
	if nodeID >= h.clusterSize {
		panic("nodeID must be less than clusterSize")
	}

	ranges := make(map[int64]struct{})
	for hashRange := int64(0); hashRange < h.totalHashRanges; hashRange++ {
		m := h.consistent.GetPartitionOwner(int(hashRange))
		rangeNodeID, err := strconv.ParseInt(m.String(), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if rangeNodeID == nodeID {
			ranges[hashRange] = struct{}{}
		}
	}
	return ranges
}

func (h *Hash) GetNodeHashRangesList(nodeID int64) []int64 {
	if nodeID >= h.clusterSize {
		panic("nodeID must be less than clusterSize")
	}

	ranges := make([]int64, 0)
	for i := int64(0); i < h.totalHashRanges; i++ {
		m := h.consistent.GetPartitionOwner(int(i))
		rangeNodeID, err := strconv.ParseInt(m.String(), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if rangeNodeID == nodeID {
			ranges = append(ranges, i)
		}
	}
	return ranges
}

func GetHashRangeMovements(
	oldClusterSize,
	newClusterSize,
	totalHashRanges int64,
) (
	map[int64][]int64,
	map[int64][]int64,
) {
	if oldClusterSize == 0 {
		panic("oldClusterSize must be greater than 0")
	}
	if newClusterSize == 0 {
		panic("newClusterSize must be greater than 0")
	}
	if totalHashRanges < oldClusterSize {
		panic("totalHashRanges must be greater than or equal to oldClusterSize")
	}
	if totalHashRanges < newClusterSize {
		panic("totalHashRanges must be greater than or equal to newClusterSize")
	}

	oldClusterHasher := New(oldClusterSize, totalHashRanges)
	newClusterHasher := New(newClusterSize, totalHashRanges)

	sourceNodeMovements := make(map[int64][]int64)
	destinationNodeMovements := make(map[int64][]int64)

	for hashRange := int64(0); hashRange < totalHashRanges; hashRange++ {
		oldMember := oldClusterHasher.consistent.GetPartitionOwner(int(hashRange))
		oldNodeID, err := strconv.ParseInt(oldMember.String(), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		newMember := newClusterHasher.consistent.GetPartitionOwner(int(hashRange))
		newNodeID, err := strconv.ParseInt(newMember.String(), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if oldNodeID != newNodeID {
			sourceNodeMovements[oldNodeID] = append(sourceNodeMovements[oldNodeID], hashRange)
			destinationNodeMovements[newNodeID] = append(destinationNodeMovements[newNodeID], hashRange)
		}
	}

	return sourceNodeMovements, destinationNodeMovements
}

func (h *Hash) getHashRange(key string) int64 {
	return h.getHashRangeFromValue(h.hasher.Sum64([]byte(key)))
}

func (h *Hash) getHashRangeBytes(key []byte) int64 {
	return h.getHashRangeFromValue(h.hasher.Sum64(key))
}

func (h *Hash) getHashRangeFromValue(hashValue uint64) int64 {
	return int64(hashValue % uint64(h.totalHashRanges))
}

type member string

func (m member) String() string { return string(m) }

// xxhashHasher implements consistent.Hasher using xxhash
type xxhashHasher struct{}

func (h xxhashHasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// fnvHasher implements consistent.Hasher using FNV
type fnvHasher struct{}

func (h fnvHasher) Sum64(data []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(data)
	return hasher.Sum64()
}
