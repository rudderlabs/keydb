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
	clusterSize     uint32
	totalHashRanges uint32
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

func New(clusterSize, totalHashRanges uint32, opts ...Option) *Hash {
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
	for i := uint32(0); i < clusterSize; i++ {
		c.Add(member(strconv.FormatUint(uint64(i), 10)))
	}

	return &Hash{
		clusterSize:     clusterSize,
		totalHashRanges: totalHashRanges,
		consistent:      c,
		hasher:          cfg.Hasher,
	}
}

func (h *Hash) ClusterSize() uint32 {
	return h.clusterSize
}

func (h *Hash) TotalHashRanges() uint32 {
	return h.totalHashRanges
}

func (h *Hash) GetNodeNumber(key string) uint32 {
	m := h.consistent.LocateKey([]byte(key))
	nodeID, err := strconv.ParseUint(m.String(), 10, 32)
	if err != nil {
		panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
	}
	return uint32(nodeID)
}

func (h *Hash) GetKeysByHashRange(keys []string, nodeID uint32) (
	map[uint32][]string,
	error,
) {
	m := make(map[uint32][]string)
	for _, key := range keys {
		bk := []byte(key)
		mem := h.consistent.LocateKey(bk)
		keyNodeID, err := strconv.ParseUint(mem.String(), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if nodeID != uint32(keyNodeID) {
			hashRange := h.getHashRangeBytes(bk)
			return nil, fmt.Errorf("hashRange %d not for node %d: %w", hashRange, nodeID, ErrWrongNode)
		}

		hashRange := h.getHashRangeBytes(bk)
		m[hashRange] = append(m[hashRange], key)
	}

	return m, nil
}

func (h *Hash) GetKeysByHashRangeWithIndexes(keys []string, nodeID uint32) (
	map[uint32][]string,
	map[string]int,
	error,
) {
	m := make(map[uint32][]string)
	indexes := make(map[string]int, len(keys))
	for i, key := range keys {
		bk := []byte(key)
		mem := h.consistent.LocateKey(bk).(member)
		keyNodeID, err := strconv.ParseUint(string(mem), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if nodeID != uint32(keyNodeID) {
			hashRange := h.getHashRangeBytes(bk)
			return nil, nil, fmt.Errorf("hashRange %d not for node %d: %w", hashRange, nodeID, ErrWrongNode)
		}

		hashRange := h.getHashRangeBytes(bk)
		m[hashRange] = append(m[hashRange], key)
		indexes[key] = i
	}

	return m, indexes, nil
}

func (h *Hash) GetNodeHashRanges(nodeID uint32) map[uint32]struct{} {
	if nodeID >= h.clusterSize {
		panic("nodeID must be less than clusterSize")
	}

	ranges := make(map[uint32]struct{})
	for hashRange := uint32(0); hashRange < h.totalHashRanges; hashRange++ {
		m := h.consistent.GetPartitionOwner(int(hashRange)).(member)
		rangeNodeID, err := strconv.ParseUint(string(m), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if uint32(rangeNodeID) == nodeID {
			ranges[hashRange] = struct{}{}
		}
	}
	return ranges
}

func (h *Hash) GetNodeHashRangesList(nodeID uint32) []uint32 {
	if nodeID >= h.clusterSize {
		panic("nodeID must be less than clusterSize")
	}

	ranges := make([]uint32, 0)
	for i := uint32(0); i < h.totalHashRanges; i++ {
		m := h.consistent.GetPartitionOwner(int(i)).(member)
		rangeNodeID, err := strconv.ParseUint(string(m), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if uint32(rangeNodeID) == nodeID {
			ranges = append(ranges, i)
		}
	}
	return ranges
}

func GetHashRangeMovements(
	oldClusterSize,
	newClusterSize,
	totalHashRanges uint32,
) (
	map[uint32][]uint32,
	map[uint32][]uint32,
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

	sourceNodeMovements := make(map[uint32][]uint32)
	destinationNodeMovements := make(map[uint32][]uint32)

	for hashRange := uint32(0); hashRange < totalHashRanges; hashRange++ {
		oldMember := oldClusterHasher.consistent.GetPartitionOwner(int(hashRange)).(member)
		oldNodeID, err := strconv.ParseUint(string(oldMember), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		newMember := newClusterHasher.consistent.GetPartitionOwner(int(hashRange)).(member)
		newNodeID, err := strconv.ParseUint(string(newMember), 10, 32)
		if err != nil {
			panic(fmt.Errorf("implementation error: members must be unsigned integers: %w", err))
		}

		if uint32(oldNodeID) != uint32(newNodeID) {
			sourceNodeMovements[uint32(oldNodeID)] = append(sourceNodeMovements[uint32(oldNodeID)], hashRange)
			destinationNodeMovements[uint32(newNodeID)] = append(destinationNodeMovements[uint32(newNodeID)], hashRange)
		}
	}

	return sourceNodeMovements, destinationNodeMovements
}

func (h *Hash) getHashRange(key string) uint32 {
	return h.getHashRangeFromValue(h.hasher.Sum64([]byte(key)))
}

func (h *Hash) getHashRangeBytes(key []byte) uint32 {
	return h.getHashRangeFromValue(h.hasher.Sum64(key))
}

func (h *Hash) getHashRangeFromValue(hashValue uint64) uint32 {
	return uint32(hashValue % uint64(h.totalHashRanges))
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
