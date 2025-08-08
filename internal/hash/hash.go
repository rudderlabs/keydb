package hash

import (
	"fmt"
	"hash/fnv"
)

var ErrWrongNode = fmt.Errorf("wrong node")

// GetNodeNumber computes the hash of a key and determines which node should handle it
// given the total number of nodes and hash ranges.
//
// Parameters:
// - key: The key to hash
// - clusterSize: The total number of nodes in the cluster
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - The node number (0-based) that should handle this key
func GetNodeNumber(key string, clusterSize, totalHashRanges uint32) (uint32, uint32) {
	if clusterSize == 0 {
		panic("clusterSize must be greater than 0")
	}
	if totalHashRanges < clusterSize {
		panic("totalHashRanges must be greater than or equal to clusterSize")
	}

	// Calculate the hash of the key
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	hashValue := h.Sum32()

	// Determine which hash range this key belongs to
	hashRange := hashValue % totalHashRanges

	// Determine which node handles this hash range
	return hashRange, hashRange % clusterSize
}

func GetKeysByHashRange(keys []string, nodeID, clusterSize, totalHashRanges uint32) (
	map[uint32][]string, // keysByHashRange
	error,
) {
	if clusterSize == 0 {
		panic("clusterSize must be greater than 0")
	}
	if totalHashRanges < clusterSize {
		panic("totalHashRanges must be greater than or equal to clusterSize")
	}

	m := make(map[uint32][]string)
	for _, key := range keys {
		// Create a new hash object for each key
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		hashValue := h.Sum32()
		hashRange := hashValue % totalHashRanges

		if nodeID != hashRange%clusterSize {
			return nil, fmt.Errorf("hashRange %d not for node %d: %w", hashRange, nodeID, ErrWrongNode)
		}

		m[hashRange] = append(m[hashRange], key)
	}

	return m, nil
}

func GetKeysByHashRangeWithIndexes(keys []string, nodeID, clusterSize, totalHashRanges uint32) (
	map[uint32][]string, // keysByHashRange
	map[string]int, // indexes
	error,
) {
	if clusterSize == 0 {
		panic("clusterSize must be greater than 0")
	}
	if totalHashRanges < clusterSize {
		panic("totalHashRanges must be greater than or equal to clusterSize")
	}

	m := make(map[uint32][]string)
	indexes := make(map[string]int, len(keys))
	for i, key := range keys {
		// Create a new hash object for each key
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		hashValue := h.Sum32()
		hashRange := hashValue % totalHashRanges

		if nodeID != hashRange%clusterSize {
			return nil, nil, fmt.Errorf("hashRange %d not for node %d: %w", hashRange, nodeID, ErrWrongNode)
		}

		m[hashRange] = append(m[hashRange], key)
		indexes[key] = i
	}

	return m, indexes, nil
}

// GetNodeHashRanges returns the hash ranges that a specific node should handle
// given the total number of nodes and hash ranges.
//
// Parameters:
// - nodeID: The ID of the node (0-based)
// - clusterSize: The total number of nodes in the cluster
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - A map with the key representing the hash ranges that this node should handle
func GetNodeHashRanges(nodeID, clusterSize, totalHashRanges uint32) map[uint32]struct{} {
	if clusterSize == 0 {
		panic("clusterSize must be greater than 0")
	}
	if totalHashRanges < clusterSize {
		panic("totalHashRanges must be greater than or equal to clusterSize")
	}
	if nodeID >= clusterSize {
		panic("nodeID must be less than clusterSize")
	}
	ranges := make(map[uint32]struct{})
	for hashRange := uint32(0); hashRange < totalHashRanges; hashRange++ {
		if hashRange%clusterSize == nodeID {
			ranges[hashRange] = struct{}{}
		}
	}
	return ranges
}

// GetNodeHashRangesList returns the hash ranges that a specific node should handle
// given the total number of nodes and hash ranges.
//
// Parameters:
// - nodeID: The ID of the node (0-based)
// - clusterSize: The total number of nodes in the cluster
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - A slice of hash ranges that this node should handle
func GetNodeHashRangesList(nodeID, clusterSize, totalHashRanges uint32) []uint32 {
	if clusterSize == 0 {
		panic("clusterSize must be greater than 0")
	}
	if totalHashRanges < clusterSize {
		panic("totalHashRanges must be greater than or equal to clusterSize")
	}
	if nodeID >= clusterSize {
		panic("nodeID must be less than clusterSize")
	}
	ranges := make([]uint32, 0)
	for i := uint32(0); i < totalHashRanges; i++ {
		if i%clusterSize == nodeID {
			ranges = append(ranges, i)
		}
	}
	return ranges
}

// GetHashRangeMovements determines which hash ranges need to be moved from old nodes to new nodes
// during cluster scaling operations.
//
// Parameters:
// - oldClusterSize: The number of nodes in the cluster before scaling
// - newClusterSize: The number of nodes in the cluster after scaling
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - A map where key is the source nodeID and value is a slice of hash ranges to move from that node
func GetHashRangeMovements(oldClusterSize, newClusterSize, totalHashRanges uint32) map[uint32][]uint32 {
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

	movements := make(map[uint32][]uint32)

	// For each hash range, determine if it's moving from one node to another
	for hashRange := uint32(0); hashRange < totalHashRanges; hashRange++ {
		oldNodeID := hashRange % oldClusterSize
		newNodeID := hashRange % newClusterSize

		// If the hash range is moving to a different node, record the movement
		if oldNodeID != newNodeID {
			movements[oldNodeID] = append(movements[oldNodeID], hashRange)
		}
	}

	return movements
}

// GetNewNodeHashRanges determines which hash ranges each new node should receive
// during cluster scale up operations.
//
// Parameters:
// - oldClusterSize: The number of nodes in the cluster before scaling
// - newClusterSize: The number of nodes in the cluster after scaling
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - A map where key is the new nodeID and value is a slice of hash ranges for that node
func GetNewNodeHashRanges(oldClusterSize, newClusterSize, totalHashRanges uint32) map[uint32][]uint32 {
	if oldClusterSize == 0 {
		panic("oldClusterSize must be greater than 0")
	}
	if newClusterSize == 0 {
		panic("newClusterSize must be greater than 0")
	}
	if newClusterSize <= oldClusterSize {
		panic("newClusterSize must be greater than oldClusterSize for scale up operations")
	}
	if totalHashRanges < oldClusterSize {
		panic("totalHashRanges must be greater than or equal to oldClusterSize")
	}
	if totalHashRanges < newClusterSize {
		panic("totalHashRanges must be greater than or equal to newClusterSize")
	}

	newNodeRanges := make(map[uint32][]uint32)

	// Only consider nodes that are actually new (nodeID >= oldClusterSize)
	for nodeID := oldClusterSize; nodeID < newClusterSize; nodeID++ {
		ranges := GetNodeHashRangesList(nodeID, newClusterSize, totalHashRanges)
		newNodeRanges[nodeID] = ranges
	}

	return newNodeRanges
}
