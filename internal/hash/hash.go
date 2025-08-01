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
// - numberOfNodes: The total number of nodes in the cluster
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - The node number (0-based) that should handle this key
func GetNodeNumber(key string, numberOfNodes, totalHashRanges uint32) (uint32, uint32) {
	if numberOfNodes == 0 {
		panic("numberOfNodes must be greater than 0")
	}
	if totalHashRanges < numberOfNodes {
		panic("totalHashRanges must be greater than or equal to numberOfNodes")
	}

	// Calculate the hash of the key
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	hashValue := h.Sum32()

	// Determine which hash range this key belongs to
	hashRange := hashValue % totalHashRanges

	// Determine which node handles this hash range
	return hashRange, hashRange % numberOfNodes
}

func GetKeysByHashRange(keys []string, nodeID, numberOfNodes, totalHashRanges uint32) (
	map[uint32][]string, // keysByHashRange
	error,
) {
	if numberOfNodes == 0 {
		panic("numberOfNodes must be greater than 0")
	}
	if totalHashRanges < numberOfNodes {
		panic("totalHashRanges must be greater than or equal to numberOfNodes")
	}

	m := make(map[uint32][]string)
	for _, key := range keys {
		// Create a new hash object for each key
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		hashValue := h.Sum32()
		hashRange := hashValue % totalHashRanges

		if nodeID != hashRange%numberOfNodes {
			return nil, fmt.Errorf("hashRange %d not for node %d: %w", hashRange, nodeID, ErrWrongNode)
		}

		m[hashRange] = append(m[hashRange], key)
	}

	return m, nil
}

func GetKeysByHashRangeWithIndexes(keys []string, nodeID, numberOfNodes, totalHashRanges uint32) (
	map[uint32][]string, // keysByHashRange
	map[string]int, // indexes
	error,
) {
	if numberOfNodes == 0 {
		panic("numberOfNodes must be greater than 0")
	}
	if totalHashRanges < numberOfNodes {
		panic("totalHashRanges must be greater than or equal to numberOfNodes")
	}

	m := make(map[uint32][]string)
	indexes := make(map[string]int, len(keys))
	for i, key := range keys {
		// Create a new hash object for each key
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		hashValue := h.Sum32()
		hashRange := hashValue % totalHashRanges

		if nodeID != hashRange%numberOfNodes {
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
// - numberOfNodes: The total number of nodes in the cluster
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - A slice of hash ranges that this node should handle
func GetNodeHashRanges(nodeID, numberOfNodes, totalHashRanges uint32) map[uint32]struct{} {
	if numberOfNodes == 0 {
		panic("numberOfNodes must be greater than 0")
	}
	if totalHashRanges < numberOfNodes {
		panic("totalHashRanges must be greater than or equal to numberOfNodes")
	}
	if nodeID >= numberOfNodes {
		panic("nodeID must be less than numberOfNodes")
	}
	ranges := make(map[uint32]struct{})
	for i := uint32(0); i < totalHashRanges; i++ {
		if i%numberOfNodes == nodeID {
			ranges[i] = struct{}{}
		}
	}
	return ranges
}
