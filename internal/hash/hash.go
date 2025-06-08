package hash

import (
	"hash/fnv"
)

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
func GetNodeNumber(key string, numberOfNodes, totalHashRanges uint32) uint32 {
	if numberOfNodes == 0 {
		return 0
	}

	// Calculate the hash of the key
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	hashValue := h.Sum32()

	// Determine which hash range this key belongs to
	hashRange := hashValue % totalHashRanges

	// Determine which node handles this hash range
	return hashRange % numberOfNodes
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
func GetNodeHashRanges(nodeID, numberOfNodes, totalHashRanges uint32) []uint32 {
	if numberOfNodes == 0 || nodeID >= numberOfNodes {
		return nil
	}

	var ranges []uint32
	for i := uint32(0); i < totalHashRanges; i++ {
		if i%numberOfNodes == nodeID {
			ranges = append(ranges, i)
		}
	}
	return ranges
}

// GetHashRangeForKey returns the hash range ID for a given key
//
// Parameters:
// - key: The key to hash
// - totalHashRanges: The total number of hash ranges (default 128)
//
// Returns:
// - The hash range ID for this key
func GetHashRangeForKey(key string, totalHashRanges uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	hashValue := h.Sum32()
	return hashValue % totalHashRanges
}
