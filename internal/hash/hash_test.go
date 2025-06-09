package hash

import (
	"testing"
)

func TestGetNodeNumber(t *testing.T) {
	tests := []struct {
		name            string
		key             string
		numberOfNodes   uint32
		totalHashRanges uint32
		expected        uint32
	}{
		{
			name:            "Single node",
			key:             "test-key",
			numberOfNodes:   1,
			totalHashRanges: 128,
			expected:        0,
		},
		{
			name:            "Zero nodes (edge case)",
			key:             "test-key",
			numberOfNodes:   0,
			totalHashRanges: 128,
			expected:        0,
		},
		// TODO update GetNodeNumber that now returns the hashRange as well
		//{
		//	name:            "Multiple nodes",
		//	key:             "test-key",
		//	numberOfNodes:   3,
		//	totalHashRanges: 128,
		//	expected:        GetNodeNumber("test-key", 3, 128), // Deterministic check
		//},
		//{
		//	name:            "Same key should always hash to same node",
		//	key:             "consistent-key",
		//	numberOfNodes:   5,
		//	totalHashRanges: 128,
		//	expected:        GetNodeNumber("consistent-key", 5, 128), // Deterministic check
		//},
		//{
		//	name:            "Different keys can hash to different nodes",
		//	key:             "key1",
		//	numberOfNodes:   10,
		//	totalHashRanges: 128,
		//	expected:        GetNodeNumber("key1", 10, 128), // Deterministic check
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, result := GetNodeNumber(tt.key, tt.numberOfNodes, tt.totalHashRanges)
			if result != tt.expected {
				t.Errorf("GetNodeNumber(%s, %d, %d) = %d, want %d",
					tt.key, tt.numberOfNodes, tt.totalHashRanges, result, tt.expected)
			}

			// Test determinism - calling again should yield same result
			_, result2 := GetNodeNumber(tt.key, tt.numberOfNodes, tt.totalHashRanges)
			if result != result2 {
				t.Errorf("GetNodeNumber not deterministic: first call = %d, second call = %d",
					result, result2)
			}
		})
	}
}

func TestGetNodeHashRanges(t *testing.T) {
	tests := []struct {
		name            string
		nodeID          uint32
		numberOfNodes   uint32
		totalHashRanges uint32
		expectedCount   int
	}{
		{
			name:            "Single node gets all ranges",
			nodeID:          0,
			numberOfNodes:   1,
			totalHashRanges: 128,
			expectedCount:   128,
		},
		{
			name:            "Invalid node ID",
			nodeID:          5,
			numberOfNodes:   3,
			totalHashRanges: 128,
			expectedCount:   0, // Should return nil
		},
		{
			name:            "Zero nodes (edge case)",
			nodeID:          0,
			numberOfNodes:   0,
			totalHashRanges: 128,
			expectedCount:   0, // Should return nil
		},
		{
			name:            "Even distribution with 2 nodes",
			nodeID:          0,
			numberOfNodes:   2,
			totalHashRanges: 128,
			expectedCount:   64, // Should get half the ranges
		},
		{
			name:            "Even distribution with 4 nodes",
			nodeID:          2,
			numberOfNodes:   4,
			totalHashRanges: 128,
			expectedCount:   32, // Should get quarter of the ranges
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := GetNodeHashRanges(tt.nodeID, tt.numberOfNodes, tt.totalHashRanges)

			if len(ranges) != tt.expectedCount {
				t.Errorf("GetNodeHashRanges(%d, %d, %d) returned %d ranges, want %d",
					tt.nodeID, tt.numberOfNodes, tt.totalHashRanges, len(ranges), tt.expectedCount)
			}

			// Verify each range is assigned to this node
			if len(ranges) > 0 {
				for r := range ranges {
					if r%tt.numberOfNodes != tt.nodeID {
						t.Errorf("Range %d incorrectly assigned to node %d", r, tt.nodeID)
					}
				}
			}
		})
	}
}

func TestGetHashRangeForKey(t *testing.T) {
	tests := []struct {
		name            string
		key             string
		totalHashRanges uint32
	}{
		{
			name:            "Standard case",
			key:             "test-key",
			totalHashRanges: 128,
		},
		{
			name:            "Empty key",
			key:             "",
			totalHashRanges: 128,
		},
		{
			name:            "Single hash range",
			key:             "test-key",
			totalHashRanges: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetHashRangeForKey(tt.key, tt.totalHashRanges)

			// Check range is valid
			if result >= tt.totalHashRanges {
				t.Errorf("GetHashRangeForKey(%s, %d) = %d, which is outside valid range [0,%d)",
					tt.key, tt.totalHashRanges, result, tt.totalHashRanges)
			}

			// Test determinism
			result2 := GetHashRangeForKey(tt.key, tt.totalHashRanges)
			if result != result2 {
				t.Errorf("GetHashRangeForKey not deterministic: first call = %d, second call = %d",
					result, result2)
			}
		})
	}
}

func TestConsistencyBetweenFunctions(t *testing.T) {
	key := "test-key"
	totalHashRanges := uint32(128)
	numberOfNodes := uint32(3)

	// Get the hash range for the key
	hashRange := GetHashRangeForKey(key, totalHashRanges)

	// Get the node for the key
	_, node := GetNodeNumber(key, numberOfNodes, totalHashRanges)

	// The node should be the hash range modulo number of nodes
	expectedNode := hashRange % numberOfNodes

	if node != expectedNode {
		t.Errorf("Inconsistency: GetNodeNumber(%s, %d, %d) = %d, but expected %d (hashRange %% numberOfNodes)",
			key, numberOfNodes, totalHashRanges, node, expectedNode)
	}

	// Get all hash ranges for the node
	nodeRanges := GetNodeHashRanges(node, numberOfNodes, totalHashRanges)

	// The hash range should be in the node's ranges
	found := false
	for r := range nodeRanges {
		if r == hashRange {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Inconsistency: Hash range %d for key %s should be handled by node %d, but it's not in the node's ranges",
			hashRange, key, node)
	}
}
