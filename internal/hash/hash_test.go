package hash

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kitrand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

// testIterations controls how many times each test runs to ensure consistency
const testIterations = 100

// TestGetNodeNumberConsistency verifies that GetNodeNumber is deterministically consistent
func TestGetNodeNumberConsistency(t *testing.T) {
	// Test with various keys, node counts, and hash ranges
	testCases := []struct {
		key             string
		clusterSize     uint32
		totalHashRanges uint32
	}{
		{"key1", 10, 128},
		{"key2", 5, 128},
		{"key3", 20, 256},
		{"longerkeywithalotofcharacters", 15, 128},
		{"", 10, 128},    // Empty key
		{"key4", 1, 128}, // Single node
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			// Get initial result
			initialHashRange, initialNodeID := GetNodeNumber(tc.key, tc.clusterSize, tc.totalHashRanges)

			// Run multiple iterations to verify consistency
			for i := 0; i < testIterations; i++ {
				hashRange, nodeID := GetNodeNumber(tc.key, tc.clusterSize, tc.totalHashRanges)

				if hashRange != initialHashRange || nodeID != initialNodeID {
					t.Errorf("Iteration %d: GetNodeNumber not consistent for key %q. Expected (%d, %d), got (%d, %d)",
						i, tc.key, initialHashRange, initialNodeID, hashRange, nodeID)
				}
			}
		})
	}
}

// TestGetNodeHashRangesConsistency verifies that GetNodeHashRanges is deterministically consistent
func TestGetNodeHashRangesConsistency(t *testing.T) {
	// Test with various node IDs, node counts, and hash ranges
	testCases := []struct {
		nodeID          uint32
		clusterSize     uint32
		totalHashRanges uint32
	}{
		{0, 10, 128},
		{4, 5, 128},
		{10, 20, 256},
		{0, 1, 128}, // Single node
	}

	for _, tc := range testCases {
		testName := "nodeID=" + strconv.Itoa(int(tc.nodeID)) +
			"_nodes=" + strconv.Itoa(int(tc.clusterSize)) +
			"_ranges=" + strconv.Itoa(int(tc.totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			// Get initial result
			initialRanges := GetNodeHashRanges(tc.nodeID, tc.clusterSize, tc.totalHashRanges)

			// Run multiple iterations to verify consistency
			for i := 0; i < testIterations; i++ {
				ranges := GetNodeHashRanges(tc.nodeID, tc.clusterSize, tc.totalHashRanges)

				// Check that the maps have the same size
				if len(ranges) != len(initialRanges) {
					t.Errorf("Iteration %d: GetNodeHashRanges not consistent. Expected %d ranges, got %d ranges",
						i, len(initialRanges), len(ranges))
					continue
				}

				// Check that all keys in initialRanges are in ranges
				for hashRange := range initialRanges {
					if _, exists := ranges[hashRange]; !exists {
						t.Errorf("Iteration %d: GetNodeHashRanges not consistent. Hash range %d missing",
							i, hashRange)
					}
				}
			}
		})
	}
}

// TestHashRangeInNodeHashRanges verifies that for random keys, the hashRange from GetNodeNumber
// is always present in the map returned by GetNodeHashRanges for the same nodeID
func TestHashRangeInNodeHashRanges(t *testing.T) {
	// Number of random keys to test
	const numRandomKeys = 10
	const numOfRandomNodeIDs = 10

	// Initialize random number generator
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomNodeIDs := make([]uint32, 0, numOfRandomNodeIDs)
	for i := 0; i < numOfRandomNodeIDs; i++ {
		randomNodeIDs = append(randomNodeIDs, uint32(i))
	}

	// Test with different random nodeIDs
	for _, nodeID := range randomNodeIDs {
		// we add the nodeID to make sure the nodeID is always <= clusterSize
		clusterSize := uint32(rnd.Intn(20) + int(nodeID) + 1)
		// we add the clusterSize to make sure that totalHashRanges is always >= clusterSize
		totalHashRanges := uint32(rnd.Intn(128) + int(clusterSize) + 1)

		testName := "nodeID=" + strconv.Itoa(int(nodeID)) +
			"_nodes=" + strconv.Itoa(int(clusterSize)) +
			"_ranges=" + strconv.Itoa(int(totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			for i := 0; i < testIterations; i++ {
				// Get hash ranges for this node
				nodeHashRanges := GetNodeHashRanges(nodeID, clusterSize, totalHashRanges)
				hashResultsForNodeID := getKeysForNodeID(numRandomKeys, nodeID, clusterSize, totalHashRanges)

				// Run multiple iterations to verify consistency
				for _, hres := range hashResultsForNodeID {
					if _, exists := nodeHashRanges[hres.hashRange]; !exists {
						t.Fatalf("Hash range %d missing for nodeID %d", hres.hashRange, nodeID)
					}
					hr, nid := GetNodeNumber(hres.key, clusterSize, totalHashRanges)
					require.Equalf(t, nodeID, nid, "NodeID mismatch for key %q", hres.key)
					require.Equalf(t, hres.hashRange, hr, "HashRange mismatch for key %q", hres.key)
				}
			}
		})
	}
}

func TestCollision(t *testing.T) {
	var clusterSize, totalHashRanges uint32 = 3, 128
	seen := make(map[uint32]struct{})
	for nodeID := uint32(0); nodeID < clusterSize; nodeID++ {
		hashRanges := GetNodeHashRanges(nodeID, clusterSize, totalHashRanges)
		for hashRange := range hashRanges {
			if _, exists := seen[hashRange]; exists {
				t.Errorf("Collision for hashRange %d", hashRange)
			}
			seen[hashRange] = struct{}{}
		}
	}
}

// TestGetNodeHashRangesListConsistency verifies that GetNodeHashRangesList is deterministically consistent
func TestGetNodeHashRangesListConsistency(t *testing.T) {
	// Test with various node IDs, node counts, and hash ranges
	testCases := []struct {
		nodeID          uint32
		clusterSize     uint32
		totalHashRanges uint32
	}{
		{0, 10, 128},
		{4, 5, 128},
		{10, 20, 256},
		{0, 1, 128}, // Single node
	}

	for _, tc := range testCases {
		testName := "nodeID=" + strconv.Itoa(int(tc.nodeID)) +
			"_nodes=" + strconv.Itoa(int(tc.clusterSize)) +
			"_ranges=" + strconv.Itoa(int(tc.totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			// Get initial result
			initialRanges := GetNodeHashRangesList(tc.nodeID, tc.clusterSize, tc.totalHashRanges)

			// Run multiple iterations to verify consistency
			for i := 0; i < testIterations; i++ {
				ranges := GetNodeHashRangesList(tc.nodeID, tc.clusterSize, tc.totalHashRanges)

				// Check that the slices have the same length
				require.Equal(t, len(initialRanges), len(ranges),
					"[%d]: GetNodeHashRangesList not consistent. Expected %d ranges, got %d ranges",
					i, len(initialRanges), len(ranges))

				// Check that all elements are the same and in the same order
				for j, hashRange := range initialRanges {
					require.Equal(t, hashRange, ranges[j],
						"[%d]: GetNodeHashRangesList not consistent. Expected hash range %d at position %d, got %d",
						i, hashRange, j, ranges[j])
				}
			}
		})
	}
}

// TestGetNodeHashRangesListMatchesMap verifies that GetNodeHashRangesList returns the same hash ranges
// as GetNodeHashRanges but in slice format
func TestGetNodeHashRangesListMatchesMap(t *testing.T) {
	testCases := []struct {
		nodeID          uint32
		clusterSize     uint32
		totalHashRanges uint32
	}{
		{0, 10, 128},
		{4, 5, 128},
		{10, 20, 256},
		{0, 1, 128}, // Single node
		{2, 3, 128}, // Different distribution
	}

	for _, tc := range testCases {
		testName := "nodeID=" + strconv.Itoa(int(tc.nodeID)) +
			"_nodes=" + strconv.Itoa(int(tc.clusterSize)) +
			"_ranges=" + strconv.Itoa(int(tc.totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			// Get results from both functions
			rangesMap := GetNodeHashRanges(tc.nodeID, tc.clusterSize, tc.totalHashRanges)
			rangesList := GetNodeHashRangesList(tc.nodeID, tc.clusterSize, tc.totalHashRanges)

			// Check that they have the same number of elements
			require.Equal(t, len(rangesMap), len(rangesList),
				"Map and list should have the same number of hash ranges")

			// Convert list to map for comparison
			listAsMap := make(map[uint32]struct{})
			for _, hashRange := range rangesList {
				listAsMap[hashRange] = struct{}{}
			}

			// Check that all elements in the map are in the list
			for hashRange := range rangesMap {
				_, exists := listAsMap[hashRange]
				require.True(t, exists, "Hash range %d from map not found in list", hashRange)
			}

			// Check that all elements in the list are in the map
			for _, hashRange := range rangesList {
				_, exists := rangesMap[hashRange]
				require.True(t, exists, "Hash range %d from list not found in map", hashRange)
			}
		})
	}
}

// TestGetNodeHashRangesListSorted verifies that GetNodeHashRangesList returns hash ranges in sorted order
func TestGetNodeHashRangesListSorted(t *testing.T) {
	testCases := []struct {
		nodeID          uint32
		clusterSize     uint32
		totalHashRanges uint32
	}{
		{0, 10, 128},
		{4, 5, 128},
		{10, 20, 256},
		{0, 1, 128}, // Single node
		{2, 3, 128}, // Different distribution
	}

	for _, tc := range testCases {
		testName := "nodeID=" + strconv.Itoa(int(tc.nodeID)) +
			"_nodes=" + strconv.Itoa(int(tc.clusterSize)) +
			"_ranges=" + strconv.Itoa(int(tc.totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			ranges := GetNodeHashRangesList(tc.nodeID, tc.clusterSize, tc.totalHashRanges)

			// Check that the slice is sorted
			for i := 1; i < len(ranges); i++ {
				require.Less(t, ranges[i-1], ranges[i],
					"Hash ranges should be in ascending order. ranges[%d]=%d should be less than ranges[%d]=%d",
					i-1, ranges[i-1], i, ranges[i])
			}
		})
	}
}

// TestHashRangeInNodeHashRangesList verifies that for random keys, the hashRange from GetNodeNumber
// is always present in the slice returned by GetNodeHashRangesList for the same nodeID
func TestHashRangeInNodeHashRangesList(t *testing.T) {
	// Number of random keys to test
	const numRandomKeys = 10
	const numOfRandomNodeIDs = 10

	// Initialize random number generator
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomNodeIDs := make([]uint32, 0, numOfRandomNodeIDs)
	for i := 0; i < numOfRandomNodeIDs; i++ {
		randomNodeIDs = append(randomNodeIDs, uint32(i))
	}

	// Test with different random nodeIDs
	for _, nodeID := range randomNodeIDs {
		// we add the nodeID to make sure the nodeID is always <= clusterSize
		clusterSize := uint32(rnd.Intn(20) + int(nodeID) + 1)
		// we add the clusterSize to make sure that totalHashRanges is always >= clusterSize
		totalHashRanges := uint32(rnd.Intn(128) + int(clusterSize) + 1)

		testName := "nodeID=" + strconv.Itoa(int(nodeID)) +
			"_nodes=" + strconv.Itoa(int(clusterSize)) +
			"_ranges=" + strconv.Itoa(int(totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			for i := 0; i < testIterations; i++ {
				// Get hash ranges for this node
				nodeHashRangesList := GetNodeHashRangesList(nodeID, clusterSize, totalHashRanges)
				hashResultsForNodeID := getKeysForNodeID(numRandomKeys, nodeID, clusterSize, totalHashRanges)

				// Convert slice to map for faster lookup
				nodeHashRangesMap := make(map[uint32]struct{})
				for _, hashRange := range nodeHashRangesList {
					nodeHashRangesMap[hashRange] = struct{}{}
				}

				// Run multiple iterations to verify consistency
				for _, hres := range hashResultsForNodeID {
					_, exists := nodeHashRangesMap[hres.hashRange]
					require.True(t, exists, "Hash range %d missing for nodeID %d", hres.hashRange, nodeID)

					hr, nid := GetNodeNumber(hres.key, clusterSize, totalHashRanges)
					require.Equal(t, nodeID, nid, "NodeID mismatch for key %q", hres.key)
					require.Equal(t, hres.hashRange, hr, "HashRange mismatch for key %q", hres.key)
				}
			}
		})
	}
}

// TestGetNodeHashRangesListPanics verifies that GetNodeHashRangesList panics with invalid parameters
func TestGetNodeHashRangesListPanics(t *testing.T) {
	testCases := []struct {
		name            string
		nodeID          uint32
		clusterSize     uint32
		totalHashRanges uint32
		expectedPanic   string
	}{
		{
			name:            "zero_nodes",
			nodeID:          0,
			clusterSize:     0,
			totalHashRanges: 128,
			expectedPanic:   "clusterSize must be greater than 0",
		},
		{
			name:            "totalHashRanges_less_than_clusterSize",
			nodeID:          0,
			clusterSize:     10,
			totalHashRanges: 5,
			expectedPanic:   "totalHashRanges must be greater than or equal to clusterSize",
		},
		{
			name:            "nodeID_greater_than_clusterSize",
			nodeID:          10,
			clusterSize:     5,
			totalHashRanges: 128,
			expectedPanic:   "nodeID must be less than clusterSize",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, tc.expectedPanic, func() {
				GetNodeHashRangesList(tc.nodeID, tc.clusterSize, tc.totalHashRanges)
			})
		})
	}
}

type hashResult struct {
	key       string
	hashRange uint32
	nodeID    uint32
}

func getKeysForNodeID(noOfKeys int, nodeID, clusterSize, totalHashRanges uint32) []hashResult {
	keys := make([]hashResult, 0, noOfKeys)
	for len(keys) < noOfKeys {
		key := kitrand.String(20)
		hr, nn := GetNodeNumber(key, clusterSize, totalHashRanges)
		if nn == nodeID {
			keys = append(keys, hashResult{key, hr, nn})
		}
	}
	return keys
}

// TestGetHashRangeMovements verifies that GetHashRangeMovements correctly identifies
// which hash ranges need to be moved during scaling operations
func TestGetHashRangeMovements(t *testing.T) {
	testCases := []struct {
		name            string
		oldClusterSize  uint32
		newClusterSize  uint32
		totalHashRanges uint32
	}{
		{
			name:            "scale_up_1_to_2",
			oldClusterSize:  1,
			newClusterSize:  2,
			totalHashRanges: 4,
		},
		{
			name:            "scale_up_2_to_3",
			oldClusterSize:  2,
			newClusterSize:  3,
			totalHashRanges: 6,
		},
		{
			name:            "scale_down_3_to_2",
			oldClusterSize:  3,
			newClusterSize:  2,
			totalHashRanges: 6,
		},
		{
			name:            "scale_down_2_to_1",
			oldClusterSize:  2,
			newClusterSize:  1,
			totalHashRanges: 4,
		},
		{
			name:            "large_scale_up",
			oldClusterSize:  5,
			newClusterSize:  10,
			totalHashRanges: 128,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			movements := GetHashRangeMovements(tc.oldClusterSize, tc.newClusterSize, tc.totalHashRanges)

			// Verify that all moved hash ranges are valid
			for sourceNodeID, hashRanges := range movements {
				require.Less(t, sourceNodeID, tc.oldClusterSize, "Source node ID should be valid")

				for _, hashRange := range hashRanges {
					require.Less(t, hashRange, tc.totalHashRanges, "Hash range should be valid")

					// Verify that this hash range actually moves from sourceNodeID to a different node
					oldNodeID := hashRange % tc.oldClusterSize
					newNodeID := hashRange % tc.newClusterSize
					require.Equal(t, sourceNodeID, oldNodeID, "Hash range should belong to source node in old cluster")
					require.NotEqual(t, oldNodeID, newNodeID, "Hash range should move to different node in new cluster")
				}
			}

			// Verify that no hash ranges are duplicated
			allMovedRanges := make(map[uint32]bool)
			for _, hashRanges := range movements {
				for _, hashRange := range hashRanges {
					require.False(t, allMovedRanges[hashRange], "Hash range %d should not be duplicated", hashRange)
					allMovedRanges[hashRange] = true
				}
			}
		})
	}
}

// TestGetHashRangeMovementsPanics verifies that GetHashRangeMovements panics with invalid parameters
func TestGetHashRangeMovementsPanics(t *testing.T) {
	testCases := []struct {
		name            string
		oldClusterSize  uint32
		newClusterSize  uint32
		totalHashRanges uint32
		expectedPanic   string
	}{
		{
			name:            "zero_old_cluster_size",
			oldClusterSize:  0,
			newClusterSize:  2,
			totalHashRanges: 128,
			expectedPanic:   "oldClusterSize must be greater than 0",
		},
		{
			name:            "zero_new_cluster_size",
			oldClusterSize:  2,
			newClusterSize:  0,
			totalHashRanges: 128,
			expectedPanic:   "newClusterSize must be greater than 0",
		},
		{
			name:            "totalHashRanges_less_than_oldClusterSize",
			oldClusterSize:  10,
			newClusterSize:  5,
			totalHashRanges: 5,
			expectedPanic:   "totalHashRanges must be greater than or equal to oldClusterSize",
		},
		{
			name:            "totalHashRanges_less_than_newClusterSize",
			oldClusterSize:  5,
			newClusterSize:  10,
			totalHashRanges: 5,
			expectedPanic:   "totalHashRanges must be greater than or equal to newClusterSize",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, tc.expectedPanic, func() {
				GetHashRangeMovements(tc.oldClusterSize, tc.newClusterSize, tc.totalHashRanges)
			})
		})
	}
}

// TestGetNewNodeHashRanges verifies that GetNewNodeHashRanges correctly identifies
// which hash ranges each new node should receive during scale up operations
func TestGetNewNodeHashRanges(t *testing.T) {
	testCases := []struct {
		name            string
		oldClusterSize  uint32
		newClusterSize  uint32
		totalHashRanges uint32
	}{
		{
			name:            "scale_up_1_to_2",
			oldClusterSize:  1,
			newClusterSize:  2,
			totalHashRanges: 4,
		},
		{
			name:            "scale_up_2_to_3",
			oldClusterSize:  2,
			newClusterSize:  3,
			totalHashRanges: 6,
		},
		{
			name:            "scale_up_3_to_5",
			oldClusterSize:  3,
			newClusterSize:  5,
			totalHashRanges: 15,
		},
		{
			name:            "large_scale_up",
			oldClusterSize:  5,
			newClusterSize:  10,
			totalHashRanges: 128,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			newNodeRanges := GetNewNodeHashRanges(tc.oldClusterSize, tc.newClusterSize, tc.totalHashRanges)

			// Verify that only new nodes are included
			for nodeID := range newNodeRanges {
				require.GreaterOrEqual(t, nodeID, tc.oldClusterSize, "Only new nodes should be included")
				require.Less(t, nodeID, tc.newClusterSize, "Node ID should be valid")
			}

			// Verify that all new nodes are included
			for nodeID := tc.oldClusterSize; nodeID < tc.newClusterSize; nodeID++ {
				_, exists := newNodeRanges[nodeID]
				require.True(t, exists, "New node %d should be included", nodeID)
			}

			// Verify that hash ranges are correct for each new node
			for nodeID, hashRanges := range newNodeRanges {
				expectedRanges := GetNodeHashRangesList(nodeID, tc.newClusterSize, tc.totalHashRanges)
				require.ElementsMatch(t, expectedRanges, hashRanges,
					"Hash ranges should match GetNodeHashRangesList result",
				)
			}

			// Verify that no hash ranges are duplicated across new nodes
			allNewNodeRanges := make(map[uint32]bool)
			for _, hashRanges := range newNodeRanges {
				for _, hashRange := range hashRanges {
					require.False(t, allNewNodeRanges[hashRange], "Hash range %d should not be duplicated", hashRange)
					allNewNodeRanges[hashRange] = true
				}
			}
		})
	}
}

// TestGetNewNodeHashRangesPanics verifies that GetNewNodeHashRanges panics with invalid parameters
func TestGetNewNodeHashRangesPanics(t *testing.T) {
	testCases := []struct {
		name            string
		oldClusterSize  uint32
		newClusterSize  uint32
		totalHashRanges uint32
		expectedPanic   string
	}{
		{
			name:            "zero_old_cluster_size",
			oldClusterSize:  0,
			newClusterSize:  2,
			totalHashRanges: 128,
			expectedPanic:   "oldClusterSize must be greater than 0",
		},
		{
			name:            "zero_new_cluster_size",
			oldClusterSize:  2,
			newClusterSize:  0,
			totalHashRanges: 128,
			expectedPanic:   "newClusterSize must be greater than 0",
		},
		{
			name:            "new_cluster_size_equal_to_old",
			oldClusterSize:  5,
			newClusterSize:  5,
			totalHashRanges: 128,
			expectedPanic:   "newClusterSize must be greater than oldClusterSize for scale up operations",
		},
		{
			name:            "new_cluster_size_less_than_old",
			oldClusterSize:  5,
			newClusterSize:  3,
			totalHashRanges: 128,
			expectedPanic:   "newClusterSize must be greater than oldClusterSize for scale up operations",
		},
		{
			name:            "totalHashRanges_less_than_oldClusterSize",
			oldClusterSize:  10,
			newClusterSize:  15,
			totalHashRanges: 5,
			expectedPanic:   "totalHashRanges must be greater than or equal to oldClusterSize",
		},
		{
			name:            "totalHashRanges_less_than_newClusterSize",
			oldClusterSize:  5,
			newClusterSize:  10,
			totalHashRanges: 8,
			expectedPanic:   "totalHashRanges must be greater than or equal to newClusterSize",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, tc.expectedPanic, func() {
				GetNewNodeHashRanges(tc.oldClusterSize, tc.newClusterSize, tc.totalHashRanges)
			})
		})
	}
}
