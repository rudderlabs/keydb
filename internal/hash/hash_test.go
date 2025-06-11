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
		numberOfNodes   uint32
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
			initialHashRange, initialNodeID := GetNodeNumber(tc.key, tc.numberOfNodes, tc.totalHashRanges)

			// Run multiple iterations to verify consistency
			for i := 0; i < testIterations; i++ {
				hashRange, nodeID := GetNodeNumber(tc.key, tc.numberOfNodes, tc.totalHashRanges)

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
		numberOfNodes   uint32
		totalHashRanges uint32
	}{
		{0, 10, 128},
		{4, 5, 128},
		{10, 20, 256},
		{0, 1, 128}, // Single node
	}

	for _, tc := range testCases {
		testName := "nodeID=" + strconv.Itoa(int(tc.nodeID)) +
			"_nodes=" + strconv.Itoa(int(tc.numberOfNodes)) +
			"_ranges=" + strconv.Itoa(int(tc.totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			// Get initial result
			initialRanges := GetNodeHashRanges(tc.nodeID, tc.numberOfNodes, tc.totalHashRanges)

			// Run multiple iterations to verify consistency
			for i := 0; i < testIterations; i++ {
				ranges := GetNodeHashRanges(tc.nodeID, tc.numberOfNodes, tc.totalHashRanges)

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
		numberOfNodes := uint32(rnd.Intn(20) + int(nodeID) + 1)
		// we add the numberOfNodes to make sure that totalHashRanges is always >= clusterSize
		totalHashRanges := uint32(rnd.Intn(128) + int(numberOfNodes) + 1)

		testName := "nodeID=" + strconv.Itoa(int(nodeID)) +
			"_nodes=" + strconv.Itoa(int(numberOfNodes)) +
			"_ranges=" + strconv.Itoa(int(totalHashRanges))
		t.Run(testName, func(t *testing.T) {
			for i := 0; i < testIterations; i++ {
				// Get hash ranges for this node
				nodeHashRanges := GetNodeHashRanges(nodeID, numberOfNodes, totalHashRanges)
				hashResultsForNodeID := getKeysForNodeID(numRandomKeys, nodeID, numberOfNodes, totalHashRanges)

				// Run multiple iterations to verify consistency
				for _, hres := range hashResultsForNodeID {
					if _, exists := nodeHashRanges[hres.hashRange]; !exists {
						t.Fatalf("Hash range %d missing for nodeID %d", hres.hashRange, nodeID)
					}
					hr, nid := GetNodeNumber(hres.key, numberOfNodes, totalHashRanges)
					require.Equalf(t, nodeID, nid, "NodeID mismatch for key %q", hres.key)
					require.Equalf(t, hres.hashRange, hr, "HashRange mismatch for key %q", hres.key)
				}
			}
		})
	}
}

type hashResult struct {
	key       string
	hashRange uint32
	nodeID    uint32
}

func getKeysForNodeID(noOfKeys int, nodeID, numberOfNodes, totalHashRanges uint32) []hashResult {
	keys := make([]hashResult, 0, noOfKeys)
	for len(keys) < noOfKeys {
		key := kitrand.String(20)
		hr, nn := GetNodeNumber(key, numberOfNodes, totalHashRanges)
		if nn == nodeID {
			keys = append(keys, hashResult{key, hr, nn})
		}
	}
	return keys
}
