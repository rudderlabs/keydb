// package hash
//
// # Load Distribution Metrics
//
// - Min/Max/Avg: Number of keys assigned to each node
//   - Min: Lightest loaded node (fewest keys)
//   - Max: Heaviest loaded node (most keys)
//   - Avg: Average keys per node (should be total_keys / cluster_size)
//   - Ideal: Min and Max should be close to Avg for even distribution
//
// - StdDev: Standard deviation of key distribution across nodes
//   - Lower = better: Shows how evenly keys are spread
//   - High StdDev = some nodes are overloaded while others are underused
//   - Low StdDev = consistent hashing is working well
//
// # Hash Quality
//
// - Collision%: Percentage of duplicate hash values
//   - Lower = better: Fewer collisions mean better hash function quality
//   - High collisions can cause uneven distribution
//
// # Performance
//
// - Hash Time: Time to compute hash values for all keys
//   - Pure hashing performance comparison
//
// - Distribution Time: Time to assign keys to nodes using consistent hashing
//   - Includes the consistent hashing algorithm overhead
//   - This is the real-world performance impact
//
// # What to Look For
//
// - Good distribution: StdDev < 10% of average load
// - Quality hash: Collision rate near 0%
// - Performance trade-off: FNV might be faster but xxhash should distribute more evenly
//
// # Real-World Consequences
//
// - Hotspots: Some nodes become overloaded
// - Underutilization: Other nodes sit idle
// - Performance degradation: Uneven resource usage
// - Scaling issues: Load doesn't distribute evenly when adding/removing nodes
//
// # Why Collision Rate Matters
//
// The collision percentage in the test measures hash quality:
// - 0% collisions: Perfect distribution potential
// - High collisions: Poor distribution guaranteed
package hash

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/google/uuid"
)

type distributionStats struct {
	hasherName       string
	clusterSize      uint32
	keyCount         int
	minLoad          int
	maxLoad          int
	avgLoad          float64
	stdDev           float64
	collisionRate    float64
	hashTime         time.Duration
	distributionTime time.Duration
}

/*
* - ====================================================================================================================
*   HASH DISTRIBUTION COMPARISON REPORT
*   ====================================================================================================================
*   Test Case            Hasher   Keys       Min      Max      Avg      StdDev     Collision%   Hash Time    Distr. Time
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-10-keys-1000 xxhash    1000       58       125      100.0    20.15      0.0000       46.062µs     363.039µs
*   cluster-10-keys-1000 fnv       1000       32       147      100.0    33.98      0.0000       221.779µs    568.473µs
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-10-keys-10000 xxhash   10000      600      1298     1000.0   222.49     0.0000       532.326µs    3.590379ms
*   cluster-10-keys-10000 fnv      10000      395      1301     1000.0   283.13     0.0000       2.620181ms   5.524622ms
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-20-keys-10000 xxhash   10000      279      650      500.0    106.76     0.0000       529.567µs    3.790061ms
*   cluster-20-keys-10000 fnv      10000      248      635      500.0    122.53     0.0000       2.247561ms   5.745194ms
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-20-keys-100000 xxhash  100000     3050     6252     5000.0   948.31     0.0000       7.499055ms   39.91835ms
*   cluster-20-keys-100000 fnv     100000     2784     6352     5000.0   1204.42    0.0010       24.034284ms  60.74438ms
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-3-keys-1000  xxhash    1000       256      389      333.3    56.42      0.0000       89.441µs     394.653µs
*   cluster-3-keys-1000  fnv       1000       293      414      333.3    57.04      0.0000       266.651µs    587.932µs
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-3-keys-10000 xxhash    10000      2593     3765     3333.3   525.90     0.0000       1.023043ms   3.861558ms
*   cluster-3-keys-10000 fnv       10000      2803     4238     3333.3   642.86     0.0000       2.506057ms   5.74343ms
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-5-keys-1000  xxhash    1000       150      269      200.0    43.79      0.0000       49.12µs      362.933µs
*   cluster-5-keys-1000  fnv       1000       92       266      200.0    67.75      0.0000       239.046µs    587.006µs
*   --------------------------------------------------------------------------------------------------------------------
*   cluster-5-keys-10000 xxhash    10000      1400     2431     2000.0   345.76     0.0000       596.919µs    3.558692ms
*   cluster-5-keys-10000 fnv       10000      906      2568     2000.0   665.29     0.0000       2.46181ms    5.550845ms
*   --------------------------------------------------------------------------------------------------------------------
*   SUMMARY ANALYSIS:
*   - Lower StdDev = better load distribution
*   - Lower Collision% = better hash quality
*   - Shorter times = better performance
*   ====================================================================================================================
 */
func TestHasherDistribution(t *testing.T) {
	testCases := []struct {
		clusterSize uint32
		keyCount    int
	}{
		{3, 1000},
		{3, 10000},
		{5, 1000},
		{5, 10000},
		{10, 1000},
		{10, 10000},
		{20, 10000},
		{20, 100000},
	}

	hashers := []struct {
		name   string
		hasher consistent.Hasher
		isFnv  bool
	}{
		{
			name:  "xxhash",
			isFnv: false,
		},
		{
			name:  "fnv",
			isFnv: true,
		},
	}

	var allStats []distributionStats

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("cluster-%d-keys-%d", tc.clusterSize, tc.keyCount), func(t *testing.T) {
			// Generate random keys
			keys := make([]string, tc.keyCount)
			for i := 0; i < tc.keyCount; i++ {
				keys[i] = uuid.New().String()
			}

			for _, h := range hashers {
				stats := measureDistribution(h.name, tc.clusterSize, keys, h.isFnv)
				allStats = append(allStats, stats)

				t.Logf(
					"%s - Cluster: %d, Keys: %d, StdDev: %.2f, Collision Rate: %.4f%%, Hash Time: %v",
					h.name, tc.clusterSize, tc.keyCount, stats.stdDev, stats.collisionRate*100, stats.hashTime,
				)
			}
		})
	}

	// Print comprehensive report
	printDistributionReport(t, allStats)
}

func measureDistribution(hasherName string, clusterSize uint32, keys []string, isFnv bool) distributionStats {
	totalHashRanges := clusterSize * 32 // Use reasonable hash ranges
	var h *Hash
	if isFnv {
		h = New(clusterSize, totalHashRanges, WithFnvHasher())
	} else {
		h = New(clusterSize, totalHashRanges)
	}

	// Measure hash time
	start := time.Now()
	hashValues := make([]uint64, len(keys))
	for i, key := range keys {
		hashValues[i] = h.hasher.Sum64([]byte(key))
	}
	hashTime := time.Since(start)

	// Count keys per node and measure distribution time
	start = time.Now()
	nodeLoads := make(map[uint32]int)
	for _, key := range keys {
		nodeID := h.GetNodeNumber(key)
		nodeLoads[nodeID]++
	}
	distributionTime := time.Since(start)

	// Calculate collision rate (duplicate hash values)
	hashSet := make(map[uint64]struct{})
	collisions := 0
	for _, hashVal := range hashValues {
		if _, exists := hashSet[hashVal]; exists {
			collisions++
		} else {
			hashSet[hashVal] = struct{}{}
		}
	}
	collisionRate := float64(collisions) / float64(len(keys))

	// Calculate distribution statistics
	loads := make([]int, 0, len(nodeLoads))
	totalLoad := 0
	minLoad := len(keys)
	maxLoad := 0

	for _, load := range nodeLoads {
		loads = append(loads, load)
		totalLoad += load
		if load < minLoad {
			minLoad = load
		}
		if load > maxLoad {
			maxLoad = load
		}
	}

	avgLoad := float64(totalLoad) / float64(clusterSize)

	// Calculate standard deviation
	variance := 0.0
	for _, load := range loads {
		diff := float64(load) - avgLoad
		variance += diff * diff
	}
	variance /= float64(clusterSize)
	stdDev := math.Sqrt(variance)

	return distributionStats{
		hasherName:       hasherName,
		clusterSize:      clusterSize,
		keyCount:         len(keys),
		minLoad:          minLoad,
		maxLoad:          maxLoad,
		avgLoad:          avgLoad,
		stdDev:           stdDev,
		collisionRate:    collisionRate,
		hashTime:         hashTime,
		distributionTime: distributionTime,
	}
}

func printDistributionReport(t *testing.T, stats []distributionStats) {
	t.Log("\n" + strings.Repeat("=", 120))
	t.Log("HASH DISTRIBUTION COMPARISON REPORT")
	t.Log(strings.Repeat("=", 120))

	// Group by test case
	testGroups := make(map[string][]distributionStats)
	for _, stat := range stats {
		key := fmt.Sprintf("cluster-%d-keys-%d", stat.clusterSize, stat.keyCount)
		testGroups[key] = append(testGroups[key], stat)
	}

	// Sort test case keys
	testKeys := make([]string, 0, len(testGroups))
	for key := range testGroups {
		testKeys = append(testKeys, key)
	}
	sort.Strings(testKeys)

	// Print header
	t.Logf("%-20s %-8s %-10s %-8s %-8s %-8s %-10s %-12s %-12s %-15s",
		"Test Case", "Hasher", "Keys", "Min", "Max", "Avg", "StdDev", "Collision%", "Hash Time", "Distribution Time")
	t.Log(strings.Repeat("-", 120))

	for _, testKey := range testKeys {
		testStats := testGroups[testKey]

		for _, stat := range testStats {
			t.Logf("%-20s %-8s %-10d %-8d %-8d %-8.1f %-10.2f %-12.4f %-12v %-15v",
				testKey,
				stat.hasherName,
				stat.keyCount,
				stat.minLoad,
				stat.maxLoad,
				stat.avgLoad,
				stat.stdDev,
				stat.collisionRate*100,
				stat.hashTime,
				stat.distributionTime,
			)
		}
		t.Log(strings.Repeat("-", 120))
	}

	// Summary analysis
	t.Log("\nSUMMARY ANALYSIS:")
	t.Log("- Lower StdDev = better load distribution")
	t.Log("- Lower Collision% = better hash quality")
	t.Log("- Shorter times = better performance")
	t.Log(strings.Repeat("=", 120))
}
