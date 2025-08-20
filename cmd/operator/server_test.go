package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/internal/operator"
	keydbth "github.com/rudderlabs/keydb/internal/testhelper"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	miniokit "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
)

const (
	testTTL = "5m" // 5 minutes
)

func TestScaleUpAndDown(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the node service
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address)

	// Test Put
	_ = op.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Test Get
	body := op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Test CreateSnapshots
	_ = op.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 0, FullSync: false}, true)

	keydbth.RequireExpectedFiles(ctx, t, minioContainer,
		regexp.MustCompile("^hr_0_s_0_1.snapshot$"),
		regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
		regexp.MustCompile("^hr_2_s_0_1.snapshot$"),
	)

	node1Conf := newConf()
	node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           1,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address},
	}, node1Conf)

	// Test scaling procedure
	_ = op.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address, node1Address},
	}, true)
	_ = op.Do("/loadSnapshots", LoadSnapshotsRequest{
		NodeID:     1,
		HashRanges: hash.GetNodeHashRangesList(1, 2, totalHashRanges),
	}, true)
	_ = op.Do("/scale", ScaleRequest{NodeIDs: []uint32{0, 1}}, true)
	_ = op.Do("/scaleComplete", ScaleCompleteRequest{NodeIDs: []uint32{0, 1}}, true)

	// Test node info 0
	body = op.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []uint32{0, 2}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, uint64(0))

	// Test node info 1
	body = op.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []uint32{1}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, uint64(0))

	// Get again now that the cluster is made of two nodes
	body = op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Let's write something to generate more snapshots due to incremental updates
	_ = op.Do("/put", PutRequest{
		Keys: []string{"key5", "key6", "key7"}, TTL: testTTL,
	}, true)

	// Scale down by removing node1. Then node0 should pick up all keys.
	// WARNING: when scaling up you can only add nodes to the right e.g. if the clusterSize is 2, and you add a node
	// then it will be node2 and the clusterSize will be 3
	// WARNING: when scaling down you can only remove nodes from the right i.e. if you have 2 nodes you can't
	// remove node0, you have to remove node1
	_ = op.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 0, FullSync: false}, true)
	_ = op.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 1, FullSync: false}, true)
	keydbth.RequireExpectedFiles(ctx, t, minioContainer,
		regexp.MustCompile("^hr_0_s_0_1.snapshot$"),
		regexp.MustCompile("^hr_0_s_1_2.snapshot$"),
		regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
		regexp.MustCompile("^hr_1_s_1_2.snapshot$"),
		regexp.MustCompile("^hr_2_s_0_1.snapshot$"),
		regexp.MustCompile("^hr_2_s_1_2.snapshot$"),
	)
	_ = op.Do("/loadSnapshots", LoadSnapshotsRequest{
		NodeID:     0,
		HashRanges: hash.GetNodeHashRangesList(0, 1, totalHashRanges),
	}, true)
	_ = op.Do("/updateClusterData", UpdateClusterDataRequest{Addresses: []string{node0Address}}, true)
	_ = op.Do("/scale", ScaleRequest{NodeIDs: []uint32{0}}, true)
	_ = op.Do("/scaleComplete", ScaleCompleteRequest{NodeIDs: []uint32{0}}, true)

	// Get node info again
	body = op.Do("/info", InfoRequest{
		NodeID: 0,
	})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.ElementsMatch(t, []uint32{0, 1, 2}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, uint64(0))

	cancel()
	node0.Close()
	node1.Close()
}

func TestAutoScale(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the node service
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address)

	// Test Put some initial data
	_ = op.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Test Get to verify data exists
	body := op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Create second node for scale up test
	node1Conf := newConf()
	node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           1,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address},
	}, node1Conf)

	// Test Scale Up using autoScale
	_ = op.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address, node1Address},
	}, true)

	// Verify scale up worked - check node info
	body = op.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)

	body = op.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)

	keydbth.RequireExpectedFiles(ctx, t, minioContainer,
		regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
	)

	// Verify data is still accessible after scale up
	body = op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Write more keys after scale up to test data preservation during scale down
	_ = op.Do("/put", PutRequest{
		Keys: []string{"key5", "key6", "key7", "key8"}, TTL: testTTL,
	}, true)

	// Verify all keys are accessible before scale down
	body = op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"},
	})
	require.JSONEq(t,
		`{"key1":true,"key2":true,"key3":true,"key4":false,"key5":true,"key6":true,"key7":true,"key8":true}`, body,
	)

	// Test Scale Down using autoScale
	_ = op.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address, node1Address},
		NewNodesAddresses: []string{node0Address},
	}, true)

	keydbth.RequireExpectedFiles(ctx, t, minioContainer,
		regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
		regexp.MustCompile("^hr_1_s_1_2.snapshot$"),
	)

	// Verify scale down worked - check node info
	body = op.Do("/info", InfoRequest{NodeID: 0})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)

	// Verify all data is still accessible after scale down
	body = op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"},
	})
	require.JSONEq(t,
		`{"key1":true,"key2":true,"key3":true,"key4":false,"key5":true,"key6":true,"key7":true,"key8":true}`,
		body,
	)

	cancel()
	node0.Close()
	node1.Close()
}

func TestHandleAutoScaleErrors(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the node service
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address)

	// Test error cases
	testCases := []struct {
		name           string
		request        AutoScaleRequest
		expectedStatus int
	}{
		{
			name: "empty old addresses",
			request: AutoScaleRequest{
				OldNodesAddresses: []string{},
				NewNodesAddresses: []string{node0Address},
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "empty new addresses",
			request: AutoScaleRequest{
				OldNodesAddresses: []string{node0Address},
				NewNodesAddresses: []string{},
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "auto-healing with same cluster size",
			request: AutoScaleRequest{
				OldNodesAddresses: []string{node0Address},
				NewNodesAddresses: []string{node0Address},
			},
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := jsonrs.Marshal(tc.request)
			require.NoError(t, err)
			req, err := http.NewRequest(http.MethodPost, op.url+"/autoScale", bytes.NewBuffer(buf))
			require.NoError(t, err)

			resp, err := op.client.Do(req)
			require.NoError(t, err)
			defer func() { httputil.CloseResponse(resp) }()

			require.Equal(t, tc.expectedStatus, resp.StatusCode)
		})
	}

	cancel()
	node0.Close()
}

func TestAutoHealing(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the node service
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address)

	// Test Put some initial data
	_ = op.Do("/put", PutRequest{
		Keys: []string{"heal1", "heal2", "heal3"}, TTL: testTTL,
	}, true)

	// Test Get to verify data exists
	body := op.Do("/get", GetRequest{
		Keys: []string{"heal1", "heal2", "heal3", "heal4"},
	})
	require.JSONEq(t, `{"heal1":true,"heal2":true,"heal3":true,"heal4":false}`, body)

	// Test Auto-Healing with same cluster size (should trigger auto-healing instead of error)
	_ = op.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address},
	}, true)

	// Verify auto-healing worked - check node info
	body = op.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])

	// Verify data is still accessible after auto-healing
	body = op.Do("/get", GetRequest{
		Keys: []string{"heal1", "heal2", "heal3", "heal4"},
	})
	require.JSONEq(t, `{"heal1":true,"heal2":true,"heal3":true,"heal4":false}`, body)

	cancel()
	node0.Close()
}

func TestHashRangeMovements(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a simple node service for the HTTP server
	totalHashRanges := uint32(8)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address)

	// Test successful hash range movements preview
	t.Run("successful scale up preview", func(t *testing.T) {
		body := op.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  2,
			NewClusterSize:  3,
			TotalHashRanges: 8,
		})

		var movements []HashRangeMovement
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &movements))

		// Verify we have some movements
		require.NotEmpty(t, movements)

		// Verify each movement has valid data
		for _, movement := range movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(2), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(3), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}
	})

	t.Run("successful scale down preview", func(t *testing.T) {
		body := op.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  3,
			NewClusterSize:  2,
			TotalHashRanges: 8,
		})

		var movements []HashRangeMovement
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &movements))

		// Verify we have some movements
		require.NotEmpty(t, movements)

		// Verify each movement has valid data
		for _, movement := range movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(3), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(2), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}
	})

	t.Run("no movements when cluster size unchanged", func(t *testing.T) {
		body := op.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  2,
			NewClusterSize:  2,
			TotalHashRanges: 8,
		})

		var movements []HashRangeMovement
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &movements))

		// Should be empty when cluster size doesn't change
		require.Empty(t, movements)
	})

	// Test error cases
	testCases := []struct {
		name           string
		request        HashRangeMovementsRequest
		expectedStatus int
	}{
		{
			name: "zero old cluster size",
			request: HashRangeMovementsRequest{
				OldClusterSize:  0,
				NewClusterSize:  2,
				TotalHashRanges: 8,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "zero new cluster size",
			request: HashRangeMovementsRequest{
				OldClusterSize:  2,
				NewClusterSize:  0,
				TotalHashRanges: 8,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "zero total hash ranges",
			request: HashRangeMovementsRequest{
				OldClusterSize:  2,
				NewClusterSize:  3,
				TotalHashRanges: 0,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "total hash ranges less than old cluster size",
			request: HashRangeMovementsRequest{
				OldClusterSize:  5,
				NewClusterSize:  3,
				TotalHashRanges: 4,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "total hash ranges less than new cluster size",
			request: HashRangeMovementsRequest{
				OldClusterSize:  2,
				NewClusterSize:  5,
				TotalHashRanges: 4,
			},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := jsonrs.Marshal(tc.request)
			require.NoError(t, err)
			req, err := http.NewRequest(http.MethodPost, op.url+"/hashRangeMovements", bytes.NewBuffer(buf))
			require.NoError(t, err)

			resp, err := op.client.Do(req)
			require.NoError(t, err)
			defer func() { httputil.CloseResponse(resp) }()

			require.Equal(t, tc.expectedStatus, resp.StatusCode)
		})
	}

	t.Run("upload", func(t *testing.T) {
		_ = op.Do("/put", PutRequest{
			Keys: []string{
				"key1", "key2", "key3", "key4",
				"key5", "key6", "key7", "key8",
				"key9", "key10", "key11", "key12",
				"key13", "key14", "key15", "key16",
			}, TTL: testTTL,
		}, true)

		body := op.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: 8,
			Upload:          true,
		})

		var movements []HashRangeMovement
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &movements))

		// Verify we still get movements even with upload=true
		require.NotEmpty(t, movements)

		// Verify each movement has valid data
		for _, movement := range movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(1), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(2), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}

		keydbth.RequireExpectedFiles(context.Background(), t, minioContainer,
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_3_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_5_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_7_s_0_1.snapshot$"),
		)

		_ = op.Do("/put", PutRequest{
			Keys: []string{
				"key17", "key18", "key19", "key20",
				"key21", "key22", "key23", "key24",
				"key25", "key26", "key27", "key28",
				"key29", "key30", "key31", "key32",
			}, TTL: testTTL,
		}, true)

		body = op.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: 8,
			Upload:          true,
			SplitUploads:    true,
		})

		require.NoError(t, jsonrs.Unmarshal([]byte(body), &movements))

		// Verify we still get movements even with upload=true and splitUploads=true
		require.NotEmpty(t, movements)

		// Verify each movement has valid data
		for _, movement := range movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(1), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(2), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}

		// When splitUploads is true, each hash range gets its own CreateSnapshots call,
		// resulting in separate snapshot files with different naming patterns.
		// We expect both the files from the previous test and the new split upload files.
		keydbth.RequireExpectedFiles(context.Background(), t, minioContainer,
			// Files from the previous "upload" test
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_3_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_5_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_7_s_0_1.snapshot$"),
			// Files from the current "upload with split uploads" test
			regexp.MustCompile("^hr_1_s_1_2.snapshot$"),
			regexp.MustCompile("^hr_3_s_1_2.snapshot$"),
			regexp.MustCompile("^hr_5_s_1_2.snapshot$"),
			regexp.MustCompile("^hr_7_s_1_2.snapshot$"),
		)
	})

	cancel()
	node0.Close()
}

func TestHandleLastOperation(t *testing.T) {

	// Start test server
	op := startOperatorHTTPServer(t, 128, "localhost:0")

	// Record an operation
	op.operator.RecordOperation(operator.ScaleUp, 2, 3, []string{"node1", "node2"}, []string{"node1", "node2", "node3"})

	// Make request to /lastOperation endpoint
	resp, err := http.Get(op.url + "/lastOperation")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	// Parse response
	var response struct {
		Operation *operator.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, response.Operation)
	require.Equal(t, operator.ScaleUp, response.Operation.Type)
	require.Equal(t, uint32(2), response.Operation.OldClusterSize)
	require.Equal(t, uint32(3), response.Operation.NewClusterSize)
	require.Equal(t, []string{"node1", "node2"}, response.Operation.OldAddresses)
	require.Equal(t, []string{"node1", "node2", "node3"}, response.Operation.NewAddresses)
}

func getService(
	ctx context.Context, t testing.TB, cs filemanager.S3Manager, nodeConfig node.Config, conf *config.Config,
) (*node.Service, string) {
	t.Helper()

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	address := "localhost:" + strconv.Itoa(freePort)
	nodeConfig.Addresses = append(nodeConfig.Addresses, address)

	log := logger.NOP
	if testing.Verbose() {
		log = logger.NewLogger()
	}
	conf.Set("BadgerDB.Dedup.NopLogger", true)
	service, err := node.NewService(ctx, nodeConfig, cs, conf, stats.NOP, log)
	require.NoError(t, err)

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	lis, err := net.Listen("tcp", address)
	require.NoError(t, err)

	// Start the server
	go func() {
		require.NoError(t, server.Serve(lis))
	}()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = lis.Close()
	})

	return service, address
}

func startOperatorHTTPServer(t testing.TB, totalHashRanges uint32, addresses ...string) *opClient { // nolint:unparam
	t.Helper()

	c, err := client.NewClient(client.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
	}, logger.NOP)
	require.NoError(t, err)

	op, err := operator.NewClient(operator.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
		RetryCount:      3,
		RetryDelay:      time.Second,
	}, logger.NOP)
	require.NoError(t, err)

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	addr := fmt.Sprintf(":%d", freePort)

	opServer := newHTTPServer(c, op, addr, logger.NOP)
	go func() {
		err := opServer.Start()
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Operator server error: %v", err)
		}
	}()
	t.Cleanup(func() {
		_ = opServer.Stop(context.Background())
	})

	return &opClient{
		t:        t,
		client:   http.DefaultClient,
		url:      fmt.Sprintf("http://localhost:%d", freePort),
		operator: op,
	}
}

type opClient struct {
	t        testing.TB
	client   *http.Client
	url      string
	operator *operator.Client
}

func (c *opClient) Do(endpoint string, data any, success ...bool) string {
	c.t.Helper()

	buf, err := jsonrs.Marshal(data)
	require.NoError(c.t, err)
	req, err := http.NewRequest(http.MethodPost, c.url+endpoint, bytes.NewBuffer(buf))
	require.NoError(c.t, err)

	resp, err := c.client.Do(req)
	require.NoError(c.t, err)

	defer func() { httputil.CloseResponse(resp) }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)

	if resp.StatusCode != http.StatusOK {
		c.t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	if len(success) > 0 && success[0] {
		require.JSONEq(c.t, `{"success":true}`, string(body))
	}

	return string(body)
}

func TestScaleUpFailureAndRollback(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the initial node service
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address)

	// Test Put some data
	_ = op.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Verify data can be retrieved
	body := op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	// Try to scale up with the non-running node - this should trigger rollback
	autoScaleReq := AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address, "random-no1-address:12345"}, // Simulating a non-running node
		FullSync:          false,
	}

	// This should fail and trigger rollback
	buf, err := jsonrs.Marshal(autoScaleReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, op.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := op.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node1 is not actually running
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded and rolled back
	lastOpResp, err := http.Get(op.url + "/lastOperation")
	require.NoError(t, err)
	defer lastOpResp.Body.Close()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *operator.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, operator.ScaleUp, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(1), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(2), lastOpResponse.Operation.NewClusterSize)
	require.Equal(t, []string{node0Address}, lastOpResponse.Operation.OldAddresses)
	// For the failed step, we expect some error message
	require.Equal(t, operator.RolledBack, lastOpResponse.Operation.Status)

	// Verify that node0 still has the original cluster size
	body = op.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])

	// Verify data is still accessible after rollback
	body = op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	cancel()
	node0.Close()
}

func TestScaleDownFailureAndRollback(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a 2-node cluster
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	node1Conf := newConf()
	_, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           1,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address},
	}, node1Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address, node1Address)

	// Test Put some data
	_ = op.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Verify data can be retrieved
	body := op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	// Try to scale down by removing node1 - this should trigger rollback
	randomAdd := "random-wefaofwef-address:12345"
	autoScaleReq := AutoScaleRequest{
		OldNodesAddresses: []string{node0Address, node1Address},
		NewNodesAddresses: []string{randomAdd}, // Simulating a non-running node
		FullSync:          false,
	}

	// This should fail and trigger rollback
	buf, err := jsonrs.Marshal(autoScaleReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, op.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := op.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node1 is closed and can't participate in the operation
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded and rolled back
	lastOpResp, err := http.Get(op.url + "/lastOperation")
	require.NoError(t, err)
	defer lastOpResp.Body.Close()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *operator.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, operator.ScaleDown, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(2), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(1), lastOpResponse.Operation.NewClusterSize)
	require.Equal(t, []string{node0Address, node1Address}, lastOpResponse.Operation.OldAddresses)
	require.Equal(t, []string{randomAdd}, lastOpResponse.Operation.NewAddresses)
	require.Equal(t, operator.RolledBack, lastOpResponse.Operation.Status)

	// Verify that both nodes still exist (rolled back to original state)
	// Node 0 should still think there are 2 nodes in the cluster
	body = op.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.Contains(t, infoResponse.NodesAddresses, node0Address)
	require.Contains(t, infoResponse.NodesAddresses, node1Address)

	// Verify data is still accessible after rollback
	body = op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	cancel()
	node0.Close()
}

func TestAutoHealingFailureAndRollback(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a 2-node cluster
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	_, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	node1Conf := newConf()
	_, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           1,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address},
	}, node1Conf)

	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, totalHashRanges, node0Address, node1Address)

	// Test Put some data
	_ = op.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Verify data can be retrieved
	body := op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	// Try auto-healing with only node0 available - this should fail and trigger rollback
	randomAdd := "random-wefaofwef-address:12345"
	autoHealReq := AutoScaleRequest{
		OldNodesAddresses: []string{node0Address, node1Address},
		NewNodesAddresses: []string{node0Address, randomAdd}, // Only node0 is available now
	}

	// This should fail and trigger rollback
	buf, err := jsonrs.Marshal(autoHealReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, op.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := op.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node1 is closed and can't participate in the operation
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded and rolled back
	lastOpResp, err := http.Get(op.url + "/lastOperation")
	require.NoError(t, err)
	defer lastOpResp.Body.Close()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *operator.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, operator.AutoHealing, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(2), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(2), lastOpResponse.Operation.NewClusterSize) // Same size for auto-healing
	require.Equal(t, []string{node0Address, node1Address}, lastOpResponse.Operation.OldAddresses)
	require.Equal(t, []string{node0Address, randomAdd}, lastOpResponse.Operation.NewAddresses)
	require.Equal(t, operator.RolledBack, lastOpResponse.Operation.Status)

	// Verify that node0 still has the original cluster configuration
	body = op.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.Contains(t, infoResponse.NodesAddresses, node0Address)
	require.Contains(t, infoResponse.NodesAddresses, node1Address)

	// Verify data is still accessible after rollback
	body = op.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	cancel()
}

func TestRollbackFailure(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	node0Address := "random-node0-address:12345"
	node1Address := "random-node1-address:12345"
	// Start the Operator HTTP Server
	op := startOperatorHTTPServer(t, 3, node0Address)

	// Try to scale up with no nodes running - this should fail and rollback should also fail
	autoScaleReq := AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address, node1Address},
		FullSync:          false,
	}

	// This should fail and rollback should also fail
	buf, err := jsonrs.Marshal(autoScaleReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, op.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := op.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node0 is closed
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded with failed status
	lastOpResp, err := http.Get(op.url + "/lastOperation")
	require.NoError(t, err)
	defer lastOpResp.Body.Close()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *operator.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, operator.ScaleUp, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(1), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(2), lastOpResponse.Operation.NewClusterSize)
	require.Equal(t, []string{node0Address}, lastOpResponse.Operation.OldAddresses)
	require.Equal(t, []string{node0Address, node1Address}, lastOpResponse.Operation.NewAddresses)
	require.Equal(t, operator.Failed, lastOpResponse.Operation.Status) // Should be failed, not rolled back
}
