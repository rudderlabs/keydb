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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/internal/scaler"
	keydbth "github.com/rudderlabs/keydb/internal/testhelper"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/tcpproxy"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	miniokit "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
)

const (
	testTTL                 = "5m" // 5 minutes
	defaultBackupFolderName = "default"
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

	ctx0, cancel0 := context.WithCancel(context.Background())
	defer cancel0()

	// Create the node service
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx0, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf)

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{}, node0Address)

	// Test Put
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Test Get
	body := s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Test CreateSnapshots
	_ = s.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 0, FullSync: false}, true)

	keydbth.RequireExpectedFiles(context.Background(), t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_0_s_0_1.snapshot$"),
		regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
	)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	node1Conf := newConf()
	node1, node1Address := getService(ctx1, t, cloudStorage, node.Config{
		NodeID:           1,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address},
	}, node1Conf)

	// Test scaling procedure
	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address, node1Address},
	}, true)
	_ = s.Do("/loadSnapshots", LoadSnapshotsRequest{
		NodeID:     1,
		HashRanges: hash.New(2, totalHashRanges).GetNodeHashRangesList(1),
	}, true)
	_ = s.Do("/scale", ScaleRequest{NodeIDs: []uint32{0, 1}}, true)
	_ = s.Do("/scaleComplete", ScaleCompleteRequest{NodeIDs: []uint32{0, 1}}, true)

	// Test node info 0
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []uint32{0, 1}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, uint64(0))

	// Test node info 1
	body = s.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []uint32{2}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, uint64(0))

	// Get again now that the cluster is made of two nodes
	body = s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Let's write something to generate more snapshots due to incremental updates
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key5", "key6", "key7"}, TTL: testTTL,
	}, true)

	// Scale down by removing node1. Then node0 should pick up all keys.
	// WARNING: when scaling up you can only add nodes to the right e.g. if the clusterSize is 2, and you add a node
	// then it will be node2 and the clusterSize will be 3
	// WARNING: when scaling down you can only remove nodes from the right i.e. if you have 2 nodes you can't
	// remove node0, you have to remove node1
	_ = s.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 0, FullSync: false}, true)
	_ = s.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 1, FullSync: false}, true)
	keydbth.RequireExpectedFiles(context.Background(), t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_0_s_0_1.snapshot$"),
		regexp.MustCompile("^.+/hr_0_s_1_2.snapshot$"),
		regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
		regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
	)
	_ = s.Do("/loadSnapshots", LoadSnapshotsRequest{
		NodeID:     0,
		HashRanges: hash.New(1, totalHashRanges).GetNodeHashRangesList(0),
	}, true)
	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{Addresses: []string{node0Address}}, true)
	_ = s.Do("/scale", ScaleRequest{NodeIDs: []uint32{0}}, true)
	_ = s.Do("/scaleComplete", ScaleCompleteRequest{NodeIDs: []uint32{0}}, true)

	// Get node info again
	body = s.Do("/info", InfoRequest{
		NodeID: 0,
	})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.ElementsMatch(t, []uint32{0, 1, 2}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, uint64(0))

	// Close node1 before doing a GET to make sure node1 won't pick that request
	cancel1()
	node1.Close()

	// Test Get
	body = s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	cancel0()
	node0.Close()
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

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address)

	// Test Put some initial data
	_ = s.Do("/put", PutRequest{
		// Hash ranges: keyA → 1, keyB → 0, keyC → 2
		Keys: []string{"keyA", "keyB", "keyC"},
		TTL:  testTTL,
	}, true)

	// Test Get to verify data exists
	body := s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD"},
	})
	require.JSONEq(t, `{"keyA":true,"keyB":true,"keyC":true,"keyD":false}`, body)

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
	_ = s.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address, node1Address},
	}, true)

	// Verify scale up worked - check node info
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []uint32{0, 1}, infoResponse.HashRanges)

	body = s.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []uint32{2}, infoResponse.HashRanges)

	keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
	)

	// Verify data is still accessible after scale up
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD"},
	})
	require.JSONEq(t, `{"keyA":true,"keyB":true,"keyC":true,"keyD":false}`, body)

	// Write more keys after scale up to test data preservation during scale down
	_ = s.Do("/put", PutRequest{
		// Hash ranges: AAA → 0, BBB → 2, CCC → 0, DDD → 0
		Keys: []string{"AAA", "BBB", "CCC", "DDD"},
		TTL:  testTTL,
	}, true)

	// Verify all keys are accessible before scale down
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "AAA", "BBB", "CCC", "DDD"},
	})
	require.JSONEq(t,
		`{"keyA":true,"keyB":true,"keyC":true,"keyD":false,"AAA":true,"BBB":true,"CCC":true,"DDD":true}`,
		body,
	)

	// Keys status at this point
	// Node0 should have AAA(0), CCC(0), DDD(0), keyB(0), keyA(1)
	// Node1 should have BBB(2), keyC(2)

	t.Log("Scaling down from 2 nodes to 1 node...")

	// Test Scale Down using autoScale
	_ = s.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address, node1Address},
		NewNodesAddresses: []string{node0Address},
	}, true)

	keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
		regexp.MustCompile("^.+/hr_2_s_1_2.snapshot$"),
	)

	// Verify scale down worked - check node info
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.ElementsMatch(t, []uint32{0, 1, 2}, infoResponse.HashRanges)

	// Verify all data is still accessible after scale down
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "AAA", "BBB", "CCC", "DDD"},
	})
	require.JSONEq(t,
		`{"keyA":true,"keyB":true,"keyC":true,"keyD":false,"AAA":true,"BBB":true,"CCC":true,"DDD":true}`,
		body,
	)

	cancel()
	node0.Close()
	node1.Close()
}

func TestAutoScaleTransientNetworkFailure(t *testing.T) {
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

	// Start proxy for simulating transient failure with node0
	proxyPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	proxy := &tcpproxy.Proxy{
		LocalAddr: "localhost:" + strconv.Itoa(proxyPort),
	}

	// Create the node service
	totalHashRanges := uint32(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, node0Conf, withProxy(proxy))
	go proxy.Start(t) // Starting the proxy after we get the service to populate RemoteAddr

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		InitialInterval: time.Second,
		Multiplier:      1,
		MaxInterval:     time.Second,
		MaxElapsedTime:  3 * time.Second,
	}, node0Address)

	// Test Put some initial data
	_ = s.Do("/put", PutRequest{
		// keyA → 1, keyB → 0, keyC → 2
		Keys: []string{"keyA", "keyB", "keyC"},
		TTL:  testTTL,
	}, true)

	// Test Get to verify data exists
	body := s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD"},
	})
	require.JSONEq(t, `{"keyA":true,"keyB":true,"keyC":true,"keyD":false}`, body)

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
	t.Log("Stopping proxy to simulate transient failure...")
	proxy.Stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.Log("Firing autoScale request")
		_ = s.Do("/autoScale", AutoScaleRequest{
			OldNodesAddresses: []string{node0Address},
			NewNodesAddresses: []string{node0Address, node1Address},
		}, true)
	}()
	go func() {
		time.Sleep(1 * time.Second)
		t.Log("Starting proxy again")
		proxy.Start(t)
	}()
	<-done

	// Verify scale up worked - check node info
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)

	body = s.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)

	keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
	)

	// Verify data is still accessible after scale up
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD"},
	})
	require.JSONEq(t, `{"keyA":true,"keyB":true,"keyC":true,"keyD":false}`, body)

	// Write more keys after scale up to test data preservation during scale down
	_ = s.Do("/put", PutRequest{
		// Hash ranges: AAA → 0, BBB → 2, CCC → 0, DDD → 0
		Keys: []string{"AAA", "BBB", "CCC", "DDD"},
		TTL:  testTTL,
	}, true)

	// Verify all keys are accessible before scale down
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "AAA", "BBB", "CCC", "DDD"},
	})
	require.JSONEq(t,
		`{"keyA":true,"keyB":true,"keyC":true,"keyD":false,"AAA":true,"BBB":true,"CCC":true,"DDD":true}`,
		body,
	)

	t.Log("Scaling down from 2 nodes to 1 node...")

	// Test Scale Down using autoScale
	_ = s.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address, node1Address},
		NewNodesAddresses: []string{node0Address},
	}, true)

	keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
		regexp.MustCompile("^.+/hr_2_s_1_2.snapshot$"),
	)

	// Verify scale down worked - check node info
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)

	// Verify all data is still accessible after scale down
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "AAA", "BBB", "CCC", "DDD"},
	})
	require.JSONEq(t,
		`{"keyA":true,"keyB":true,"keyC":true,"keyD":false,"AAA":true,"BBB":true,"CCC":true,"DDD":true}`,
		body,
	)

	cancel()
	node0.Close()
	node1.Close()
	proxy.Stop()
}

func TestAutoScaleTransientError(t *testing.T) {
	// Create the node service
	totalHashRanges := uint32(3)
	node0 := startMockNodeService(t, "node0")
	node1 := startMockNodeService(t, "node1")

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		InitialInterval: 10 * time.Millisecond,
		Multiplier:      1,
		MaxInterval:     10 * time.Millisecond,
		MaxElapsedTime:  time.Second,
	}, node0.address)

	t.Log("Scaling up from 1 node to 2 nodes...")
	node0.createSnapshotsReturnError.Store(true)
	node1.loadSnapshotsReturnError.Store(true)
	node0.scaleReturnError.Store(true)
	node0.scaleCompleteReturnError.Store(true)
	done := make(chan struct{})
	go func() {
		close(done)
		_ = s.Do("/autoScale", AutoScaleRequest{
			OldNodesAddresses: []string{node0.address},
			NewNodesAddresses: []string{node0.address, node1.address},
		}, true)
	}()

	waitForRetries := uint64(10)

	t.Logf("Waiting for at least %d retries to be done on CreateSnapshots", waitForRetries)
	require.Eventuallyf(t, func() bool {
		return node0.createSnapshotsCalls.Load() >= waitForRetries // wait for at least 10 retries
	}, 10*time.Second, time.Millisecond, "Calls %d", node0.createSnapshotsCalls.Load())
	node0.createSnapshotsReturnError.Store(false)

	t.Logf("Waiting for at least %d retries to be done on LoadSnapshots", waitForRetries)
	require.Eventuallyf(t, func() bool {
		return node1.loadSnapshotsCalls.Load() >= waitForRetries // wait for at least 10 retries
	}, 10*time.Second, time.Millisecond, "Calls %d", node1.loadSnapshotsCalls.Load())
	node1.loadSnapshotsReturnError.Store(false)

	t.Logf("Waiting for at least %d retries to be done on Scale", waitForRetries)
	require.Eventuallyf(t, func() bool {
		return node0.scaleCalls.Load() >= waitForRetries // wait for at least 10 retries
	}, 10*time.Second, time.Millisecond, "Calls %d", node0.scaleCalls.Load())
	node0.scaleReturnError.Store(false)

	t.Logf("Waiting for at least %d retries to be done on ScaleComplete", waitForRetries)
	require.Eventuallyf(t, func() bool {
		return node0.scaleCompleteCalls.Load() >= waitForRetries // wait for at least 10 retries
	}, 10*time.Second, time.Millisecond, "Calls %d", node0.scaleCompleteCalls.Load())
	node0.scaleCompleteReturnError.Store(false)

	<-done

	t.Log("Scaling down from 2 nodes to 1 node...")
	done = make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Do("/autoScale", AutoScaleRequest{
			OldNodesAddresses: []string{node0.address, node1.address},
			NewNodesAddresses: []string{node0.address},
		}, true)
	}()
	<-done
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

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{}, node0Address)

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
			req, err := http.NewRequest(http.MethodPost, s.url+"/autoScale", bytes.NewBuffer(buf))
			require.NoError(t, err)

			resp, err := s.client.Do(req)
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

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address)

	// Test Put some initial data
	_ = s.Do("/put", PutRequest{
		Keys: []string{"heal1", "heal2", "heal3"}, TTL: testTTL,
	}, true)

	// Test Get to verify data exists
	body := s.Do("/get", GetRequest{
		Keys: []string{"heal1", "heal2", "heal3", "heal4"},
	})
	require.JSONEq(t, `{"heal1":true,"heal2":true,"heal3":true,"heal4":false}`, body)

	// Test Auto-Healing with same cluster size (should trigger auto-healing instead of error)
	_ = s.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address},
	}, true)

	// Verify auto-healing worked - check node info
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])

	// Verify data is still accessible after auto-healing
	body = s.Do("/get", GetRequest{
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

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{}, node0Address)

	// Test successful hash range movements preview
	t.Run("successful scale up preview", func(t *testing.T) {
		body := s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  2,
			NewClusterSize:  3,
			TotalHashRanges: 8,
		})

		var response HashRangeMovementsResponse
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &response))

		// Verify we have some movements
		require.EqualValues(t, 3, response.Total)
		require.Len(t, response.Movements, 3)

		// Verify each movement has valid data
		for _, movement := range response.Movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(2), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(3), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}
	})

	t.Run("successful scale down preview", func(t *testing.T) {
		body := s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  3,
			NewClusterSize:  2,
			TotalHashRanges: 8,
		})

		var response HashRangeMovementsResponse
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &response))

		// Verify we have some movements
		require.EqualValues(t, 3, response.Total)
		require.Len(t, response.Movements, 3)

		// Verify each movement has valid data
		for _, movement := range response.Movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(3), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(2), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}
	})

	t.Run("no movements when cluster size unchanged", func(t *testing.T) {
		body := s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  2,
			NewClusterSize:  2,
			TotalHashRanges: 8,
		})

		var response HashRangeMovementsResponse
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &response))

		// Should be empty when cluster size doesn't change
		require.EqualValues(t, 0, response.Total)
		require.Len(t, response.Movements, 0)
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
			req, err := http.NewRequest(http.MethodPost, s.url+"/hashRangeMovements", bytes.NewBuffer(buf))
			require.NoError(t, err)

			resp, err := s.client.Do(req)
			require.NoError(t, err)
			defer func() { httputil.CloseResponse(resp) }()

			require.Equal(t, tc.expectedStatus, resp.StatusCode)
		})
	}

	t.Run("upload and download", func(t *testing.T) {
		_ = s.Do("/put", PutRequest{
			Keys: []string{
				"key1", "key2", "key3", "key4",
				"key5", "key6", "key7", "key8",
				"key9", "key10", "key11", "key12",
				"key13", "key14", "key15", "key16",
			}, TTL: testTTL,
		}, true)

		body := s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: 8,
			Upload:          true,
		})

		var response HashRangeMovementsResponse
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &response))

		// Verify we still get movements even with upload=true
		require.EqualValues(t, 5, response.Total)
		require.Len(t, response.Movements, 5)

		// Verify each movement has valid data
		for _, movement := range response.Movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(1), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(2), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}

		keydbth.RequireExpectedFiles(context.Background(), t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_5_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_6_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_7_s_0_1.snapshot$"),
		)

		_ = s.Do("/put", PutRequest{
			Keys: []string{
				"key17", "key18", "key19", "key20",
				"key21", "key22", "key23", "key24",
				"key25", "key26", "key27", "key28",
				"key29", "key30", "key31", "key32",
			}, TTL: testTTL,
		}, true)

		body = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: 8,
			Upload:          true,
		})

		require.NoError(t, jsonrs.Unmarshal([]byte(body), &response))

		// Verify we still get movements even with upload=true and splitUploads=true
		require.EqualValues(t, 5, response.Total)
		require.Len(t, response.Movements, 5)

		// Verify each movement has valid data
		for _, movement := range response.Movements {
			require.Less(t, movement.HashRange, uint32(8), "hash range should be less than total")
			require.Less(t, movement.From, uint32(1), "from node should be less than old cluster size")
			require.Less(t, movement.To, uint32(2), "to node should be less than new cluster size")
			require.NotEqual(t, movement.From, movement.To, "from and to should be different")
		}

		// When splitUploads is true, each hash range gets its own CreateSnapshots call,
		// resulting in separate snapshot files with different naming patterns.
		// We expect both the files from the previous test and the new split upload files.
		keydbth.RequireExpectedFiles(context.Background(), t, minioContainer, defaultBackupFolderName,
			// Files from the previous "upload" test
			regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_5_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_6_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_7_s_0_1.snapshot$"),
			// Files from the current "upload with split uploads" test
			regexp.MustCompile("^.+/hr_2_s_1_2.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_1_2.snapshot$"),
			regexp.MustCompile("^.+/hr_5_s_1_2.snapshot$"),
			regexp.MustCompile("^.+/hr_6_s_1_2.snapshot$"),
			regexp.MustCompile("^.+/hr_7_s_1_2.snapshot$"),
		)

		newNodeCtx, newNodeCancel := context.WithCancel(context.Background())
		defer newNodeCancel()

		newNodeConf := newConf()
		_, newNodeAddress := getService(newNodeCtx, t, cloudStorage, node.Config{
			NodeID:           1,
			ClusterSize:      2,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, newNodeConf)

		s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{}, node0Address, newNodeAddress)

		body = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: 8,
			Download:        true,
		})

		require.NoError(t, jsonrs.Unmarshal([]byte(body), &response))

		// Verify we still get movements even with download=true
		require.EqualValues(t, 5, response.Total)
		require.Len(t, response.Movements, 5)

		// Try to fetch only keys that are served by the new node to see if they exist
		keys := []string{
			"key1", "key2", "key3", "key4",
			"key5", "key6", "key7", "key8",
			"key9", "key10", "key11", "key12",
			"key13", "key14", "key15", "key16",
			"key17", "key18", "key19", "key20",
			"key21", "key22", "key23", "key24",
			"key25", "key26", "key27", "key28",
			"key29", "key30", "key31", "key32",
		}
		var node1Keys []string
		h := hash.New(2, totalHashRanges)
		for _, key := range keys {
			nodeID := h.GetNodeNumber(key)
			if nodeID == 1 {
				node1Keys = append(node1Keys, key)
			}
		}
		require.Greater(t, len(node1Keys), 0, "node 1 should have some keys")

		body = s.Do("/get", GetRequest{
			Keys: node1Keys,
		})
		expectedKeys := strings.Builder{}
		expectedKeys.WriteString("{")
		for i, key := range node1Keys {
			if i > 0 {
				expectedKeys.WriteString(",")
			}
			expectedKeys.WriteString(`"` + key + `":true`)
		}
		expectedKeys.WriteString("}")
		require.JSONEq(t, expectedKeys.String(), body)
	})

	cancel()
	node0.Close()
}

func TestHandleLastOperation(t *testing.T) {
	// Start test server
	s := startScalerHTTPServer(t, 128, scaler.RetryPolicy{}, "localhost:0")

	// Record an operation
	s.scaler.RecordOperation(scaler.ScaleUp, 2, 3, []string{"node1", "node2"}, []string{"node1", "node2", "node3"})

	// Make request to /lastOperation endpoint
	resp, err := http.Get(s.url + "/lastOperation")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	// Parse response
	var response struct {
		Operation *scaler.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, response.Operation)
	require.Equal(t, scaler.ScaleUp, response.Operation.Type)
	require.Equal(t, uint32(2), response.Operation.OldClusterSize)
	require.Equal(t, uint32(3), response.Operation.NewClusterSize)
	require.Equal(t, []string{"node1", "node2"}, response.Operation.OldAddresses)
	require.Equal(t, []string{"node1", "node2", "node3"}, response.Operation.NewAddresses)
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

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address)

	// Test Put some data
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Verify data can be retrieved
	body := s.Do("/get", GetRequest{
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
	req, err := http.NewRequest(http.MethodPost, s.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node1 is not actually running
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded and rolled back
	lastOpResp, err := http.Get(s.url + "/lastOperation")
	require.NoError(t, err)
	defer func() { _ = lastOpResp.Body.Close() }()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *scaler.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, scaler.ScaleUp, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(1), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(2), lastOpResponse.Operation.NewClusterSize)
	require.Equal(t, []string{node0Address}, lastOpResponse.Operation.OldAddresses)
	// For the failed step, we expect some error message
	require.Equal(t, scaler.RolledBack, lastOpResponse.Operation.Status)

	// Verify that node0 still has the original cluster size
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])

	// Verify data is still accessible after rollback
	body = s.Do("/get", GetRequest{
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

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address, node1Address)

	// Test Put some data
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Verify data can be retrieved
	body := s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	// Try to scale down by removing node1 - this should trigger rollback
	unreachableAddr := "unreachable-address:12345"
	autoScaleReq := AutoScaleRequest{
		OldNodesAddresses: []string{node0Address, node1Address},
		NewNodesAddresses: []string{unreachableAddr}, // Simulating a non-running node
		FullSync:          false,
	}

	// This should fail and trigger rollback
	buf, err := jsonrs.Marshal(autoScaleReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, s.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node1 is closed and can't participate in the operation
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded and rolled back
	lastOpResp, err := http.Get(s.url + "/lastOperation")
	require.NoError(t, err)
	defer func() { _ = lastOpResp.Body.Close() }()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *scaler.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, scaler.ScaleDown, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(2), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(1), lastOpResponse.Operation.NewClusterSize)
	require.Equal(t, []string{node0Address, node1Address}, lastOpResponse.Operation.OldAddresses)
	require.Equal(t, []string{unreachableAddr}, lastOpResponse.Operation.NewAddresses)
	require.Equal(t, scaler.RolledBack, lastOpResponse.Operation.Status)

	// Verify that both nodes still exist (rolled back to original state)
	// Node 0 should still think there are 2 nodes in the cluster
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.Contains(t, infoResponse.NodesAddresses, node0Address)
	require.Contains(t, infoResponse.NodesAddresses, node1Address)

	// Verify data is still accessible after rollback
	body = s.Do("/get", GetRequest{
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

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address, node1Address)

	// Test Put some data
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Verify data can be retrieved
	body := s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true}`, body)

	// Try auto-healing with only node0 available - this should fail and trigger rollback
	unreachableAddr := "unreachable-address:12345"
	autoHealReq := AutoScaleRequest{
		OldNodesAddresses: []string{node0Address, node1Address},
		NewNodesAddresses: []string{node0Address, unreachableAddr}, // Only node0 is available now
	}

	// This should fail and trigger rollback
	buf, err := jsonrs.Marshal(autoHealReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, s.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node1 is closed and can't participate in the operation
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded and rolled back
	lastOpResp, err := http.Get(s.url + "/lastOperation")
	require.NoError(t, err)
	defer func() { _ = lastOpResp.Body.Close() }()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *scaler.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, scaler.AutoHealing, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(2), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(2), lastOpResponse.Operation.NewClusterSize) // Same size for auto-healing
	require.Equal(t, []string{node0Address, node1Address}, lastOpResponse.Operation.OldAddresses)
	require.Equal(t, []string{node0Address, unreachableAddr}, lastOpResponse.Operation.NewAddresses)
	require.Equal(t, scaler.RolledBack, lastOpResponse.Operation.Status)

	// Verify that node0 still has the original cluster configuration
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.Contains(t, infoResponse.NodesAddresses, node0Address)
	require.Contains(t, infoResponse.NodesAddresses, node1Address)

	// Verify data is still accessible after rollback
	body = s.Do("/get", GetRequest{
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
	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, 3, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address)

	// Try to scale up with no nodes running - this should fail and rollback should also fail
	autoScaleReq := AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address, node1Address},
		FullSync:          false,
	}

	// This should fail and rollback should also fail
	buf, err := jsonrs.Marshal(autoScaleReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, s.url+"/autoScale", bytes.NewBuffer(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer func() { httputil.CloseResponse(resp) }()

	// Expect failure as node0 is closed
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check that the last operation was recorded with failed status
	lastOpResp, err := http.Get(s.url + "/lastOperation")
	require.NoError(t, err)
	defer func() { _ = lastOpResp.Body.Close() }()

	require.Equal(t, http.StatusOK, lastOpResp.StatusCode)
	require.Equal(t, "application/json", lastOpResp.Header.Get("Content-Type"))

	var lastOpResponse struct {
		Operation *scaler.ScalingOperation `json:"operation"`
	}
	err = jsonrs.NewDecoder(lastOpResp.Body).Decode(&lastOpResponse)
	require.NoError(t, err)

	// Verify operation details
	require.NotNil(t, lastOpResponse.Operation)
	require.Equal(t, scaler.ScaleUp, lastOpResponse.Operation.Type)
	require.Equal(t, uint32(1), lastOpResponse.Operation.OldClusterSize)
	require.Equal(t, uint32(2), lastOpResponse.Operation.NewClusterSize)
	require.Equal(t, []string{node0Address}, lastOpResponse.Operation.OldAddresses)
	require.Equal(t, []string{node0Address, node1Address}, lastOpResponse.Operation.NewAddresses)
	require.Equal(t, scaler.Failed, lastOpResponse.Operation.Status) // Should be failed, not rolled back
}

type serviceConfig struct {
	proxy *tcpproxy.Proxy
}

type option func(*serviceConfig)

func withProxy(proxy *tcpproxy.Proxy) option {
	return func(c *serviceConfig) { c.proxy = proxy }
}

func getService(
	ctx context.Context, t testing.TB, cs *filemanager.S3Manager, nodeConfig node.Config, conf *config.Config,
	opts ...option,
) (*node.Service, string) {
	t.Helper()

	var svcConf serviceConfig
	for _, opt := range opts {
		opt(&svcConf)
	}

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	var address string
	if svcConf.proxy != nil {
		address = svcConf.proxy.LocalAddr
		svcConf.proxy.RemoteAddr = "localhost:" + strconv.Itoa(freePort)
		nodeConfig.Addresses = append(nodeConfig.Addresses, svcConf.proxy.RemoteAddr)
		t.Logf("Using proxy, client will connect to %s but node is %s", address, svcConf.proxy.RemoteAddr)
	} else {
		address = "localhost:" + strconv.Itoa(freePort)
		nodeConfig.Addresses = append(nodeConfig.Addresses, address)
	}

	log := logger.NOP
	if testing.Verbose() {
		lf := logger.NewFactory(conf)
		require.NoError(t, lf.SetLogLevel("", "DEBUG"))
		log = lf.NewLogger()
	}
	conf.Set("BadgerDB.Dedup.NopLogger", true)
	nodeConfig.BackupFolderName = defaultBackupFolderName
	service, err := node.NewService(ctx, nodeConfig, cs, conf, stats.NOP, log)
	require.NoError(t, err)

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	lis, err := net.Listen("tcp", nodeConfig.Addresses[len(nodeConfig.Addresses)-1])
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

func startScalerHTTPServer(t testing.TB, totalHashRanges uint32, rp scaler.RetryPolicy, addresses ...string) *opClient {
	t.Helper()

	log := logger.NOP
	if testing.Verbose() {
		lf := logger.NewFactory(config.New())
		require.NoError(t, lf.SetLogLevel("", "DEBUG"))
		log = lf.NewLogger()
	}

	c, err := client.NewClient(client.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
	}, log)
	require.NoError(t, err)

	op, err := scaler.NewClient(scaler.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
		RetryPolicy:     rp,
	}, log)
	require.NoError(t, err)

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	addr := fmt.Sprintf(":%d", freePort)

	opServer := newHTTPServer(c, op, addr, log)
	go func() {
		err := opServer.Start(context.Background())
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Scaler server error: %v", err)
		}
	}()
	t.Cleanup(func() {
		_ = opServer.Stop(context.Background())
	})

	return &opClient{
		t:      t,
		client: http.DefaultClient,
		url:    fmt.Sprintf("http://localhost:%d", freePort),
		scaler: op,
	}
}

type opClient struct {
	t      testing.TB
	client *http.Client
	url    string
	scaler *scaler.Client
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

type mockNodeServiceServer struct {
	pb.UnimplementedNodeServiceServer

	t          testing.TB
	address    string
	identifier string

	scaleCalls       atomic.Uint64
	scaleReturnError atomic.Bool

	scaleCompleteCalls       atomic.Uint64
	scaleCompleteReturnError atomic.Bool

	createSnapshotsCalls       atomic.Uint64
	createSnapshotsReturnError atomic.Bool

	loadSnapshotsCalls       atomic.Uint64
	loadSnapshotsReturnError atomic.Bool
}

func (m *mockNodeServiceServer) Scale(_ context.Context, _ *pb.ScaleRequest) (*pb.ScaleResponse, error) {
	m.t.Logf("mockNodeServiceServer.Scale called on %s", m.identifier)
	defer m.scaleCalls.Add(1)
	if m.scaleReturnError.Load() {
		return nil, errors.New("scale mock error on " + m.identifier)
	}
	return &pb.ScaleResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) ScaleComplete(_ context.Context, _ *pb.ScaleCompleteRequest) (
	*pb.ScaleCompleteResponse, error,
) {
	m.t.Logf("mockNodeServiceServer.ScaleComplete called on %s", m.identifier)
	defer m.scaleCompleteCalls.Add(1)
	if m.scaleCompleteReturnError.Load() {
		return nil, errors.New("scale complete mock error on " + m.identifier)
	}
	return &pb.ScaleCompleteResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) CreateSnapshots(_ context.Context, _ *pb.CreateSnapshotsRequest) (
	*pb.CreateSnapshotsResponse, error,
) {
	m.t.Logf("mockNodeServiceServer.CreateSnapshots called on %s", m.identifier)
	defer m.createSnapshotsCalls.Add(1)
	if m.createSnapshotsReturnError.Load() {
		return nil, errors.New("create snapshots mock error on " + m.identifier)
	}
	return &pb.CreateSnapshotsResponse{Success: true}, nil
}

func (m *mockNodeServiceServer) LoadSnapshots(_ context.Context, _ *pb.LoadSnapshotsRequest) (
	*pb.LoadSnapshotsResponse, error,
) {
	m.t.Logf("mockNodeServiceServer.LoadSnapshots called on %s", m.identifier)
	defer m.loadSnapshotsCalls.Add(1)
	if m.loadSnapshotsReturnError.Load() {
		return nil, errors.New("load snapshots mock error on " + m.identifier)
	}
	return &pb.LoadSnapshotsResponse{Success: true}, nil
}

func startMockNodeService(t testing.TB, identifier string) *mockNodeServiceServer {
	t.Helper()

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	address := "localhost:" + strconv.Itoa(freePort)
	lis, err := net.Listen("tcp", address)
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	mockServer := &mockNodeServiceServer{
		t:          t,
		identifier: identifier,
		address:    lis.Addr().String(),
	}
	pb.RegisterNodeServiceServer(grpcServer, mockServer)

	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	return mockServer
}

func TestDegradedModeDuringScaling(t *testing.T) {
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

	totalHashRanges := uint32(3)

	// Create a variable to hold degraded state that can be updated during the test
	degradedNodes := make([]bool, 2)

	// Create two nodes with DegradedNodes function
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          0,
		ClusterSize:     2,
		TotalHashRanges: totalHashRanges,
		DegradedNodes: func() []bool {
			return degradedNodes
		},
	}, node0Conf)

	node1Conf := newConf()
	node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          1,
		ClusterSize:     2,
		TotalHashRanges: totalHashRanges,
		Addresses:       []string{node0Address},
		DegradedNodes: func() []bool {
			return degradedNodes
		},
	}, node1Conf)

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address, node1Address)

	// Test Put some initial data
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	// Test Get to verify data exists
	body := s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Mark node 1 as degraded
	degradedNodes[1] = true

	// Verify that node 1 rejects Get requests
	resp, err := node1.Get(ctx, &pb.GetRequest{Keys: []string{"key1"}})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_SCALING, resp.ErrorCode)
	require.Len(t, resp.NodesAddresses, 1, "Only non-degraded node should be in NodesAddresses")
	require.Equal(t, node0Address, resp.NodesAddresses[0])

	// Verify that node 1 rejects Put requests
	putResp, err := node1.Put(ctx, &pb.PutRequest{Keys: []string{"key5"}, TtlSeconds: uint64(5 * 60)})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_SCALING, putResp.ErrorCode)
	require.Len(t, putResp.NodesAddresses, 1, "Only non-degraded node should be in NodesAddresses")
	require.Equal(t, node0Address, putResp.NodesAddresses[0])

	// Verify that GetNodeInfo returns only non-degraded addresses
	nodeInfo, err := node0.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{NodeId: 0})
	require.NoError(t, err)
	require.Len(t, nodeInfo.NodesAddresses, 1, "Only non-degraded node should be in NodesAddresses")
	require.Equal(t, node0Address, nodeInfo.NodesAddresses[0])

	// Mark node 1 as non-degraded again
	degradedNodes[1] = false

	// Verify that node 1 now accepts requests (should not return SCALING error)
	resp, err = node1.Get(ctx, &pb.GetRequest{Keys: []string{"key1"}})
	require.NoError(t, err)
	require.NotEqual(t, pb.ErrorCode_SCALING, resp.ErrorCode, "Node 1 should not be in degraded mode")
	require.Len(t, resp.NodesAddresses, 2, "All non-degraded nodes should be in NodesAddresses")

	cancel()
	node0.Close()
	node1.Close()
}

func TestScaleUpInDegradedMode(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	newConf := func() *config.Config {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		return conf
	}

	minioContainer, err := miniokit.Setup(pool, t)
	require.NoError(t, err)

	cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	totalHashRanges := uint32(3)

	// Create a variable to hold degraded state that can be updated during the test
	degradedNodes := make([]bool, 1)

	// Step 1: Create a cluster with 1 node
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          0,
		ClusterSize:     1,
		TotalHashRanges: totalHashRanges,
		DegradedNodes:   func() []bool { return degradedNodes },
	}, node0Conf)

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, node0Address)

	// Step 2: Add keys via Put and verify them via Get
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3"}, TTL: testTTL,
	}, true)

	body := s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Step 3: Create a second node
	node1Conf := newConf()
	node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          1,
		ClusterSize:     2,
		TotalHashRanges: totalHashRanges,
		Addresses:       []string{node0Address},
		DegradedNodes:   func() []bool { return degradedNodes },
	}, node1Conf)

	// Step 4: Update degradedNodes - mark node 1 as degraded
	degradedNodes = append(degradedNodes, true)

	// Verify that node 1 is in degraded mode
	resp, err := node1.Get(ctx, &pb.GetRequest{Keys: []string{"key1"}})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_SCALING, resp.ErrorCode)

	// Step 5: Use /autoScale to scale the cluster while node1 is degraded
	_ = s.Do("/autoScale", AutoScaleRequest{
		OldNodesAddresses: []string{node0Address},
		NewNodesAddresses: []string{node0Address, node1Address},
	}, true)

	// Verify scale up worked - check node info
	// Note: While node1 is degraded, NodesAddresses will only include non-degraded nodes
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1,
		"Only non-degraded node should be in NodesAddresses while node1 is degraded",
	)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])
	require.ElementsMatch(t, []uint32{0, 1}, infoResponse.HashRanges)

	body = s.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1,
		"Only non-degraded node should be in NodesAddresses while node1 is degraded",
	)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])
	require.ElementsMatch(t, []uint32{2}, infoResponse.HashRanges)

	// Step 6: mark node 1 as non-degraded
	degradedNodes[1] = false

	// Verify that node 1 now accepts requests
	resp, err = node1.Get(ctx, &pb.GetRequest{Keys: []string{"key1", "key2", "key3", "key4"}})
	require.NoError(t, err)
	require.NotEqual(t, pb.ErrorCode_SCALING, resp.ErrorCode, "Node 1 should not be in degraded mode")
	t.Logf("resp: %v", resp.Exists) // TODO why is this empty?

	// Verify that now both nodes appear in NodesAddresses
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2, "Both nodes should be in NodesAddresses after node1 is no longer degraded")
	require.Contains(t, infoResponse.NodesAddresses, node0Address)
	require.Contains(t, infoResponse.NodesAddresses, node1Address)

	// Step 7: Verify that the cluster is scaled and Get and Put are now served by both nodes
	// Verify data is accessible via scaler
	body = s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	// Test Put with new keys now that both nodes are operational
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key5", "key6", "key7"}, TTL: testTTL,
	}, true)

	// Verify all keys are accessible
	body = s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key5", "key6", "key7"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key5":true,"key6":true,"key7":true}`, body)

	cancel()
	node0.Close()
	node1.Close()
}
