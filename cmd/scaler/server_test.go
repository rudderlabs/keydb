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
	"sync"
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
	nodeAddressesConfKey    = "nodeAddresses"
)

func TestScaleUpAndDownManual(t *testing.T) {
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
	totalHashRanges := int64(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx0, t, cloudStorage, node.Config{
		NodeID:           0,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf))

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{}, withAddress(node0Address))

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
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf, node1Conf))

	// Test scaling procedure
	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address, node1Address},
	}, true)
	_ = s.Do("/loadSnapshots", LoadSnapshotsRequest{
		NodeID:     1,
		HashRanges: hash.New(2, totalHashRanges).GetNodeHashRangesList(1),
	}, true)
	// Trigger hasher reinitialization based on updated addresses
	node0.DegradedNodesChanged()
	node1.DegradedNodesChanged()

	// Test node info 0
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []int64{0, 1}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, int64(0))

	// Test node info 1
	body = s.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []int64{2}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, int64(0))

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
	// Update addresses on node0 (simulating DevOps config change for scale down)
	node0Conf.Set(nodeAddressesConfKey, node0Address)
	// Trigger hasher reinitialization based on updated addresses
	node0.DegradedNodesChanged()

	// Get node info again
	body = s.Do("/info", InfoRequest{
		NodeID: 0,
	})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.ElementsMatch(t, []int64{0, 1, 2}, infoResponse.HashRanges)
	require.Greater(t, infoResponse.LastSnapshotTimestamp, int64(0))

	// Close node1 before doing a GET to make sure node1 won't pick that request
	cancel1()
	node1.Close()

	// Test Get
	body = s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4"},
	})
	require.JSONEq(t, `{"key1":true,"key2":true,"key3":true,"key4":false}`, body)

	s.Close()
	cancel0()
	node0.Close()
}

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
	totalHashRanges := int64(3)
	node0Conf := newConf()
	degradedConfig := &degradedNodesConfig{}
	node0, node0Address := getService(ctx0, t, cloudStorage, node.Config{
		NodeID:           0,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf), withDegradedConfig(degradedConfig))

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, withAddress(node0Address))

	// Test Put some initial data
	_ = s.Do("/put", PutRequest{
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
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	node1, node1Address := getService(ctx1, t, cloudStorage, node.Config{
		NodeID:           1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf, node1Conf), withDegradedConfig(degradedConfig))

	// Test Scale Up using hashRangeMovements with degraded mode
	// Step 1: Mark node1 as degraded before scaling
	degradedConfig.set(false, true)

	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address, node1Address},
	}, true)

	// Step 3: Upload snapshots from source nodes
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  1,
		NewClusterSize:  2,
		TotalHashRanges: totalHashRanges,
		Upload:          true,
	})

	// Step 4: Download snapshots to destination nodes
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  1,
		NewClusterSize:  2,
		TotalHashRanges: totalHashRanges,
		Download:        true,
	})

	// Step 5: Mark node1 as non-degraded and trigger hasher reinitialization
	degradedConfig.set(false, false)

	// Verify scale up worked - check node info
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []int64{0, 1}, infoResponse.HashRanges)

	body = s.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2)
	require.ElementsMatch(t, []int64{2}, infoResponse.HashRanges)

	keydbth.RequireExpectedFiles(ctx0, t, minioContainer, defaultBackupFolderName,
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

	// Test Scale Down using hashRangeMovements with degraded mode
	// Step 1: Upload snapshots from nodes being removed
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  2,
		NewClusterSize:  1,
		TotalHashRanges: totalHashRanges,
		Upload:          true,
	})

	// Step 2: Download snapshots to remaining nodes
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  2,
		NewClusterSize:  1,
		TotalHashRanges: totalHashRanges,
		Download:        true,
	})

	// Step 3: Update cluster data
	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address},
	}, true)

	// Update addresses on node0 (simulating DevOps config change for scale down)
	cancel1()
	node1.Close()
	node0Conf.Set(nodeAddressesConfKey, node0Address)
	// Trigger hasher reinitialization based on updated addresses
	node0.DegradedNodesChanged()

	keydbth.RequireExpectedFiles(ctx0, t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
		// cache.Put() used to mark the snapshot as loaded incremented badger's version counter, so the next
		// snapshot file won't be hr_2_s_1_2.snapshot but hr_2_s_1_3.snapshot
		regexp.MustCompile("^.+/hr_2_s_1_3.snapshot$"),
	)

	// Verify scale down worked - check node info
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 1)
	require.ElementsMatch(t, []int64{0, 1, 2}, infoResponse.HashRanges)

	// Verify all data is still accessible after scale down
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "AAA", "BBB", "CCC", "DDD"},
	})
	require.JSONEq(t,
		`{"keyA":true,"keyB":true,"keyC":true,"keyD":false,"AAA":true,"BBB":true,"CCC":true,"DDD":true}`,
		body,
	)

	s.Close()
	cancel0()
	node0.Close()
}

func TestScaleTransientNetworkFailure(t *testing.T) {
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

	// Create degraded config to manage degraded state across nodes
	degradedConfig := &degradedNodesConfig{}

	// Start proxy for simulating transient failure with node0
	proxyPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	proxy := &tcpproxy.Proxy{
		LocalAddr: "localhost:" + strconv.Itoa(proxyPort),
	}

	// Create the node service
	totalHashRanges := int64(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx0, t, cloudStorage, node.Config{
		NodeID:           0,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf), withProxy(proxy), withDegradedConfig(degradedConfig))
	go proxy.Start(t) // Starting the proxy after we get the service to populate RemoteAddr

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		InitialInterval: time.Second,
		Multiplier:      1,
		MaxInterval:     time.Second,
		MaxElapsedTime:  3 * time.Second,
	}, withAddress(node0Address))

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
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	node1, node1Address := getService(ctx1, t, cloudStorage, node.Config{
		NodeID:           1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf, node1Conf), withDegradedConfig(degradedConfig))

	// Test Scale Up using hashRangeMovements with degraded mode
	// Step 1: Mark node1 as degraded before scaling
	degradedConfig.set(false, true)

	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address, node1Address},
	}, true)

	// Step 2: Simulate transient failure during upload
	t.Log("Stopping proxy to simulate transient failure...")
	proxy.Stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.Log("Firing hashRangeMovements upload request")
		_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: totalHashRanges,
			Upload:          true,
		})
	}()
	go func() {
		time.Sleep(1 * time.Second)
		t.Log("Starting proxy again")
		proxy.Start(t)
	}()
	<-done

	// Step 3: Download snapshots to destination nodes
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  1,
		NewClusterSize:  2,
		TotalHashRanges: totalHashRanges,
		Download:        true,
	})

	// Step 4: Mark node1 as non-degraded and trigger hasher reinitialization
	degradedConfig.set(false, false)

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

	keydbth.RequireExpectedFiles(ctx0, t, minioContainer, defaultBackupFolderName,
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

	// Test Scale Down using hashRangeMovements with degraded mode
	// Step 1: Upload snapshots from nodes being removed
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  2,
		NewClusterSize:  1,
		TotalHashRanges: totalHashRanges,
		Upload:          true,
	})

	// Step 2: Download snapshots to remaining nodes
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  2,
		NewClusterSize:  1,
		TotalHashRanges: totalHashRanges,
		Download:        true,
	})

	// Step 3: Update cluster data
	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address},
	}, true)

	// Update addresses on node0 to reflect the scale down (simulating DevOps config change)
	cancel1()
	node1.Close()
	node0Conf.Set(nodeAddressesConfKey, node0Address)
	// Trigger hasher reinitialization based on updated addresses
	node0.DegradedNodesChanged()

	keydbth.RequireExpectedFiles(ctx0, t, minioContainer, defaultBackupFolderName,
		regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
		// cache.Put() used to mark the snapshot as loaded incremented badger's version counter, so the next
		// snapshot file won't be hr_2_s_1_2.snapshot but hr_2_s_1_3.snapshot
		regexp.MustCompile("^.+/hr_2_s_1_3.snapshot$"),
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

	s.Close()
	cancel0()
	node0.Close()
	proxy.Stop()
}

func TestScaleTransientError(t *testing.T) {
	// Create the node service
	totalHashRanges := int64(3)
	node0 := startMockNodeService(t, "node0")
	node1 := startMockNodeService(t, "node1")

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		InitialInterval: 10 * time.Millisecond,
		Multiplier:      1,
		MaxInterval:     10 * time.Millisecond,
		MaxElapsedTime:  time.Second,
	}, withAddress(node0.address), withAddress(node1.address))

	t.Log("Scaling up from 1 node to 2 nodes...")
	node0.createSnapshotsReturnError.Store(true)
	done := make(chan struct{})
	go func() {
		close(done)
		_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: totalHashRanges,
			Upload:          true,
		})
	}()

	waitForRetries := uint64(10)

	t.Logf("Waiting for at least %d retries to be done on CreateSnapshots", waitForRetries)
	require.Eventuallyf(t, func() bool {
		return node0.createSnapshotsCalls.Load() >= waitForRetries // wait for at least 10 retries
	}, 10*time.Second, time.Millisecond, "Calls %d", node0.createSnapshotsCalls.Load())
	node0.createSnapshotsReturnError.Store(false)

	<-done

	t.Log("Loading snapshots from 1 node to 2 nodes...")
	node1.loadSnapshotsReturnError.Store(true)
	done = make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  1,
			NewClusterSize:  2,
			TotalHashRanges: totalHashRanges,
			Download:        true,
		})
	}()

	t.Logf("Waiting for at least %d retries to be done on LoadSnapshots", waitForRetries)
	require.Eventuallyf(t, func() bool {
		return node1.loadSnapshotsCalls.Load() >= waitForRetries // wait for at least 10 retries
	}, 10*time.Second, time.Millisecond, "Calls %d", node1.loadSnapshotsCalls.Load())
	node1.loadSnapshotsReturnError.Store(false)

	<-done

	t.Log("Scaling down from 2 nodes to 1 node...")
	done = make(chan struct{})
	go func() {
		defer close(done)
		// Move snapshots
		_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  2,
			NewClusterSize:  1,
			TotalHashRanges: totalHashRanges,
			Upload:          true,
			Download:        true,
		})
	}()
	<-done
}

func TestHandleScaleErrors(t *testing.T) {
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
	totalHashRanges := int64(3)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf))
	t.Cleanup(node0.Close)

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{}, withAddress(node0Address))

	// Test error cases for /hashRangeMovements endpoint
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
				TotalHashRanges: totalHashRanges,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "zero new cluster size",
			request: HashRangeMovementsRequest{
				OldClusterSize:  1,
				NewClusterSize:  0,
				TotalHashRanges: totalHashRanges,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "zero total hash ranges",
			request: HashRangeMovementsRequest{
				OldClusterSize:  1,
				NewClusterSize:  2,
				TotalHashRanges: 0,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "same cluster size with no upload or download",
			request: HashRangeMovementsRequest{
				OldClusterSize:  1,
				NewClusterSize:  1,
				TotalHashRanges: totalHashRanges,
			},
			expectedStatus: http.StatusOK,
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

	cancel()
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
	totalHashRanges := int64(8)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf))
	t.Cleanup(node0.Close)

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{}, withAddress(node0Address))

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
			require.Less(t, movement.HashRange, int64(8), "hash range should be less than total")
			require.Less(t, movement.From, int64(2), "from node should be less than old cluster size")
			require.Less(t, movement.To, int64(3), "to node should be less than new cluster size")
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
			require.Less(t, movement.HashRange, int64(8), "hash range should be less than total")
			require.Less(t, movement.From, int64(3), "from node should be less than old cluster size")
			require.Less(t, movement.To, int64(2), "to node should be less than new cluster size")
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
			require.Less(t, movement.HashRange, int64(8), "hash range should be less than total")
			require.Less(t, movement.From, int64(1), "from node should be less than old cluster size")
			require.Less(t, movement.To, int64(2), "to node should be less than new cluster size")
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
			require.Less(t, movement.HashRange, int64(8), "hash range should be less than total")
			require.Less(t, movement.From, int64(1), "from node should be less than old cluster size")
			require.Less(t, movement.To, int64(2), "to node should be less than new cluster size")
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
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, withConfigs(node0Conf, newNodeConf))

		s := startScalerHTTPServer(
			t, totalHashRanges, scaler.RetryPolicy{}, withAddress(node0Address), withAddress(newNodeAddress),
		)

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

	totalHashRanges := int64(3)

	// Create degraded config to manage degraded state across nodes
	degradedConfig := &degradedNodesConfig{}

	// Create two nodes with DegradedNodes function via degradedConfig
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          0,
		TotalHashRanges: totalHashRanges,
	}, withConfigs(node0Conf), withDegradedConfig(degradedConfig))
	t.Cleanup(node0.Close)

	node1Conf := newConf()
	node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          1,
		TotalHashRanges: totalHashRanges,
	}, withConfigs(node0Conf, node1Conf), withDegradedConfig(degradedConfig))
	t.Cleanup(node1.Close)

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, withAddress(node0Address), withAddress(node1Address))

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
	degradedConfig.set(false, true)

	// Verify that node 1 rejects Get requests
	resp, err := node1.Get(ctx, &pb.GetRequest{Keys: []string{"key1"}})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_SCALING, resp.ErrorCode)
	require.Len(t, resp.NodesAddresses, 1, "Only non-degraded node should be in NodesAddresses")
	require.Equal(t, node0Address, resp.NodesAddresses[0])

	// Verify that node 1 rejects Put requests
	putResp, err := node1.Put(ctx, &pb.PutRequest{Keys: []string{"key5"}, TtlSeconds: int64(5 * 60)})
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
	degradedConfig.set(false, false)

	// Verify that node 1 now accepts requests (should not return SCALING error)
	resp, err = node1.Get(ctx, &pb.GetRequest{Keys: []string{"key1"}})
	require.NoError(t, err)
	require.NotEqual(t, pb.ErrorCode_SCALING, resp.ErrorCode, "Node 1 should not be in degraded mode")
	require.Len(t, resp.NodesAddresses, 2, "All non-degraded nodes should be in NodesAddresses")
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

	totalHashRanges := int64(3)

	// Create degraded config to manage degraded state across nodes
	degradedConfig := &degradedNodesConfig{}

	// Step 1: Create a cluster with 1 node
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          0,
		TotalHashRanges: totalHashRanges,
	}, withConfigs(node0Conf), withDegradedConfig(degradedConfig))
	t.Cleanup(node0.Close)

	// Start the Scaler HTTP Server
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, withAddress(node0Address))

	// Step 2: Add keys via Put and verify them via Get
	// Add enough keys to ensure distribution across hash ranges
	// Based on hash distribution with clusterSize=2, totalHashRanges=3:
	// key1, key2, key3 → node0; key4, key5, key6, key7, key8 → will distribute across nodes
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"}, TTL: testTTL,
	}, true)

	body := s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"},
	})
	require.JSONEq(t,
		`{"key1":true,"key2":true,"key3":true,"key4":true,"key5":true,"key6":true,"key7":true,"key8":true}`,
		body,
	)

	// Step 3: Create a second node (starts as degraded for this test)
	node1Conf := newConf()
	node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:          1,
		TotalHashRanges: totalHashRanges,
	}, withConfigs(node0Conf, node1Conf), withDegradedConfig(degradedConfig))
	t.Cleanup(node1.Close)

	// Step 4: Mark node 1 as degraded
	degradedConfig.set(false, true)

	// Verify that node 1 is in degraded mode
	resp, err := node1.Get(ctx, &pb.GetRequest{Keys: []string{"key1"}})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_SCALING, resp.ErrorCode)

	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address, node1Address},
	}, true)

	// Step 5: Use /hashRangeMovements to scale the cluster while node1 is degraded
	// Move snapshots from source nodes
	_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
		OldClusterSize:  1,
		NewClusterSize:  2,
		TotalHashRanges: totalHashRanges,
		Upload:          true,
		Download:        true,
	})

	// After Scale, trigger hasher reinitialization based on current degraded nodes
	node0.DegradedNodesChanged()
	// Note: node1 is degraded, so DegradedNodesChanged will skip reinitialization for it

	// Verify scale up worked - check node info
	// Note: While node1 is degraded, NodesAddresses will only include non-degraded nodes
	// and ClusterSize will be 1 (only non-degraded nodes count)
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse := pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize,
		"ClusterSize should be 1 because node1 is degraded",
	)
	require.Len(t, infoResponse.NodesAddresses, 1,
		"Only non-degraded node should be in NodesAddresses while node1 is degraded",
	)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])
	require.ElementsMatch(t, []int64{0, 1, 2}, infoResponse.HashRanges,
		"node0 should handle all hash ranges when node1 is degraded",
	)

	body = s.Do("/info", InfoRequest{NodeID: 1})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 1, infoResponse.ClusterSize,
		"ClusterSize should be 1 because node1 is degraded",
	)
	require.Len(t, infoResponse.NodesAddresses, 1,
		"Only non-degraded node should be in NodesAddresses while node1 is degraded",
	)
	require.Equal(t, node0Address, infoResponse.NodesAddresses[0])
	require.ElementsMatch(t, []int64{2}, infoResponse.HashRanges)

	// Step 6: mark node 1 as non-degraded
	degradedConfig.set(false, false)

	// Verify that node 1 now accepts requests and has loaded snapshots correctly
	// Node1 owns hash range 2, which contains key4 and other keys
	// Let's determine which keys belong to node1's hash range
	h := hash.New(2, totalHashRanges)
	allKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"}
	var node1Keys []string
	for _, key := range allKeys {
		if h.GetNodeNumber(key) == 1 {
			node1Keys = append(node1Keys, key)
		}
	}
	require.Greater(t, len(node1Keys), 0, "Node 1 should own at least one key")
	t.Logf("Node 1 owns %d keys: %v", len(node1Keys), node1Keys)

	// Query node1 directly for keys that belong to its hash range
	resp, err = node1.Get(ctx, &pb.GetRequest{Keys: node1Keys})
	require.NoError(t, err)
	require.NotEqual(t, pb.ErrorCode_SCALING, resp.ErrorCode, "Node 1 should not be in degraded mode")
	require.Len(t, resp.Exists, len(node1Keys), "Node 1 should return results for all its keys")
	for _, exists := range resp.Exists {
		require.True(t, exists, "All keys belonging to node1 should exist after loading snapshots")
	}

	// Verify that now both nodes appear in NodesAddresses
	body = s.Do("/info", InfoRequest{NodeID: 0})
	infoResponse = pb.GetNodeInfoResponse{}
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
	require.EqualValues(t, 2, infoResponse.ClusterSize)
	require.Len(t, infoResponse.NodesAddresses, 2,
		"Both nodes should be in NodesAddresses after node1 is no longer degraded",
	)
	require.Contains(t, infoResponse.NodesAddresses, node0Address)
	require.Contains(t, infoResponse.NodesAddresses, node1Address)

	// Step 7: Verify that the cluster is scaled and Get and Put are now served by both nodes
	// Verify all existing data is accessible via scaler
	body = s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"},
	})
	require.JSONEq(t,
		`{"key1":true,"key2":true,"key3":true,"key4":true,"key5":true,"key6":true,"key7":true,"key8":true}`,
		body,
	)

	// Test Put with new keys now that both nodes are operational
	_ = s.Do("/put", PutRequest{
		Keys: []string{"key9", "key10", "key11"}, TTL: testTTL,
	}, true)

	// Verify all keys including new ones are accessible
	body = s.Do("/get", GetRequest{
		Keys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10", "key11"},
	})
	require.JSONEq(t,
		`{
			"key1":true,"key2":true,"key3":true,"key4":true,"key5":true,"key6":true,
			"key7":true,"key8":true,"key9":true,"key10":true,"key11":true
		}`,
		body,
	)
}

func TestScaleStreaming(t *testing.T) {
	// Define scale operation types
	type scaleOperation func(
		s *opClient,
		oldClusterSize, newClusterSize int64,
		totalHashRanges int64,
		oldAddresses, newAddresses []string,
	)

	// Streaming mode: direct node-to-node transfer
	streamingOp := func(
		s *opClient,
		oldClusterSize, newClusterSize int64,
		totalHashRanges int64,
		_, newAddresses []string,
	) {
		// For scale up: update cluster data first so scaler knows about new nodes
		// For scale down: update cluster data last (after data is moved)
		if newClusterSize > oldClusterSize {
			_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
				Addresses: newAddresses,
			}, true)
		}
		_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  oldClusterSize,
			NewClusterSize:  newClusterSize,
			TotalHashRanges: totalHashRanges,
			Streaming:       true,
		})
		if newClusterSize < oldClusterSize {
			_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
				Addresses: newAddresses,
			}, true)
		}
	}

	// Upload-download mode: via cloud storage
	uploadDownloadOp := func(
		s *opClient,
		oldClusterSize, newClusterSize int64,
		totalHashRanges int64,
		_, newAddresses []string,
	) {
		// For scale up: update cluster data first so scaler knows about new nodes
		// For scale down: update cluster data last (after data is moved)
		if newClusterSize > oldClusterSize {
			_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
				Addresses: newAddresses,
			}, true)
		}
		_ = s.Do("/hashRangeMovements", HashRangeMovementsRequest{
			OldClusterSize:  oldClusterSize,
			NewClusterSize:  newClusterSize,
			TotalHashRanges: totalHashRanges,
			Upload:          true,
			Download:        true,
		})
		if newClusterSize < oldClusterSize {
			_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
				Addresses: newAddresses,
			}, true)
		}
	}

	testCases := []struct {
		name    string
		scaleOp scaleOperation
	}{
		{"streaming", streamingOp},
		{"upload-download", uploadDownloadOp},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			// Create degraded config to manage degraded state across nodes
			degradedConfig := &degradedNodesConfig{}

			// Create initial cluster with 1 node
			totalHashRanges := int64(6)
			node0Conf := newConf()
			node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
				NodeID:           0,
				TotalHashRanges:  totalHashRanges,
				SnapshotInterval: 60 * time.Second,
			}, withConfigs(node0Conf), withDegradedConfig(degradedConfig))
			t.Cleanup(node0.Close)

			// Start the Scaler HTTP Server
			s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
				Disabled: true,
			}, withAddress(node0Address))

			// Put initial data
			_ = s.Do("/put", PutRequest{
				Keys: []string{"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH"},
				TTL:  testTTL,
			}, true)

			// Verify data exists
			body := s.Do("/get", GetRequest{
				Keys: []string{"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH", "keyZ"},
			})
			require.JSONEq(t,
				`{"keyA":true,"keyB":true,"keyC":true,"keyD":true,
				"keyE":true,"keyF":true,"keyG":true,"keyH":true,"keyZ":false}`,
				body,
			)

			// Verify node 0 info before scaling
			body = s.Do("/info", InfoRequest{NodeID: 0})
			infoResponse := pb.GetNodeInfoResponse{}
			require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
			require.EqualValues(t, 1, infoResponse.ClusterSize)
			require.ElementsMatch(t, []int64{0, 1, 2, 3, 4, 5}, infoResponse.HashRanges)

			// Create node 1 and node 2 for scale up to 3 nodes
			node1Conf := newConf()
			node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
				NodeID:           1,
				TotalHashRanges:  totalHashRanges,
				SnapshotInterval: 60 * time.Second,
			}, withConfigs(node0Conf, node1Conf), withDegradedConfig(degradedConfig))
			t.Cleanup(node1.Close)

			node2Conf := newConf()
			node2, node2Address := getService(ctx, t, cloudStorage, node.Config{
				NodeID:           2,
				TotalHashRanges:  totalHashRanges,
				SnapshotInterval: 60 * time.Second,
			}, withConfigs(node0Conf, node1Conf, node2Conf), withDegradedConfig(degradedConfig))
			t.Cleanup(node2.Close)

			// Mark new nodes as degraded before scaling
			degradedConfig.set(false, true, true)

			// Scale up from 1 to 3 nodes
			t.Logf("Scaling up from 1 node to 3 nodes using %s...", tc.name)
			tc.scaleOp(s, 1, 3, totalHashRanges,
				[]string{node0Address},
				[]string{node0Address, node1Address, node2Address},
			)

			// Mark nodes as non-degraded and trigger hasher reinitialization
			degradedConfig.set(false, false, false)

			// Calculate expected hash ranges for 3 nodes
			hasher3Nodes := hash.New(3, totalHashRanges)
			expectedNode0HRs := hasher3Nodes.GetNodeHashRangesList(0)
			require.Greater(t, len(expectedNode0HRs), 0)
			expectedNode1HRs := hasher3Nodes.GetNodeHashRangesList(1)
			require.Greater(t, len(expectedNode1HRs), 0)
			expectedNode2HRs := hasher3Nodes.GetNodeHashRangesList(2)
			require.Greater(t, len(expectedNode2HRs), 0)

			// Verify scale up worked - check node info for all 3 nodes
			body = s.Do("/info", InfoRequest{NodeID: 0})
			infoResponse = pb.GetNodeInfoResponse{}
			require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
			require.EqualValues(t, 3, infoResponse.ClusterSize)
			require.Len(t, infoResponse.NodesAddresses, 3)
			require.ElementsMatch(t, expectedNode0HRs, infoResponse.HashRanges)

			body = s.Do("/info", InfoRequest{NodeID: 1})
			infoResponse = pb.GetNodeInfoResponse{}
			require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
			require.EqualValues(t, 3, infoResponse.ClusterSize)
			require.Len(t, infoResponse.NodesAddresses, 3)
			require.ElementsMatch(t, expectedNode1HRs, infoResponse.HashRanges)

			body = s.Do("/info", InfoRequest{NodeID: 2})
			infoResponse = pb.GetNodeInfoResponse{}
			require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
			require.EqualValues(t, 3, infoResponse.ClusterSize)
			require.Len(t, infoResponse.NodesAddresses, 3)
			require.ElementsMatch(t, expectedNode2HRs, infoResponse.HashRanges)

			// Verify all data is still accessible after scale up
			body = s.Do("/get", GetRequest{
				Keys: []string{"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH", "keyZ"},
			})
			require.JSONEq(t,
				`{"keyA":true,"keyB":true,"keyC":true,"keyD":true,"keyE":true,
				"keyF":true,"keyG":true,"keyH":true,"keyZ":false}`,
				body,
			)

			// Add more data after scale up
			_ = s.Do("/put", PutRequest{
				Keys: []string{"key1", "key2", "key3", "key4"},
				TTL:  testTTL,
			}, true)

			// Verify all keys including new ones
			body = s.Do("/get", GetRequest{
				Keys: []string{
					"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH",
					"key1", "key2", "key3", "key4",
				},
			})
			require.JSONEq(t,
				`{"keyA":true,"keyB":true,"keyC":true,"keyD":true,"keyE":true,"keyF":true,
				"keyG":true,"keyH":true,"key1":true,"key2":true,"key3":true,"key4":true}`,
				body,
			)

			// Scale down from 3 to 2 nodes
			t.Logf("Scaling down from 3 nodes to 2 nodes using %s...", tc.name)
			tc.scaleOp(s, 3, 2, totalHashRanges,
				[]string{node0Address, node1Address, node2Address},
				[]string{node0Address, node1Address},
			)

			// Update addresses on remaining nodes (simulating DevOps config change for scale down)
			node0Conf.Set(nodeAddressesConfKey, node0Address+","+node1Address)
			node1Conf.Set(nodeAddressesConfKey, node0Address+","+node1Address)

			// Trigger hasher reinitialization
			node0.DegradedNodesChanged()
			node1.DegradedNodesChanged()

			// Calculate expected hash ranges for 2 nodes
			hasher2Nodes := hash.New(2, totalHashRanges)
			expectedNode0HRs = hasher2Nodes.GetNodeHashRangesList(0)
			expectedNode1HRs = hasher2Nodes.GetNodeHashRangesList(1)

			// Verify scale down worked - check node info
			body = s.Do("/info", InfoRequest{NodeID: 0})
			infoResponse = pb.GetNodeInfoResponse{}
			require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
			require.EqualValues(t, 2, infoResponse.ClusterSize)
			require.Len(t, infoResponse.NodesAddresses, 2)
			require.ElementsMatch(t, expectedNode0HRs, infoResponse.HashRanges)

			body = s.Do("/info", InfoRequest{NodeID: 1})
			infoResponse = pb.GetNodeInfoResponse{}
			require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
			require.EqualValues(t, 2, infoResponse.ClusterSize)
			require.Len(t, infoResponse.NodesAddresses, 2)
			require.ElementsMatch(t, expectedNode1HRs, infoResponse.HashRanges)

			// Verify all data is still accessible after scale down
			body = s.Do("/get", GetRequest{
				Keys: []string{
					"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH",
					"key1", "key2", "key3", "key4",
					"keyZ", // should not exist
				},
			})
			require.JSONEq(t,
				`{"keyA":true,"keyB":true,"keyC":true,"keyD":true,"keyE":true,"keyF":true,"keyG":true,
				"keyH":true,"key1":true,"key2":true,"key3":true,"key4":true,"keyZ":false}`,
				body,
			)

			s.Close()
		})
	}
}

func TestBackup(t *testing.T) {
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

	// Create degraded config to manage degraded state across nodes
	degradedConfig := &degradedNodesConfig{}

	// Create 3 nodes
	totalHashRanges := int64(6)
	node0Conf := newConf()
	node0, node0Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           0,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf), withDegradedConfig(degradedConfig))
	t.Cleanup(node0.Close)

	node1Conf := newConf()
	node1, node1Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf, node1Conf), withDegradedConfig(degradedConfig))
	t.Cleanup(node1.Close)

	node2Conf := newConf()
	node2, node2Address := getService(ctx, t, cloudStorage, node.Config{
		NodeID:           2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	}, withConfigs(node0Conf, node1Conf, node2Conf), withDegradedConfig(degradedConfig))
	t.Cleanup(node2.Close)

	// Start the Scaler HTTP Server with all 3 nodes
	s := startScalerHTTPServer(t, totalHashRanges, scaler.RetryPolicy{
		Disabled: true,
	}, withAddress(node0Address), withAddress(node1Address), withAddress(node2Address))
	t.Cleanup(s.Close)

	// Update cluster data to include all nodes
	_ = s.Do("/updateClusterData", UpdateClusterDataRequest{
		Addresses: []string{node0Address, node1Address, node2Address},
	}, true)

	// Put initial data
	_ = s.Do("/put", PutRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH"},
		TTL:  testTTL,
	}, true)

	// Verify data exists before backup
	body := s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH", "keyZ"},
	})
	require.JSONEq(t,
		`{"keyA":true,"keyB":true,"keyC":true,"keyD":true,
		"keyE":true,"keyF":true,"keyG":true,"keyH":true,"keyZ":false}`,
		body,
	)

	// Call backup with upload and download
	body = s.Do("/backup", BackupRequest{
		Upload:   true,
		Download: true,
	})
	var backupResp BackupResponse
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &backupResp))
	require.True(t, backupResp.Success)
	require.Equal(t, 3, backupResp.Total)

	// Verify snapshots were created in cloud storage (only hash ranges with data get snapshots)
	files, err := minioContainer.Contents(context.Background(), defaultBackupFolderName+"/hr_")
	require.NoError(t, err)
	require.Greater(t, len(files), 0, "expected at least one snapshot file")
	// Verify all files match the expected pattern
	snapshotPattern := regexp.MustCompile("^.+/hr_[0-5]_s_\\d+_\\d+\\.snapshot$")
	for _, file := range files {
		require.True(t, snapshotPattern.MatchString(file.Key), "unexpected file: %s", file.Key)
	}

	// Verify data is still accessible after backup
	body = s.Do("/get", GetRequest{
		Keys: []string{"keyA", "keyB", "keyC", "keyD", "keyE", "keyF", "keyG", "keyH", "keyZ"},
	})
	require.JSONEq(t,
		`{"keyA":true,"keyB":true,"keyC":true,"keyD":true,
		"keyE":true,"keyF":true,"keyG":true,"keyH":true,"keyZ":false}`,
		body,
	)

	// Verify node info for all 3 nodes
	for nodeID := int64(0); nodeID < 3; nodeID++ {
		body = s.Do("/info", InfoRequest{NodeID: nodeID})
		infoResponse := pb.GetNodeInfoResponse{}
		require.NoError(t, jsonrs.Unmarshal([]byte(body), &infoResponse))
		require.EqualValues(t, 3, infoResponse.ClusterSize)
		require.Len(t, infoResponse.NodesAddresses, 3)
		require.Greater(t, infoResponse.LastSnapshotTimestamp, int64(0))
	}
}

// Helper types and functions

type serviceConfig struct {
	proxy          *tcpproxy.Proxy
	configs        []*config.Config
	degradedConfig *degradedNodesConfig
}

type serviceOption func(*serviceConfig)

func withProxy(proxy *tcpproxy.Proxy) serviceOption {
	return func(c *serviceConfig) { c.proxy = proxy }
}

func withConfigs(configs ...*config.Config) serviceOption {
	return func(c *serviceConfig) { c.configs = configs }
}

func withDegradedConfig(dc *degradedNodesConfig) serviceOption {
	return func(c *serviceConfig) { c.degradedConfig = dc }
}

// degradedNodesConfig manages degraded node state across a cluster
type degradedNodesConfig struct {
	nodes           []*node.Service
	degradedNodes   []bool
	degradedNodesMu sync.RWMutex
}

func (d *degradedNodesConfig) addNode(n *node.Service, degraded bool) {
	d.nodes = append(d.nodes, n)
	d.degradedNodesMu.Lock()
	d.degradedNodes = append(d.degradedNodes, degraded)
	d.degradedNodesMu.Unlock()
	for _, n := range d.nodes {
		n.DegradedNodesChanged()
	}
}

func (d *degradedNodesConfig) set(b ...bool) {
	d.degradedNodesMu.Lock()
	d.degradedNodes = b
	d.degradedNodesMu.Unlock()
	for _, n := range d.nodes {
		n.DegradedNodesChanged()
	}
}

func (d *degradedNodesConfig) load() []bool {
	d.degradedNodesMu.RLock()
	defer d.degradedNodesMu.RUnlock()
	return d.degradedNodes
}

func getService(
	ctx context.Context, t testing.TB, cs *filemanager.S3Manager, nodeConfig node.Config,
	opts ...serviceOption,
) (*node.Service, string) {
	t.Helper()

	// Apply options
	cfg := &serviceConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if len(cfg.configs) < 1 {
		t.Fatal("no config provided, use withConfigs option")
	}

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	listenAddr := "localhost:" + strconv.Itoa(freePort)

	// Determine the address clients will use
	var address string
	if cfg.proxy != nil {
		address = cfg.proxy.LocalAddr
		cfg.proxy.RemoteAddr = listenAddr
		t.Logf("Using proxy, client will connect to %s but node is %s", address, listenAddr)
	} else {
		address = listenAddr
	}

	// Simulating reloadable addresses for all configs
	nodeAddresses := cfg.configs[0].GetReloadableStringVar("", nodeAddressesConfKey)
	var addrList []string
	if rawAddresses := strings.TrimSpace(nodeAddresses.Load()); rawAddresses != "" {
		addrList = append(strings.Split(rawAddresses, ","), address)
	} else {
		addrList = []string{address}
	}
	for _, c := range cfg.configs {
		c.Set(nodeAddressesConfKey, strings.Join(addrList, ","))
	}

	// Set the Addresses function to return our modifiable slice
	nodeConfig.Addresses = func() []string {
		return strings.Split(nodeAddresses.Load(), ",")
	}
	nodeConfig.BackupFolderName = defaultBackupFolderName

	// Set DegradedNodes function if degradedConfig is provided
	if cfg.degradedConfig != nil {
		nodeConfig.DegradedNodes = cfg.degradedConfig.load
	}

	log := logger.NOP
	if testing.Verbose() {
		log = logger.NewLogger()
	}
	cfg.configs[nodeConfig.NodeID].Set("BadgerDB.Dedup.NopLogger", true)
	service, err := node.NewService(ctx, nodeConfig, cs, cfg.configs[nodeConfig.NodeID], stats.NOP, log)
	require.NoError(t, err)

	// Register node with degradedConfig if provided
	if cfg.degradedConfig != nil {
		// Determine if this node starts as degraded (default: false for existing nodes)
		degraded := false
		if int(nodeConfig.NodeID) < len(cfg.degradedConfig.load()) {
			degraded = cfg.degradedConfig.load()[nodeConfig.NodeID]
		}
		cfg.degradedConfig.addNode(service, degraded)
	}

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	lis, err := net.Listen("tcp", listenAddr)
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

type scalerHTTPServerOpts struct {
	addresses            []string
	clusterUpdateTimeout time.Duration
}

type scalerHTTPServerOpt func(*scalerHTTPServerOpts)

func withAddress(addr string) scalerHTTPServerOpt {
	return func(o *scalerHTTPServerOpts) { o.addresses = append(o.addresses, addr) }
}

func startScalerHTTPServer(
	t testing.TB, totalHashRanges int64, rp scaler.RetryPolicy, opts ...scalerHTTPServerOpt,
) *opClient {
	t.Helper()

	log := logger.NOP
	if testing.Verbose() {
		lf := logger.NewFactory(config.New())
		require.NoError(t, lf.SetLogLevel("", "DEBUG"))
		log = lf.NewLogger()
	}

	var o scalerHTTPServerOpts
	for _, opt := range opts {
		opt(&o)
	}
	c, err := client.NewClient(client.Config{
		Addresses:       o.addresses,
		TotalHashRanges: totalHashRanges,
	}, log)
	require.NoError(t, err)

	op, err := scaler.NewClient(scaler.Config{
		Addresses:            o.addresses,
		TotalHashRanges:      totalHashRanges,
		RetryPolicy:          rp,
		ClusterUpdateTimeout: o.clusterUpdateTimeout,
	}, log)
	require.NoError(t, err)

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	addr := fmt.Sprintf(":%d", freePort)

	ctx, cancel := context.WithCancel(context.Background())
	opServer := newHTTPServer(c, op, addr, stats.NOP, log)
	go func() {
		err := opServer.Start(ctx)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Scaler server error: %v", err)
		}
	}()

	oc := &opClient{
		t:        t,
		client:   http.DefaultClient,
		url:      fmt.Sprintf("http://localhost:%d", freePort),
		scaler:   op,
		c:        c,
		opServer: opServer,
		cancel:   cancel,
	}
	t.Cleanup(func() {
		oc.Close()
	})

	return oc
}

type opClient struct {
	t        testing.TB
	client   *http.Client
	url      string
	scaler   *scaler.Client
	c        *client.Client
	opServer *httpServer
	cancel   context.CancelFunc
	closed   sync.Once
}

func (oc *opClient) Close() {
	oc.closed.Do(func() {
		// Close gRPC clients first to terminate connections cleanly
		_ = oc.c.Close()
		_ = oc.scaler.Close()
		// Then cancel context and stop HTTP server
		oc.cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = oc.opServer.Stop(shutdownCtx)
	})
}

func (oc *opClient) Do(endpoint string, data any, success ...bool) string {
	oc.t.Helper()

	buf, err := jsonrs.Marshal(data)
	require.NoError(oc.t, err)
	req, err := http.NewRequest(http.MethodPost, oc.url+endpoint, bytes.NewBuffer(buf))
	require.NoError(oc.t, err)

	resp, err := oc.client.Do(req)
	require.NoError(oc.t, err)

	defer func() { httputil.CloseResponse(resp) }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(oc.t, err)

	if resp.StatusCode != http.StatusOK {
		oc.t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	if len(success) > 0 && success[0] {
		require.JSONEq(oc.t, `{"success":true}`, string(body))
	}

	return string(body)
}

type mockNodeServiceServer struct {
	pb.UnimplementedNodeServiceServer

	t          testing.TB
	address    string
	identifier string

	createSnapshotsCalls       atomic.Uint64
	createSnapshotsReturnError atomic.Bool

	loadSnapshotsCalls atomic.Uint64

	loadSnapshotsReturnError atomic.Bool
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
