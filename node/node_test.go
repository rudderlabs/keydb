package node

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/internal/scaler"
	keydbth "github.com/rudderlabs/keydb/internal/testhelper"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	miniokit "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
)

const (
	testTTL                 = 5 * time.Minute
	defaultBackupFolderName = "default"
)

func TestSimple(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, newConf func() *config.Config) {
		t.Parallel()

		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)

		cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(4)
		node0Conf := newConf()
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c := getClient(t, totalHashRanges, node0Address)
		op := getScaler(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		exists, err := c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		err = op.CreateSnapshots(ctx, 0, false)
		require.NoError(t, err)

		// we expect one hash range to be empty so the file won't be uploaded
		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_1.snapshot$"),
		)

		cancel()
		node0.Close()

		// Let's start again from scratch to see if the data is properly loaded from the snapshots
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0Conf = newConf()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c = getClient(t, totalHashRanges, node0Address)
		require.NoError(t, op.UpdateClusterData(node0Address))
		require.NoError(t, op.LoadSnapshots(ctx, 0, 0))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", false)
			return conf
		})
	})

	t.Run("badger compressed", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", true)
			return conf
		})
	})
}

func TestLoadSnapshotsMaxConcurrency(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, newConf func() *config.Config) {
		t.Parallel()

		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)

		cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(128)
		node0Conf := newConf()
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c := getClient(t, totalHashRanges, node0Address)
		op := getScaler(t, totalHashRanges, node0Address)

		// Test Put
		keys := make([]string, 1000)
		for i := 0; i < len(keys); i++ {
			keys[i] = fmt.Sprintf("key%d", i)
		}
		require.NoError(t, c.Put(ctx, keys, testTTL))

		exists, err := c.Get(ctx, keys)
		require.NoError(t, err)
		require.Len(t, exists, len(keys))
		for i, res := range exists {
			require.Truef(t, res, "Key %s should exist", keys[i])
		}

		err = op.CreateSnapshots(ctx, 0, false)
		require.NoError(t, err)

		files, err := minioContainer.Contents(ctx, defaultBackupFolderName+"/hr_")
		require.NoError(t, err)
		require.Len(t, files, int(totalHashRanges), "All hash ranges should contain at least one key")

		cancel()
		node0.Close()

		// Let's start again from scratch to see if the data is properly loaded from the snapshots
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0Conf = newConf()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c = getClient(t, totalHashRanges, node0Address)
		require.NoError(t, op.UpdateClusterData(node0Address))
		maxConcurrency := uint32(2)
		require.NoError(t, op.LoadSnapshots(ctx, 0, maxConcurrency))

		exists, err = c.Get(ctx, keys)
		require.NoError(t, err)
		require.Len(t, exists, len(keys))
		for i, res := range exists {
			require.Truef(t, res, "Key %s should exist", keys[i])
		}

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", false)
			return conf
		})
	})

	t.Run("badger compressed", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", true)
			return conf
		})
	})
}

func TestScaleUpAndDown(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, newConf func() *config.Config) {
		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)

		cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(3)
		node0Conf := newConf()
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c := getClient(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		keys := []string{"key1", "key2", "key3", "key4"}
		exists, err := c.Get(ctx, keys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		op := getScaler(t, totalHashRanges, node0Address)
		require.NoError(t, op.CreateSnapshots(ctx, 0, false))

		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_0_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
		)

		node1Conf := newConf()
		node1, node1Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           1,
			ClusterSize:      2,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
			Addresses:        []string{node0Address},
		}, node1Conf)
		require.Equal(t, map[uint32]uint64{0: 1, 1: 1}, node1.since,
			"Node should populate the since map upon start-up",
		)
		require.NoError(t, op.UpdateClusterData(node0Address, node1Address))
		require.NoError(t, op.LoadSnapshots(ctx, 1, 0, node1.hasher.GetNodeHashRangesList(1)...))
		require.NoError(t, op.Scale(ctx, []uint32{0, 1}))
		require.NoError(t, op.ScaleComplete(ctx, []uint32{0, 1}))

		respNode0, err := op.GetNodeInfo(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 2, respNode0.ClusterSize)
		require.Len(t, respNode0.HashRanges, 2)

		respNode1, err := op.GetNodeInfo(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 2, respNode1.ClusterSize)
		require.Len(t, respNode1.HashRanges, 1)

		exists, err = c.Get(ctx, keys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		// Scale down by removing node1. Then node0 should pick up all keys.
		// WARNING: when scaling up you can only add nodes to the right e.g. if the clusterSize is 2, and you add a node
		// then it will be node2 and the clusterSize will be 3
		// WARNING: when scaling down you can only remove nodes from the right i.e. if you have 2 nodes you can't
		// remove node0, you have to remove node1
		require.NoError(t, op.CreateSnapshots(ctx, 0, false))
		require.NoError(t, op.CreateSnapshots(ctx, 1, false))
		require.NoError(t, op.LoadSnapshots(ctx, 0, 0, node0.hasher.GetNodeHashRangesList(0)...))
		require.NoError(t, op.UpdateClusterData(node0Address))
		require.NoError(t, op.Scale(ctx, []uint32{0}))
		require.NoError(t, op.ScaleComplete(ctx, []uint32{0}))

		respNode0, err = op.GetNodeInfo(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, respNode0.ClusterSize)
		require.Len(t, respNode0.HashRanges, 3)

		cancel()
		node0.Close()
		node1.Close()
	}

	t.Run("badger", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", false)
			return conf
		})
	})
}

func TestGetPutAddressBroadcast(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, newConf func() *config.Config) {
		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)

		cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(3)
		node0Conf := newConf()
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c := getClient(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		keys := []string{"key1", "key2", "key3", "key4"}
		exists, err := c.Get(ctx, keys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		op := getScaler(t, totalHashRanges, node0Address)
		require.NoError(t, op.CreateSnapshots(ctx, 0, false))

		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_0_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
		)

		// Using a different path for the new node1 to avoid a conflict with node0
		node1Conf := newConf()
		node1, node1Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           1,
			ClusterSize:      2,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
			Addresses:        []string{node0Address},
		}, node1Conf)
		require.NoError(t, op.UpdateClusterData(node0Address, node1Address))
		require.NoError(t, op.LoadSnapshots(ctx, 1, 0, node1.hasher.GetNodeHashRangesList(1)...))
		require.NoError(t, op.Scale(ctx, []uint32{0, 1}))
		require.NoError(t, op.ScaleComplete(ctx, []uint32{0, 1}))

		require.Equal(t, 1, c.ClusterSize(), "The client should still believe that the cluster size is 1")
		exists, err = c.Get(context.Background(), keys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)
		require.Equal(t, 2, c.ClusterSize(), "Now the cluster size should be updated to 2")

		// Add a 3rd node to the cluster
		// Using a different path for the new node2 to avoid a conflict with node0 and node1
		node2Conf := newConf()
		node2, node2Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           2,
			ClusterSize:      3,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
			Addresses:        []string{node0Address, node1Address},
		}, node2Conf)
		require.NoError(t, op.UpdateClusterData(node0Address, node1Address, node2Address))
		require.NoError(t, op.LoadSnapshots(ctx, 2, 0, node2.hasher.GetNodeHashRangesList(2)...))
		require.NoError(t, op.Scale(ctx, []uint32{0, 1, 2}))
		require.NoError(t, op.ScaleComplete(ctx, []uint32{0, 1, 2}))

		// Verify that the client's cluster size is still 2 (not updated yet)
		require.Equal(t, 2, c.ClusterSize())

		// Perform a PUT operation which should update the client's internal cluster data
		require.NoError(t, c.Put(ctx, []string{"key5"}, testTTL))

		// Verify that the client's cluster size is now 3 (updated after PUT)
		require.Equal(t, 3, c.ClusterSize())

		// Verify that all keys are still accessible
		allKeys := []string{"key1", "key2", "key3", "key4", "key5"}
		exists, err = c.Get(ctx, allKeys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, true}, exists)

		// Scale down by removing node1 and node2. Then node0 should pick up all keys.
		// WARNING: when scaling up you can only add nodes to the right e.g. if the clusterSize is 2, and you add a node
		// then it will be node2 and the clusterSize will be 3
		// WARNING: when scaling down you can only remove nodes from the right i.e. if you have 2 nodes you can't remove
		// node0, you have to remove node1
		sourceNodeMovements, destinationNodeMovements := hash.GetHashRangeMovements(3, 1, totalHashRanges)
		for sourceNodeID, hashRanges := range sourceNodeMovements {
			require.NoError(t, op.CreateSnapshots(ctx, sourceNodeID, false, hashRanges...))
		}
		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_0_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_2_s_0_2.snapshot$"),
		)
		for destinationNodeID, hashRanges := range destinationNodeMovements {
			require.NoError(t, op.LoadSnapshots(ctx, destinationNodeID, totalHashRanges, hashRanges...))
		}
		require.NoError(t, op.UpdateClusterData(node0Address))
		require.NoError(t, op.Scale(ctx, []uint32{0}))
		require.NoError(t, op.ScaleComplete(ctx, []uint32{0}))

		exists, err = c.Get(ctx, allKeys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, true}, exists) // all served by node0

		cancel()
		node0.Close()
		node1.Close()
		node2.Close()
	}

	t.Run("badger", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", false)
			return conf
		})
	})
}

func TestIncrementalSnapshots(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, newConf func() *config.Config) {
		t.Parallel()

		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)

		cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(4)
		node0Conf := newConf()
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c := getClient(t, totalHashRanges, node0Address)
		op := getScaler(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		exists, err := c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		require.NoError(t, op.CreateSnapshots(ctx, 0, false))
		// we expect one hash range to be empty so the file won't be uploaded
		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_1.snapshot$"),
		)

		require.NoError(t, c.Put(ctx, []string{"key5"}, testTTL))
		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4", "key5"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, true}, exists)

		require.NoError(t, op.CreateSnapshots(ctx, 0, false))
		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_1_s_1_2.snapshot$"),
		)

		cancel()
		node0.Close()

		// Let's start again from scratch to see if the data is properly loaded from the snapshots
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0Conf = newConf()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c = getClient(t, totalHashRanges, node0Address)
		require.NoError(t, op.UpdateClusterData(node0Address))
		require.NoError(t, op.LoadSnapshots(ctx, 0, 0))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4", "key5"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, true}, exists)

		// Testing full sync capabilities.
		// All files should be removed but the new ones.
		// Being "full syncs" they start from the beginning (i.e. 0) up to the latest recorded (i.e. 2).
		require.NoError(t, op.CreateSnapshots(ctx, 0, true))
		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_1_s_0_2.snapshot$"),
			regexp.MustCompile("^.+/hr_2_s_0_2.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_2.snapshot$"),
		)

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", false)
			return conf
		})
	})

	t.Run("badger compressed", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", true)
			return conf
		})
	})
}

func TestSelectedSnapshots(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, newConf func() *config.Config) {
		t.Parallel()

		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)

		cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(4)
		node0Conf := newConf()
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c := getClient(t, totalHashRanges, node0Address)
		op := getScaler(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		exists, err := c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		// Create only the snapshots for the hash ranges 0 and 1.
		// We expect one hash range to be empty so the file won't be uploaded.
		require.NoError(t, op.CreateSnapshots(ctx, 0, false, 0, 1))
		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
		)

		// Now create the snapshot for the remaining hash ranges
		require.NoError(t, op.CreateSnapshots(ctx, 0, false, 2, 3))
		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_1.snapshot$"),
		)

		// Now try to create a snapshot for a hash range that is not handled by the node
		require.ErrorContains(t, op.CreateSnapshots(ctx, 0, false, 4), "hash range 4 not handled by this node")

		cancel()
		node0.Close()

		// Let's start again from scratch to see if the data is properly loaded from the snapshots
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0Conf = newConf()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c = getClient(t, totalHashRanges, node0Address)
		require.NoError(t, op.UpdateClusterData(node0Address))
		require.NoError(t, op.LoadSnapshots(ctx, 0, 0))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4", "key5"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, false}, exists)

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", false)
			return conf
		})
	})

	t.Run("badger compressed", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", true)
			return conf
		})
	})
}

func TestForceSkipFilesListing(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, newConf func() *config.Config) {
		t.Parallel()

		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)

		cloudStorage := keydbth.GetCloudStorage(t, newConf(), minioContainer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		totalHashRanges := uint32(4)
		node0Conf := newConf()
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c := getClient(t, totalHashRanges, node0Address)
		op := getScaler(t, totalHashRanges, node0Address)

		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		exists, err := c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		err = op.CreateSnapshots(ctx, 0, false)
		require.NoError(t, err)

		keydbth.RequireExpectedFiles(ctx, t, minioContainer, defaultBackupFolderName,
			regexp.MustCompile("^.+/hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^.+/hr_3_s_0_1.snapshot$"),
		)

		cancel()
		node0.Close()

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0Conf = newConf()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c = getClient(t, totalHashRanges, node0Address)

		require.Equal(t, map[uint32]uint64{1: 1, 2: 1, 3: 1}, node0.since,
			"Without forceSkipFilesListing, the since map should be populated from existing snapshots on startup",
		)

		require.NoError(t, op.UpdateClusterData(node0Address))
		require.NoError(t, op.LoadSnapshots(ctx, 0, 0))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		cancel()
		node0.Close()

		// Repeat with forceSkipFilesListing=true
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0Conf = newConf()
		node0Conf.Set("NodeService.forceSkipFilesListing", true)
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, node0Conf)
		c = getClient(t, totalHashRanges, node0Address)

		require.Empty(t, node0.since,
			"With forceSkipFilesListing, the since map should NOT be populated on startup",
		)

		require.NoError(t, op.UpdateClusterData(node0Address))
		require.NoError(t, op.LoadSnapshots(ctx, 0, 0))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{false, false, false, false}, exists,
			"With forceSkipFilesListing, snapshots cannot be loaded even with explicit LoadSnapshots call")

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", false)
			return conf
		})
	})

	t.Run("badger compressed", func(t *testing.T) {
		run(t, func() *config.Config {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", t.TempDir())
			conf.Set("BadgerDB.Dedup.Compress", true)
			return conf
		})
	})
}

func getService(
	ctx context.Context, t testing.TB, cs cloudStorage, nodeConfig Config, conf *config.Config,
) (*Service, string) {
	t.Helper()

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	address := "localhost:" + strconv.Itoa(freePort)
	nodeConfig.Addresses = append(nodeConfig.Addresses, address)
	nodeConfig.BackupFolderName = defaultBackupFolderName

	log := logger.NOP
	if testing.Verbose() {
		log = logger.NewLogger()
	}
	conf.Set("BadgerDB.Dedup.NopLogger", true)
	service, err := NewService(ctx, nodeConfig, cs, conf, stats.NOP, log)
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

func getClient(t testing.TB, totalHashRanges uint32, addresses ...string) *client.Client {
	t.Helper()

	clientConfig := client.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
		RetryPolicy:     client.RetryPolicy{Disabled: true},
	}

	c, err := client.NewClient(clientConfig, logger.NOP)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func getScaler(t testing.TB, totalHashRanges uint32, addresses ...string) *scaler.Client {
	t.Helper()

	opConfig := scaler.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
		RetryPolicy:     scaler.RetryPolicy{Disabled: true},
	}

	op, err := scaler.NewClient(opConfig, logger.NOP)
	require.NoError(t, err)
	t.Cleanup(func() { _ = op.Close() })

	return op
}
