package node

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/cloudstorage"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
)

const (
	testTTL         = 5 * time.Minute
	accessKeyId     = "MYACCESSKEYID"
	secretAccessKey = "MYSECRETACCESSKEY"
	region          = "MYREGION"
	bucket          = "bucket-name"
)

func TestSimple(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, conf *config.Config) {
		t.Parallel()

		_, cloudStorage := getCloudStorage(t, pool, conf)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(4)
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c := getClient(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		exists, err := c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		err = c.CreateSnapshots(ctx)
		require.NoError(t, err)

		session := cloudStorage.ListFilesWithPrefix(context.Background(), "", "", 500)
		files, err := session.Next()
		require.NoError(t, err)
		require.Len(t, files, 3) // we expect one hash range to be empty so the file won't be uploaded

		cancel()
		node0.Close()
		// TODO: add test without the path change to test that it loads fine from disk as well if there is nothing
		// on Cloud Storage
		conf.Set("BadgerDB.Dedup.Path", t.TempDir()) // Force a path change, the data will be loaded from Cloud Storage

		// Let's start again from scratch to see if the data is properly loaded from the snapshots
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c = getClient(t, totalHashRanges, node0Address)
		require.NoError(t, c.LoadSnapshots(ctx))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", false)
		run(t, conf)
	})

	t.Run("badger compressed", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		run(t, conf)
	})
}

func TestScaleUpAndDown(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, conf *config.Config) {
		minioClient, cloudStorage := getCloudStorage(t, pool, conf)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(3)
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c := getClient(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		keys := []string{"key1", "key2", "key3", "key4"}
		exists, err := c.Get(ctx, keys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		operator := getClient(t, totalHashRanges, node0Address)
		require.NoError(t, operator.CreateSnapshots(ctx))

		requireExpectedFiles(ctx, t, minioClient,
			regexp.MustCompile("^hr_0_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_2_s_0_1.snapshot$"),
		)

		// Using a different path for the new node1 to avoid a conflict with node0
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		node1, node1Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           1,
			ClusterSize:      2,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
			Addresses:        []string{node0Address},
		}, conf)
		require.NoError(t, operator.Scale(ctx, node0Address, node1Address))
		require.NoError(t, operator.ScaleComplete(ctx))

		respNode0, err := c.GetNodeInfo(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 2, respNode0.ClusterSize)
		require.Len(t, respNode0.HashRanges, 2)

		respNode1, err := c.GetNodeInfo(ctx, 1)
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
		require.NoError(t, operator.CreateSnapshots(ctx))
		require.NoError(t, operator.Scale(ctx, node0Address))
		require.NoError(t, operator.ScaleComplete(ctx))

		respNode0, err = c.GetNodeInfo(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, respNode0.ClusterSize)
		require.Len(t, respNode0.HashRanges, 3)

		cancel()
		node0.Close()
		node1.Close()
	}

	t.Run("badger", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", false)
		run(t, conf)
	})
}

func TestGetPutAddressBroadcast(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, conf *config.Config) {
		minioClient, cloudStorage := getCloudStorage(t, pool, conf)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(3)
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c := getClient(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		keys := []string{"key1", "key2", "key3", "key4"}
		exists, err := c.Get(ctx, keys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		operator := getClient(t, totalHashRanges, node0Address)
		require.NoError(t, operator.CreateSnapshots(ctx))

		requireExpectedFiles(ctx, t, minioClient,
			regexp.MustCompile("^hr_0_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_2_s_0_1.snapshot$"),
		)

		// Using a different path for the new node1 to avoid a conflict with node0
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		node1, node1Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           1,
			ClusterSize:      2,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
			Addresses:        []string{node0Address},
		}, conf)
		require.NoError(t, operator.Scale(ctx, node0Address, node1Address))
		require.NoError(t, operator.ScaleComplete(ctx))

		require.Equal(t, 1, c.ClusterSize(), "The client should still believe that the cluster size is 1")
		exists, err = c.Get(context.Background(), keys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)
		require.Equal(t, 2, c.ClusterSize(), "Now the cluster size should be updated to 2")

		// Add a 3rd node to the cluster
		// Using a different path for the new node2 to avoid a conflict with node0 and node1
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		node2, node2Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           2,
			ClusterSize:      3,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
			Addresses:        []string{node0Address, node1Address},
		}, conf)
		require.NoError(t, operator.Scale(ctx, node0Address, node1Address, node2Address))
		require.NoError(t, operator.ScaleComplete(ctx))

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
		require.NoError(t, operator.CreateSnapshots(ctx))
		require.NoError(t, operator.Scale(ctx, node0Address))
		require.NoError(t, operator.ScaleComplete(ctx))

		exists, err = c.Get(ctx, allKeys)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, true}, exists) // all served by node0

		cancel()
		node0.Close()
		node1.Close()
		node2.Close()
	}

	t.Run("badger", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", false)
		run(t, conf)
	})
}

func TestIncrementalSnapshots(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, conf *config.Config) {
		t.Parallel()

		minioClient, cloudStorage := getCloudStorage(t, pool, conf)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(4)
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c := getClient(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		exists, err := c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		require.NoError(t, c.CreateSnapshots(ctx))
		// we expect one hash range to be empty so the file won't be uploaded
		requireExpectedFiles(ctx, t, minioClient,
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_3_s_0_1.snapshot$"),
		)

		require.NoError(t, c.Put(ctx, []string{"key5"}, testTTL))
		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4", "key5"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, true}, exists)

		require.NoError(t, c.CreateSnapshots(ctx))
		requireExpectedFiles(ctx, t, minioClient,
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_3_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_3_s_([1-9])+_2.snapshot$"),
		)

		cancel()
		node0.Close()
		// TODO: add test without the path change to test that it loads fine from disk as well if there is nothing
		// on Cloud Storage
		conf.Set("BadgerDB.Dedup.Path", t.TempDir()) // Force a path change, the data will be loaded from Cloud Storage

		// Let's start again from scratch to see if the data is properly loaded from the snapshots
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c = getClient(t, totalHashRanges, node0Address)
		require.NoError(t, c.LoadSnapshots(ctx))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4", "key5"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, true}, exists)

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", false)
		run(t, conf)
	})

	t.Run("badger compressed", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		run(t, conf)
	})
}

func TestSelectedSnapshots(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, conf *config.Config) {
		t.Parallel()

		minioClient, cloudStorage := getCloudStorage(t, pool, conf)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the node service
		totalHashRanges := uint32(4)
		node0, node0Address := getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c := getClient(t, totalHashRanges, node0Address)

		// Test Put
		require.NoError(t, c.Put(ctx, []string{"key1", "key2", "key3"}, testTTL))

		exists, err := c.Get(ctx, []string{"key1", "key2", "key3", "key4"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false}, exists)

		// Create only the snapshots for the hash ranges 0 and 1.
		// We expect one hash range to be empty so the file won't be uploaded.
		require.NoError(t, c.CreateSnapshots(ctx, 0, 1))
		requireExpectedFiles(ctx, t, minioClient,
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
		)

		// Now create the snapshot for the remaining hash ranges
		require.NoError(t, c.CreateSnapshots(ctx, 2, 3))
		requireExpectedFiles(ctx, t, minioClient,
			regexp.MustCompile("^hr_1_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_2_s_0_1.snapshot$"),
			regexp.MustCompile("^hr_3_s_0_1.snapshot$"),
		)

		// Now try to create a snapshot for a hash range that is not handled by the node
		require.ErrorContains(t, c.CreateSnapshots(ctx, 4), "hash range 4 not handled by this node")

		cancel()
		node0.Close()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir()) // Force a path change, the data will be loaded from Cloud Storage

		// Let's start again from scratch to see if the data is properly loaded from the snapshots
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		node0, node0Address = getService(ctx, t, cloudStorage, Config{
			NodeID:           0,
			ClusterSize:      1,
			TotalHashRanges:  totalHashRanges,
			SnapshotInterval: 60 * time.Second,
		}, conf)
		c = getClient(t, totalHashRanges, node0Address)
		require.NoError(t, c.LoadSnapshots(ctx))

		exists, err = c.Get(ctx, []string{"key1", "key2", "key3", "key4", "key5"})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, false, false}, exists)

		cancel()
		node0.Close()
	}

	t.Run("badger", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", false)
		run(t, conf)
	})

	t.Run("badger compressed", func(t *testing.T) {
		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		conf.Set("BadgerDB.Dedup.Compress", true)
		run(t, conf)
	})
}

func getCloudStorage(t testing.TB, pool *dockertest.Pool, conf *config.Config) (*minio.Client, cloudStorage) {
	t.Helper()

	minioEndpoint, minioClient := createMinioResource(t, pool, accessKeyId, secretAccessKey, region, bucket)
	conf.Set("Storage.Bucket", bucket)
	conf.Set("Storage.Endpoint", minioEndpoint)
	conf.Set("Storage.AccessKeyId", accessKeyId)
	conf.Set("Storage.AccessKey", secretAccessKey)
	conf.Set("Storage.Region", region)
	conf.Set("Storage.DisableSsl", true)
	conf.Set("Storage.S3ForcePathStyle", true)
	conf.Set("Storage.UseGlue", true)

	cloudStorage, err := cloudstorage.GetCloudStorage(conf, logger.NOP)
	require.NoError(t, err)

	return minioClient, cloudStorage
}

func getService(
	ctx context.Context, t testing.TB, cs cloudStorage, nodeConfig Config, conf *config.Config,
) (*Service, string) {
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
	clientConfig := client.Config{
		Addresses:       addresses,
		TotalHashRanges: totalHashRanges,
		RetryCount:      3,
		RetryDelay:      100 * time.Millisecond,
	}

	c, err := client.NewClient(clientConfig, logger.NOP)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func createMinioResource(
	t testing.TB,
	pool *dockertest.Pool, accessKeyId, secretAccessKey, region, bucket string,
) (string, *minio.Client) {
	t.Helper()
	// running minio container on docker
	minioResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		Env: []string{
			fmt.Sprintf("MINIO_ACCESS_KEY=%s", accessKeyId),
			fmt.Sprintf("MINIO_SECRET_KEY=%s", secretAccessKey),
			fmt.Sprintf("MINIO_SITE_REGION=%s", region),
		},
		ExposedPorts: []string{"9000/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Purge(minioResource) })

	minioEndpoint := fmt.Sprintf("localhost:%s", minioResource.GetPort("9000/tcp"))

	// check if minio server is up & running.
	err = pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func() { httputil.CloseResponse(resp) }()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	})
	require.NoError(t, err)

	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyId, secretAccessKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	// creating bucket inside minio where testing will happen.
	err = minioClient.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: region})
	require.NoError(t, err)

	return minioEndpoint, minioClient
}

type File struct {
	Key                  string
	Content              string
	Etag                 string
	LastModificationTime time.Time
}

// TODO use go-kit minio resource instead
func getContents(ctx context.Context, bucket, prefix string, client *minio.Client) ([]File, error) {
	contents := make([]File, 0)

	opts := minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    prefix,
	}
	for objInfo := range client.ListObjects(ctx, bucket, opts) {
		if objInfo.Err != nil {
			return nil, fmt.Errorf("list objects: %w", objInfo.Err)
		}

		o, err := client.GetObject(ctx, bucket, objInfo.Key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("get object: %w", err)
		}

		b, err := io.ReadAll(bufio.NewReader(o))
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		contents = append(contents, File{
			Key:                  objInfo.Key,
			Content:              string(b),
			Etag:                 objInfo.ETag,
			LastModificationTime: objInfo.LastModified,
		})
	}

	slices.SortStableFunc(contents, func(a, b File) int {
		return strings.Compare(a.Key, b.Key)
	})

	return contents, nil
}

func requireExpectedFiles(
	ctx context.Context, t *testing.T, client *minio.Client, expectedFiles ...*regexp.Regexp,
) {
	t.Helper()
	files, err := getContents(ctx, bucket, "hr_", client)
	require.NoError(t, err)
	require.Len(t, files, len(expectedFiles))
	for _, file := range files {
		var found bool
		for _, expectedFile := range expectedFiles {
			if expectedFile.MatchString(file.Key) {
				found = true
				break
			}
		}
		require.True(t, found, "Unexpected file: %s", file.Key)
	}
}
