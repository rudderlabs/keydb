package node

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
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

	"github.com/rudderlabs/keydb/internal/cache/memory"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/cloudstorage"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
)

const (
	testTTL         = 10
	accessKeyId     = "MYACCESSKEYID"
	secretAccessKey = "MYSECRETACCESSKEY"
	region          = "MYREGION"
	bucket          = "bucket-name"
)

func TestSimple(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	minioClient, cloudStorage := getCloudStorage(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the node service
	now := time.Now()
	totalHashRanges := uint32(128)
	node0, node0Address := getService(ctx, t, cloudStorage, now, Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	})
	c := getClient(t, totalHashRanges, node0Address)

	// Test Put
	items := []*pb.KeyWithTTL{
		{Key: "key1", TtlSeconds: testTTL},
		{Key: "key2", TtlSeconds: testTTL},
		{Key: "key3", TtlSeconds: testTTL},
	}
	require.NoError(t, c.Put(ctx, items))

	keys := []string{"key1", "key2", "key3", "key4"}
	exists, err := c.Get(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)

	err = c.CreateSnapshot(ctx)
	require.NoError(t, err)

	files, err := getContents(ctx, bucket, "", minioClient)
	require.NoError(t, err)
	require.Len(t, files, 3)
	requireTTLInSnapshot(t, 0, 103, files[0], "key1", now)
	requireTTLInSnapshot(t, 0, 122, files[1], "key2", now)
	requireTTLInSnapshot(t, 0, 13, files[2], "key3", now)

	cancel()
	node0.Close()

	// Let's start again from scratch to see if the data is properly loaded from the snapshots
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	node0, node0Address = getService(ctx, t, cloudStorage, now, Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	})
	c = getClient(t, totalHashRanges, node0Address)

	exists, err = c.Get(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)

	cancel()
	node0.Close()
}

func TestScaleUpAndDown(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	minioClient, cloudStorage := getCloudStorage(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the node service
	now := time.Now()
	totalHashRanges := uint32(3)
	node0, node0Address := getService(ctx, t, cloudStorage, now, Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	})
	c := getClient(t, totalHashRanges, node0Address)

	// Test Put
	items := []*pb.KeyWithTTL{
		{Key: "key1", TtlSeconds: testTTL},
		{Key: "key2", TtlSeconds: testTTL},
		{Key: "key3", TtlSeconds: testTTL},
	}
	require.NoError(t, c.Put(ctx, items))

	keys := []string{"key1", "key2", "key3", "key4"}
	exists, err := c.Get(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)

	operator := getClient(t, totalHashRanges, node0Address)
	require.NoError(t, operator.CreateSnapshot(ctx))

	files, err := getContents(ctx, bucket, "", minioClient)
	require.NoError(t, err)
	require.Len(t, files, 3)
	requireTTLInSnapshot(t, 0, 2, files[2], "key1", now)
	requireTTLInSnapshot(t, 0, 1, files[1], "key2", now)
	requireTTLInSnapshot(t, 0, 0, files[0], "key3", now)

	node1, node1Address := getService(ctx, t, cloudStorage, now, Config{
		NodeID:           1,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address},
	})
	require.NoError(t, operator.Scale(ctx, node0Address, node1Address))
	require.NoError(t, operator.ScaleComplete(ctx))

	respNode0, err := c.GetNodeInfo(ctx, 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, respNode0.ClusterSize)
	require.Len(t, respNode0.HashRanges, 2)
	require.EqualValues(t, 2, respNode0.KeysCount)
	require.Equal(t, `{key3:true}`, node0.caches[0].String())
	require.Equal(t, `{key1:true}`, node0.caches[2].String())

	respNode1, err := c.GetNodeInfo(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, respNode1.ClusterSize)
	require.Len(t, respNode1.HashRanges, 1)
	require.EqualValues(t, 1, respNode1.KeysCount)
	require.Equal(t, `{key2:true}`, node1.caches[1].String())

	exists, err = c.Get(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)

	// Scale down by removing node1. Then node0 should pick up all keys.
	// WARNING: when scaling up you can only add nodes to the right e.g. if the clusterSize is 2, and you add a node then it will be node2 and the clusterSize will be 3
	// WARNING: when scaling down you can only remove nodes from the right i.e. if you have 2 nodes you can't remove node0, you have to remove node1
	require.NoError(t, operator.CreateSnapshot(ctx))
	require.NoError(t, operator.Scale(ctx, node0Address))
	require.NoError(t, operator.ScaleComplete(ctx))

	respNode0, err = c.GetNodeInfo(ctx, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, respNode0.ClusterSize)
	require.Len(t, respNode0.HashRanges, 3)
	require.EqualValues(t, 3, respNode0.KeysCount)
	require.Equal(t, `{key3:true}`, node0.caches[0].String())
	require.Equal(t, `{key2:true}`, node0.caches[1].String())
	require.Equal(t, `{key1:true}`, node0.caches[2].String())

	cancel()
	node0.Close()
	node1.Close()
}

func TestGetPutAddressBroadcast(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	minioClient, cloudStorage := getCloudStorage(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the node service
	now := time.Now()
	totalHashRanges := uint32(3)
	node0, node0Address := getService(ctx, t, cloudStorage, now, Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
	})
	c := getClient(t, totalHashRanges, node0Address)

	// Test Put
	items := []*pb.KeyWithTTL{
		{Key: "key1", TtlSeconds: testTTL},
		{Key: "key2", TtlSeconds: testTTL},
		{Key: "key3", TtlSeconds: testTTL},
	}
	require.NoError(t, c.Put(ctx, items))

	keys := []string{"key1", "key2", "key3", "key4"}
	exists, err := c.Get(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)

	operator := getClient(t, totalHashRanges, node0Address)
	require.NoError(t, operator.CreateSnapshot(ctx))

	files, err := getContents(ctx, bucket, "", minioClient)
	require.NoError(t, err)
	require.Len(t, files, 3)
	requireTTLInSnapshot(t, 0, 2, files[2], "key1", now)
	requireTTLInSnapshot(t, 0, 1, files[1], "key2", now)
	requireTTLInSnapshot(t, 0, 0, files[0], "key3", now)

	node1, node1Address := getService(ctx, t, cloudStorage, now, Config{
		NodeID:           1,
		ClusterSize:      2,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address},
	})
	require.NoError(t, operator.Scale(ctx, node0Address, node1Address))
	require.NoError(t, operator.ScaleComplete(ctx))

	require.Equal(t, 1, c.ClusterSize(), "The client should still believe that the cluster size is 1")
	exists, err = c.Get(context.Background(), keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)
	require.Equal(t, 2, c.ClusterSize(), "Now the cluster size should be updated to 2")

	// Add a 3rd node to the cluster
	node2, node2Address := getService(ctx, t, cloudStorage, now, Config{
		NodeID:           2,
		ClusterSize:      3,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 60 * time.Second,
		Addresses:        []string{node0Address, node1Address},
	})
	require.NoError(t, operator.Scale(ctx, node0Address, node1Address, node2Address))
	require.NoError(t, operator.ScaleComplete(ctx))

	// Verify that the client's cluster size is still 2 (not updated yet)
	require.Equal(t, 2, c.ClusterSize())

	// Perform a PUT operation which should update the client's internal cluster data
	newItems := []*pb.KeyWithTTL{
		{Key: "key5", TtlSeconds: testTTL},
	}
	require.NoError(t, c.Put(ctx, newItems))

	// Verify that the client's cluster size is now 3 (updated after PUT)
	require.Equal(t, 3, c.ClusterSize())

	// Verify that all keys are still accessible
	allKeys := []string{"key1", "key2", "key3", "key4", "key5"}
	exists, err = c.Get(ctx, allKeys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false, true}, exists)

	// Scale down by removing node1 and node2. Then node0 should pick up all keys.
	// WARNING: when scaling up you can only add nodes to the right e.g. if the clusterSize is 2, and you add a node then it will be node2 and the clusterSize will be 3
	// WARNING: when scaling down you can only remove nodes from the right i.e. if you have 2 nodes you can't remove node0, you have to remove node1
	require.NoError(t, operator.CreateSnapshot(ctx))
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

func getCloudStorage(t testing.TB, pool *dockertest.Pool) (*minio.Client, cloudStorage) {
	t.Helper()

	minioEndpoint, minioClient := createMinioResource(t, pool, accessKeyId, secretAccessKey, region, bucket)
	conf := config.New(config.WithEnvPrefix("KEYDB"))
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

func getService(ctx context.Context, t testing.TB, cs cloudStorage, now time.Time, nodeConfig Config) (*Service, string) {
	t.Helper()

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	address := "localhost:" + strconv.Itoa(freePort)
	nodeConfig.Addresses = append(nodeConfig.Addresses, address)

	service, err := NewService(ctx, nodeConfig, func() cache { return memory.New() }, cs, logger.NOP)
	require.NoError(t, err)
	service.now = func() time.Time { return now }

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

	c, err := client.NewClient(clientConfig)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func requireTTLInSnapshot(t testing.TB, nodeNumber, hashRange int64, file File, key string, now time.Time) {
	t.Helper()
	expectedFilename := "range_" + strconv.FormatInt(hashRange, 10) + ".snapshot"
	require.Equal(t, expectedFilename, file.Key)
	require.Contains(t, file.Content, key+":")
	ttl, err := strconv.ParseFloat(strings.Split(file.Content[0:len(file.Content)-1], ":")[1], 64)
	require.NoError(t, err)
	require.InDelta(t, now.UnixMilli(), ttl, (testTTL+3)*1000) // with 3s max difference allowed
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
			return nil, objInfo.Err
		}

		o, err := client.GetObject(ctx, bucket, objInfo.Key, minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}

		var r io.Reader
		br := bufio.NewReader(o)
		magic, err := br.Peek(2)
		// check if the file is gzipped using the magic number
		if err == nil && magic[0] == 31 && magic[1] == 139 {
			r, err = gzip.NewReader(br)
			if err != nil {
				return nil, fmt.Errorf("gunzip: %w", err)
			}
		} else {
			r = br
		}

		b, err := io.ReadAll(r)
		if err != nil {
			return nil, err
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
