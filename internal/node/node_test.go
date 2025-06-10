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

	"github.com/rudderlabs/keydb/internal/client"
	"github.com/rudderlabs/keydb/internal/cloudstorage"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
)

const (
	bufSize = 1024 * 1024
	testTTL = 10
)

func TestNode(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	var (
		accessKeyId                = "MYACCESSKEYID"
		secretAccessKey            = "MYSECRETACCESSKEY"
		region                     = "MYREGION"
		bucket                     = "bucket-name"
		minioEndpoint, minioClient = createMinioResource(t, pool, accessKeyId, secretAccessKey, region, bucket)
	)
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

	// Create the node service
	nodeConfig := Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  128,
		SnapshotInterval: 60 * time.Second,
	}

	now := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service, c := getServiceAndClient(ctx, t, nodeConfig, cloudStorage, now)

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

	resp, err := c.CreateSnapshot(ctx, 0)
	require.NoError(t, err)
	require.True(t, resp.Success)

	files, err := getContents(ctx, bucket, "", minioClient)
	require.NoError(t, err)
	require.Len(t, files, 3)
	requireTTLInSnapshot(t, 0, 103, files[0], "key1", now)
	requireTTLInSnapshot(t, 0, 122, files[1], "key2", now)
	requireTTLInSnapshot(t, 0, 13, files[2], "key3", now)

	cancel()
	service.Close()

	// Let's start again from scratch to see if the data is properly loaded from the snapshots
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	service, c = getServiceAndClient(ctx, t, nodeConfig, cloudStorage, now)

	exists, err = c.Get(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, exists)

	cancel()
	service.Close()
}

func getServiceAndClient(
	ctx context.Context, t testing.TB, nodeConfig Config, cs cloudStorage, now time.Time,
) (
	*Service, *client.Client,
) {
	service, err := NewService(ctx, nodeConfig, cs, logger.NOP)
	require.NoError(t, err)
	service.now = func() time.Time { return now }

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	// Create a bufconn listener
	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	port := strconv.Itoa(freePort)
	lis, err := net.Listen("tcp", "localhost:"+port)
	require.NoError(t, err)

	// Start the server
	go func() {
		require.NoError(t, server.Serve(lis))
	}()
	t.Cleanup(server.GracefulStop)

	clientConfig := client.Config{
		Addresses:       []string{"localhost:" + port},
		TotalHashRanges: 128,
		RetryCount:      3,
		RetryDelay:      100 * time.Millisecond,
	}

	c, err := client.NewClient(clientConfig)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return service, c
}

func requireTTLInSnapshot(t testing.TB, nodeNumber, hashRange int64, file File, key string, now time.Time) {
	expectedFilename := "node_" + strconv.FormatInt(nodeNumber, 10) + "_range_" + strconv.FormatInt(hashRange, 10) + ".snapshot"
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
