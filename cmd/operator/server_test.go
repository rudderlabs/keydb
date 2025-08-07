package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/keydb/internal/hash"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/cloudstorage"
	"github.com/rudderlabs/keydb/internal/operator"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
)

const (
	testTTL         = "5m" // 5 minutes
	accessKeyId     = "MYACCESSKEYID"
	secretAccessKey = "MYSECRETACCESSKEY"
	region          = "MYREGION"
	bucket          = "bucket-name"
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

	_, cloudStorage := getCloudStorage(t, pool, newConf())

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

	// Scale down by removing node1. Then node0 should pick up all keys.
	// WARNING: when scaling up you can only add nodes to the right e.g. if the clusterSize is 2, and you add a node
	// then it will be node2 and the clusterSize will be 3
	// WARNING: when scaling down you can only remove nodes from the right i.e. if you have 2 nodes you can't
	// remove node0, you have to remove node1
	_ = op.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 0, FullSync: false}, true)
	_ = op.Do("/createSnapshots", CreateSnapshotsRequest{NodeID: 1, FullSync: false}, true)
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

func getCloudStorage(t testing.TB, pool *dockertest.Pool, conf *config.Config) (*minio.Client, filemanager.S3Manager) {
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

func startOperatorHTTPServer(t testing.TB, totalHashRanges uint32, addresses ...string) *opClient {
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
		t:      t,
		client: http.DefaultClient,
		url:    fmt.Sprintf("http://localhost:%d", freePort),
	}
}

type opClient struct {
	t      testing.TB
	client *http.Client
	url    string
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
