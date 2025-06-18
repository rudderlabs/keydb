package main

import (
	"context"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/rudderlabs/keydb/cache"
	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
)

func BenchmarkSingleNode(b *testing.B) {
	var (
		totalHashRanges    uint32 = 128
		warmUpWithKeys            = 1_000_000 // must be divisible by warmUpBatchSize
		warmUpBatchSize           = 1_000
		concurrentGetCalls        = 5_000
		getBatchSize              = 1_000
		concurrentPutCalls        = 1_000
		putBatchSize              = 100
		defaultTTL                = time.Hour
	)

	nodeConfig := node.Config{
		NodeID:           0,
		ClusterSize:      1,
		TotalHashRanges:  totalHashRanges,
		SnapshotInterval: 24 * time.Hour,
	}

	freePort, err := testhelper.GetFreePort()
	require.NoError(b, err)
	address := "localhost:" + strconv.Itoa(freePort)
	nodeConfig.Addresses = append(nodeConfig.Addresses, address)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := config.New()
	conf.GetString("BadgerDB.Dedup.Path", b.TempDir())
	cs := &mockedCloudStorage{}
	cf := func(hashRange uint32) (node.Cache, error) {
		return cache.BadgerFactory(conf, logger.NOP)(hashRange)
	}
	service, err := node.NewService(ctx, nodeConfig, cf, cs, logger.NOP)
	require.NoError(b, err)

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	lis, err := net.Listen("tcp", address)
	require.NoError(b, err)

	// Start the server
	go func() {
		require.NoError(b, server.Serve(lis))
	}()
	b.Cleanup(func() {
		server.GracefulStop()
		_ = lis.Close()
	})

	clientConfig := client.Config{
		Addresses:       nodeConfig.Addresses,
		TotalHashRanges: totalHashRanges,
		RetryCount:      3,
		RetryDelay:      100 * time.Millisecond,
	}

	c, err := client.NewClient(clientConfig, logger.NOP)
	require.NoError(b, err)
	b.Cleanup(func() { _ = c.Close() })

	var wg sync.WaitGroup
	for i := 0; i < warmUpWithKeys/warmUpBatchSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keys := make([]string, 0, warmUpBatchSize)
			for j := 0; j < warmUpBatchSize; j++ {
				keys = append(keys, strconv.Itoa(i*warmUpBatchSize+j))
			}
			require.NoError(b, c.Put(ctx, keys, defaultTTL))
		}()
	}
	wg.Wait()

	getKeys := make([]string, getBatchSize)
	for k := range getKeys {
		getKeys[k] = "key-" + strconv.Itoa(rand.Intn(warmUpWithKeys))
	}

	putKeys := make([]string, putBatchSize)
	for k := range putKeys {
		putKeys[k] = "key-" + strconv.Itoa(rand.Intn(warmUpWithKeys))
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for g := 0; g < concurrentGetCalls; g++ {
			wg.Add(1)
			go func() {
				_, _ = c.Get(ctx, getKeys)
				wg.Done()
			}()
		}
		for p := 0; p < concurrentPutCalls; p++ {
			wg.Add(1)
			go func() {
				_ = c.Put(ctx, putKeys, defaultTTL)
				wg.Done()
			}()
		}
		wg.Wait()
	}

	elapsed := time.Since(start)
	totalGetCalls := concurrentGetCalls * b.N
	b.ReportMetric(float64(totalGetCalls)/elapsed.Seconds(), "get/sec")
	totalPutCalls := concurrentPutCalls * b.N
	b.ReportMetric(float64(totalPutCalls)/elapsed.Seconds(), "put/sec")
	b.ReportMetric(float64(totalGetCalls+totalPutCalls)/elapsed.Seconds(), "ops/sec")
}

type mockedFilemanagerSession struct{}

func (m *mockedFilemanagerSession) Next() (fileObjects []*filemanager.FileInfo, err error) {
	return nil, nil
}

type mockedCloudStorage struct{}

func (m *mockedCloudStorage) Download(_ context.Context, _ io.WriterAt, _ string) error {
	return nil
}

func (m *mockedCloudStorage) ListFilesWithPrefix(_ context.Context, _, _ string, _ int64) filemanager.ListSession {
	return &mockedFilemanagerSession{}
}

func (m *mockedCloudStorage) UploadReader(_ context.Context, _ string, _ io.Reader) (filemanager.UploadedFile, error) {
	return filemanager.UploadedFile{}, nil
}
