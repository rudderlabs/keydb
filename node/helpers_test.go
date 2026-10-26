package node

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"

	pb "github.com/rudderlabs/keydb/proto"
)

// noopStorage is a cloud storage that stores nothing, for tests that never touch S3.
type noopStorage struct{}

func (noopStorage) Download(context.Context, io.WriterAt, string, ...filemanager.DownloadOption) error {
	return filemanager.ErrKeyNotFound
}

func (noopStorage) ListFilesWithPrefix(context.Context, string, string, int64) filemanager.ListSession {
	return noopListSession{}
}

func (noopStorage) UploadReader(context.Context, string, io.Reader) (filemanager.UploadedFile, error) {
	return filemanager.UploadedFile{}, nil
}

func (noopStorage) Delete(context.Context, []string) error { return nil }

type noopListSession struct{}

func (noopListSession) Next() ([]*filemanager.FileInfo, error) { return nil, nil }

// newTestConf returns a config with a fresh Badger directory.
func newTestConf(t testing.TB) *config.Config {
	t.Helper()
	conf := config.New()
	conf.Set("BadgerDB.Dedup.Path", t.TempDir())
	conf.Set("BadgerDB.Dedup.NopLogger", true)
	return conf
}

// newTestService creates a service with the given config, without a gRPC server.
// The service's background goroutines stop when the test ends.
func newTestService(ctx context.Context, t testing.TB, nodeConfig Config, conf *config.Config) *Service {
	t.Helper()
	return newTestServiceWithStats(ctx, t, nodeConfig, conf, stats.NOP)
}

// newTestServiceWithStats is newTestService with the given stats, so tests can assert on metrics.
func newTestServiceWithStats(
	ctx context.Context, t testing.TB, nodeConfig Config, conf *config.Config, stat stats.Stats,
) *Service {
	t.Helper()
	if nodeConfig.TotalHashRanges == 0 {
		nodeConfig.TotalHashRanges = 4
	}
	ctx, cancel := context.WithCancel(ctx)
	service, err := NewService(ctx, nodeConfig, noopStorage{}, conf, stat, logger.NOP)
	if err != nil {
		cancel()
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		service.Close()
	})
	return service
}

// staticList returns a function that always returns the given list, for Config.Addresses and ShadowAddresses.
func staticList(list ...string) func() []string {
	return func() []string { return list }
}

// freeAddress returns a localhost address with a free port.
func freeAddress(t testing.TB) string {
	t.Helper()
	port, err := testhelper.GetFreePort()
	require.NoError(t, err)
	return "localhost:" + strconv.Itoa(port)
}

// serveTestService serves the service on the given address until the test ends.
func serveTestService(t testing.TB, service *Service, address string) {
	t.Helper()
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)
	lis, err := net.Listen("tcp", address)
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
}

// nodeClient returns a raw gRPC client for the node at the given address.
func nodeClient(t testing.TB, address string) pb.NodeServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewNodeServiceClient(conn)
}

// sendChunks streams the given chunks to the node's ReceiveSnapshot and returns its response.
func sendChunks(t testing.TB, c pb.NodeServiceClient, chunks ...*pb.SnapshotChunk) *pb.ReceiveSnapshotResponse {
	t.Helper()
	stream, err := c.ReceiveSnapshot(context.Background())
	require.NoError(t, err)
	for _, chunk := range chunks {
		if err := stream.Send(chunk); err != nil {
			break // the server closed the stream early, its response says why
		}
	}
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	return resp
}
