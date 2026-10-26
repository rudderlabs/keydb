package node

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
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
	if nodeConfig.TotalHashRanges == 0 {
		nodeConfig.TotalHashRanges = 4
	}
	ctx, cancel := context.WithCancel(ctx)
	service, err := NewService(ctx, nodeConfig, noopStorage{}, conf, stats.NOP, logger.NOP)
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
