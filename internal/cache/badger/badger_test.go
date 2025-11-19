package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	keydbth "github.com/rudderlabs/keydb/internal/testhelper"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	miniokit "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
)

func TestSnapshots(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, compress bool) {
		t.Parallel()

		conf := config.New()
		conf.Set("BadgerDB.Dedup.Compress", compress)
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		bdb, err := New(conf, logger.NOP)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, bdb.Close())
		})

		err = bdb.Put(map[int64][]string{0: {"key1", "key2"}}, time.Hour)
		require.NoError(t, err)

		exists, err := bdb.Get(map[int64][]string{0: {"key1", "key2"}}, map[string]int{"key1": 0, "key2": 1})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true}, exists)

		// Create snapshot
		buf := new(bytes.Buffer)
		mp := map[int64]io.Writer{
			0: buf,
		}
		since, _, err := bdb.CreateSnapshots(context.Background(), mp, map[int64]uint64{})
		require.NoError(t, err)

		filename := fmt.Sprintf("snapshot-%d.badger", since)

		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)
		cloudStorage := keydbth.GetCloudStorage(t, conf, minioContainer)

		uploadedFile1, err := cloudStorage.UploadReader(context.Background(), filename, buf)
		require.NoError(t, err)
		files, err := minioContainer.Contents(context.Background(), "")
		require.NoError(t, err)
		require.Len(t, files, 1)
		t.Logf("1st snapshot created: %+v", uploadedFile1)

		err = bdb.Put(map[int64][]string{0: {"key3", "key4"}}, time.Hour)
		require.NoError(t, err)
		exists, err = bdb.Get(map[int64][]string{0: {"key1", "key2", "key3", "key4"}},
			map[string]int{"key1": 0, "key2": 1, "key3": 2, "key4": 3})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, true}, exists)

		// Create another snapshot which should contain also key3 and key4
		buf.Reset()
		mp = map[int64]io.Writer{
			0: buf,
		}
		since, _, err = bdb.CreateSnapshots(context.Background(), mp, map[int64]uint64{})
		require.NoError(t, err)

		filename = fmt.Sprintf("snapshot-%d.badger", since)
		uploadedFile2, err := cloudStorage.UploadReader(context.Background(), filename, buf)
		require.NoError(t, err)
		files, err = minioContainer.Contents(context.Background(), "")
		require.NoError(t, err)
		require.Len(t, files, 2)
		t.Logf("2nd snapshot created: %+v", uploadedFile2)

		// Load 1st snapshot into a new BadgerDB
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		newBdb, err := New(conf, logger.NOP)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, newBdb.Close())
		})

		tmpFile, err := os.CreateTemp(t.TempDir(), "snapshot-*.badger")
		require.NoError(t, err)
		defer func(name string) {
			_ = os.Remove(name)
		}(tmpFile.Name())
		err = cloudStorage.Download(context.Background(), tmpFile, uploadedFile1.ObjectName)
		require.NoError(t, err)
		err = newBdb.LoadSnapshots(context.Background(), tmpFile)
		require.NoError(t, err)

		exists, err = newBdb.Get(map[int64][]string{0: {"key1", "key2", "key3", "key4"}},
			map[string]int{"key1": 0, "key2": 1, "key3": 2, "key4": 3})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, false, false}, exists)

		tmpFile2, err := os.CreateTemp(t.TempDir(), "snapshot-*.badger")
		require.NoError(t, err)
		defer func(name string) {
			_ = os.Remove(name)
		}(tmpFile2.Name())
		err = cloudStorage.Download(context.Background(), tmpFile2, uploadedFile2.ObjectName)
		require.NoError(t, err)
		err = newBdb.LoadSnapshots(context.Background(), tmpFile2)
		require.NoError(t, err)

		exists, err = newBdb.Get(map[int64][]string{0: {"key1", "key2", "key3", "key4"}},
			map[string]int{"key1": 0, "key2": 1, "key3": 2, "key4": 3})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, true}, exists)
	}

	t.Run("no compression", func(t *testing.T) {
		run(t, false)
	})

	t.Run("compression", func(t *testing.T) {
		run(t, true)
	})
}

func TestCancelSnapshot(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 1 * time.Minute

	run := func(t *testing.T, compress bool) {
		t.Parallel()

		conf := config.New()
		conf.Set("BadgerDB.Dedup.Compress", compress)
		conf.Set("BadgerDB.Dedup.Path", t.TempDir())
		bdb, err := New(conf, logger.NOP)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, bdb.Close())
		})

		err = bdb.Put(map[int64][]string{0: {"key1", "key2"}}, time.Hour)
		require.NoError(t, err)

		exists, err := bdb.Get(map[int64][]string{0: {"key1", "key2"}}, map[string]int{"key1": 0, "key2": 1})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true}, exists)

		// First we try to create a snapshot with a canceled context then we try again to see if we're in a bad state
		snapshotCtx, cancelSnapshot := context.WithCancel(context.Background())
		cancelSnapshot()
		since, _, err := bdb.CreateSnapshots(snapshotCtx, map[int64]io.Writer{0: &bytes.Buffer{}}, map[int64]uint64{})
		require.ErrorContains(t, err, context.Canceled.Error())
		require.EqualValues(t, 0, since) // since shouldn't have been updated

		// Create snapshot
		buf := new(bytes.Buffer)
		mp := map[int64]io.Writer{
			0: buf,
		}
		since, _, err = bdb.CreateSnapshots(context.Background(), mp, map[int64]uint64{})
		require.NoError(t, err)

		filename := fmt.Sprintf("snapshot-%d.badger", since)

		minioContainer, err := miniokit.Setup(pool, t)
		require.NoError(t, err)
		cloudStorage := keydbth.GetCloudStorage(t, conf, minioContainer)

		uploadedFile1, err := cloudStorage.UploadReader(context.Background(), filename, buf)
		require.NoError(t, err)
		files, err := minioContainer.Contents(context.Background(), "")
		require.NoError(t, err)
		require.Len(t, files, 1)
		t.Logf("Snapshot created: %+v", uploadedFile1)
	}

	t.Run("no compression", func(t *testing.T) {
		run(t, false)
	})

	t.Run("compression", func(t *testing.T) {
		run(t, true)
	})
}

func TestSnapshotContextCancellationResourceCleanup(t *testing.T) {
	conf := config.New()
	conf.Set("BadgerDB.Dedup.Compress", true)
	conf.Set("BadgerDB.Dedup.Path", t.TempDir())

	bdb, err := New(conf, logger.NOP)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bdb.Close())
	})

	// Add some data to make the snapshot process take some time
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	err = bdb.Put(map[int64][]string{0: keys}, time.Hour)
	require.NoError(t, err)

	// Create a context that we'll cancel during snapshot creation
	snapshotCtx, cancel := context.WithCancel(context.Background())

	// Run snapshot creation in a separate goroutine
	snapshotErr := make(chan error, 1)
	go func() {
		_, _, err := bdb.CreateSnapshots(snapshotCtx, map[int64]io.Writer{0: &bytes.Buffer{}}, map[int64]uint64{})
		snapshotErr <- err
	}()

	// Give the snapshot process a moment to start, then cancel the context
	time.Sleep(100 * time.Nanosecond)
	cancel()

	// Wait for the snapshot process to complete
	err = <-snapshotErr
	close(snapshotErr)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "Expected context.Canceled error, got: %v", err)

	// Try to create another snapshot to ensure the cache is in a good state
	_, _, err = bdb.CreateSnapshots(context.Background(), map[int64]io.Writer{0: &bytes.Buffer{}}, map[int64]uint64{})
	require.NoError(t, err, "Cache should be in a good state after context cancellation")
}
