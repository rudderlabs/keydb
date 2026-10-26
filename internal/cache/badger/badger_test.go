package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	miniokit "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"

	keydbth "github.com/rudderlabs/keydb/internal/testhelper"
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
		err = newBdb.LoadSnapshot(context.Background(), tmpFile)
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
		err = newBdb.LoadSnapshot(context.Background(), tmpFile2)
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

func TestGetPutValue(t *testing.T) {
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

		hashRange := int64(5)

		// Test GetValue on non-existent key
		value, found, err := bdb.GetValue("nonexistent", hashRange)
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)

		// Test PutValue and GetValue with string data
		testKey := "test-key"
		testValue := []byte("test-value-123")
		err = bdb.PutValue(testKey, hashRange, testValue, time.Hour)
		require.NoError(t, err)

		value, found, err = bdb.GetValue(testKey, hashRange)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, testValue, value)

		// Test PutValue and GetValue with binary data (uint64 timestamp)
		timestampKey := "snapshot-timestamp"
		var timestamp uint64 = 1234567890123456789
		timestampBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(timestampBytes, timestamp)

		err = bdb.PutValue(timestampKey, hashRange, timestampBytes, time.Hour)
		require.NoError(t, err)

		value, found, err = bdb.GetValue(timestampKey, hashRange)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, value, 8)
		retrievedTimestamp := binary.BigEndian.Uint64(value)
		require.Equal(t, timestamp, retrievedTimestamp)

		// Test updating an existing value
		var newTimestamp uint64 = 9876543210987654321
		binary.BigEndian.PutUint64(timestampBytes, newTimestamp)
		err = bdb.PutValue(timestampKey, hashRange, timestampBytes, time.Hour)
		require.NoError(t, err)

		value, found, err = bdb.GetValue(timestampKey, hashRange)
		require.NoError(t, err)
		require.True(t, found)
		retrievedTimestamp = binary.BigEndian.Uint64(value)
		require.Equal(t, newTimestamp, retrievedTimestamp)

		// Test different hash ranges are independent
		otherHashRange := int64(10)
		err = bdb.PutValue(testKey, otherHashRange, []byte("other-value"), time.Hour)
		require.NoError(t, err)

		// Original hash range should still have original value
		value, found, err = bdb.GetValue(testKey, hashRange)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, testValue, value)

		// Other hash range should have different value
		value, found, err = bdb.GetValue(testKey, otherHashRange)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("other-value"), value)

		// Test PutValue with empty value
		emptyKey := "empty-value-key"
		err = bdb.PutValue(emptyKey, hashRange, []byte{}, time.Hour)
		require.NoError(t, err)

		value, found, err = bdb.GetValue(emptyKey, hashRange)
		require.NoError(t, err)
		require.True(t, found)
		require.Empty(t, value)

		// Test PutValue with nil value (should work like Put)
		nilKey := "nil-value-key"
		err = bdb.PutValue(nilKey, hashRange, nil, time.Hour)
		require.NoError(t, err)

		value, found, err = bdb.GetValue(nilKey, hashRange)
		require.NoError(t, err)
		require.True(t, found)
		require.Empty(t, value)
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
	for i := range 1000 {
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

func TestCrashRecoveryPastOneBillionCommits(t *testing.T) {
	dir := t.TempDir()
	fastForwardCommitTs(t, dir)

	conf := config.New()
	conf.Set("BadgerDB.Dedup.Path", dir)
	bdb, err := New(conf, logger.NOP)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bdb.Close())
	})

	keys := make([]string, 100)
	indexes := make(map[string]int, len(keys))
	for i := range keys {
		keys[i] = "key" + strconv.Itoa(i)
		indexes[keys[i]] = i
		require.NoError(t, bdb.Put(map[int64][]string{0: {keys[i]}}, time.Hour))
	}

	// Copying the directory of an open database leaves the writes in the WAL only, as after a crash.
	crashConf := config.New()
	crashConf.Set("BadgerDB.Dedup.Path", copyDir(t, dir))
	recovered, err := New(crashConf, logger.NOP)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, recovered.Close())
	})

	exists, err := recovered.Get(map[int64][]string{0: keys}, indexes)
	require.NoError(t, err)
	require.NotContains(t, exists, false, "keys lost after replaying the WAL")
}

// fastForwardCommitTs commits a single entry at timestamp 1e9 so that the commit timestamps of a
// database opened later in dir have at least 10 decimal digits.
func fastForwardCommitTs(t *testing.T, dir string) {
	t.Helper()
	const commitTs = 1_000_000_000
	db, err := badger.OpenManaged(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	txn := db.NewTransactionAt(commitTs, true)
	require.NoError(t, txn.SetEntry(badger.NewEntry([]byte("seed"), nil)))
	require.NoError(t, txn.CommitAt(commitTs, nil))
	require.NoError(t, db.Close())
}

// copyDir copies the files of a badger directory, except for its lock file, into a new directory.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		if e.Name() == "LOCK" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600))
	}
	return dst
}

func TestPurgeValueLog(t *testing.T) {
	// leakValueLog writes keys with a threshold that sends the transaction finish markers to the value log,
	// reopening the database a few times so that the markers span several value log files.
	leakValueLog := func(t *testing.T, dir string) ([]string, map[string]int) {
		t.Helper()
		fastForwardCommitTs(t, dir)
		var (
			keys    []string
			indexes = make(map[string]int)
		)
		for range 3 {
			conf := config.New()
			conf.Set("BadgerDB.Dedup.Path", dir)
			conf.Set("BadgerDB.Dedup.valueThreshold", 10)
			bdb, err := New(conf, logger.NOP)
			require.NoError(t, err)
			for range 10 {
				key := "key" + strconv.Itoa(len(keys))
				indexes[key] = len(keys)
				keys = append(keys, key)
				require.NoError(t, bdb.Put(map[int64][]string{0: {key}}, time.Hour))
			}
			require.NoError(t, bdb.Close())
		}
		return keys, indexes
	}

	t.Run("purges all but the last value log file", func(t *testing.T) {
		dir := t.TempDir()
		keys, indexes := leakValueLog(t, dir)
		leaked, err := valueLogFiles(dir)
		require.NoError(t, err)
		require.Greater(t, len(leaked), 2)
		last := leaked[len(leaked)-1].Name()

		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", dir)
		conf.Set("BadgerDB.Dedup.purgeValueLog", true)
		bdb, err := New(conf, logger.NOP)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, bdb.Close())
		})

		remaining, err := valueLogFiles(dir)
		require.NoError(t, err)
		require.Len(t, remaining, 2, "the last value log file and the one badger creates on open")
		require.Equal(t, last, remaining[0].Name())

		exists, err := bdb.Get(map[int64][]string{0: keys}, indexes)
		require.NoError(t, err)
		require.NotContains(t, exists, false)
	})

	t.Run("skips the purge with a small value threshold", func(t *testing.T) {
		dir := t.TempDir()
		leakValueLog(t, dir)
		leaked, err := valueLogFiles(dir)
		require.NoError(t, err)

		conf := config.New()
		conf.Set("BadgerDB.Dedup.Path", dir)
		conf.Set("BadgerDB.Dedup.valueThreshold", 10)
		conf.Set("BadgerDB.Dedup.purgeValueLog", true)
		bdb, err := New(conf, logger.NOP)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, bdb.Close())
		})

		remaining, err := valueLogFiles(dir)
		require.NoError(t, err)
		require.Len(t, remaining, len(leaked)+1, "the leaked value log files and the one badger creates on open")
	})
}
