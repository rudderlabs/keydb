package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
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

func TestHasData(t *testing.T) {
	conf := config.New()
	conf.Set("BadgerDB.Dedup.Path", t.TempDir())
	bdb, err := New(conf, logger.NOP)
	require.NoError(t, err)
	defer func() { require.NoError(t, bdb.Close()) }()

	hasData, err := bdb.HasData()
	require.NoError(t, err)
	require.False(t, hasData, "an empty cache has no data")

	require.NoError(t, bdb.PutValue("_marker", internalHashRange, []byte("x"), 0))
	hasData, err = bdb.HasData()
	require.NoError(t, err)
	require.False(t, hasData, "internal keys are not data")

	require.NoError(t, bdb.Put(map[int64][]string{3: {"key1"}}, time.Hour))
	hasData, err = bdb.HasData()
	require.NoError(t, err)
	require.True(t, hasData)
}

func TestStreamSnapshotRoundTrip(t *testing.T) {
	for _, compress := range []bool{false, true} {
		t.Run("compress="+strconv.FormatBool(compress), func(t *testing.T) {
			source := newStreamTestCache(t, compress)
			require.NoError(t, source.Put(map[int64][]string{0: {"k1", "k2"}, 1: {"other"}}, time.Hour))
			require.NoError(t, source.Put(map[int64][]string{0: {"expired"}}, time.Second))
			time.Sleep(2 * time.Second) // let "expired" expire

			var buf bytes.Buffer
			maxVersion, err := source.StreamSnapshot(context.Background(), 0, 0, &buf, StreamOptions{
				CompressionLevel: 1, BatchSize: 1 << 20,
			})
			require.NoError(t, err)
			require.Positive(t, maxVersion)

			destination := newStreamTestCache(t, compress)
			require.NoError(t, destination.LoadSnapshot(context.Background(), &buf))
			require.Equal(t, []bool{true, true, false}, existsIn(t, destination, 0, "k1", "k2", "expired"))
			require.Equal(t, []bool{false}, existsIn(t, destination, 1, "other"), "other ranges are not streamed")
		})
	}
}

func TestStreamSnapshotUnchangedRangeWritesNothing(t *testing.T) {
	for _, compress := range []bool{false, true} {
		t.Run("compress="+strconv.FormatBool(compress), func(t *testing.T) {
			cache := newStreamTestCache(t, compress)
			require.NoError(t, cache.Put(map[int64][]string{0: {"k1"}}, time.Hour))
			var first bytes.Buffer
			since, err := cache.StreamSnapshot(context.Background(), 0, 0, &first, StreamOptions{})
			require.NoError(t, err)
			require.Positive(t, first.Len())

			var second bytes.Buffer
			maxVersion, err := cache.StreamSnapshot(context.Background(), 0, since, &second, StreamOptions{})
			require.NoError(t, err)
			require.Zero(t, maxVersion)
			require.Zero(t, second.Len(), "the receiver starts a load for any bytes, even an empty zstd frame")
		})
	}
}

// TestLoadSnapshotClosesDecompressor protects the decompressor's C memory, which only Close frees. Go can't read that
// memory and the process's resident size swings by more than it. But Close also gives the reader's two 128 KiB buffers
// back to zstd's pool, so a reader that isn't closed allocates them again on every load.
func TestLoadSnapshotClosesDecompressor(t *testing.T) {
	allocatedPerLoad := func(compress bool) int64 {
		source := newStreamTestCache(t, compress)
		require.NoError(t, source.Put(map[int64][]string{0: {"k1"}}, time.Hour))
		var snapshot bytes.Buffer
		_, err := source.StreamSnapshot(context.Background(), 0, 0, &snapshot, StreamOptions{})
		require.NoError(t, err)
		destination := newStreamTestCache(t, compress)

		const loads = 100
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range loads {
			require.NoError(t, destination.LoadSnapshot(context.Background(), bytes.NewReader(snapshot.Bytes())))
		}
		runtime.ReadMemStats(&after)
		return int64(after.TotalAlloc-before.TotalAlloc) / loads
	}

	extra := allocatedPerLoad(true) - allocatedPerLoad(false)
	require.Less(t, extra, int64(128<<10), "the pool hands out the same buffers, with -race it drops a quarter of them")
}

func TestInterruptedLoadSnapshot(t *testing.T) {
	for _, compress := range []bool{false, true} {
		t.Run("compress="+strconv.FormatBool(compress), func(t *testing.T) {
			source := newStreamTestCache(t, compress)
			for i := range 50 { // raise the source's versions above the destination's
				require.NoError(t, source.Put(map[int64][]string{0: {"k" + strconv.Itoa(i)}}, time.Hour))
			}
			var buf bytes.Buffer
			_, err := source.StreamSnapshot(context.Background(), 0, 0, &buf, StreamOptions{})
			require.NoError(t, err)
			snapshot := buf.Bytes()

			destination := newStreamTestCache(t, compress)
			cut := erroringReader{err: errors.New("stream cancelled")}
			err = destination.LoadSnapshot(context.Background(), io.MultiReader(bytes.NewReader(snapshot), cut))
			require.Error(t, err)

			read := make(chan error, 1)
			go func() {
				_, err := destination.Get(map[int64][]string{0: {"k0"}}, map[string]int{"k0": 0})
				read <- err
			}()
			require.Eventually(t, func() bool { return len(read) == 1 }, 5*time.Second, 10*time.Millisecond,
				"a read after an interrupted load must not block")
			require.NoError(t, <-read)
			require.NoError(t, destination.PutValue("after", internalHashRange, []byte("x"), 0),
				"writes must not block either")

			require.NoError(t, destination.LoadSnapshot(context.Background(), bytes.NewReader(snapshot)),
				"the range can be loaded again")
			require.Equal(t, []bool{true, true}, existsIn(t, destination, 0, "k0", "k49"))
		})
	}
}

func TestStreamSnapshotWaitCalled(t *testing.T) {
	source := newStreamTestCache(t, false)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	require.NoError(t, source.Put(map[int64][]string{0: keys}, time.Hour))

	var buf bytes.Buffer
	var waited int
	_, err := source.StreamSnapshot(context.Background(), 0, 0, &buf, StreamOptions{
		Wait: func(_ context.Context, n int) error {
			waited += n
			return nil
		},
	})
	require.NoError(t, err)
	require.Positive(t, waited)
	require.Equal(t, buf.Len(), waited, "every byte written must go through the budget")

	t.Run("a wait error stops the stream", func(t *testing.T) {
		waitErr := errors.New("paused for good")
		_, err := source.StreamSnapshot(context.Background(), 0, 0, io.Discard, StreamOptions{
			Wait: func(context.Context, int) error { return waitErr },
		})
		require.ErrorIs(t, err, waitErr)
	})
}

func TestStreamSnapshotSince(t *testing.T) {
	source := newStreamTestCache(t, true)
	require.NoError(t, source.Put(map[int64][]string{0: {"old"}}, time.Hour))
	since, err := source.StreamSnapshot(context.Background(), 0, 0, io.Discard, StreamOptions{})
	require.NoError(t, err)

	require.NoError(t, source.Put(map[int64][]string{0: {"new"}}, time.Hour))
	var buf bytes.Buffer
	maxVersion, err := source.StreamSnapshot(context.Background(), 0, since, &buf, StreamOptions{})
	require.NoError(t, err)
	require.Greater(t, maxVersion, since)

	destination := newStreamTestCache(t, true)
	require.NoError(t, destination.LoadSnapshot(context.Background(), &buf))
	require.Equal(t, []bool{false, true}, existsIn(t, destination, 0, "old", "new"))

	t.Run("nothing new", func(t *testing.T) {
		var buf bytes.Buffer
		version, err := source.StreamSnapshot(context.Background(), 0, maxVersion, &buf, StreamOptions{})
		require.NoError(t, err)
		require.Zero(t, version)
	})
}

// The first pass skips expired keys, as not copying them is what keeps a new disk small, but a later pass must stream
// an expired newer version: it is what hides the older live version the receiver holds.
func TestStreamSnapshotExpiredVersions(t *testing.T) {
	ctx := context.Background()
	source := newStreamTestCache(t, true)
	require.NoError(t, source.Put(map[int64][]string{0: {"updated", "stays"}}, time.Hour))
	require.NoError(t, source.Put(map[int64][]string{0: {"expired-early"}}, time.Second))

	time.Sleep(2100 * time.Millisecond) // "expired-early" expires
	var first bytes.Buffer
	since, err := source.StreamSnapshot(ctx, 0, 0, &first, StreamOptions{})
	require.NoError(t, err)
	receiver := newStreamTestCache(t, true)
	require.NoError(t, receiver.LoadSnapshot(ctx, &first))
	require.Equal(t, []bool{true, true}, existsIn(t, receiver, 0, "updated", "stays"))

	onlyExpired := newStreamTestCache(t, false)
	require.NoError(t, onlyExpired.Put(map[int64][]string{0: {"gone"}}, time.Second))
	time.Sleep(2100 * time.Millisecond)
	var skipped bytes.Buffer
	_, err = onlyExpired.StreamSnapshot(ctx, 0, 0, &skipped, StreamOptions{})
	require.NoError(t, err)
	require.Zero(t, skipped.Len(), "the first pass must not copy expired keys")

	require.NoError(t, source.Put(map[int64][]string{0: {"updated"}}, time.Second))
	time.Sleep(2100 * time.Millisecond)
	require.Equal(t, []bool{false, true}, existsIn(t, source, 0, "updated", "stays"))

	var next bytes.Buffer
	version, err := source.StreamSnapshot(ctx, 0, since, &next, StreamOptions{})
	require.NoError(t, err)
	require.Greater(t, version, since, "the marker must move past the expired version")
	require.NoError(t, receiver.LoadSnapshot(ctx, &next))
	require.Equal(t, []bool{false, true}, existsIn(t, receiver, 0, "updated", "stays"),
		"the receiver must stop answering for a key that expired on the source")

	var again bytes.Buffer
	version, err = source.StreamSnapshot(ctx, 0, version, &again, StreamOptions{})
	require.NoError(t, err)
	require.Zero(t, version)
	require.Zero(t, again.Len(), "an expired version is streamed once")
}

func TestStreamSnapshotBatchesBounded(t *testing.T) {
	// a range much bigger than a batch
	source := newStreamTestCache(t, false)
	keys := make([]string, 50_000)
	for b := range 12 {
		for i := range keys {
			keys[i] = "a-reasonably-long-key-" + strconv.Itoa(b) + "-" + strconv.Itoa(i)
		}
		require.NoError(t, source.Put(map[int64][]string{0: keys}, time.Hour))
	}

	var largest, total int
	_, err := source.StreamSnapshot(context.Background(), 0, 0, io.Discard, StreamOptions{
		BatchSize: 64 << 10,
		Wait: func(_ context.Context, n int) error {
			largest = max(largest, n)
			total += n
			return nil
		},
	})
	require.NoError(t, err)
	t.Logf("streamed %d bytes, largest batch %d bytes", total, largest)
	require.Greater(t, total, 20<<20)
	require.Less(t, largest, total/2, "the range must be streamed in batches, never held whole in memory")
	require.LessOrEqual(t, largest, 128<<10, "a batch is bounded by the batch size, not by the range size")
}

func TestStreamSnapshotIncrementalAllocations(t *testing.T) {
	source := newStreamTestCache(t, true)
	keys := make([]string, 50_000)
	for b := range 4 {
		for i := range keys {
			keys[i] = "a-reasonably-long-key-" + strconv.Itoa(b) + "-" + strconv.Itoa(i)
		}
		require.NoError(t, source.Put(map[int64][]string{0: keys}, time.Hour))
	}
	since, err := source.StreamSnapshot(context.Background(), 0, 0, io.Discard, StreamOptions{})
	require.NoError(t, err)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	version, err := source.StreamSnapshot(context.Background(), 0, since, io.Discard, StreamOptions{})
	require.NoError(t, err)
	runtime.ReadMemStats(&after)
	require.Zero(t, version)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("an incremental pass with nothing new allocated %d bytes", allocated)
	require.Less(t, allocated, uint64(8<<20), "an incremental pass over unchanged data must stay cheap")
}

// A range that is mostly expired must not be scanned to its end without consulting Wait: a pause or the latency brake
// that starts mid-scan has to stop the scan, though no batch is ever written.
func TestStreamSnapshotScanConsultsWait(t *testing.T) {
	source := newStreamTestCache(t, false)
	keys := make([]string, 200_000)
	for i := range keys {
		keys[i] = "an-expired-key-with-a-realistic-length-" + strconv.Itoa(i)
	}
	require.NoError(t, source.Put(map[int64][]string{0: keys}, time.Second))
	time.Sleep(2100 * time.Millisecond)

	t.Run("wait is called while skipping", func(t *testing.T) {
		var calls, waited int
		_, err := source.StreamSnapshot(context.Background(), 0, 0, io.Discard, StreamOptions{
			Wait: func(_ context.Context, n int) error {
				calls++
				waited += n
				return nil
			},
		})
		require.NoError(t, err)
		require.Positive(t, calls, "a scan over expired keys never consulted wait")
		require.Positive(t, waited)
	})

	t.Run("a pause stops the scan", func(t *testing.T) {
		paused := errors.New("paused")
		var buf bytes.Buffer
		_, err := source.StreamSnapshot(context.Background(), 0, 0, &buf, StreamOptions{
			Wait: func(context.Context, int) error { return paused },
		})
		require.ErrorIs(t, err, paused, "the scan ran to the end despite the pause")
		require.Zero(t, buf.Len(), "no empty batch is written")
	})

	t.Run("a pause starting mid-scan stops it early", func(t *testing.T) {
		paused := errors.New("paused")
		var calls int
		_, err := source.StreamSnapshot(context.Background(), 0, 0, io.Discard, StreamOptions{
			Wait: func(context.Context, int) error {
				calls++
				if calls > 3 {
					return paused
				}
				return nil
			},
		})
		require.ErrorIs(t, err, paused)
		require.Equal(t, 4, calls, "the scan must stop at the first refused wait")
	})
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

func newStreamTestCache(t *testing.T, compress bool) *Cache {
	t.Helper()
	conf := config.New()
	conf.Set("BadgerDB.Dedup.Path", t.TempDir())
	conf.Set("BadgerDB.Dedup.Compress", compress)
	bdb, err := New(conf, logger.NOP)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bdb.Close()) })
	return bdb
}

func existsIn(t *testing.T, c *Cache, hashRange int64, keys ...string) []bool {
	t.Helper()
	indexes := make(map[string]int, len(keys))
	for i, k := range keys {
		indexes[k] = i
	}
	exists, err := c.Get(map[int64][]string{hashRange: keys}, indexes)
	require.NoError(t, err)
	return exists
}

// erroringReader fails every read, like a snapshot stream cut short.
type erroringReader struct{ err error }

func (r erroringReader) Read([]byte) (int, error) { return 0, r.err }
