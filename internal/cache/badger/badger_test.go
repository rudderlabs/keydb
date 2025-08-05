package badger

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/keydb/internal/cloudstorage"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	accessKeyId     = "MYACCESSKEYID"
	secretAccessKey = "MYSECRETACCESSKEY"
	region          = "MYREGION"
	bucket          = "bucket-name"
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

		err = bdb.Put(map[uint32][]string{0: {"key1", "key2"}}, time.Hour)
		require.NoError(t, err)

		exists, err := bdb.Get(map[uint32][]string{0: {"key1", "key2"}}, map[string]int{"key1": 0, "key2": 1})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true}, exists)

		// Create snapshot
		buf := new(bytes.Buffer)
		mp := map[uint32]io.Writer{
			0: buf,
		}
		since, _, err := bdb.CreateSnapshots(context.Background(), mp)
		require.NoError(t, err)

		filename := fmt.Sprintf("snapshot-%d.badger", since)
		minioClient, cloudStorage := getCloudStorage(t, pool, conf)
		uploadedFile1, err := cloudStorage.UploadReader(context.Background(), filename, buf)
		require.NoError(t, err)
		files, err := getContents(context.Background(), bucket, "", minioClient)
		require.NoError(t, err)
		require.Len(t, files, 1)
		t.Logf("1st snapshot created: %+v", uploadedFile1)

		err = bdb.Put(map[uint32][]string{0: {"key3", "key4"}}, time.Hour)
		require.NoError(t, err)
		exists, err = bdb.Get(map[uint32][]string{0: {"key1", "key2", "key3", "key4"}},
			map[string]int{"key1": 0, "key2": 1, "key3": 2, "key4": 3})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, true, true}, exists)

		// Create another snapshot which should contain also key3 and key4
		buf.Reset()
		mp = map[uint32]io.Writer{
			0: buf,
		}
		since, _, err = bdb.CreateSnapshots(context.Background(), mp)
		require.NoError(t, err)

		filename = fmt.Sprintf("snapshot-%d.badger", since)
		uploadedFile2, err := cloudStorage.UploadReader(context.Background(), filename, buf)
		require.NoError(t, err)
		files, err = getContents(context.Background(), bucket, "", minioClient)
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

		exists, err = newBdb.Get(map[uint32][]string{0: {"key1", "key2", "key3", "key4"}},
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

		exists, err = newBdb.Get(map[uint32][]string{0: {"key1", "key2", "key3", "key4"}},
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

		err = bdb.Put(map[uint32][]string{0: {"key1", "key2"}}, time.Hour)
		require.NoError(t, err)

		exists, err := bdb.Get(map[uint32][]string{0: {"key1", "key2"}}, map[string]int{"key1": 0, "key2": 1})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true}, exists)

		// First we try to create a snapshot with a canceled context then we try again to see if we're in a bad state
		snapshotCtx, cancelSnapshot := context.WithCancel(context.Background())
		cancelSnapshot()
		_, _, err = bdb.CreateSnapshots(snapshotCtx, map[uint32]io.Writer{0: new(bytes.Buffer)})
		require.ErrorContains(t, err, context.Canceled.Error())
		require.EqualValues(t, 0, bdb.snapshotSince) // since shouldn't have been updated

		// Create snapshot
		buf := new(bytes.Buffer)
		mp := map[uint32]io.Writer{
			0: buf,
		}
		since, _, err := bdb.CreateSnapshots(context.Background(), mp)
		require.NoError(t, err)

		filename := fmt.Sprintf("snapshot-%d.badger", since)
		minioClient, cloudStorage := getCloudStorage(t, pool, conf)
		uploadedFile1, err := cloudStorage.UploadReader(context.Background(), filename, buf)
		require.NoError(t, err)
		files, err := getContents(context.Background(), bucket, "", minioClient)
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
