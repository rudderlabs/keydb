package testhelper

import (
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/keydb/internal/cloudstorage"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	miniokit "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
)

func RequireExpectedFiles(
	ctx context.Context, t testing.TB, minio *miniokit.Resource, folder string, expectedFiles ...*regexp.Regexp,
) {
	t.Helper()

	files, err := minio.Contents(ctx, folder+"/hr_")
	require.NoError(t, err)
	require.Len(t, files, len(expectedFiles))

	for _, file := range files {
		var found bool
		for _, expectedFile := range expectedFiles {
			if expectedFile.MatchString(file.Key) {
				found = true
				break
			}
		}
		require.True(t, found, "Unexpected file: %s", file.Key)
	}
}

func GetCloudStorage(t testing.TB, conf *config.Config, minio *miniokit.Resource) filemanager.S3Manager {
	t.Helper()

	conf.Set("Storage.Bucket", minio.BucketName)
	conf.Set("Storage.Endpoint", minio.Endpoint)
	conf.Set("Storage.AccessKeyId", minio.AccessKeyID)
	conf.Set("Storage.AccessKey", minio.AccessKeySecret)
	conf.Set("Storage.Region", minio.Region)
	conf.Set("Storage.DisableSsl", true)
	conf.Set("Storage.S3ForcePathStyle", true)
	conf.Set("Storage.UseGlue", true)

	cloudStorage, err := cloudstorage.GetCloudStorage(conf, logger.NOP)
	require.NoError(t, err)

	return cloudStorage
}
