package cloudstorage

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func GetCloudStorage(conf *config.Config, log logger.Logger) (*filemanager.S3Manager, error) {
	var (
		bucket           = conf.GetStringVar("", "Storage.Bucket", "AWS_BUCKET")
		regionHint       = conf.GetStringVar("us-east-1", "Storage.RegionHint", "AWS_S3_REGION_HINT")
		endpoint         = conf.GetStringVar("", "Storage.Endpoint")
		accessKeyID      = conf.GetStringVar("", "Storage.AccessKeyId", "AWS_ACCESS_KEY_ID")
		accessKey        = conf.GetStringVar("", "Storage.AccessKey", "AWS_SECRET_ACCESS_KEY")
		s3ForcePathStyle = conf.GetBoolVar(false, "Storage.S3ForcePathStyle")
		disableSSL       = conf.GetBoolVar(false, "Storage.DisableSsl")
		enableSSE        = conf.GetBoolVar(false, "Storage.EnableSse", "AWS_ENABLE_SSE")
		useGlue          = conf.GetBoolVar(false, "Storage.UseGlue")
		region           = conf.GetStringVar("us-east-1", "Storage.Region", "AWS_DEFAULT_REGION")
	)

	s3Config := map[string]any{
		"bucketName":       bucket,
		"endpoint":         endpoint,
		"accessKeyID":      accessKeyID,
		"accessKey":        accessKey,
		"s3ForcePathStyle": s3ForcePathStyle,
		"disableSSL":       disableSSL,
		"enableSSE":        enableSSE,
		"regionHint":       regionHint,
		"useGlue":          useGlue,
		"region":           region,
	}

	return filemanager.NewS3Manager(
		conf,
		s3Config,
		log.Withn(logger.NewStringField("component", "cloudStorage")),
		func() time.Duration {
			return conf.GetDuration("Storage.DefaultTimeout", 180, time.Second)
		},
	)
}
