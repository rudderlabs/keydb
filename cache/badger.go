package cache

import (
	"fmt"
	"os"
	"path"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/keydb/internal/cache/badger"
)

func BadgerFactory(conf *config.Config, log logger.Logger) func(hashRange uint32) (*badger.Cache, error) {
	return func(hashRange uint32) (*badger.Cache, error) {
		badgerPath := conf.GetString("BadgerDB.Dedup.Path", "/tmp/badger")
		badgerPath = path.Join(badgerPath, fmt.Sprintf("%d", hashRange))
		if err := os.MkdirAll(badgerPath, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create badger directory for hash range %d: %w", hashRange, err)
		}
		if conf.GetBool("BadgerDB.TestMode", false) {
			// TODO use this to enable usage of String() string and Len() methods
		}
		badgerCache, err := badger.New(badgerPath, conf, log)
		if err != nil {
			return nil, fmt.Errorf("failed to create cache factory: %w", err)
		}
		return badgerCache, nil
	}
}
