package node

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

// shadowSourceKey is the internal key where a shadow remembers the node it is being filled from, as sourceID
const shadowSourceKey = "_shadow_source"

// sourceID is how a shadow remembers its source: 8 bytes, because the value log purge relies on keydb storing no
// value longer than that (see purgeValueLog), and an address can be longer than the value threshold.
func sourceID(source string) []byte {
	sum := sha256.Sum256([]byte(source))
	return sum[:8]
}

// advanceSnapshotTimestamp stores the timestamp for a hash range, never moving an existing marker backwards.
func (s *Service) advanceSnapshotTimestamp(hashRange int64, timestamp uint64) error {
	existing, err := s.getSnapshotTimestamp(hashRange)
	if err != nil || existing >= timestamp {
		return err
	}
	return s.setSnapshotTimestamp(hashRange, timestamp)
}

// sinceMarker returns the marker a sender must stream a hash range from. A shadow that has no source yet answers 0:
// any marker on its disk is a version of an earlier source, which checkShadowSource only drops once a chunk arrives.
func (s *Service) sinceMarker(hashRange int64) (uint64, error) {
	if s.isShadow() {
		_, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
		if err != nil || !found {
			return 0, err
		}
	}
	return s.getSnapshotTimestamp(hashRange)
}

// checkShadowSource makes sure a shadow only ever receives data from one source.
// The first time it receives data it remembers the source. If it already holds data without remembering a source,
// or remembers a different one, its disk is a leftover from an earlier move and it must not merge into it.
func (s *Service) checkShadowSource(source string) error {
	if source == "" {
		return errors.New("no source address")
	}
	value, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
	if err != nil {
		return fmt.Errorf("reading the shadow source: %w", err)
	}
	if found {
		if !bytes.Equal(value, sourceID(source)) {
			return fmt.Errorf("leftover disk: it holds data from another source, refusing data from %q", source)
		}
		return nil
	}
	hasData, err := s.cache.HasData()
	if err != nil {
		return fmt.Errorf("checking for existing data: %w", err)
	}
	if hasData {
		return fmt.Errorf("leftover disk: it holds data from an unknown source, refusing data from %q", source)
	}
	// markers left by an earlier source are versions of that node, which would make this one skip data
	if err := s.forgetSnapshotTimestamps(); err != nil {
		return err
	}
	if err := s.cache.PutValue(shadowSourceKey, internalKeysHashRange, sourceID(source), 0); err != nil {
		return fmt.Errorf("storing the shadow source: %w", err)
	}
	return nil
}

// forgetShadowSource drops the shadow source marker once this node is the active one for its index, and returns true
// once there is no marker left to drop. Slot addresses come back every two moves, so a disk that keeps the marker
// would pass the leftover check if it were ever reused as a shadow again: without it, any data marks a leftover.
func (s *Service) forgetShadowSource() bool {
	if s.config.AdvertisedAddress == "" {
		return true
	}
	// a load must not run under a transaction: when one holds the locks, the next reconcile tick tries again
	if s.activeAddress() != s.config.AdvertisedAddress || !s.mu.TryRLock() {
		return false
	}
	defer s.mu.RUnlock()
	if !s.loadMu.TryRLock() {
		return false
	}
	defer s.loadMu.RUnlock()
	_, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
	if err == nil && found {
		err = s.cache.DeleteValue(shadowSourceKey, internalKeysHashRange)
		if err == nil {
			s.logger.Infon("Forgot the shadow source now that the node is active")
		}
	}
	if err != nil {
		s.logger.Warnn("Forgetting the shadow source", obskit.Error(err))
	}
	return err == nil
}

// forgetSnapshotTimestamps drops every since marker.
func (s *Service) forgetSnapshotTimestamps() error {
	for hashRange := range s.config.TotalHashRanges {
		if err := s.cache.DeleteValue(getSnapshotTimestampKey(hashRange), internalKeysHashRange); err != nil {
			return fmt.Errorf("deleting the snapshot timestamp of hash range %d: %w", hashRange, err)
		}
	}
	return nil
}
