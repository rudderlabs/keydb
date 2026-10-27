package node

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

// shadowSourceKey is the internal key where a shadow remembers the node it is being filled from
const shadowSourceKey = "_shadow_source"

// advanceSnapshotTimestamp stores the timestamp for a hash range, never moving an existing marker backwards.
// A marker is stored even for an empty delta, so that "synced, nothing there" can be told apart from "never synced".
func (s *Service) advanceSnapshotTimestamp(hashRange int64, timestamp uint64) error {
	existing, found, err := s.getSnapshotTimestamp(hashRange)
	if err != nil {
		return err
	}
	if found && existing >= timestamp {
		return nil
	}
	return s.setSnapshotTimestamp(hashRange, timestamp)
}

// allRangesSynced returns true when every hash range this node owns has a since marker,
// meaning the node received at least one full pass for each of them.
func (s *Service) allRangesSynced() bool {
	for hashRange := range s.getCurrentRanges() {
		_, found, err := s.getSnapshotTimestamp(hashRange)
		if err != nil {
			s.logger.Warnn("Checking snapshot timestamp", logger.NewIntField("hashRange", hashRange), obskit.Error(err))
			return false
		}
		if !found {
			return false
		}
	}
	return true
}

// checkShadowSource makes sure a shadow only ever receives data from one source.
// The first time it receives data it remembers the source. If it already holds data without remembering a source,
// or remembers a different one, its disk is a leftover from an earlier move and it must not merge into it.
// Senders that don't provide their address are not checked.
func (s *Service) checkShadowSource(source string) error {
	if source == "" {
		return nil
	}
	value, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
	if err != nil {
		return fmt.Errorf("reading the shadow source: %w", err)
	}
	if found {
		if string(value) != source {
			return fmt.Errorf("leftover disk: it holds data from %q, refusing data from %q", string(value), source)
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
	if err := s.cache.PutValue(shadowSourceKey, internalKeysHashRange, []byte(source), 0); err != nil {
		return fmt.Errorf("storing the shadow source: %w", err)
	}
	return nil
}

// forgetShadowSource drops the shadow source marker once this node is the active one for its index. Slot addresses
// come back every two moves, so a disk that keeps the marker would pass the leftover check if it were ever reused
// as a shadow again: without the marker, any data on it marks it as a leftover.
func (s *Service) forgetShadowSource() {
	if s.config.AdvertisedAddress == "" || s.config.Addresses == nil {
		return
	}
	// a load must not run under a transaction: when one holds the locks, the next reconcile tick tries again
	if !s.mu.TryRLock() {
		return
	}
	defer s.mu.RUnlock()
	if !s.loadMu.TryRLock() {
		return
	}
	defer s.loadMu.RUnlock()
	addresses := s.config.Addresses()
	if s.config.NodeID < 0 || s.config.NodeID >= int64(len(addresses)) ||
		addresses[s.config.NodeID] != s.config.AdvertisedAddress {
		return
	}
	_, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
	if err != nil {
		s.logger.Warnn("Reading the shadow source", obskit.Error(err))
		return
	}
	if !found {
		return
	}
	if err := s.cache.DeleteValue(shadowSourceKey, internalKeysHashRange); err != nil {
		s.logger.Warnn("Forgetting the shadow source", obskit.Error(err))
		return
	}
	s.logger.Infon("Forgot the shadow source now that the node is active")
}
