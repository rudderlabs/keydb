package node

import (
	pb "github.com/rudderlabs/keydb/proto"
)

// role returns SHADOW when this pod is listed as the shadow for its own node index, ACTIVE otherwise.
func (s *Service) role() pb.NodeRole {
	if s.config.AdvertisedAddress == "" {
		return pb.NodeRole_ACTIVE
	}
	if s.shadowAddress() == s.config.AdvertisedAddress {
		return pb.NodeRole_SHADOW
	}
	return pb.NodeRole_ACTIVE
}

// shadowAddress returns the address of the shadow listed for this node's index, or an empty string.
func (s *Service) shadowAddress() string {
	if s.config.ShadowAddresses == nil {
		return ""
	}
	shadows := s.config.ShadowAddresses()
	if s.config.NodeID < 0 || s.config.NodeID >= int64(len(shadows)) {
		return ""
	}
	return shadows[s.config.NodeID]
}

// replaced returns true when the configuration gives this pod's node index to another pod and doesn't list this pod
// as its shadow either: the other pod is taking over, so writes this pod acknowledged now would never reach it.
// A pod without an advertised address, or whose index is past the end of the list, is never replaced.
func (s *Service) replaced() bool {
	if s.config.AdvertisedAddress == "" {
		return false
	}
	active := s.activeAddress()
	return active != "" && active != s.config.AdvertisedAddress && s.shadowAddress() != s.config.AdvertisedAddress
}

// activeAddress returns the address listed for this node's index, or an empty string.
func (s *Service) activeAddress() string {
	if s.config.Addresses == nil {
		return ""
	}
	addresses := s.config.Addresses()
	if s.config.NodeID < 0 || s.config.NodeID >= int64(len(addresses)) {
		return ""
	}
	return addresses[s.config.NodeID]
}
