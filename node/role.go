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
