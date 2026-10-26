package node

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/rudderlabs/keydb/client"
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
	if s.config.NodeID < 0 || int(s.config.NodeID) >= len(shadows) {
		return ""
	}
	return shadows[s.config.NodeID]
}

// trackLegacyClient records a Get or Put from a client that doesn't send the features header.
// Such a client never learns that a node moved, so readiness to switch waits until none has been seen for a while.
func (s *Service) trackLegacyClient(ctx context.Context) {
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(client.FeaturesHeader)) > 0 {
		return
	}
	s.lastLegacyClientRequest.Store(s.now().Unix())
	s.metrics.legacyClientRequests.Increment()
}
