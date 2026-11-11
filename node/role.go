package node

// isShadow returns true when this pod is listed as the shadow for its own node index, and after a promotion until the
// node it was filled from is gone, as that node may still be handing its index over (see awaitPredecessor).
func (s *Service) isShadow() bool {
	return s.config.AdvertisedAddress != "" &&
		(s.shadowAddress() == s.config.AdvertisedAddress || s.predecessor.Load() != nil)
}

// replaced returns true when the configuration gives this pod's node index to another pod and doesn't list this pod
// as its shadow either: the other pod is taking over, so writes this pod acknowledged now would never reach it.
// A pod without an advertised address, or whose index is past the end of the list, is never replaced.
func (s *Service) replaced() bool {
	active := s.activeAddress()
	return s.config.AdvertisedAddress != "" && active != "" && active != s.config.AdvertisedAddress && !s.isShadow()
}

// shadowAddress returns the address of the shadow listed for this node's index, or an empty string.
func (s *Service) shadowAddress() string { return s.addressAt(s.config.ShadowAddresses) }

// activeAddress returns the address listed for this node's index, or an empty string.
func (s *Service) activeAddress() string { return s.addressAt(s.config.Addresses) }

// addressAt returns the entry of list for this node's index, or an empty string.
func (s *Service) addressAt(list func() []string) string {
	if list == nil {
		return ""
	}
	addresses := list()
	if s.config.NodeID < 0 || s.config.NodeID >= int64(len(addresses)) {
		return ""
	}
	return addresses[s.config.NodeID]
}
