package node

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc/peer"

	"github.com/rudderlabs/rudder-go-kit/logger"

	pb "github.com/rudderlabs/keydb/proto"
)

const (
	// predecessorPollInterval is how often a promoted shadow checks whether the node it was filled from is gone
	predecessorPollInterval = 500 * time.Millisecond
	// predecessorPollTimeout bounds each check: a node that doesn't answer in time counts as gone
	predecessorPollTimeout = 2 * time.Second
	// predecessorMaxWait bounds the wait of a promoted shadow for the node it was filled from, which normally ends
	// within its handover timeout of getting SIGTERM
	predecessorMaxWait = time.Minute
)

// predecessor is a node this one was filled from as a shadow: its advertised address, and the address it sent from
// with the advertised port. A terminating pod leaves the headless Service at once, so its name stops resolving while
// it still sends its handover, and only the direct address reaches it then.
type predecessor struct {
	address, direct string
}

// Handover hands the node's index over when it shuts down during a move, so that the move loses no write: to the
// shadow it fills, or to the pod the configuration gave its index to. The node stops acknowledging Get and Put, waits
// for those in flight, then sends what changed since its last pass. It serves nothing anymore, so that pass ignores
// the byte budget and the brake, though not a pause. ctx bounds it. A node that isn't being moved does nothing.
func (s *Service) Handover(ctx context.Context) {
	successor := s.successor()
	if successor == "" {
		return
	}
	s.mu.Lock() // waits for the Get and Put requests in flight, the later ones see handingOver
	s.handingOver.Store(true)
	s.mu.Unlock()
	s.replication.handover(ctx, successor)
}

// successor returns the node taking this node's index over: its shadow, or the pod the configuration gave its index
// to. It is empty for a node that isn't being moved, or that is a shadow itself.
func (s *Service) successor() string {
	if s.config.AdvertisedAddress == "" || s.isShadow() {
		return ""
	}
	if shadow := s.shadowAddress(); shadow != "" {
		return shadow
	}
	if s.replaced() {
		return s.activeAddress()
	}
	return ""
}

// awaitPredecessor keeps a node that the configuration promoted a shadow while the node it was filled from still
// answers, and for at most predecessorMaxWait (see isShadow): that node may still be sending its handover, which must
// land before the promoted node serves. The configmap refresh that promotes it can come before the old node's
// SIGTERM, or while the handover runs. It waits in the background, starting at most one wait at a time.
func (s *Service) awaitPredecessor(ctx context.Context) {
	p := s.predecessor.Load()
	if p == nil || s.shadowAddress() == s.config.AdvertisedAddress ||
		!s.awaitingPredecessor.CompareAndSwap(false, true) {
		return
	}
	s.waitGroup.Go(func() {
		defer s.awaitingPredecessor.Store(false)
		log := s.logger.Withn(
			logger.NewStringField("predecessor", p.address),
			logger.NewStringField("predecessorDirect", p.direct),
		)
		var nodes []pb.NodeServiceClient
		for _, address := range []string{p.address, p.direct} {
			if address == "" || (address == p.direct && p.direct == p.address) {
				continue
			}
			conn, node, err := s.connectToNode(address)
			if err != nil {
				continue
			}
			defer s.closeNodeConnection(conn)
			nodes = append(nodes, node)
		}
		log.Infon("Waiting for the node this one was filled from to stop before serving")
		deadline := s.now().Add(predecessorMaxWait)
		// a node with a shadow of its own is past its switch, even though the address it was filled from may
		// answer again: slot addresses come back every two moves
		for s.shadowAddress() == "" && anyAnswers(ctx, nodes) {
			if s.now().After(deadline) {
				log.Warnn("The node this one was filled from still answers, serving anyway")
				break
			}
			if !sleepCtx(ctx, predecessorPollInterval) {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		// a stream from another source stored another pointer meanwhile: the next reconcile waits for that one
		if s.predecessor.CompareAndSwap(p, nil) {
			log.Infon("The node this one was filled from is gone, serving")
		}
	})
}

// anyAnswers tells whether one of nodes answers a GetNodeInfo within predecessorPollTimeout
func anyAnswers(ctx context.Context, nodes []pb.NodeServiceClient) bool {
	for _, node := range nodes {
		pollCtx, cancel := context.WithTimeout(ctx, predecessorPollTimeout)
		_, err := node.GetNodeInfo(pollCtx, &pb.GetNodeInfoRequest{})
		cancel()
		if err == nil {
			return true
		}
	}
	return false
}

// directAddress returns the address a stream's sender sent from, with the port of its advertised address, or an
// empty string when either is unknown
func directAddress(ctx context.Context, advertised string) string {
	from, ok := peer.FromContext(ctx)
	if !ok || from.Addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(from.Addr.String())
	if err != nil {
		return ""
	}
	_, port, err := net.SplitHostPort(advertised)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, port)
}
