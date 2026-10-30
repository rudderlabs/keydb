package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/rudderlabs/keydb/proto"
)

func TestRole(t *testing.T) {
	tests := []struct {
		name       string
		nodeID     int64
		advertised string
		shadows    []string
		expected   pb.NodeRole
	}{
		{name: "no advertised address", nodeID: 0, shadows: []string{"a:1"}, expected: pb.NodeRole_ACTIVE},
		{name: "no shadow list", nodeID: 0, advertised: "a:1", expected: pb.NodeRole_ACTIVE},
		{name: "empty entry", nodeID: 1, advertised: "a:1", shadows: []string{"a:1", ""}, expected: pb.NodeRole_ACTIVE},
		{name: "short list", nodeID: 2, advertised: "a:1", shadows: []string{"", ""}, expected: pb.NodeRole_ACTIVE},
		{name: "other address", nodeID: 0, advertised: "a:1", shadows: []string{"b:1"}, expected: pb.NodeRole_ACTIVE},
		{
			name: "listed as shadow", nodeID: 1, advertised: "b:1", shadows: []string{"", "b:1"},
			expected: pb.NodeRole_SHADOW,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{config: Config{
				NodeID:            tc.nodeID,
				AdvertisedAddress: tc.advertised,
				ShadowAddresses:   staticList(tc.shadows...),
			}}
			require.Equal(t, tc.expected, s.role())
		})
	}

	t.Run("nil shadow addresses func", func(t *testing.T) {
		s := &Service{config: Config{AdvertisedAddress: "a:1"}}
		require.Equal(t, pb.NodeRole_ACTIVE, s.role())
		require.Empty(t, s.shadowAddress())
	})

	t.Run("shadow address for own index", func(t *testing.T) {
		s := &Service{config: Config{NodeID: 1, ShadowAddresses: staticList("x:1", "y:1")}}
		require.Equal(t, "y:1", s.shadowAddress())
	})
}

func TestShadowRefusesRequests(t *testing.T) {
	ctx := context.Background()
	addresses := []string{"active:1", "active2:1"}
	shadow := newTestService(ctx, t, Config{
		NodeID:            1,
		Addresses:         staticList(addresses...),
		AdvertisedAddress: "shadow:1",
		ShadowAddresses:   staticList("", "shadow:1"),
	}, newTestConf(t))

	getResp, err := shadow.Get(ctx, &pb.GetRequest{Keys: []string{"k1"}})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_NOT_READY, getResp.ErrorCode)
	require.EqualValues(t, len(addresses), getResp.ClusterSize)
	require.Equal(t, addresses, getResp.NodesAddresses)

	putResp, err := shadow.Put(ctx, &pb.PutRequest{Keys: []string{"k1"}, TtlSeconds: 60})
	require.NoError(t, err)
	require.False(t, putResp.Success)
	require.Equal(t, pb.ErrorCode_NOT_READY, putResp.ErrorCode)
	require.EqualValues(t, len(addresses), putResp.ClusterSize)
	require.Equal(t, addresses, putResp.NodesAddresses)

	info, err := shadow.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.NodeRole_SHADOW, info.Role)
}

func TestReplacedNodeRefusesRequests(t *testing.T) {
	ctx := context.Background()
	addresses := &shadowList{}
	addresses.set("old:1", "other:1")
	shadows := &shadowList{}
	shadows.set("new:1", "")
	old := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         addresses.load,
		AdvertisedAddress: "old:1",
		ShadowAddresses:   shadows.load,
	}, newTestConf(t))
	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"} // some of them belong to node 0

	putResp, err := old.Put(ctx, &pb.PutRequest{Keys: keys, TtlSeconds: 60})
	require.NoError(t, err)
	require.NotEqual(t, pb.ErrorCode_NOT_READY, putResp.ErrorCode, "listed for its index, the node serves")

	// the second PR reaches this pod before it is stopped: its index now belongs to the shadow
	addresses.set("new:1", "other:1")
	shadows.set("", "")

	putResp, err = old.Put(ctx, &pb.PutRequest{Keys: keys, TtlSeconds: 60})
	require.NoError(t, err)
	require.False(t, putResp.Success)
	require.Equal(t, pb.ErrorCode_NOT_READY, putResp.ErrorCode, "writes it acknowledged now would be lost")
	require.Equal(t, []string{"new:1", "other:1"}, putResp.NodesAddresses)

	getResp, err := old.Get(ctx, &pb.GetRequest{Keys: keys})
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_NOT_READY, getResp.ErrorCode)
	require.Equal(t, []string{"new:1", "other:1"}, getResp.NodesAddresses)

	t.Run("a node without an advertised address keeps serving", func(t *testing.T) {
		s := newTestService(ctx, t, Config{NodeID: 0, Addresses: staticList("somewhere:1")}, newTestConf(t))
		resp, err := s.Get(ctx, &pb.GetRequest{Keys: keys})
		require.NoError(t, err)
		require.NotEqual(t, pb.ErrorCode_NOT_READY, resp.ErrorCode)
	})
}
