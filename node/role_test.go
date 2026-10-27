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
