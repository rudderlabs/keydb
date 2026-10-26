package node

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/keydb/client"
	pb "github.com/rudderlabs/keydb/proto"
)

func TestEmptyDeltaKeepsMarker(t *testing.T) {
	ctx := context.Background()
	address := freeAddress(t)
	s := newTestService(ctx, t, Config{NodeID: 0, Addresses: staticList(address)}, newTestConf(t))
	serveTestService(t, s, address)

	require.NoError(t, s.setSnapshotTimestamp(0, 100))
	resp := sendChunks(t, nodeClient(t, address), &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 0})
	require.True(t, resp.Success, resp.ErrorMessage)

	ts, found, err := s.getSnapshotTimestamp(0)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 100, ts, "an empty delta must not move the marker backwards")
}

func TestEmptyRangeGetsMarker(t *testing.T) {
	ctx := context.Background()
	address := freeAddress(t)
	s := newTestService(ctx, t, Config{NodeID: 0, Addresses: staticList(address)}, newTestConf(t))
	serveTestService(t, s, address)

	_, found, err := s.getSnapshotTimestamp(0)
	require.NoError(t, err)
	require.False(t, found)

	resp := sendChunks(t, nodeClient(t, address), &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 0})
	require.True(t, resp.Success, resp.ErrorMessage)

	ts, found, err := s.getSnapshotTimestamp(0)
	require.NoError(t, err)
	require.True(t, found, "an empty range must be marked as synced")
	require.Zero(t, ts)
}

func TestAllRangesSynced(t *testing.T) {
	ctx := context.Background()
	address := freeAddress(t)
	s := newTestService(ctx, t, Config{NodeID: 0, TotalHashRanges: 4, Addresses: staticList(address)}, newTestConf(t))
	serveTestService(t, s, address)
	c := nodeClient(t, address)

	synced := func() bool {
		info, err := s.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
		require.NoError(t, err)
		return info.AllRangesSynced
	}

	require.False(t, synced())
	for hr := int64(0); hr < 3; hr++ {
		resp := sendChunks(t, c, &pb.SnapshotChunk{HashRange: hr, IsLast: true, Timestamp: 1})
		require.True(t, resp.Success, resp.ErrorMessage)
	}
	require.False(t, synced(), "one range has no marker yet")
	resp := sendChunks(t, c, &pb.SnapshotChunk{HashRange: 3, IsLast: true, Timestamp: 1})
	require.True(t, resp.Success, resp.ErrorMessage)
	require.True(t, synced())
}

func TestLeftoverDiskGuard(t *testing.T) {
	ctx := context.Background()
	newShadow := func(t *testing.T, stat stats.Stats) (*Service, pb.NodeServiceClient) {
		address := freeAddress(t)
		s := newTestServiceWithStats(ctx, t, Config{
			NodeID:            0,
			Addresses:         staticList("active:1"),
			AdvertisedAddress: address,
			ShadowAddresses:   staticList(address),
		}, newTestConf(t), stat)
		serveTestService(t, s, address)
		return s, nodeClient(t, address)
	}

	t.Run("data without a source marker is refused", func(t *testing.T) {
		store, err := memstats.New()
		require.NoError(t, err)
		s, c := newShadow(t, store)
		require.NoError(t, s.cache.Put(map[int64][]string{0: {"stale"}}, time.Hour))

		resp := sendChunks(t, c, &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 5, SourceAddress: "active:1"})
		require.False(t, resp.Success)
		require.Contains(t, resp.ErrorMessage, "leftover")
		require.EqualValues(t, 1, store.Get("keydb_shadow_leftover_disk_total", stats.Tags{"nodeID": "0"}).LastValue())
	})

	t.Run("a different source is refused", func(t *testing.T) {
		_, c := newShadow(t, stats.NOP)

		resp := sendChunks(t, c, &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 5, SourceAddress: "active:1"})
		require.True(t, resp.Success, resp.ErrorMessage)
		resp = sendChunks(t, c, &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 6, SourceAddress: "other:1"})
		require.False(t, resp.Success)
		resp = sendChunks(t, c, &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 7, SourceAddress: "active:1"})
		require.True(t, resp.Success, resp.ErrorMessage)
	})

	t.Run("senders without a source address are not checked", func(t *testing.T) {
		s, c := newShadow(t, stats.NOP)
		require.NoError(t, s.cache.Put(map[int64][]string{0: {"existing"}}, time.Hour))

		resp := sendChunks(t, c, &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 5})
		require.True(t, resp.Success, resp.ErrorMessage)
	})
}

func TestShadowReceiveMultiChunk(t *testing.T) {
	ctx := metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs(client.FeaturesHeader, client.FeatureDiscovery))
	newConf := func() *config.Config {
		conf := newTestConf(t)
		conf.Set("BadgerDB.Dedup.Compress", false)
		return conf
	}

	sourceAddress, shadowAddress := freeAddress(t), freeAddress(t)
	source := newTestService(ctx, t,
		Config{NodeID: 0, TotalHashRanges: 1, Addresses: staticList(sourceAddress)}, newConf(),
	)
	serveTestService(t, source, sourceAddress)
	shadow := newTestService(ctx, t, Config{
		NodeID:            0,
		TotalHashRanges:   1,
		Addresses:         staticList(sourceAddress),
		AdvertisedAddress: shadowAddress,
		ShadowAddresses:   staticList(shadowAddress),
	}, newConf())
	serveTestService(t, shadow, shadowAddress)

	// enough keys for the range to span several 1 MiB chunks
	keys := make([]string, 60_000)
	for i := range keys {
		keys[i] = "a-reasonably-long-key-" + strconv.Itoa(i)
	}
	require.NoError(t, source.cache.Put(map[int64][]string{0: keys}, time.Hour))

	resp, err := nodeClient(t, sourceAddress).SendSnapshot(ctx, &pb.SendSnapshotRequest{
		HashRange: 0, DestinationAddress: shadowAddress,
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.ErrorMessage)

	keysByHashRange, indexes, err := shadow.GetKeysByHashRangeWithIndexes(keys)
	require.NoError(t, err)
	exists, err := shadow.cache.Get(keysByHashRange, indexes)
	require.NoError(t, err)
	for i, e := range exists {
		require.True(t, e, "key %d missing on the shadow", i)
	}
	_, found, err := shadow.getSnapshotTimestamp(0)
	require.NoError(t, err)
	require.True(t, found)
}
