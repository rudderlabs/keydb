package node

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/cache/badger"
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
	for hr := range int64(3) {
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

func TestShadowSourceClearedOnActivation(t *testing.T) {
	ctx := context.Background()
	address := freeAddress(t)
	shadows := &shadowList{}
	shadows.set(address)
	active := &shadowList{}
	active.set("old-active:1")
	s := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         active.load,
		AdvertisedAddress: address,
		ShadowAddresses:   shadows.load,
		Replication:       ReplicationConfig{ReconcileInterval: 10 * time.Millisecond},
	}, newTestConf(t))
	serveTestService(t, s, address)

	resp := sendChunks(t, nodeClient(t, address), &pb.SnapshotChunk{
		HashRange: 0, IsLast: true, Timestamp: 1, SourceAddress: "old-active:1",
	})
	require.True(t, resp.Success, resp.ErrorMessage)
	_, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
	require.NoError(t, err)
	require.True(t, found)

	// the move completes: this node is now the active one for its index
	active.set(address)
	shadows.set("")
	require.Eventually(t, func() bool {
		_, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
		return err == nil && !found
	}, 5*time.Second, 10*time.Millisecond, "an active node must forget the source it was filled from")
}

func TestShadowGetNodeInfoDuringReceive(t *testing.T) {
	ctx := context.Background()
	address := freeAddress(t)
	shadows := &shadowList{}
	shadows.set(address)
	s := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         staticList("active:1"),
		AdvertisedAddress: address,
		ShadowAddresses:   shadows.load,
	}, newTestConf(t))
	serveTestService(t, s, address)
	c := nodeClient(t, address)

	// a receive in progress: the sender is slow to send the next chunk
	stream, err := c.ReceiveSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.SnapshotChunk{HashRange: 0, SourceAddress: "active:1"}))
	require.Eventually(t, func() bool { return s.receiving.Load() }, 5*time.Second, 10*time.Millisecond)

	infoCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	info, err := c.GetNodeInfo(infoCtx, &pb.GetNodeInfoRequest{})
	require.NoError(t, err, "a shadow must answer GetNodeInfo while it receives a range")
	require.Equal(t, pb.NodeRole_SHADOW, info.Role)

	t.Run("a shadow that became active refuses writes until the receive ends", func(t *testing.T) {
		shadows.set("")
		resp, err := s.Put(ctx, &pb.PutRequest{Keys: []string{"k"}, TtlSeconds: 60})
		require.NoError(t, err)
		require.Equal(t, pb.ErrorCode_NOT_READY, resp.ErrorCode)

		_, err = stream.CloseAndRecv()
		require.NoError(t, err)
		require.Eventually(t, func() bool { return !s.receiving.Load() }, 5*time.Second, 10*time.Millisecond)
		resp, err = s.Put(ctx, &pb.PutRequest{Keys: []string{"k"}, TtlSeconds: 60})
		require.NoError(t, err)
		require.Equal(t, pb.ErrorCode_NO_ERROR, resp.ErrorCode)
	})
}

func TestShadowStreamEndingEarlyIsAFailure(t *testing.T) {
	ctx := context.Background()
	address := freeAddress(t)
	s := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         staticList("active:1"),
		AdvertisedAddress: address,
		ShadowAddresses:   staticList(address),
	}, newTestConf(t))
	serveTestService(t, s, address)

	stream, err := nodeClient(t, address).ReceiveSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.SnapshotChunk{HashRange: 0, SourceAddress: "active:1"}))
	resp, err := stream.CloseAndRecv() // no last chunk
	require.NoError(t, err)
	require.False(t, resp.Success, "a stream that ends before its last chunk must not count as a sync")
	_, found, err := s.getSnapshotTimestamp(0)
	require.NoError(t, err)
	require.False(t, found, "no marker over a partial range")
}

// loadGuardCache records any GetValue made while a snapshot is being loaded: badger's Load moves the oracle
// without synchronisation, so nothing else may open a transaction meanwhile
type loadGuardCache struct {
	Cache
	inLoad     atomic.Bool
	violations atomic.Int64
}

func (c *loadGuardCache) LoadSnapshot(ctx context.Context, r io.Reader) error {
	c.inLoad.Store(true)
	defer c.inLoad.Store(false)
	return c.Cache.LoadSnapshot(ctx, r)
}

func (c *loadGuardCache) GetValue(key string, hashRange int64) ([]byte, bool, error) {
	if c.inLoad.Load() {
		c.violations.Add(1)
	}
	return c.Cache.GetValue(key, hashRange)
}

func TestShadowMetadataReadsDuringLoad(t *testing.T) {
	ctx := context.Background()
	source := newTestService(ctx, t, Config{NodeID: 0, Addresses: staticList("active:1")}, newTestConf(t))
	keysByHashRange, err := source.GetKeysByHashRange(testKeys(5000))
	require.NoError(t, err)
	require.NoError(t, source.cache.Put(keysByHashRange, time.Hour))
	var snapshot bytes.Buffer
	_, _, err = source.cache.StreamSnapshot(ctx, 0, 0, &snapshot, badger.StreamOptions{})
	require.NoError(t, err)
	require.Greater(t, snapshot.Len(), 100)

	address := freeAddress(t)
	var guard *loadGuardCache
	shadow := newTestService(ctx, t, Config{
		NodeID: 0, Addresses: staticList("active:1"), AdvertisedAddress: address, ShadowAddresses: staticList(address),
		wrapCache: func(c Cache) Cache {
			guard = &loadGuardCache{Cache: c}
			return guard
		},
	}, newTestConf(t))
	serveTestService(t, shadow, address)
	c := nodeClient(t, address)

	stream, err := c.ReceiveSnapshot(ctx)
	require.NoError(t, err)
	data := snapshot.Bytes()
	require.NoError(t, stream.Send(&pb.SnapshotChunk{HashRange: 0, Data: data[:50], SourceAddress: "active:1"}))
	require.Eventually(t, guard.inLoad.Load, 5*time.Second, 10*time.Millisecond, "the load never started")

	_, err = c.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	require.NoError(t, err)
	_, err = c.GetSnapshotSince(ctx, &pb.GetSnapshotSinceRequest{HashRange: 0})
	require.NoError(t, err)
	require.Zero(t, guard.violations.Load(), "metadata reads must not open transactions while a range loads")

	require.NoError(t, stream.Send(&pb.SnapshotChunk{HashRange: 0, Data: data[50:], IsLast: true, Timestamp: 7}))
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.True(t, resp.Success, resp.ErrorMessage)
	since, err := c.GetSnapshotSince(ctx, &pb.GetSnapshotSinceRequest{HashRange: 0})
	require.NoError(t, err)
	require.EqualValues(t, 7, since.SinceTimestamp)
}

func TestForgetShadowSourceSkipsWhileLocked(t *testing.T) {
	ctx := context.Background()
	address := "self:1"
	// the startup reconcile reads the shadow list right after its own forgetShadowSource, which would otherwise
	// delete the marker planted below
	reconciled := make(chan struct{})
	var once sync.Once
	s := newTestService(ctx, t, Config{
		NodeID: 0, Addresses: staticList(address), AdvertisedAddress: address,
		ShadowAddresses: func() []string {
			once.Do(func() { close(reconciled) })
			return nil
		},
		Replication: ReplicationConfig{ReconcileInterval: time.Hour},
	}, newTestConf(t))
	select {
	case <-reconciled:
	case <-time.After(5 * time.Second):
		t.Fatal("the startup reconcile never ran")
	}
	require.NoError(t, s.cache.PutValue(shadowSourceKey, internalKeysHashRange, []byte("old:1"), 0))

	s.mu.Lock() // a load holds the write lock
	done := make(chan struct{})
	go func() {
		s.forgetShadowSource()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.mu.Unlock()
		t.Fatal("forgetShadowSource waited for the lock instead of trying again later")
	}
	s.mu.Unlock()
	_, found, err := s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
	require.NoError(t, err)
	require.True(t, found, "nothing may be deleted while a load holds the lock")

	s.forgetShadowSource()
	_, found, err = s.cache.GetValue(shadowSourceKey, internalKeysHashRange)
	require.NoError(t, err)
	require.False(t, found)
}

func TestActiveNodeRefusesReplicationStream(t *testing.T) {
	ctx := context.Background()
	address := freeAddress(t)
	s := newTestService(ctx, t, Config{NodeID: 0, Addresses: staticList(address), AdvertisedAddress: address},
		newTestConf(t))
	serveTestService(t, s, address)
	c := nodeClient(t, address)

	// a replication stream reaching a node that was just promoted, from a source that hasn't stopped yet
	stream, err := c.ReceiveSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.SnapshotChunk{HashRange: 0, Data: []byte("x"), SourceAddress: "old:1"}))

	infoCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = c.GetNodeInfo(infoCtx, &pb.GetNodeInfoRequest{})
	require.NoError(t, err, "the active node must keep serving while a replication stream is refused")
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.False(t, resp.Success)
	require.Contains(t, resp.ErrorMessage, "not a shadow")
}
