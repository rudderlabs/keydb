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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

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

	t.Run("senders without a source address are refused", func(t *testing.T) {
		s, c := newShadow(t, stats.NOP)

		resp := sendChunks(t, c, &pb.SnapshotChunk{HashRange: 0, Data: []byte("x"), IsLast: true, Timestamp: 5})
		require.False(t, resp.Success, "a shadow only takes replication streams, which name their source")
		require.Contains(t, resp.ErrorMessage, "source address")
		_, found, err := s.getSnapshotTimestamp(0)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("markers without a source are dropped with the first source", func(t *testing.T) {
		s, c := newShadow(t, stats.NOP)
		// a disk whose data expired keeps the markers of the node that filled it, in that node's versions
		for hashRange := range s.config.TotalHashRanges {
			require.NoError(t, s.setSnapshotTimestamp(hashRange, 1_000_000))
		}
		require.True(t, s.allRangesSynced())

		resp := sendChunks(t, c, &pb.SnapshotChunk{HashRange: 0, IsLast: true, Timestamp: 5, SourceAddress: "active:1"})
		require.True(t, resp.Success, resp.ErrorMessage)
		ts, found, err := s.getSnapshotTimestamp(0)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 5, ts, "the new source's marker replaces the old one")
		_, found, err = s.getSnapshotTimestamp(1)
		require.NoError(t, err)
		require.False(t, found, "a marker from another source would skip data the shadow lacks")
		require.False(t, s.allRangesSynced())
		_, found, err = s.cache.GetValue(getSnapshotTimestampKey(1), internalKeysHashRange)
		require.NoError(t, err)
		require.False(t, found, "the marker is gone from the disk too, not only from memory")
	})
}

func TestShadowReceiveMultiChunk(t *testing.T) {
	ctx := context.Background()
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

	// stream the range the way replication does, in 1 MiB chunks that name their source
	var buf bytes.Buffer
	maxVersion, _, err := source.cache.StreamSnapshot(ctx, 0, 0, &buf, badger.StreamOptions{})
	require.NoError(t, err)
	var chunks []*pb.SnapshotChunk
	for data := buf.Bytes(); len(data) > 0; {
		n := min(len(data), DefaultStreamingChunkSize)
		chunks = append(chunks, &pb.SnapshotChunk{HashRange: 0, Data: data[:n], SourceAddress: sourceAddress})
		data = data[n:]
	}
	require.Greater(t, len(chunks), 1, "the range must span several chunks")
	chunks = append(chunks, &pb.SnapshotChunk{
		HashRange: 0, IsLast: true, Timestamp: maxVersion, SourceAddress: sourceAddress,
	})
	resp := sendChunks(t, nodeClient(t, shadowAddress), chunks...)
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
	actives, shadows := &shadowList{}, &shadowList{}
	actives.set("active:1")
	shadows.set(address)
	s := newTestService(ctx, t, Config{
		NodeID:            0,
		Addresses:         actives.load,
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
		actives.set(address)
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

// rangeSnapshot returns an uncompressed snapshot of range 0 holding keys written in many commits, so that loading
// it moves the destination's transaction timestamps forward. Small batches make any prefix of it loadable.
func rangeSnapshot(t *testing.T) (snapshot []byte, keys []string) {
	t.Helper()
	conf := newTestConf(t)
	conf.Set("BadgerDB.Dedup.Compress", false)
	source, err := badger.New(conf, logger.NOP)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close()) }()
	for i := range 50 {
		key := "key-" + strconv.Itoa(i)
		keys = append(keys, key)
		require.NoError(t, source.Put(map[int64][]string{0: {key}}, time.Hour))
	}
	var buf bytes.Buffer
	_, _, err = source.StreamSnapshot(context.Background(), 0, 0, &buf, badger.StreamOptions{BatchSize: 64})
	require.NoError(t, err)
	return buf.Bytes(), keys
}

// returnsWithin reports whether f returns within the timeout. f keeps running in the background if it doesn't.
func returnsWithin(timeout time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func TestShadowRetriesAnInterruptedRange(t *testing.T) {
	ctx := context.Background()
	conf := newTestConf(t)
	conf.Set("BadgerDB.Dedup.Compress", false)
	address := freeAddress(t)
	s := newTestService(ctx, t, Config{
		NodeID:            0,
		TotalHashRanges:   1,
		Addresses:         staticList("active:1"),
		AdvertisedAddress: address,
		ShadowAddresses:   staticList(address),
	}, conf)
	serveTestService(t, s, address)
	c := nodeClient(t, address)
	snapshot, keys := rangeSnapshot(t)

	// the sender stops after the first chunk with data, as when its pass is paused for too long or it restarts
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.ReceiveSnapshot(streamCtx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.SnapshotChunk{
		HashRange: 0, Timestamp: 1, SourceAddress: "active:1", Data: snapshot[:len(snapshot)/2],
	}))
	require.Eventually(t, func() bool { return s.receiving.Load() }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.Eventually(t, func() bool { return !s.receiving.Load() }, 5*time.Second, 10*time.Millisecond)

	var resp *pb.ReceiveSnapshotResponse
	require.True(t, returnsWithin(10*time.Second, func() {
		resp = sendChunks(t, c, &pb.SnapshotChunk{
			HashRange: 0, Timestamp: 2, SourceAddress: "active:1", Data: snapshot, IsLast: true,
		})
	}), "the retry of an interrupted range must not block")
	require.True(t, resp.Success, resp.ErrorMessage)
	keysByHashRange, indexes, err := s.GetKeysByHashRangeWithIndexes(keys)
	require.NoError(t, err)
	exists, err := s.cache.Get(keysByHashRange, indexes)
	require.NoError(t, err)
	require.NotContains(t, exists, false, "every key of the retried range is on the shadow")
}

func TestShadowPromotedMidRange(t *testing.T) {
	ctx := context.Background()
	conf := newTestConf(t)
	conf.Set("BadgerDB.Dedup.Compress", false)
	address := freeAddress(t)
	actives, shadows := &shadowList{}, &shadowList{}
	actives.set("active:1")
	shadows.set(address)
	s := newTestService(ctx, t, Config{
		NodeID:            0,
		TotalHashRanges:   1,
		Addresses:         actives.load,
		AdvertisedAddress: address,
		ShadowAddresses:   shadows.load,
	}, conf)
	serveTestService(t, s, address)
	snapshot, _ := rangeSnapshot(t)

	stream, err := nodeClient(t, address).ReceiveSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.SnapshotChunk{
		HashRange: 0, Timestamp: 1, SourceAddress: "active:1", Data: snapshot[:len(snapshot)/2],
	}))
	require.Eventually(t, func() bool { return s.receiving.Load() }, 5*time.Second, 10*time.Millisecond)

	// the move completes while the range is half loaded, then the rest of the range arrives
	actives.set(address)
	shadows.set("")
	_ = stream.Send(&pb.SnapshotChunk{
		HashRange: 0, Timestamp: 1, SourceAddress: "active:1", Data: snapshot[len(snapshot)/2:], IsLast: true,
	})
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.False(t, resp.Success, "the receive stops once the shadow is promoted")
	require.Eventually(t, func() bool { return !s.receiving.Load() }, 5*time.Second, 10*time.Millisecond)

	var putResp *pb.PutResponse
	var getResp *pb.GetResponse
	require.True(t, returnsWithin(10*time.Second, func() {
		putResp, err = s.Put(ctx, &pb.PutRequest{Keys: []string{"new"}, TtlSeconds: 60})
		if err == nil {
			getResp, err = s.Get(ctx, &pb.GetRequest{Keys: []string{"new"}})
		}
	}), "the promoted node must serve after an interrupted receive")
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_NO_ERROR, putResp.ErrorCode)
	require.Equal(t, pb.ErrorCode_NO_ERROR, getResp.ErrorCode)
	require.Equal(t, []bool{true}, getResp.Exists)
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

func TestCloseWaitsForReceive(t *testing.T) {
	conf := newTestConf(t)
	conf.Set("BadgerDB.Dedup.Compress", false)
	address := freeAddress(t)
	s := newTestService(context.Background(), t, Config{
		NodeID:            0,
		TotalHashRanges:   1,
		Addresses:         staticList("active:1"),
		AdvertisedAddress: address,
		ShadowAddresses:   staticList(address),
	}, conf)
	serveTestService(t, s, address)
	snapshot, _ := rangeSnapshot(t)

	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := nodeClient(t, address).ReceiveSnapshot(streamCtx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.SnapshotChunk{
		HashRange: 0, Timestamp: 1, SourceAddress: "active:1", Data: snapshot[:len(snapshot)/2],
	}))
	require.Eventually(t, func() bool { return s.receiving.Load() }, 5*time.Second, 10*time.Millisecond)

	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	require.Never(t, func() bool { return isClosed(closed) }, 300*time.Millisecond,
		20*time.Millisecond, "the cache must not be closed under a receive in progress")

	cancel() // the gRPC server cancels in-flight calls on shutdown
	require.Eventually(t, func() bool { return isClosed(closed) }, 10*time.Second, 10*time.Millisecond,
		"Close must return once the receive ends")
}

// isClosed returns true when c is closed.
func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
