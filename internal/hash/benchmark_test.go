package hash

import (
	"hash/crc32"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func BenchmarkHashing(b *testing.B) {
	key := uuid.New().String()
	for i := 0; i < b.N; i++ {
		_, _ = GetNodeNumber(key, 3, 128)
	}
}

func TestSequentialVsParallel(t *testing.T) {
	noOfKeys := 10_000
	keys := make([]string, 0, noOfKeys)
	for range noOfKeys {
		keys = append(keys, uuid.New().String())
	}
	t.Run("sequential", func(t *testing.T) {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			t.Logf("Elapsed: %s", elapsed)
		}()
		keysByNode := make(map[uint32][]string)
		for _, key := range keys {
			_, nodeID := GetNodeNumber(key, 3, 128)
			keysByNode[nodeID] = append(keysByNode[nodeID], key)
		}
	})
	t.Run("parallel", func(t *testing.T) {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			t.Logf("Elapsed: %s", elapsed)
		}()
		type kv struct {
			key    string
			nodeID uint32
		}
		ch := make(chan kv, 100)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _, key := range keys[0 : len(keys)/2] {
				nodeID, _ := GetNodeNumber(key, 3, 128)
				ch <- kv{key, nodeID}
			}
		}()
		go func() {
			defer wg.Done()
			for _, key := range keys[len(keys)/2:] {
				nodeID, _ := GetNodeNumber(key, 3, 128)
				ch <- kv{key, nodeID}
			}
		}()
		go func() {
			wg.Wait()
			close(ch)
		}()
		keysByNode := make(map[uint32][]string)
		for range noOfKeys {
			kv := <-ch
			keysByNode[kv.nodeID] = append(keysByNode[kv.nodeID], kv.key)
		}
	})
}

func BenchmarkHashingFnv(b *testing.B) {
	b.Run("fnv", func(b *testing.B) {
		key := uuid.New().String()
		for i := 0; i < b.N; i++ {
			fnvTest(key)
		}
	})
	b.Run("crc32", func(b *testing.B) {
		key := uuid.New().String()
		for i := 0; i < b.N; i++ {
			crc32Test(key)
		}
	})
}

func fnvTest(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func crc32Test(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
