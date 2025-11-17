package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// BenchmarkConnectionPoolSize benchmarks different pool sizes without any concurrency
// This benchmark shows that when there is no concurrency, having a pool with more connections doesn't really help
// with throughput.
func BenchmarkConnectionPoolSizeNoConcurrency(b *testing.B) {
	var processingDelay time.Duration
	poolSizes := []int{1, 2, 5, 10, 20, 50}

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			server, listener, address, mockServer := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, poolSize, address)
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			keys := []string{"key1", "key2", "key3"}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := client.Get(ctx, keys)
				if err != nil {
					b.Fatalf("Get failed: %v", err)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(mockServer.GetRequestCount()), "requests")
		})
	}
}

// BenchmarkConcurrentRequests benchmarks concurrent requests with different pool sizes
/*
BenchmarkConcurrentRequests
BenchmarkConcurrentRequests/PoolSize_1_Delays_0s-24         	   54345	     19527 ns/op	     51210 requests/sec	     54345 total_requests
BenchmarkConcurrentRequests/PoolSize_1_Delays_5ms-24        	    4195	    242681 ns/op	      4121 requests/sec	      4195 total_requests
BenchmarkConcurrentRequests/PoolSize_1_Delays_10ms-24       	    2647	    450333 ns/op	      2221 requests/sec	      2647 total_requests
BenchmarkConcurrentRequests/PoolSize_5_Delays_0s-24         	   57218	     21053 ns/op	     47500 requests/sec	     57218 total_requests
BenchmarkConcurrentRequests/PoolSize_5_Delays_5ms-24        	    5101	    239094 ns/op	      4182 requests/sec	      5101 total_requests
BenchmarkConcurrentRequests/PoolSize_5_Delays_10ms-24       	    2660	    447879 ns/op	      2233 requests/sec	      2660 total_requests
BenchmarkConcurrentRequests/PoolSize_10_Delays_0s-24        	   55064	     21236 ns/op	     47089 requests/sec	     55064 total_requests
BenchmarkConcurrentRequests/PoolSize_10_Delays_5ms-24       	    5004	    238139 ns/op	      4199 requests/sec	      5004 total_requests
BenchmarkConcurrentRequests/PoolSize_10_Delays_10ms-24      	    2300	    444404 ns/op	      2250 requests/sec	      2300 total_requests
BenchmarkConcurrentRequests/PoolSize_20_Delays_0s-24        	  154706	      7374 ns/op	    135616 requests/sec	    154706 total_requests
BenchmarkConcurrentRequests/PoolSize_20_Delays_5ms-24       	    4126	    246476 ns/op	      4057 requests/sec	      4126 total_requests
BenchmarkConcurrentRequests/PoolSize_20_Delays_10ms-24      	    2232	    450412 ns/op	      2220 requests/sec	      2232 total_requests
BenchmarkConcurrentRequests/PoolSize_50_Delays_0s-24        	  196645	      5858 ns/op	    170719 requests/sec	    196645 total_requests
BenchmarkConcurrentRequests/PoolSize_50_Delays_5ms-24       	    4378	    243105 ns/op	      4113 requests/sec	      4378 total_requests
BenchmarkConcurrentRequests/PoolSize_50_Delays_10ms-24      	    2282	    453753 ns/op	      2204 requests/sec	      2282 total_requests
BenchmarkConcurrentRequests/PoolSize_100_Delays_0s-24       	  219207	      5291 ns/op	    189009 requests/sec	    219207 total_requests
BenchmarkConcurrentRequests/PoolSize_100_Delays_5ms-24      	    4339	    237152 ns/op	      4217 requests/sec	      4339 total_requests
BenchmarkConcurrentRequests/PoolSize_100_Delays_10ms-24     	    2271	    449967 ns/op	      2222 requests/sec	      2271 total_requests
PASS
*/
func BenchmarkConcurrentRequests(b *testing.B) {
	poolSizes := []int{1, 5, 10, 20, 50, 100}
	processingDelays := []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}

	for _, poolSize := range poolSizes {
		for _, processingDelay := range processingDelays {
			b.Run(fmt.Sprintf("PoolSize_%d_Delays_%s", poolSize, processingDelay), func(b *testing.B) {
				server, listener, address, mockServer := setupBenchmarkServer(processingDelay)
				defer server.Stop()
				defer func() { _ = listener.Close() }()

				client := createBenchmarkClient(b, poolSize, address)
				defer func() { _ = client.Close() }()

				ctx := context.Background()
				keys := []string{"key1", "key2", "key3", "key4", "key5", "key6"}

				b.ResetTimer()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, err := client.Get(ctx, keys)
						if err != nil {
							b.Errorf("Get failed: %v", err)
						}
					}
				})

				b.StopTimer()
				totalRequests := mockServer.GetRequestCount()
				b.ReportMetric(float64(totalRequests), "total_requests")
				b.ReportMetric(float64(totalRequests)/b.Elapsed().Seconds(), "requests/sec")
			})
		}
	}
}

// BenchmarkPoolSizeVsThroughput measures throughput with different pool sizes
/*
BenchmarkPoolSizeVsThroughput/PoolSize_1-24         	      118999 requests/sec	    120000 total_requests
BenchmarkPoolSizeVsThroughput/PoolSize_5-24         	      206456 requests/sec	    240000 total_requests
BenchmarkPoolSizeVsThroughput/PoolSize_10-24        	      213345 requests/sec	    240000 total_requests
BenchmarkPoolSizeVsThroughput/PoolSize_20-24        	      208512 requests/sec	    240000 total_requests
BenchmarkPoolSizeVsThroughput/PoolSize_50-24        	      199104 requests/sec	    210000 total_requests
BenchmarkPoolSizeVsThroughput/PoolSize_100-24       	      197046 requests/sec	    220000 total_requests
*/
func BenchmarkPoolSizeVsThroughput(b *testing.B) {
	numOfRoutines := 10_000
	poolSizes := []int{1, 5, 10, 20, 50, 100}
	processingDelay := 3 * time.Millisecond

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			server, listener, address, mockServer := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, poolSize, address)
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			keys := []string{"key1", "key2", "key3", "key4", "key5"}

			b.ResetTimer()
			startTime := time.Now()

			var wg sync.WaitGroup
			for i := 0; i < numOfRoutines; i++ {
				wg.Go(func() {
					for i := 0; i < b.N; i++ {
						_, _ = client.Get(ctx, keys)
					}
				})
			}

			wg.Wait()
			elapsed := time.Since(startTime)

			b.StopTimer()

			totalRequests := mockServer.GetRequestCount()
			throughput := float64(totalRequests) / elapsed.Seconds()

			b.ReportMetric(elapsed.Seconds(), "total_time_sec")
			b.ReportMetric(float64(totalRequests), "total_requests")
			b.ReportMetric(throughput, "requests/sec")
			b.ReportMetric(elapsed.Seconds()/float64(totalRequests)*1000, "avg_latency_ms")
		})
	}
}

// BenchmarkPoolSizeVsLatency measures latency with different pool sizes under load
func BenchmarkPoolSizeVsLatency(b *testing.B) {
	concurrentRequests := 1000
	poolSizes := []int{1, 5, 10, 20}
	processingDelay := 5 * time.Millisecond

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d_Load_%d", poolSize, concurrentRequests), func(b *testing.B) {
			server, listener, address, _ := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, poolSize, address)
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			keys := []string{"key1", "key2", "key3"}

			// Measure latency distribution
			latencies := make(chan time.Duration, concurrentRequests)

			b.ResetTimer()

			var wg sync.WaitGroup
			for i := 0; i < concurrentRequests; i++ {
				wg.Go(func() {
					start := time.Now()
					_, err := client.Get(ctx, keys)
					latencies <- time.Since(start)
					if err != nil {
						b.Errorf("Get failed: %v", err)
						return
					}
				})
			}

			wg.Wait()
			b.StopTimer()
			close(latencies)

			// Calculate statistics
			var (
				total                  int
				sum                    time.Duration
				minLatency, maxLatency time.Duration
			)
			for lat := range latencies {
				sum += lat
				if lat > maxLatency {
					maxLatency = lat
				}
				if lat < minLatency {
					minLatency = lat
				}
				total++
			}

			avg := sum / time.Duration(total)
			b.ReportMetric(float64(avg.Milliseconds()), "avg_latency_ms")
			b.ReportMetric(float64(minLatency.Milliseconds()), "min_latency_ms")
			b.ReportMetric(float64(maxLatency.Milliseconds()), "max_latency_ms")
		})
	}
}

// BenchmarkPoolExhaustion tests behavior when pool is exhausted
func BenchmarkPoolExhaustion(b *testing.B) {
	poolSizes := []int{1, 5, 10}
	processingDelay := 100 * time.Millisecond // Long processing delay to simulate slow server

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			server, listener, address, _ := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, poolSize, address)
			defer func() { _ = client.Close() }()

			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			keys := []string{"key1"}

			b.ResetTimer()

			// Try to make more concurrent requests than pool size
			var (
				wg                         sync.WaitGroup
				successCount, timeoutCount atomic.Uint64
				numRequests                = poolSize * 3
			)
			for i := 0; i < numRequests; i++ {
				wg.Go(func() {
					_, err := client.Get(ctx, keys)
					if err != nil {
						if status.Code(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
							timeoutCount.Add(1)
						}
					} else {
						successCount.Add(1)
					}
				})
			}

			wg.Wait()
			b.StopTimer()
			b.ReportMetric(float64(successCount.Load()), "successful_requests")
			b.ReportMetric(float64(timeoutCount.Load()), "timeout_requests")
			b.ReportMetric(float64(successCount.Load())/float64(numRequests)*100, "success_rate_%")
		})
	}
}

// mockNodeServer is a mock gRPC server that simulates work
type mockNodeServer struct {
	proto.UnimplementedNodeServiceServer
	processingDelay time.Duration
	requestCount    atomic.Uint64
}

func (m *mockNodeServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	m.requestCount.Add(1)

	// Simulate processing delay
	if m.processingDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.processingDelay):
		}
	}

	// Return mock response
	exists := make([]bool, len(req.Keys))
	for i := range exists {
		exists[i] = true
	}

	return &proto.GetResponse{
		Exists:      exists,
		ClusterSize: 1,
		ErrorCode:   proto.ErrorCode_NO_ERROR,
	}, nil
}

func (m *mockNodeServer) Put(ctx context.Context, _ *proto.PutRequest) (*proto.PutResponse, error) {
	m.requestCount.Add(1)

	// Simulate processing delay
	if m.processingDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.processingDelay):
		}
	}

	return &proto.PutResponse{
		Success:     true,
		ClusterSize: 1,
		ErrorCode:   proto.ErrorCode_NO_ERROR,
	}, nil
}

func (m *mockNodeServer) GetRequestCount() uint64 { return m.requestCount.Load() }

// setupBenchmarkServer creates a mock gRPC server with specified processing delay
func setupBenchmarkServer(processingDelay time.Duration) (*grpc.Server, net.Listener, string, *mockNodeServer) {
	// Use actual TCP listener for more realistic benchmarking
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("failed to create listener: %v", err))
	}

	server := grpc.NewServer()
	mockServer := &mockNodeServer{
		processingDelay: processingDelay,
	}
	proto.RegisterNodeServiceServer(server, mockServer)

	go func() { _ = server.Serve(listener) }()

	return server, listener, listener.Addr().String(), mockServer
}

// createBenchmarkClient creates a client with specified pool size
func createBenchmarkClient(b *testing.B, poolSize int, address string) *Client {
	b.Helper()

	// Create client using NewClient with the actual address
	client, err := NewClient(Config{
		Addresses:          []string{address},
		ConnectionPoolSize: poolSize,
		RetryPolicy:        RetryPolicy{Disabled: true},
	}, logger.NOP, WithStats(stats.NOP))
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}

	return client
}
