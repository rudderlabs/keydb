package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	proto "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// mockNodeServer is a mock gRPC server that simulates work
type mockNodeServer struct {
	proto.UnimplementedNodeServiceServer
	processingDelay time.Duration
	mu              sync.Mutex
	requestCount    int
}

func (m *mockNodeServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	m.mu.Lock()
	m.requestCount++
	m.mu.Unlock()

	// Simulate processing delay
	if m.processingDelay > 0 {
		time.Sleep(m.processingDelay)
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

func (m *mockNodeServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	m.mu.Lock()
	m.requestCount++
	m.mu.Unlock()

	// Simulate processing delay
	if m.processingDelay > 0 {
		time.Sleep(m.processingDelay)
	}

	return &proto.PutResponse{
		Success:     true,
		ClusterSize: 1,
		ErrorCode:   proto.ErrorCode_NO_ERROR,
	}, nil
}

func (m *mockNodeServer) GetRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requestCount
}

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

	go func() {
		_ = server.Serve(listener)
	}()

	return server, listener, listener.Addr().String(), mockServer
}

// createBenchmarkClient creates a client with specified pool size
func createBenchmarkClient(b *testing.B, poolSize int, address string) *Client {
	b.Helper()

	// Create client using NewClient with the actual address
	client, err := NewClient(Config{
		Addresses:          []string{address},
		TotalHashRanges:    128,
		ConnectionPoolSize: poolSize,
		RetryPolicy:        RetryPolicy{Disabled: true},
	}, logger.NOP, WithStats(stats.NOP))
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}

	return client
}

// BenchmarkConnectionPoolSize benchmarks different pool sizes
func BenchmarkConnectionPoolSize(b *testing.B) {
	// Simulate realistic server processing time
	processingDelay := 5 * time.Millisecond

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
func BenchmarkConcurrentRequests(b *testing.B) {
	processingDelay := 5 * time.Millisecond
	concurrentClients := 100

	poolSizes := []int{1, 5, 10, 20, 50}

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d_Concurrent_%d", poolSize, concurrentClients), func(b *testing.B) {
			server, listener, address, mockServer := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, poolSize, address)
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			keys := []string{"key1", "key2", "key3"}

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

// BenchmarkPoolSizeVsThroughput measures throughput with different pool sizes
func BenchmarkPoolSizeVsThroughput(b *testing.B) {
	processingDelay := 10 * time.Millisecond

	poolSizes := []int{1, 2, 5, 10, 20}

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			server, listener, address, mockServer := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, poolSize, address)
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			keys := []string{"key1", "key2", "key3", "key4", "key5"}

			// Number of concurrent workers
			numWorkers := 50
			requestsPerWorker := 100

			b.ResetTimer()
			startTime := time.Now()

			var wg sync.WaitGroup
			wg.Add(numWorkers)

			for i := 0; i < numWorkers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < requestsPerWorker; j++ {
						_, _ = client.Get(ctx, keys)
					}
				}()
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
	processingDelay := 5 * time.Millisecond

	poolSizes := []int{1, 5, 10, 20}
	concurrentRequests := 100

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
			latencies := make([]time.Duration, 0, concurrentRequests)
			var mu sync.Mutex

			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(concurrentRequests)

			for i := 0; i < concurrentRequests; i++ {
				go func() {
					defer wg.Done()
					start := time.Now()
					_, err := client.Get(ctx, keys)
					latency := time.Since(start)

					if err != nil {
						b.Errorf("Get failed: %v", err)
						return
					}

					mu.Lock()
					latencies = append(latencies, latency)
					mu.Unlock()
				}()
			}

			wg.Wait()
			b.StopTimer()

			// Calculate statistics
			if len(latencies) > 0 {
				var sum time.Duration
				var maxLatency time.Duration
				minLatency := latencies[0]

				for _, lat := range latencies {
					sum += lat
					if lat > maxLatency {
						maxLatency = lat
					}
					if lat < minLatency {
						minLatency = lat
					}
				}

				avg := sum / time.Duration(len(latencies))

				b.ReportMetric(float64(avg.Milliseconds()), "avg_latency_ms")
				b.ReportMetric(float64(minLatency.Milliseconds()), "min_latency_ms")
				b.ReportMetric(float64(maxLatency.Milliseconds()), "max_latency_ms")
			}
		})
	}
}

// BenchmarkGetOperations benchmarks Get operations with different pool sizes
func BenchmarkGetOperations(b *testing.B) {
	processingDelay := 3 * time.Millisecond

	benchmarks := []struct {
		poolSize int
		parallel bool
	}{
		{poolSize: 1, parallel: false},
		{poolSize: 10, parallel: false},
		{poolSize: 1, parallel: true},
		{poolSize: 10, parallel: true},
	}

	for _, bm := range benchmarks {
		name := fmt.Sprintf("PoolSize_%d_Parallel_%t", bm.poolSize, bm.parallel)
		b.Run(name, func(b *testing.B) {
			server, listener, address, mockServer := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, bm.poolSize, address)
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			keys := []string{"key1", "key2", "key3"}

			b.ResetTimer()

			if bm.parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, err := client.Get(ctx, keys)
						if err != nil {
							b.Errorf("Get failed: %v", err)
						}
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					_, err := client.Get(ctx, keys)
					if err != nil {
						b.Fatalf("Get failed: %v", err)
					}
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(mockServer.GetRequestCount()), "requests")
		})
	}
}

// BenchmarkPutOperations benchmarks Put operations with different pool sizes
func BenchmarkPutOperations(b *testing.B) {
	processingDelay := 3 * time.Millisecond

	poolSizes := []int{1, 5, 10, 20}

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			server, listener, address, mockServer := setupBenchmarkServer(processingDelay)
			defer server.Stop()
			defer func() { _ = listener.Close() }()

			client := createBenchmarkClient(b, poolSize, address)
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			keys := []string{"key1", "key2", "key3"}
			ttl := 1 * time.Hour

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := client.Put(ctx, keys, ttl)
					if err != nil {
						b.Errorf("Put failed: %v", err)
					}
				}
			})

			b.StopTimer()
			b.ReportMetric(float64(mockServer.GetRequestCount()), "requests")
			b.ReportMetric(float64(mockServer.GetRequestCount())/b.Elapsed().Seconds(), "requests/sec")
		})
	}
}

// BenchmarkPoolExhaustion tests behavior when pool is exhausted
func BenchmarkPoolExhaustion(b *testing.B) {
	// Long processing delay to simulate slow server
	processingDelay := 100 * time.Millisecond

	poolSizes := []int{1, 5, 10}

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
			numRequests := poolSize * 3
			var wg sync.WaitGroup
			wg.Add(numRequests)

			successCount := 0
			timeoutCount := 0
			var mu sync.Mutex

			for i := 0; i < numRequests; i++ {
				go func() {
					defer wg.Done()
					_, err := client.Get(ctx, keys)
					mu.Lock()
					if err != nil {
						if status.Code(err) == codes.DeadlineExceeded || err == context.DeadlineExceeded {
							timeoutCount++
						}
					} else {
						successCount++
					}
					mu.Unlock()
				}()
			}

			wg.Wait()
			b.StopTimer()

			b.ReportMetric(float64(successCount), "successful_requests")
			b.ReportMetric(float64(timeoutCount), "timeout_requests")
			b.ReportMetric(float64(successCount)/float64(numRequests)*100, "success_rate_%")
		})
	}
}
