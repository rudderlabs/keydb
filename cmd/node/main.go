package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/profiler"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/keydb/internal/cloudstorage"
	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/internal/release"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
)

var podNameRegex = regexp.MustCompile(`^keydb-(\d+)$`)

type keyDBResponse interface {
	GetSuccess() bool
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)

	conf := config.New(config.WithEnvPrefix("KEYDB"))
	logFactory := logger.NewFactory(conf)
	defer logFactory.Sync()
	log := logFactory.NewLogger()

	releaseInfo := release.NewInfo()
	statsOptions := []stats.Option{
		stats.WithServiceName(serviceName),
		stats.WithServiceVersion(releaseInfo.Version),
		stats.WithDefaultHistogramBuckets(defaultHistogramBuckets),
	}
	if conf.GetBool("enableBadgerMetrics", true) {
		registerer := prometheus.DefaultRegisterer
		gatherer := prometheus.DefaultGatherer
		badgerMetrics := NewBadgerMetricsCollector(log.Child("badger-metrics-exporter"))
		registerer.MustRegister(badgerMetrics)
		statsOptions = append(statsOptions, stats.WithPrometheusRegistry(registerer, gatherer))
	}
	for histogramName, buckets := range customBuckets {
		statsOptions = append(statsOptions, stats.WithHistogramBuckets(histogramName, buckets))
	}

	stat := stats.NewStats(conf, logFactory, svcMetric.NewManager(), statsOptions...)
	defer stat.Stop()

	if err := stat.Start(ctx, stats.DefaultGoRoutineFactory); err != nil {
		log.Errorn("Failed to start Stats", obskit.Error(err))
		os.Exit(1)
	}

	if err := run(ctx, cancel, conf, stat, log); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Errorn("Failed to run", obskit.Error(err))
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func run(ctx context.Context, cancel func(), conf *config.Config, stat stats.Stats, log logger.Logger) error {
	defer log.Infon("Service terminated")
	defer cancel()

	cloudStorage, err := cloudstorage.GetCloudStorage(conf, log)
	if err != nil {
		return fmt.Errorf("failed to create cloud storage: %w", err)
	}

	podName := conf.GetString("nodeId", "")
	if !podNameRegex.MatchString(podName) {
		return fmt.Errorf("invalid pod name %s", podName)
	}
	nodeID, err := strconv.Atoi(podNameRegex.FindStringSubmatch(podName)[1])
	if err != nil {
		return fmt.Errorf("failed to parse node ID %q: %w", podName, err)
	}
	nodeAddresses := conf.GetString("nodeAddresses", "")
	if len(nodeAddresses) == 0 {
		return fmt.Errorf("no node addresses provided")
	}

	nodeConfig := node.Config{
		NodeID:          uint32(nodeID),
		ClusterSize:     uint32(conf.GetInt("clusterSize", 1)),
		TotalHashRanges: uint32(conf.GetInt("totalHashRanges", node.DefaultTotalHashRanges)),
		MaxFilesToList:  conf.GetInt64("maxFilesToList", node.DefaultMaxFilesToList),
		SnapshotInterval: conf.GetDuration("snapshotInterval",
			0, time.Nanosecond, // node.DefaultSnapshotInterval will be used
		),
		GarbageCollectionInterval: conf.GetDuration("gcInterval", // node.DefaultGarbageCollectionInterval will be used
			0, time.Nanosecond,
		),
		Addresses:                 strings.Split(nodeAddresses, ","),
		LogTableStructureDuration: conf.GetDuration("logTableStructureDuration", 10, time.Minute),
		BackupFolderName:          conf.GetString("KUBE_NAMESPACE", ""),
	}

	port := conf.GetInt("port", 50051)
	log = log.Withn(
		logger.NewIntField("port", int64(port)),
		logger.NewIntField("nodeId", int64(nodeConfig.NodeID)),
		logger.NewIntField("clusterSize", int64(nodeConfig.ClusterSize)),
		logger.NewIntField("totalHashRanges", int64(nodeConfig.TotalHashRanges)),
		logger.NewStringField("nodeAddresses", fmt.Sprintf("%+v", nodeConfig.Addresses)),
		logger.NewIntField("noOfAddresses", int64(len(nodeConfig.Addresses))),
	)

	service, err := node.NewService(ctx, nodeConfig, cloudStorage, conf, stat, log.Child("service"))
	if err != nil {
		return fmt.Errorf("failed to create node service: %w", err)
	}

	var wg sync.WaitGroup
	defer func() {
		cancel()
		doneCh := make(chan struct{})
		shutdownStarted := time.Now()
		go func() {
			wg.Wait()
			close(doneCh)
		}()
		select {
		case <-doneCh:
			return
		case <-time.After(conf.GetDuration("shutdownTimeout", 15, time.Second)):
			log.Errorn("graceful termination failed",
				logger.NewDurationField("timeoutAfter", time.Since(shutdownStarted)))
			fmt.Print("\n\n")
			_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			fmt.Print("\n\n")
			return
		}
	}()

	// create a gRPC server with latency interceptors
	server := grpc.NewServer(
		// Unary interceptor to record latency for unary RPCs
		grpc.UnaryInterceptor(
			func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (
				interface{}, error,
			) {
				start := time.Now()
				resp, err := handler(ctx, req)
				success := err != nil
				if err == nil {
					if keyDBResp, ok := resp.(keyDBResponse); ok && !keyDBResp.GetSuccess() {
						success = false
					}
				}
				stat.NewTaggedStat("keydb_grpc_req_latency_seconds", stats.TimerType, stats.Tags{
					"method":  info.FullMethod,
					"success": strconv.FormatBool(success),
				}).Since(start)
				return resp, err
			}),
	)

	pb.RegisterNodeServiceServer(server, service)

	// Start listening
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Infon("Starting node",
		logger.NewStringField("addresses", fmt.Sprintf("%+v", nodeConfig.Addresses)),
		logger.NewIntField("hashRanges", int64(len(
			hash.GetNodeHashRanges(nodeConfig.NodeID, nodeConfig.ClusterSize, nodeConfig.TotalHashRanges),
		))),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.Infon("Profiler server terminated")
		if err := profiler.StartServer(ctx, conf.GetInt("Profiler.Port", 7777)); err != nil {
			log.Warnn("Profiler server failed", obskit.Error(err))
			return
		}
	}()

	serverErrCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("server error: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Infon("Terminating service")
		service.Close()
		server.GracefulStop()
		_ = lis.Close()
	}()

	select {
	case <-ctx.Done():
		log.Infon("Shutting down HTTP server")
		return ctx.Err()
	case err := <-serverErrCh:
		return err
	}
}
