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
	"google.golang.org/grpc/keepalive"

	"github.com/rudderlabs/keydb/internal/cloudstorage"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/keydb/release"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	_ "github.com/rudderlabs/rudder-go-kit/maxprocs"
	"github.com/rudderlabs/rudder-go-kit/profiler"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

var (
	// legacyPodNameRegex matches single statefulset pod names like keydb-0, keydb-1, etc.
	legacyPodNameRegex = regexp.MustCompile(`^keydb-(\d+)$`)

	// podNameRegex matches multi-statefulset pod names with fixed -0 suffix like keydb-0-0, keydb-1-0, etc.
	podNameRegex = regexp.MustCompile(`^keydb-(\d+)-0$`)
)

const (
	degradedNodesConfKey = "degradedNodes"
)

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
	nodeID, err := getNodeID(podName)
	if err != nil {
		return err
	}
	nodeAddresses := conf.GetString("nodeAddresses", "")
	if len(nodeAddresses) == 0 {
		return fmt.Errorf("no node addresses provided")
	}
	degradedNodes := conf.GetReloadableStringVar("", degradedNodesConfKey)

	nodeConfig := node.Config{
		NodeID:          nodeID,
		TotalHashRanges: conf.GetInt64("totalHashRanges", node.DefaultTotalHashRanges),
		MaxFilesToList:  conf.GetInt64("maxFilesToList", node.DefaultMaxFilesToList),
		SnapshotInterval: conf.GetDuration("snapshotInterval",
			0, time.Nanosecond, // node.DefaultSnapshotInterval will be used
		),
		GarbageCollectionInterval: conf.GetDuration("gcInterval", // node.DefaultGarbageCollectionInterval will be used
			0, time.Nanosecond,
		),
		Addresses: strings.Split(nodeAddresses, ","),
		DegradedNodes: func() []bool {
			raw := strings.TrimSpace(degradedNodes.Load())
			if raw == "" {
				return nil
			}
			v := strings.Split(raw, ",")
			b := make([]bool, len(v))
			for i, s := range v {
				var err error
				b[i], err = strconv.ParseBool(s)
				if err != nil {
					log.Warnn("Failed to parse degraded node", logger.NewStringField("v", raw), obskit.Error(err))
				}
			}
			return b
		},
		LogTableStructureDuration: conf.GetDuration("logTableStructureDuration", 10, time.Minute),
		BackupFolderName:          conf.GetString("KUBE_NAMESPACE", ""),
	}

	port := conf.GetInt("port", 50051)
	log = log.Withn(
		logger.NewIntField("port", int64(port)),
		logger.NewIntField("nodeId", int64(nodeConfig.NodeID)),
		logger.NewIntField("clusterSize", int64(len(nodeConfig.Addresses))),
		logger.NewIntField("totalHashRanges", int64(nodeConfig.TotalHashRanges)),
		logger.NewStringField("nodeAddresses", fmt.Sprintf("%+v", nodeConfig.Addresses)),
		logger.NewIntField("noOfAddresses", int64(len(nodeConfig.Addresses))),
	)

	service, err := node.NewService(ctx, nodeConfig, cloudStorage, conf, stat, log.Child("service"))
	if err != nil {
		return fmt.Errorf("failed to create node service: %w", err)
	}

	degradedNodesObserver := &configObserver{
		key: degradedNodesConfKey,
		f:   service.DegradedNodesChanged,
	}
	conf.RegisterObserver(degradedNodesObserver)

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

	// Configure gRPC server keepalive parameters
	grpcKeepaliveMinTime := conf.GetDuration("grpc.keepalive.minTime", 10, time.Second)
	grpcKeepalivePermitWithoutStream := conf.GetBool("grpc.keepalive.permitWithoutStream", true)
	grpcKeepaliveTime := conf.GetDuration("grpc.keepalive.time", 60, time.Second)
	grpcKeepaliveTimeout := conf.GetDuration("grpc.keepalive.timeout", 20, time.Second)

	log.Infon("gRPC server keepalive configuration",
		logger.NewDurationField("enforcementMinTime", grpcKeepaliveMinTime),
		logger.NewBoolField("enforcementPermitWithoutStream", grpcKeepalivePermitWithoutStream),
		logger.NewDurationField("serverTime", grpcKeepaliveTime),
		logger.NewDurationField("serverTimeout", grpcKeepaliveTimeout),
	)

	// create a gRPC server with latency interceptors and keepalive parameters
	server := grpc.NewServer(
		// Keepalive enforcement policy - controls what the server requires from clients
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             grpcKeepaliveMinTime,
			PermitWithoutStream: grpcKeepalivePermitWithoutStream,
		}),
		// Keepalive parameters - controls server's own keepalive behavior
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    grpcKeepaliveTime,
			Timeout: grpcKeepaliveTimeout,
		}),
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
		logger.NewStringField("degradedNodes", degradedNodes.Load()),
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

// getNodeID extracts the node ID from a pod name.
// Supports both multi-statefulset format (keydb-{nodeId}-0) and legacy format (keydb-{nodeId}).
func getNodeID(podName string) (int64, error) {
	// Try matching the multi-statefulset pattern first (keydb-0-0, keydb-1-0, etc.)
	if matches := podNameRegex.FindStringSubmatch(podName); matches != nil {
		// Extract the first number as the node ID (e.g., 0 from keydb-0-0)
		nodeID, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse node ID from %q: %w", podName, err)
		}
		return nodeID, nil
	}

	// Fallback to legacy single statefulset pattern (keydb-0, keydb-1, etc.)
	if matches := legacyPodNameRegex.FindStringSubmatch(podName); matches != nil {
		nodeID, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse node ID from %q: %w", podName, err)
		}
		return nodeID, nil
	}

	return 0, fmt.Errorf("invalid pod name %q, expected format: keydb-<nodeId>-0 or keydb-<nodeId>", podName)
}

type configObserver struct {
	key string
	f   func()
}

func (c *configObserver) OnReloadableConfigChange(key string, _, _ any) {
	if key == c.key {
		c.f()
	}
}

func (c *configObserver) OnNonReloadableConfigChange(_ string) {}
