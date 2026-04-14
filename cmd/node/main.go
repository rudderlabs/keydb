package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/rudderlabs/keydb/internal/cloudstorage"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/keydb/release"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
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
	nodeAddressesConfKey = "nodeAddresses"
	degradedNodesConfKey = "degradedNodes"
)

type keyDBResponse interface {
	GetSuccess() bool
}

// cloudStorage is the subset of *filemanager.S3Manager that node.NewService
// needs. Declared here (instead of importing node's unexported interface) so
// runWith is testable with fakes — this is the only reason it's an interface.
type cloudStorage interface {
	Download(ctx context.Context, output io.WriterAt, key string, opts ...filemanager.DownloadOption) error
	ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) filemanager.ListSession
	UploadReader(ctx context.Context, objName string, rdr io.Reader) (filemanager.UploadedFile, error)
	Delete(ctx context.Context, keys []string) error
}

func main() {
	os.Exit(runNode())
}

func runNode() int {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	conf := config.New(config.WithEnvPrefix("KEYDB"))
	logFactory := logger.NewFactory(conf)
	defer logFactory.Sync()
	log := logFactory.NewLogger()

	releaseInfo := release.NewInfo()
	statsOptions := []stats.Option{
		stats.WithServiceName(serviceName),
		stats.WithServiceVersion(releaseInfo.Version),
	}
	if conf.GetBoolVar(true, "enableBadgerMetrics") {
		registerer := prometheus.DefaultRegisterer
		gatherer := prometheus.DefaultGatherer
		badgerMetrics := NewBadgerMetricsCollector(log.Child("badger-metrics-exporter"))
		registerer.MustRegister(badgerMetrics)
		statsOptions = append(statsOptions, stats.WithPrometheusRegistry(registerer, gatherer))
	}
	if conf.GetBoolVar(false, "Stats.exponentialHistogram") {
		maxSize := conf.GetIntVar(1800 /* 30 mins */, 1, "Stats.exponentialHistogramMaxSize")
		log.Infon("Using exponential histogram for stats", logger.NewIntField("maxSize", int64(maxSize)))
		statsOptions = append(statsOptions, stats.WithDefaultExponentialHistogram(int32(maxSize)))
	} else {
		statsOptions = append(statsOptions, stats.WithDefaultHistogramBuckets(defaultHistogramBuckets))
		for histogramName, buckets := range customBuckets {
			statsOptions = append(statsOptions, stats.WithHistogramBuckets(histogramName, buckets))
		}
	}

	stat := stats.NewStats(conf, logFactory, svcMetric.NewManager(), statsOptions...)
	defer stat.Stop()

	if err := stat.Start(ctx, stats.DefaultGoRoutineFactory); err != nil {
		log.Errorn("Failed to start Stats", obskit.Error(err))
		return 1
	}

	storage, err := cloudstorage.GetCloudStorage(conf, log)
	if err != nil {
		log.Errorn("Failed to initialize cloud storage", obskit.Error(err))
		return 1
	}

	if err := run(ctx, cancel, conf, stat, log, storage); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Errorn("Failed to run", obskit.Error(err))
			return 1
		}
	}
	return 0
}

func run(ctx context.Context, cancel func(), conf *config.Config, stat stats.Stats,
	log logger.Logger, storage cloudStorage,
) error {
	podName := conf.GetStringVar("", "nodeId")
	nodeID, err := getNodeID(podName)
	if err != nil {
		return err
	}

	// nodeAddresses is reloadable to support dynamic cluster configuration updates
	nodeAddresses := conf.GetReloadableStringVar("", nodeAddressesConfKey)
	if len(strings.TrimSpace(nodeAddresses.Load())) == 0 {
		return fmt.Errorf("no node addresses provided")
	}
	degradedNodes := conf.GetReloadableStringVar("", degradedNodesConfKey)

	nodeConfig := node.Config{
		NodeID:          nodeID,
		TotalHashRanges: conf.GetInt64Var(node.DefaultTotalHashRanges, 1, "totalHashRanges"),
		MaxFilesToList:  conf.GetInt64Var(node.DefaultMaxFilesToList, 1, "maxFilesToList"),
		SnapshotInterval: conf.GetDurationVar(
			0, time.Nanosecond, "snapshotInterval", // node.DefaultSnapshotInterval will be used
		),
		GarbageCollectionInterval: conf.GetDurationVar( // node.DefaultGarbageCollectionInterval will be used
			0, time.Nanosecond, "gcInterval",
		),
		Addresses: func() []string {
			raw := strings.TrimSpace(nodeAddresses.Load())
			if raw == "" {
				return nil
			}
			return strings.Split(raw, ",")
		},
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
		CheckL0StallInterval:      conf.GetDurationVar(5, time.Second, "checkL0StallInterval"),
		LogTableStructureDuration: conf.GetDurationVar(10, time.Minute, "logTableStructureDuration"),
		BackupFolderName:          conf.GetStringVar("", "KUBE_NAMESPACE"),
	}

	port := conf.GetIntVar(50051, 1, "port")
	healthPort := conf.GetIntVar(50052, 1, "healthPort")
	log = log.Withn(
		logger.NewIntField("port", int64(port)),
		logger.NewIntField("healthPort", int64(healthPort)),
		logger.NewIntField("nodeId", nodeConfig.NodeID),
		logger.NewIntField("totalHashRanges", nodeConfig.TotalHashRanges),
	)

	addresses := nodeConfig.Addresses()
	log.Infon("Creating service",
		logger.NewIntField("clusterSize", int64(len(addresses))),
		logger.NewIntField("noOfAddresses", int64(len(addresses))),
		logger.NewStringField("nodeAddresses", strings.Join(addresses, ",")),
	)

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
		case <-time.After(conf.GetDurationVar(15, time.Second, "shutdownTimeout")):
			log.Errorn("graceful termination failed",
				logger.NewDurationField("timeoutAfter", time.Since(shutdownStarted)))
			fmt.Print("\n\n")
			_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			fmt.Print("\n\n")
			return
		}
	}()

	// Start a lightweight health-only gRPC server BEFORE the slow node.NewService init.
	// This way Kubernetes liveness/readiness probes have something to talk to from the
	// first moment the process starts listening, and the pod isn't killed during a slow
	// startup (e.g. snapshot download, cache warmup, etc.).
	//
	//   - Overall health ("") = SERVING immediately       -> liveness passes
	//   - "keydb.NodeService"  = NOT_SERVING until ready   -> readiness gates traffic
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(pb.NodeService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)

	healthGrpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(healthGrpcServer, healthServer)

	healthLis, err := net.Listen("tcp", fmt.Sprintf(":%d", healthPort))
	if err != nil {
		return fmt.Errorf("listening on health port %d: %w", healthPort, err)
	}

	healthErrCh := make(chan error, 1)
	wg.Go(func() {
		log.Infon("Starting health gRPC server")
		if err := healthGrpcServer.Serve(healthLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			healthErrCh <- fmt.Errorf("health server error: %w", err)
		}
	})

	service, err := node.NewService(ctx, nodeConfig, storage, conf, stat, log.Child("service"))
	if err != nil {
		return fmt.Errorf("creating node service: %w", err)
	}

	degradedNodesObserver := &configObserver{
		key: degradedNodesConfKey,
		f:   service.DegradedNodesChanged,
	}
	conf.RegisterObserver(degradedNodesObserver)

	// Configure gRPC server keepalive parameters
	grpcKeepaliveMinTime := conf.GetDurationVar(10, time.Second, "grpc.keepalive.minTime")
	grpcKeepalivePermitWithoutStream := conf.GetBoolVar(true, "grpc.keepalive.permitWithoutStream")
	grpcKeepaliveTime := conf.GetDurationVar(60, time.Second, "grpc.keepalive.time")
	grpcKeepaliveTimeout := conf.GetDurationVar(20, time.Second, "grpc.keepalive.timeout")

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
	// Also expose the health service on the main port so in-cluster clients that dial
	// 50051 can use gRPC health checks without a round-trip to the health port.
	healthpb.RegisterHealthServer(server, healthServer)

	// Start listening
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("listening on port %d: %w", port, err)
	}

	log.Infon("Starting node service server",
		logger.NewStringField("addresses", strings.Join(addresses, ",")),
		logger.NewStringField("degradedNodes", degradedNodes.Load()),
	)

	wg.Go(func() {
		defer log.Infon("Profiler server terminated")
		if err := profiler.StartServer(ctx, conf.GetIntVar(7777, 1, "Profiler.Port")); err != nil {
			log.Warnn("Profiler server failed", obskit.Error(err))
			return
		}
	})

	serverErrCh := make(chan error, 1)
	wg.Go(func() {
		if err := server.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("server error: %w", err)
		}
	})

	wg.Go(func() {
		<-ctx.Done()
		log.Infon("Terminating service")
		// Flip health status to NOT_SERVING first so readiness probes fail and
		// Kubernetes removes this pod from Service endpoints before we stop serving.
		healthServer.Shutdown()
		service.Close()
		server.GracefulStop()
		_ = lis.Close()
		// Stop the dedicated health server last so probes can still be answered
		// (with NOT_SERVING) while the main server is draining.
		healthGrpcServer.GracefulStop()
		_ = healthLis.Close()
	})

	// Main server is now serving -> mark NodeService ready so readiness probes
	// pass and Kubernetes starts routing traffic to this pod.
	healthServer.SetServingStatus(pb.NodeService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)

	select {
	case <-ctx.Done():
		log.Infon("Shutting down GRPC server")
		return ctx.Err()
	case err := <-serverErrCh:
		return err
	case err := <-healthErrCh:
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
