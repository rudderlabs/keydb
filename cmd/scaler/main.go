package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/scaler"
	"github.com/rudderlabs/keydb/release"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)

	conf := config.New(config.WithEnvPrefix("KEYDB_SCALER"))
	logFactory := logger.NewFactory(conf)
	defer logFactory.Sync()
	log := logFactory.NewLogger()

	releaseInfo := release.NewInfo()
	statsOptions := []stats.Option{
		stats.WithServiceName("keydb-scaler"),
		stats.WithServiceVersion(releaseInfo.Version),
		stats.WithDefaultHistogramBuckets(defaultHistogramBuckets),
	}
	for histogramName, buckets := range customBuckets {
		statsOptions = append(statsOptions, stats.WithHistogramBuckets(histogramName, buckets))
	}
	stat := stats.NewStats(conf, logFactory, svcMetric.NewManager(), statsOptions...)
	if err := stat.Start(ctx, stats.DefaultGoRoutineFactory); err != nil {
		log.Errorn("Failed to start Stats", obskit.Error(err))
		os.Exit(1)
	}
	defer stat.Stop()

	if err := run(ctx, cancel, conf, stat, log); err != nil {
		log.Errorn("Failed to run", obskit.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, cancel func(), conf *config.Config, stat stats.Stats, log logger.Logger) error {
	defer cancel()

	nodeAddresses := conf.GetStringVar("", "nodeAddresses")
	if len(nodeAddresses) == 0 {
		return fmt.Errorf("no node addresses provided")
	}

	clientConfig := client.Config{
		Addresses:          strings.Split(nodeAddresses, ","),
		TotalHashRanges:    int64(conf.GetIntVar(int(client.DefaultTotalHashRanges), 1, "totalHashRanges")),
		ConnectionPoolSize: conf.GetIntVar(0, 1, "connectionPoolSize"),
		RetryPolicy: client.RetryPolicy{
			Disabled:        conf.GetBoolVar(false, "retryPolicy.disabled"),
			InitialInterval: conf.GetDurationVar(0, time.Second, "retryPolicy.initialInterval"),
			Multiplier:      conf.GetFloat64Var(0, "retryPolicy.multiplier"),
			MaxInterval:     conf.GetDurationVar(0, time.Second, "retryPolicy.maxInterval"),
		},
		GrpcConfig: client.GrpcConfig{
			KeepAliveTime:                       conf.GetDurationVar(0, time.Second, "grpc.keepAliveTime"),
			KeepAliveTimeout:                    conf.GetDurationVar(0, time.Second, "grpc.keepAliveTimeout"),
			DisableKeepAlivePermitWithoutStream: conf.GetBoolVar(false, "grpc.disableKeepAlivePermitWithoutStream"),
			BackoffBaseDelay:                    conf.GetDurationVar(0, time.Second, "grpc.backoffBaseDelay"),
			BackoffMultiplier:                   conf.GetFloat64Var(0, "grpc.backoffMultiplier"),
			BackoffJitter:                       conf.GetFloat64Var(0, "grpc.backoffJitter"),
			BackoffMaxDelay:                     conf.GetDurationVar(0, time.Second, "grpc.backoffMaxDelay"),
			MinConnectTimeout:                   conf.GetDurationVar(0, time.Second, "grpc.minConnectTimeout"),
		},
	}
	scClient, err := scaler.NewClient(scaler.Config{
		Addresses:       strings.Split(nodeAddresses, ","),
		TotalHashRanges: int64(conf.GetIntVar(int(client.DefaultTotalHashRanges), 1, "totalHashRanges")),
		RetryPolicy: scaler.RetryPolicy{
			Disabled:        conf.GetBoolVar(false, "scalerRetryPolicy.disabled"),
			InitialInterval: conf.GetDurationVar(0, time.Second, "scalerRetryPolicy.initialInterval"),
			Multiplier:      conf.GetFloat64Var(0, "scalerRetryPolicy.multiplier"),
			MaxInterval:     conf.GetDurationVar(0, time.Second, "scalerRetryPolicy.maxInterval"),
			MaxElapsedTime:  conf.GetDurationVar(0, time.Second, "scalerRetryPolicy.maxElapsedTime"),
		},
		GrpcConfig: scaler.GrpcConfig{
			KeepAliveTime:    conf.GetDurationVar(0, time.Second, "scalerGrpc.keepAliveTime"),
			KeepAliveTimeout: conf.GetDurationVar(0, time.Second, "scalerGrpc.keepAliveTimeout"),
			DisableKeepAlivePermitWithoutStream: conf.GetBoolVar(
				false, "scalerGrpc.disableKeepAlivePermitWithoutStream",
			),
			BackoffBaseDelay:  conf.GetDurationVar(0, time.Second, "scalerGrpc.backoffBaseDelay"),
			BackoffMultiplier: conf.GetFloat64Var(0, "scalerGrpc.backoffMultiplier"),
			BackoffJitter:     conf.GetFloat64Var(0, "scalerGrpc.backoffJitter"),
			BackoffMaxDelay:   conf.GetDurationVar(0, time.Second, "scalerGrpc.backoffMaxDelay"),
			MinConnectTimeout: conf.GetDurationVar(0, time.Second, "scalerGrpc.minConnectTimeout"),
		},
	}, log.Child("scaler"))
	if err != nil {
		return fmt.Errorf("failed to create scaler: %w", err)
	}
	defer func() {
		if err := scClient.Close(); err != nil {
			log.Warnn("Failed to close scaler", obskit.Error(err))
		}
	}()

	log.Infon("Starting scaler",
		logger.NewIntField("totalHashRanges", clientConfig.TotalHashRanges),
		logger.NewBoolField("retryPolicyDisabled", clientConfig.RetryPolicy.Disabled),
		logger.NewDurationField("retryPolicyInitialInterval", clientConfig.RetryPolicy.InitialInterval),
		logger.NewFloatField("retryPolicyMultiplier", clientConfig.RetryPolicy.Multiplier),
		logger.NewDurationField("retryPolicyMaxInterval", clientConfig.RetryPolicy.MaxInterval),
		logger.NewStringField("nodeAddresses", fmt.Sprintf("%+v", clientConfig.Addresses)),
		logger.NewIntField("noOfAddresses", int64(len(clientConfig.Addresses))),
	)
	for i, addr := range clientConfig.Addresses {
		log.Infon("Detected node", logger.NewIntField("index", int64(i)), logger.NewStringField("address", addr))
	}

	// Create and start HTTP server
	serverAddr := conf.GetStringVar(":8080", "serverAddr")
	server, err := newHTTPServer(clientConfig, scClient, serverAddr, stat, log)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Warnn("Failed to close server", obskit.Error(err))
		}
	}()

	// Start server in a goroutine
	serverErrCh := make(chan error, 1)
	go func() {
		log.Infon("Starting HTTP server", logger.NewStringField("addr", serverAddr))
		if err := server.Start(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("server error: %w", err)
		}
		close(serverErrCh)
	}()

	// Wait for context cancellation or server error
	select {
	case err := <-serverErrCh:
		return err
	case <-ctx.Done():
		log.Infon("Shutting down HTTP server")
		// Create a timeout context for graceful shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := server.Stop(shutdownCtx); err != nil {
			log.Warnn("Error shutting down HTTP server", obskit.Error(err))
		}
		return ctx.Err()
	}
}
