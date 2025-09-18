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
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)

	conf := config.New(config.WithEnvPrefix("KEYDB_SCALER"))
	logFactory := logger.NewFactory(conf)
	defer logFactory.Sync()
	log := logFactory.NewLogger()

	if err := run(ctx, cancel, conf, log); err != nil {
		log.Fataln("failed to run", obskit.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, cancel func(), conf *config.Config, log logger.Logger) error {
	defer cancel()

	nodeAddresses := conf.GetString("nodeAddresses", "")
	if len(nodeAddresses) == 0 {
		return fmt.Errorf("no node addresses provided")
	}

	clientConfig := client.Config{
		Addresses:       strings.Split(nodeAddresses, ","),
		TotalHashRanges: uint32(conf.GetInt("totalHashRanges", int(client.DefaultTotalHashRanges))),
		RetryPolicy: client.RetryPolicy{
			Disabled:        conf.GetBool("retryPolicy.disabled", false),
			InitialInterval: conf.GetDuration("retryPolicy.initialInterval", 0, time.Second),
			Multiplier:      conf.GetFloat64("retryPolicy.multiplier", 0),
			MaxInterval:     conf.GetDuration("retryPolicy.maxInterval", 0, time.Second),
		},
		GrpcConfig: client.GrpcConfig{
			KeepAliveTime:                       conf.GetDuration("grpc.keepAliveTime", 0, time.Second),
			KeepAliveTimeout:                    conf.GetDuration("grpc.keepAliveTimeout", 0, time.Second),
			DisableKeepAlivePermitWithoutStream: conf.GetBool("grpc.disableKeepAlivePermitWithoutStream", false),
			BackoffBaseDelay:                    conf.GetDuration("grpc.backoffBaseDelay", 0, time.Second),
			BackoffMultiplier:                   conf.GetFloat64("grpc.backoffMultiplier", 0),
			BackoffJitter:                       conf.GetFloat64("grpc.backoffJitter", 0),
			BackoffMaxDelay:                     conf.GetDuration("grpc.backoffMaxDelay", 0, time.Second),
			MinConnectTimeout:                   conf.GetDuration("grpc.minConnectTimeout", 0, time.Second),
		},
	}
	c, err := client.NewClient(clientConfig, log.Child("client"))
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			log.Warnn("Failed to close client", obskit.Error(err))
		}
	}()
	scClient, err := scaler.NewClient(scaler.Config{
		Addresses:       strings.Split(nodeAddresses, ","),
		TotalHashRanges: uint32(conf.GetInt("totalHashRanges", int(client.DefaultTotalHashRanges))),
		RetryPolicy: scaler.RetryPolicy{
			Disabled:        conf.GetBool("scalerRetryPolicy.disabled", false),
			InitialInterval: conf.GetDuration("scalerRetryPolicy.initialInterval", 0, time.Second),
			Multiplier:      conf.GetFloat64("scalerRetryPolicy.multiplier", 0),
			MaxInterval:     conf.GetDuration("scalerRetryPolicy.maxInterval", 0, time.Second),
			MaxElapsedTime:  conf.GetDuration("scalerRetryPolicy.maxElapsedTime", 0, time.Second),
		},
		GrpcConfig: scaler.GrpcConfig{
			KeepAliveTime:                       conf.GetDuration("scalerGrpc.keepAliveTime", 0, time.Second),
			KeepAliveTimeout:                    conf.GetDuration("scalerGrpc.keepAliveTimeout", 0, time.Second),
			DisableKeepAlivePermitWithoutStream: conf.GetBool("scalerGrpc.disableKeepAlivePermitWithoutStream", false),
			BackoffBaseDelay:                    conf.GetDuration("scalerGrpc.backoffBaseDelay", 0, time.Second),
			BackoffMultiplier:                   conf.GetFloat64("scalerGrpc.backoffMultiplier", 0),
			BackoffJitter:                       conf.GetFloat64("scalerGrpc.backoffJitter", 0),
			BackoffMaxDelay:                     conf.GetDuration("scalerGrpc.backoffMaxDelay", 0, time.Second),
			MinConnectTimeout:                   conf.GetDuration("scalerGrpc.minConnectTimeout", 0, time.Second),
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
		logger.NewIntField("totalHashRanges", int64(clientConfig.TotalHashRanges)),
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
	serverAddr := conf.GetString("serverAddr", ":8080")
	server := newHTTPServer(c, scClient, serverAddr, log)

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
