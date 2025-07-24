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
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)

	conf := config.New(config.WithEnvPrefix("KEYDB_OPERATOR"))
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
		RetryCount:      conf.GetInt("retryCount", client.DefaultRetryCount),
		RetryDelay:      conf.GetDuration("retryDelay", 0, time.Nanosecond), // client.DefaultRetryDelay will be used
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

	log.Infon("Starting operator",
		logger.NewIntField("totalHashRanges", int64(clientConfig.TotalHashRanges)),
		logger.NewIntField("retryCount", int64(clientConfig.RetryCount)),
		logger.NewDurationField("retryDelay", clientConfig.RetryDelay),
		logger.NewStringField("nodeAddresses", fmt.Sprintf("%+v", clientConfig.Addresses)),
		logger.NewIntField("noOfAddresses", int64(len(clientConfig.Addresses))),
	)
	for i, addr := range clientConfig.Addresses {
		log.Infon("Detected node", logger.NewIntField("index", int64(i)), logger.NewStringField("address", addr))
	}

	// Create and start HTTP server
	serverAddr := conf.GetString("serverAddr", ":8080")
	server := newHTTPServer(c, serverAddr)

	// Start server in a goroutine
	serverErrCh := make(chan error, 1)
	go func() {
		log.Infon("Starting HTTP server", logger.NewStringField("addr", serverAddr))
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
