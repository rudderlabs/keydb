package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/rudderlabs/keydb/internal/cloudstorage"
	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/node"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)

	conf := config.New(config.WithEnvPrefix("KEYDB"))
	logFactory := logger.NewFactory(conf)
	defer logFactory.Sync()
	log := logFactory.NewLogger()

	if err := run(ctx, cancel, conf, log); err != nil {
		log.Fataln("failed to run", obskit.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, cancel func(), conf *config.Config, log logger.Logger) error {
	cloudStorage, err := cloudstorage.GetCloudStorage(conf, log)
	if err != nil {
		return fmt.Errorf("failed to create cloud storage: %w", err)
	}

	nodeConfig := node.Config{
		NodeID:           uint32(conf.GetInt("nodeId", 0)),
		ClusterSize:      uint32(conf.GetInt("clusterSize", 1)),
		TotalHashRanges:  uint32(conf.GetInt("totalHashRanges", 128)),
		SnapshotInterval: conf.GetDuration("snapshotInterval", 60, time.Second),
		Addresses:        conf.GetStringSlice("nodeAddresses", []string{"localhost:50051"}),
	}

	port := conf.GetInt("port", 50051)
	log = log.Withn(
		logger.NewIntField("port", int64(port)),
		logger.NewIntField("nodeID", int64(nodeConfig.NodeID)),
		logger.NewIntField("clusterSize", int64(nodeConfig.ClusterSize)),
		logger.NewIntField("totalHashRanges", int64(nodeConfig.TotalHashRanges)),
		logger.NewDurationField("snapshotInterval", nodeConfig.SnapshotInterval),
	)

	service, err := node.NewService(ctx, nodeConfig, cloudStorage, log.Child("service"))
	if err != nil {
		return fmt.Errorf("failed to create node service: %w", err)
	}
	defer service.Close() // TODO test graceful shutdown
	defer cancel()

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	// Start listening
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Infon("Starting node",
		logger.NewIntField("hashRanges", int64(len(
			hash.GetNodeHashRanges(nodeConfig.NodeID, nodeConfig.ClusterSize, nodeConfig.TotalHashRanges),
		))),
	)

	// Start the server
	if err := server.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return ctx.Err()
}
