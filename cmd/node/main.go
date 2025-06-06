package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/internal/node"
	pb "github.com/rudderlabs/keydb/proto"
	"google.golang.org/grpc"
)

func main() {
	// Parse command-line flags
	var (
		port             = flag.Int("port", 50051, "The server port")
		nodeID           = flag.Uint("node-id", 0, "The ID of this node (0-based)")
		clusterSize      = flag.Uint("cluster-size", 1, "The total number of nodes in the cluster")
		hashRanges       = flag.Uint("hash-ranges", 128, "The total number of hash ranges")
		snapshotDir      = flag.String("snapshot-dir", "snapshots", "The directory where snapshots are stored")
		snapshotInterval = flag.Int("snapshot-interval", 60, "The interval for creating snapshots (in seconds)")
	)
	flag.Parse()

	// Override with environment variables if provided
	if envPort := os.Getenv("NODE_PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			*port = p
		}
	}
	if envNodeID := os.Getenv("NODE_ID"); envNodeID != "" {
		if id, err := strconv.ParseUint(envNodeID, 10, 32); err == nil {
			*nodeID = uint(id)
		}
	}
	if envClusterSize := os.Getenv("CLUSTER_SIZE"); envClusterSize != "" {
		if size, err := strconv.ParseUint(envClusterSize, 10, 32); err == nil {
			*clusterSize = uint(size)
		}
	}
	if envHashRanges := os.Getenv("HASH_RANGES"); envHashRanges != "" {
		if ranges, err := strconv.ParseUint(envHashRanges, 10, 32); err == nil {
			*hashRanges = uint(ranges)
		}
	}
	if envSnapshotDir := os.Getenv("SNAPSHOT_DIR"); envSnapshotDir != "" {
		*snapshotDir = envSnapshotDir
	}
	if envSnapshotInterval := os.Getenv("SNAPSHOT_INTERVAL"); envSnapshotInterval != "" {
		if interval, err := strconv.Atoi(envSnapshotInterval); err == nil {
			*snapshotInterval = interval
		}
	}

	// Create the node service
	config := node.Config{
		NodeID:           uint32(*nodeID),
		ClusterSize:      uint32(*clusterSize),
		TotalHashRanges:  uint32(*hashRanges),
		SnapshotDir:      *snapshotDir,
		SnapshotInterval: *snapshotInterval,
	}

	service, err := node.NewService(config)
	if err != nil {
		log.Fatalf("Failed to create node service: %v", err)
	}

	// Create a gRPC server
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, service)

	// Start listening
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Starting node %d of %d on port %d", *nodeID, *clusterSize, *port)
	log.Printf("Handling %d hash ranges out of %d total", len(hash.GetNodeHashRanges(uint32(*nodeID), uint32(*clusterSize), uint32(*hashRanges))), *hashRanges)

	// Start the server
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
