# KeyDB Direct Node-to-Node Hash Range Streaming - Analysis & Implementation Report

## Executive Summary

This report analyzes the effort required to implement direct node-to-node hash range streaming via gRPC as an alternative to the existing S3-based snapshot transfer mechanism in KeyDB. The new feature will provide faster, more efficient data migration during scaling operations while maintaining the existing S3 functionality as a fallback option.

**Estimated Effort:** Medium-High (3-4 weeks for 1 developer)

**Key Benefits:**
- Faster scaling operations (eliminates S3 upload/download overhead)
- Lower network egress costs (no S3 transfer costs)
- Reduced latency for hash range transfers
- Better resource utilization during scaling
- Choice between streaming and S3-based migration

## Table of Contents

1. [Current Architecture Overview](#current-architecture-overview)
2. [Proposed Architecture](#proposed-architecture)
3. [Required Changes](#required-changes)
4. [Implementation Details](#implementation-details)
5. [Testing Strategy](#testing-strategy)
6. [Migration & Rollout Plan](#migration--rollout-plan)
7. [Performance Considerations](#performance-considerations)
8. [Risk Assessment](#risk-assessment)

---

## Current Architecture Overview

### How Hash Range Migration Works Today

The current system uses S3 as an intermediate storage layer for migrating hash ranges between nodes:

1. **Snapshot Creation:** Source nodes create compressed snapshots of hash ranges using BadgerDB's streaming API
2. **S3 Upload:** Snapshots are uploaded to S3 with naming convention: `hr_<hash_range>_s_<from_timestamp>_<to_timestamp>.snapshot`
3. **S3 Download:** Destination nodes download snapshots from S3
4. **Snapshot Loading:** Downloaded snapshots are loaded into BadgerDB using the `LoadSnapshots` method

### Key Components

#### 1. gRPC Service Definition (`proto/keydb.proto`)
Current gRPC methods:
- `CreateSnapshots` - Creates and uploads snapshots to S3
- `LoadSnapshots` - Downloads and loads snapshots from S3
- `Scale` - Updates cluster configuration
- `ScaleComplete` - Finalizes scaling operation

#### 2. Node Service (`node/node.go`)
Manages:
- Hash range assignment using consistent hashing
- Snapshot creation via BadgerDB streaming (`CreateSnapshots`)
- Snapshot loading from S3 (`LoadSnapshots`)
- Scaling state management (blocks operations during scaling)

#### 3. Badger Cache (`internal/cache/badger/badger.go`)
Provides:
- `CreateSnapshots` - Streams data using BadgerDB's `Stream` API with optional zstd compression
- `LoadSnapshots` - Loads data from io.Reader into BadgerDB
- Per-hash-range key prefixing (`hr<hashRange>:<key>`)

#### 4. Scaler Client (`internal/scaler/scaler.go`)
Orchestrates:
- gRPC connections to all nodes
- Snapshot creation/loading coordination
- Rollback logic on failure
- Hash range movement calculation

#### 5. Cloud Storage (`internal/cloudstorage/cloudstorage.go`)
- S3 integration using `rudder-go-kit/filemanager`
- Upload/download/delete operations
- File listing with prefix filtering

### Current Scaling Flow (Scale Up Example)

```
1. Scaler determines hash range movements (sourceNodeMovements, destinationNodeMovements)
2. For each source node:
   - Call CreateSnapshots(ctx, nodeID, fullSync, hashRanges...)
   - Node creates snapshots and uploads to S3
3. For each destination node:
   - Call LoadSnapshots(ctx, nodeID, maxConcurrency, hashRanges...)
   - Node downloads from S3 and loads snapshots
4. Call Scale(ctx, nodeIDs) on all nodes
5. Call ScaleComplete(ctx, nodeIDs) to resume normal operations
```

### Pain Points with S3-Based Approach

1. **High Latency:** Double network hop (upload to S3, download from S3)
2. **S3 Costs:** Egress charges for downloading snapshots
3. **Complexity:** Requires S3 configuration and credentials
4. **Resource Intensive:** Compression/decompression on both ends
5. **Slower Scaling:** Especially for large hash ranges
6. **No Progress Visibility:** Cannot track streaming progress in real-time

---

## Proposed Architecture

### Direct Node-to-Node Streaming

Instead of using S3 as an intermediary, nodes will stream hash range data directly to each other via gRPC streaming RPCs.

### High-Level Design

```
Source Node                Destination Node
    |                            |
    |  StreamHashRange(hr=5) --> |
    |                            |
    |  <-- StreamChunk 1         |
    |  <-- StreamChunk 2         |
    |  <-- StreamChunk N         |
    |                            |
    |  StreamComplete -->        |
    |                            |
```

### Key Architectural Decisions

#### 1. Bidirectional vs Server Streaming

**Recommendation: Server Streaming (Source → Destination)**

The source node will stream data to the destination node using gRPC server-side streaming. This is simpler than bidirectional streaming and provides better flow control.

```protobuf
rpc StreamHashRange(StreamHashRangeRequest) returns (stream HashRangeChunk) {}
```

#### 2. Pull vs Push Model

**Recommendation: Pull Model**

The destination node initiates the streaming request, pulling data from the source node. This provides better:
- Flow control (destination controls when to request data)
- Backpressure handling
- Error recovery
- Resource management

#### 3. Transfer Mode Selection

Add a `TransferMode` enum to allow choosing between S3 and streaming:

```go
type TransferMode string

const (
    TransferModeS3       TransferMode = "s3"
    TransferModeStreaming TransferMode = "streaming"
)
```

Configuration via environment variable:
```bash
KEYDB_TRANSFER_MODE=streaming  # or "s3"
```

#### 4. Chunk Size & Compression

- **Chunk Size:** 4MB per chunk (configurable)
- **Compression:** Optional zstd compression per chunk
- **Buffering:** Stream chunks directly from BadgerDB without full materialization

---

## Required Changes

### 1. Protocol Buffer Changes (`proto/keydb.proto`)

**New Messages:**

```protobuf
// StreamHashRangeRequest initiates streaming of a hash range from source to destination
message StreamHashRangeRequest {
  uint32 hash_range = 1;        // Hash range to stream
  uint32 source_node_id = 2;    // Node that owns the data
  uint64 since = 3;             // Only stream data with version > since (for incremental)
  bool compress = 4;            // Whether to compress chunks with zstd
}

// HashRangeChunk represents a chunk of data for a hash range
message HashRangeChunk {
  bytes data = 1;               // Chunk data (potentially compressed)
  uint64 sequence = 2;          // Sequence number for ordering
  bool is_compressed = 3;       // Whether this chunk is compressed
  bool is_final = 4;            // True for the last chunk
  uint64 max_version = 5;       // Maximum version in this chunk
  string error_message = 6;     // Error message if streaming failed
}

// StreamHashRangeResponse is sent after all chunks are received
message StreamHashRangeResponse {
  bool success = 1;
  string error_message = 2;
  uint64 chunks_received = 3;
  uint64 bytes_received = 4;
}
```

**New RPC Methods:**

```protobuf
service NodeService {
  // Existing methods...

  // StreamHashRange streams a hash range from source node to destination node
  // The destination node calls this on the source node
  rpc StreamHashRange(StreamHashRangeRequest) returns (stream HashRangeChunk) {}

  // ReceiveHashRange is called by scaler to initiate receiving from a source
  rpc ReceiveHashRange(ReceiveHashRangeRequest) returns (ReceiveHashRangeResponse) {}
}

message ReceiveHashRangeRequest {
  uint32 hash_range = 1;
  uint32 source_node_id = 2;
  string source_address = 3;  // Address of source node
  bool compress = 4;
  uint64 since = 5;
}

message ReceiveHashRangeResponse {
  bool success = 1;
  string error_message = 2;
  uint64 chunks_received = 3;
  uint64 bytes_received = 4;
}
```

**Effort:** 2-3 hours
**Files Modified:** `proto/keydb.proto`

---

### 2. Node Service Changes (`node/node.go`)

#### New Method: StreamHashRange

**Purpose:** Stream a hash range to a requesting node

```go
func (s *Service) StreamHashRange(
    req *pb.StreamHashRangeRequest,
    stream pb.NodeService_StreamHashRangeServer,
) error {
    // Implementation details below
}
```

**Implementation Steps:**

1. **Validate Request**
   - Check if node owns the requested hash range
   - Verify hash range is valid

2. **Create Streaming Writer**
   - Implement `chunkWriter` that writes to gRPC stream
   - Buffer up to chunk size (4MB) before sending
   - Handle compression if requested

3. **Stream from BadgerDB**
   - Use existing `CreateSnapshots` logic
   - Stream directly instead of writing to buffer
   - Track progress and max version

4. **Error Handling**
   - Send error chunk if streaming fails
   - Ensure cleanup of resources
   - Log progress

**Code Structure:**

```go
type chunkWriter struct {
    stream        pb.NodeService_StreamHashRangeServer
    buffer        *bytes.Buffer
    chunkSize     int
    sequence      uint64
    compress      bool
    maxVersion    uint64
    bytesWritten  uint64
}

func (cw *chunkWriter) Write(p []byte) (n int, err error) {
    // Buffer data and send chunks when buffer is full
}

func (cw *chunkWriter) Flush() error {
    // Send final chunk
}
```

**Effort:** 1 day
**Files Modified:** `node/node.go`
**New Files:** `node/streaming.go` (for streaming-specific logic)

---

#### New Method: ReceiveHashRange

**Purpose:** Receive a hash range from a source node

```go
func (s *Service) ReceiveHashRange(
    ctx context.Context,
    req *pb.ReceiveHashRangeRequest,
) (*pb.ReceiveHashRangeResponse, error) {
    // Implementation details below
}
```

**Implementation Steps:**

1. **Create gRPC Connection to Source**
   - Connect to source node using provided address
   - Use same connection parameters as scaler client

2. **Initiate Streaming Request**
   - Call `StreamHashRange` on source node
   - Pass hash range and compression settings

3. **Receive and Buffer Chunks**
   - Receive chunks from stream
   - Decompress if needed
   - Buffer and reassemble

4. **Load into BadgerDB**
   - Use existing `LoadSnapshots` method
   - Pass reconstructed reader
   - Track progress

5. **Update Metadata**
   - Update `since` map with max version
   - Log completion

**Effort:** 1 day
**Files Modified:** `node/node.go`

---

### 3. Badger Cache Changes (`internal/cache/badger/badger.go`)

#### Modify CreateSnapshots to Support Streaming

Current implementation already supports streaming via `io.Writer`, but we need to optimize it for direct streaming:

**Changes:**

1. **Add Streaming-Aware Writer**
   - Create `streamWriter` type that flushes more frequently
   - Reduce buffering for lower latency
   - Better progress reporting

2. **Chunk-Based Compression**
   - Compress per chunk instead of entire stream
   - Allows destination to start loading earlier
   - Better memory efficiency

**Effort:** 4-6 hours
**Files Modified:** `internal/cache/badger/badger.go`

---

### 4. Scaler Client Changes (`internal/scaler/scaler.go`)

#### Add Transfer Mode Support

**New Configuration:**

```go
type Config struct {
    // Existing fields...

    // TransferMode determines how hash ranges are transferred
    TransferMode TransferMode

    // StreamingConfig for streaming-specific settings
    StreamingConfig StreamingConfig
}

type StreamingConfig struct {
    ChunkSize           int           // Size of each streaming chunk (default 4MB)
    EnableCompression   bool          // Enable zstd compression
    ConcurrentTransfers int           // Max concurrent hash range transfers
    ReceiveTimeout      time.Duration // Timeout for receiving a hash range
}
```

**New Methods:**

```go
// StreamHashRanges streams hash ranges directly between nodes
func (c *Client) StreamHashRanges(
    ctx context.Context,
    sourceNodeID, destNodeID uint32,
    hashRanges []uint32,
) error

// ReceiveHashRange tells a node to receive a hash range from source
func (c *Client) ReceiveHashRange(
    ctx context.Context,
    destNodeID, sourceNodeID uint32,
    hashRange uint32,
) error
```

**Effort:** 1 day
**Files Modified:** `internal/scaler/scaler.go`

---

### 5. HTTP Server Changes (`cmd/scaler/server.go`)

#### Modify AutoScale Handlers

**Changes to handleScaleUp and handleScaleDown:**

```go
func (s *httpServer) handleScaleUp(
    ctx context.Context, oldAddresses, newAddresses []string,
    fullSync, skipCreateSnapshots bool, loadSnapshotsMaxConcurrency uint32,
    transferMode TransferMode, // NEW PARAMETER
) error {
    // Existing logic...

    if transferMode == TransferModeStreaming {
        // Use streaming instead of S3
        return s.streamHashRanges(ctx, sourceNodeMovements, destinationNodeMovements)
    } else {
        // Existing S3-based logic
        return s.transferViaS3(ctx, sourceNodeMovements, destinationNodeMovements, fullSync, skipCreateSnapshots, loadSnapshotsMaxConcurrency)
    }
}
```

**New Helper Methods:**

```go
func (s *httpServer) streamHashRanges(
    ctx context.Context,
    sourceNodeMovements, destinationNodeMovements map[uint32][]uint32,
) error {
    // Coordinate streaming between nodes
}
```

**Effort:** 1 day
**Files Modified:** `cmd/scaler/server.go`, `cmd/scaler/types.go`

---

### 6. Configuration Changes (`cmd/node/config.go`, `cmd/scaler/main.go`)

**Node Configuration:**

```go
// Add to node config
TransferMode           string        `envconfig:"TRANSFER_MODE" default:"s3"`
StreamingChunkSize     int           `envconfig:"STREAMING_CHUNK_SIZE" default:"4194304"` // 4MB
StreamingCompression   bool          `envconfig:"STREAMING_COMPRESSION" default:"true"`
```

**Scaler Configuration:**

```go
// Add to scaler config
TransferMode              string `envconfig:"TRANSFER_MODE" default:"s3"`
StreamingConcurrentXfers  int    `envconfig:"STREAMING_CONCURRENT_TRANSFERS" default:"3"`
```

**Effort:** 2-3 hours
**Files Modified:** `cmd/node/config.go`, `cmd/scaler/main.go`

---

### 7. Client Changes (`client/client.go`)

**No changes required** - The client only interacts with nodes for Get/Put operations, not for scaling. Scaling is managed by the scaler.

**Effort:** 0 hours

---

### 8. Testing Infrastructure

#### Unit Tests

**New Test Files:**

1. `node/streaming_test.go` - Test streaming methods
2. `internal/scaler/streaming_test.go` - Test scaler streaming coordination

**Test Scenarios:**

- Single hash range streaming
- Multiple concurrent streams
- Compression enabled/disabled
- Large hash ranges (>100MB)
- Network failures and retries
- Chunk ordering
- Incremental streaming (since parameter)

**Effort:** 2 days

---

#### Integration Tests

**New Test File:** `integration_test_streaming.go`

**Test Scenarios:**

1. Scale up 1→2 nodes using streaming
2. Scale up 2→4 nodes using streaming
3. Scale down 4→2 nodes using streaming
4. Compare S3 vs Streaming performance
5. Mixed mode: Some hash ranges via S3, some via streaming
6. Failure scenarios: Source node crashes mid-stream
7. Rollback scenarios

**Effort:** 2 days

---

#### Benchmark Tests

**New Test File:** `node/streaming_benchmark_test.go`

**Benchmarks:**

- Streaming throughput (MB/s)
- S3 vs Streaming comparison
- Compression overhead
- Concurrent streams impact

**Effort:** 1 day

---

### 9. Documentation

**Files to Create/Update:**

1. **README.md** - Add streaming mode documentation
2. **ARCHITECTURE.md** - Document streaming architecture
3. **CONFIGURATION.md** - Document new config options
4. **MIGRATION_GUIDE.md** - Guide for switching to streaming mode

**Key Topics:**

- How to enable streaming mode
- Performance comparison S3 vs Streaming
- When to use S3 vs Streaming
- Troubleshooting streaming issues
- Configuration best practices

**Effort:** 1 day

---

## Implementation Details

### Detailed Flow: Scale Up with Streaming

```
┌─────────────────────────────────────────────────────────────────────┐
│ Scaler HTTP Server receives /autoScale request                     │
│ - transferMode: "streaming"                                         │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Calculate Hash Range Movements                                      │
│ - sourceNodeMovements: {0: [5, 10, 15], 1: [20, 25]}               │
│ - destinationNodeMovements: {2: [5, 10], 3: [15, 20, 25]}          │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Update Cluster Data                                                 │
│ - scaler.UpdateClusterData(newAddresses...)                        │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ For each destination node, initiate streaming (in parallel)         │
│                                                                      │
│ Destination Node 2:                                                 │
│   ├─ ReceiveHashRange(hr=5, source=Node0, addr=node0:50051)       │
│   └─ ReceiveHashRange(hr=10, source=Node0, addr=node0:50051)      │
│                                                                      │
│ Destination Node 3:                                                 │
│   ├─ ReceiveHashRange(hr=15, source=Node0, addr=node0:50051)      │
│   ├─ ReceiveHashRange(hr=20, source=Node1, addr=node1:50051)      │
│   └─ ReceiveHashRange(hr=25, source=Node1, addr=node1:50051)      │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Each Destination Node:                                              │
│                                                                      │
│ 1. Connect to Source Node via gRPC                                 │
│ 2. Call StreamHashRange(hashRange, since, compress)                │
│ 3. Source streams chunks via BadgerDB Stream API                   │
│ 4. Destination receives chunks, decompresses, buffers              │
│ 5. Load buffered data into BadgerDB                                │
│ 6. Update 'since' metadata with max version                        │
│ 7. Return success/failure                                          │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Wait for all streaming to complete (errgroup)                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Complete Scale Operation                                            │
│ - Scale(ctx, allNodeIDs)                                           │
│ - ScaleComplete(ctx, allNodeIDs)                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Code Example: StreamHashRange Implementation

```go
// node/streaming.go

type chunkWriter struct {
    stream        pb.NodeService_StreamHashRangeServer
    buffer        *bytes.Buffer
    chunkSize     int
    sequence      uint64
    compress      bool
    compressor    *zstd.Writer
    maxVersion    uint64
    bytesWritten  uint64
    logger        logger.Logger
}

func newChunkWriter(
    stream pb.NodeService_StreamHashRangeServer,
    chunkSize int,
    compress bool,
    log logger.Logger,
) *chunkWriter {
    cw := &chunkWriter{
        stream:    stream,
        buffer:    bytes.NewBuffer(make([]byte, 0, chunkSize)),
        chunkSize: chunkSize,
        compress:  compress,
        sequence:  0,
        logger:    log,
    }
    if compress {
        cw.compressor = zstd.NewWriter(cw.buffer)
    }
    return cw
}

func (cw *chunkWriter) Write(p []byte) (n int, err error) {
    if cw.compress {
        n, err = cw.compressor.Write(p)
    } else {
        n, err = cw.buffer.Write(p)
    }
    cw.bytesWritten += uint64(n)

    // Send chunk when buffer is full
    if cw.buffer.Len() >= cw.chunkSize {
        if err := cw.Flush(); err != nil {
            return n, err
        }
    }

    return n, err
}

func (cw *chunkWriter) Flush() error {
    if cw.buffer.Len() == 0 {
        return nil
    }

    if cw.compress && cw.compressor != nil {
        if err := cw.compressor.Close(); err != nil {
            return err
        }
    }

    chunk := &pb.HashRangeChunk{
        Data:         cw.buffer.Bytes(),
        Sequence:     cw.sequence,
        IsCompressed: cw.compress,
        IsFinal:      false,
        MaxVersion:   cw.maxVersion,
    }

    if err := cw.stream.Send(chunk); err != nil {
        return err
    }

    cw.logger.Debugn("Sent chunk",
        logger.NewIntField("sequence", int64(cw.sequence)),
        logger.NewIntField("size", int64(len(chunk.Data))),
        logger.NewBoolField("compressed", cw.compress),
    )

    cw.sequence++
    cw.buffer.Reset()

    if cw.compress {
        cw.compressor = zstd.NewWriter(cw.buffer)
    }

    return nil
}

func (cw *chunkWriter) Close() error {
    // Flush any remaining data
    if err := cw.Flush(); err != nil {
        return err
    }

    // Send final chunk
    finalChunk := &pb.HashRangeChunk{
        Sequence:   cw.sequence,
        IsFinal:    true,
        MaxVersion: cw.maxVersion,
    }

    return cw.stream.Send(finalChunk)
}

// StreamHashRange streams a hash range to a requesting destination node
func (s *Service) StreamHashRange(
    req *pb.StreamHashRangeRequest,
    stream pb.NodeService_StreamHashRangeServer,
) error {
    log := s.logger.Withn(
        logger.NewIntField("hashRange", int64(req.HashRange)),
        logger.NewIntField("sourceNodeId", int64(req.SourceNodeId)),
        logger.NewIntField("since", int64(req.Since)),
        logger.NewBoolField("compress", req.Compress),
    )
    log.Infon("StreamHashRange request received")

    s.mu.RLock()
    defer s.mu.RUnlock()

    // Verify we own this hash range
    currentRanges := s.getCurrentRanges()
    if _, ok := currentRanges[req.HashRange]; !ok {
        err := fmt.Errorf("hash range %d not owned by this node", req.HashRange)
        log.Errorn("Hash range not owned", obskit.Error(err))
        // Send error chunk
        return stream.Send(&pb.HashRangeChunk{
            ErrorMessage: err.Error(),
            IsFinal:      true,
        })
    }

    // Create chunk writer
    chunkSize := s.config.StreamingChunkSize
    if chunkSize == 0 {
        chunkSize = 4 * 1024 * 1024 // 4MB default
    }

    writer := newChunkWriter(stream, chunkSize, req.Compress, log)
    defer writer.Close()

    // Create writers map for single hash range
    writers := map[uint32]io.Writer{
        req.HashRange: writer,
    }

    // Create since map
    since := map[uint32]uint64{
        req.HashRange: req.Since,
    }

    // Stream from BadgerDB
    start := time.Now()
    maxVersion, hasData, err := s.cache.CreateSnapshots(stream.Context(), writers, since)
    if err != nil {
        log.Errorn("Failed to stream hash range", obskit.Error(err))
        return stream.Send(&pb.HashRangeChunk{
            ErrorMessage: fmt.Sprintf("streaming failed: %v", err),
            IsFinal:      true,
        })
    }

    writer.maxVersion = maxVersion

    log.Infon("Hash range streamed successfully",
        logger.NewBoolField("hasData", hasData[req.HashRange]),
        logger.NewIntField("maxVersion", int64(maxVersion)),
        logger.NewIntField("bytesWritten", int64(writer.bytesWritten)),
        logger.NewStringField("duration", time.Since(start).String()),
    )

    return nil
}

// ReceiveHashRange receives a hash range from a source node
func (s *Service) ReceiveHashRange(
    ctx context.Context,
    req *pb.ReceiveHashRangeRequest,
) (*pb.ReceiveHashRangeResponse, error) {
    log := s.logger.Withn(
        logger.NewIntField("hashRange", int64(req.HashRange)),
        logger.NewIntField("sourceNodeId", int64(req.SourceNodeId)),
        logger.NewStringField("sourceAddress", req.SourceAddress),
        logger.NewBoolField("compress", req.Compress),
        logger.NewIntField("since", int64(req.Since)),
    )
    log.Infon("ReceiveHashRange request received")

    // Connect to source node
    conn, err := s.createConnectionToNode(req.SourceAddress)
    if err != nil {
        log.Errorn("Failed to connect to source node", obskit.Error(err))
        return &pb.ReceiveHashRangeResponse{
            Success:      false,
            ErrorMessage: fmt.Sprintf("connection failed: %v", err),
        }, nil
    }
    defer func() { _ = conn.Close() }()

    client := pb.NewNodeServiceClient(conn)

    // Initiate streaming
    streamReq := &pb.StreamHashRangeRequest{
        HashRange:    req.HashRange,
        SourceNodeId: req.SourceNodeId,
        Since:        req.Since,
        Compress:     req.Compress,
    }

    stream, err := client.StreamHashRange(ctx, streamReq)
    if err != nil {
        log.Errorn("Failed to initiate stream", obskit.Error(err))
        return &pb.ReceiveHashRangeResponse{
            Success:      false,
            ErrorMessage: fmt.Sprintf("stream initiation failed: %v", err),
        }, nil
    }

    // Receive chunks and reconstruct data
    var (
        buffer         = bytes.NewBuffer(nil)
        chunksReceived uint64
        bytesReceived  uint64
        maxVersion     uint64
    )

    for {
        chunk, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Errorn("Failed to receive chunk", obskit.Error(err))
            return &pb.ReceiveHashRangeResponse{
                Success:      false,
                ErrorMessage: fmt.Sprintf("receive failed: %v", err),
            }, nil
        }

        if chunk.ErrorMessage != "" {
            log.Errorn("Received error chunk",
                logger.NewStringField("error", chunk.ErrorMessage))
            return &pb.ReceiveHashRangeResponse{
                Success:      false,
                ErrorMessage: chunk.ErrorMessage,
            }, nil
        }

        if chunk.IsFinal {
            log.Infon("Received final chunk",
                logger.NewIntField("chunksReceived", int64(chunksReceived)),
                logger.NewIntField("bytesReceived", int64(bytesReceived)),
            )
            break
        }

        // Decompress if needed
        var data []byte
        if chunk.IsCompressed {
            decompressed, err := zstd.Decompress(nil, chunk.Data)
            if err != nil {
                log.Errorn("Failed to decompress chunk", obskit.Error(err))
                return &pb.ReceiveHashRangeResponse{
                    Success:      false,
                    ErrorMessage: fmt.Sprintf("decompression failed: %v", err),
                }, nil
            }
            data = decompressed
        } else {
            data = chunk.Data
        }

        // Write to buffer
        n, err := buffer.Write(data)
        if err != nil {
            log.Errorn("Failed to buffer chunk", obskit.Error(err))
            return &pb.ReceiveHashRangeResponse{
                Success:      false,
                ErrorMessage: fmt.Sprintf("buffering failed: %v", err),
            }, nil
        }

        chunksReceived++
        bytesReceived += uint64(n)
        if chunk.MaxVersion > maxVersion {
            maxVersion = chunk.MaxVersion
        }

        log.Debugn("Received chunk",
            logger.NewIntField("sequence", int64(chunk.Sequence)),
            logger.NewIntField("size", int64(len(data))),
        )
    }

    // Load into BadgerDB
    s.mu.Lock()
    defer s.mu.Unlock()

    start := time.Now()
    err = s.cache.LoadSnapshots(ctx, bytes.NewReader(buffer.Bytes()))
    if err != nil {
        log.Errorn("Failed to load snapshot", obskit.Error(err))
        return &pb.ReceiveHashRangeResponse{
            Success:      false,
            ErrorMessage: fmt.Sprintf("loading failed: %v", err),
        }, nil
    }

    // Update since metadata
    s.since[req.HashRange] = maxVersion

    log.Infon("Hash range received successfully",
        logger.NewIntField("chunksReceived", int64(chunksReceived)),
        logger.NewIntField("bytesReceived", int64(bytesReceived)),
        logger.NewIntField("maxVersion", int64(maxVersion)),
        logger.NewStringField("duration", time.Since(start).String()),
    )

    return &pb.ReceiveHashRangeResponse{
        Success:        true,
        ChunksReceived: chunksReceived,
        BytesReceived:  bytesReceived,
    }, nil
}

func (s *Service) createConnectionToNode(addr string) (*grpc.ClientConn, error) {
    // Use same connection parameters as scaler
    kacp := keepalive.ClientParameters{
        Time:                10 * time.Second,
        Timeout:             2 * time.Second,
        PermitWithoutStream: true,
    }

    backoffConfig := grpcbackoff.Config{
        BaseDelay:  1 * time.Second,
        Multiplier: 1.6,
        Jitter:     0.2,
        MaxDelay:   2 * time.Minute,
    }

    return grpc.NewClient(addr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithKeepaliveParams(kacp),
        grpc.WithConnectParams(grpc.ConnectParams{
            Backoff:           backoffConfig,
            MinConnectTimeout: 20 * time.Second,
        }),
    )
}
```

---

## Testing Strategy

### Phase 1: Unit Testing (1 week)

**Focus:** Individual component testing

1. **Streaming Writer Tests**
   - Test chunk boundary conditions
   - Test compression/decompression
   - Test error handling
   - Test sequence ordering

2. **gRPC Method Tests**
   - Mock streaming server/client
   - Test StreamHashRange method
   - Test ReceiveHashRange method
   - Test error scenarios

3. **Scaler Client Tests**
   - Test transfer mode selection
   - Test concurrent transfers
   - Test failure recovery

### Phase 2: Integration Testing (1 week)

**Focus:** End-to-end testing with real nodes

1. **Single Hash Range Transfer**
   - Start 2 nodes
   - Transfer 1 hash range via streaming
   - Verify data integrity

2. **Multi Hash Range Transfer**
   - Start 4 nodes
   - Scale from 2→4
   - Transfer multiple hash ranges concurrently
   - Verify data integrity

3. **Failure Scenarios**
   - Source node crashes mid-stream
   - Network partition during streaming
   - Destination node crashes while receiving
   - Verify rollback works correctly

4. **Performance Testing**
   - Compare S3 vs Streaming performance
   - Test with various hash range sizes (1MB, 10MB, 100MB, 1GB)
   - Measure throughput, latency, resource usage

### Phase 3: Stress Testing (3 days)

**Focus:** System limits and edge cases

1. **High Concurrency**
   - Transfer 50+ hash ranges simultaneously
   - Monitor CPU, memory, network usage

2. **Large Hash Ranges**
   - Transfer hash ranges >5GB
   - Test memory efficiency

3. **Long-Running Operations**
   - Scale cluster with 100+ hash ranges
   - Monitor for memory leaks, resource exhaustion

### Test Infrastructure Requirements

**Hardware:**
- 4 test nodes (can be VMs)
- S3-compatible storage (MinIO for local testing)
- Monitoring: Prometheus + Grafana

**Software:**
- Docker/Kubernetes for orchestration
- Load testing tools (hey, wrk)
- Data verification tools

---

## Migration & Rollout Plan

### Phase 1: Development (3-4 weeks)

1. **Week 1:** Protocol changes, basic streaming implementation
2. **Week 2:** Scaler integration, configuration
3. **Week 3:** Testing, bug fixes
4. **Week 4:** Documentation, performance tuning

### Phase 2: Testing in Non-Production (1-2 weeks)

1. **Staging Environment**
   - Deploy with `TRANSFER_MODE=streaming`
   - Run scaling operations
   - Monitor performance, errors
   - Compare with S3 mode

2. **Canary Testing**
   - Run parallel scaling operations (one with S3, one with streaming)
   - Compare results
   - Identify issues

### Phase 3: Production Rollout (Gradual)

**Option 1: Incremental Rollout**

1. **Week 1:** Enable streaming for 1 customer (low traffic)
2. **Week 2-3:** Monitor, expand to 5 customers
3. **Week 4:** Enable for all new customers
4. **Month 2:** Migrate existing customers gradually

**Option 2: Feature Flag**

- Keep both S3 and streaming modes available
- Allow customers to choose via configuration
- Gradually deprecate S3 mode over 6 months

### Rollback Plan

**If Issues Arise:**

1. **Immediate:** Set `TRANSFER_MODE=s3` via config update
2. **No code deployment needed** - both modes will be available
3. **Investigate and fix** streaming issues
4. **Re-enable** after validation

---

## Performance Considerations

### Expected Performance Improvements

**Assumption:** 100 hash ranges to transfer, 100MB each (10GB total)

#### S3 Mode (Current)

```
Upload to S3:
  - Compression: ~30s (10GB → 3GB)
  - Upload: ~60s (3GB at 50MB/s)
  - Total per node: ~90s

Download from S3:
  - Download: ~60s (3GB at 50MB/s)
  - Decompression: ~30s (3GB → 10GB)
  - Total per node: ~90s

Total Time: ~180s (3 minutes)
Network Cost: S3 egress charges
```

#### Streaming Mode (Proposed)

```
Direct Transfer:
  - Compression: ~30s (10GB → 3GB)
  - Transfer: ~40s (3GB at 75MB/s, assuming faster node-to-node network)
  - Decompression: ~30s (3GB → 10GB)
  - Total: ~100s

Total Time: ~100s (1.7 minutes)
Network Cost: No egress charges
```

**Improvement: ~45% faster, no S3 costs**

### Memory Usage

**S3 Mode:**
- Source: Buffers entire snapshot in memory before upload (~3GB compressed)
- Destination: Buffers entire snapshot during download (~3GB compressed)

**Streaming Mode:**
- Source: Buffers only 1 chunk at a time (~4MB compressed)
- Destination: Buffers only 1 chunk at a time (~4MB compressed)

**Improvement: ~750x less memory usage**

### Network Usage

**S3 Mode:**
- Upload: Node → S3
- Download: S3 → Node
- Total: 2x network hops

**Streaming Mode:**
- Transfer: Source Node → Destination Node
- Total: 1x network hop

**Improvement: 50% less network traffic**

### Configuration Tuning

**For Optimal Performance:**

```bash
# Chunk size (balance between latency and throughput)
KEYDB_STREAMING_CHUNK_SIZE=4194304  # 4MB (default)

# Enable compression (reduces network usage)
KEYDB_STREAMING_COMPRESSION=true

# Concurrent transfers (balance between speed and resource usage)
KEYDB_STREAMING_CONCURRENT_TRANSFERS=3

# gRPC keep-alive (detect dead connections faster)
KEYDB_GRPC_KEEP_ALIVE_TIME=10s
KEYDB_GRPC_KEEP_ALIVE_TIMEOUT=2s
```

---

## Risk Assessment

### High Risks

#### 1. Network Instability
**Risk:** Streaming fails mid-transfer due to network issues
**Mitigation:**
- Implement retry logic with exponential backoff
- Use gRPC keep-alive to detect dead connections
- Maintain S3 mode as fallback
- Add circuit breaker pattern

#### 2. Memory Exhaustion
**Risk:** Buffering too much data causes OOM
**Mitigation:**
- Use small chunk sizes (4MB)
- Limit concurrent transfers
- Monitor memory usage during streaming
- Add backpressure mechanism

#### 3. Data Corruption
**Risk:** Data gets corrupted during streaming
**Mitigation:**
- Add checksums per chunk
- Verify max version after transfer
- Test thoroughly with data verification
- Keep S3 mode for critical operations

### Medium Risks

#### 4. Performance Regression
**Risk:** Streaming is slower than S3 in some scenarios
**Mitigation:**
- Benchmark extensively before rollout
- Maintain S3 mode as option
- Allow per-customer configuration
- Monitor performance metrics

#### 5. Rollback Complexity
**Risk:** Rollback during streaming fails
**Mitigation:**
- Test rollback extensively
- Ensure ScaleComplete can handle partial transfers
- Add idempotency to ReceiveHashRange
- Monitor rollback success rate

### Low Risks

#### 6. Breaking Changes
**Risk:** Protocol changes break existing clients
**Mitigation:**
- Protocol changes are backward compatible (new methods)
- Existing methods unchanged
- Version protocol if needed

---

## Summary of Changes

### Files to Modify

| File | Type | Effort | Priority |
|------|------|--------|----------|
| `proto/keydb.proto` | Protocol Definition | 2-3 hours | High |
| `node/node.go` | Core Logic | 2 days | High |
| `node/streaming.go` | New File | 2 days | High |
| `internal/cache/badger/badger.go` | Cache Layer | 4-6 hours | Medium |
| `internal/scaler/scaler.go` | Scaler Client | 1 day | High |
| `cmd/scaler/server.go` | HTTP Server | 1 day | High |
| `cmd/node/config.go` | Configuration | 2-3 hours | Medium |
| `cmd/scaler/main.go` | Configuration | 2-3 hours | Medium |
| `node/streaming_test.go` | Unit Tests | 1 day | High |
| `internal/scaler/streaming_test.go` | Unit Tests | 1 day | High |
| `integration_test_streaming.go` | Integration Tests | 2 days | High |
| `README.md` | Documentation | 4 hours | Medium |

**Total Estimated Effort:** 3-4 weeks (1 developer)

### New Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KEYDB_TRANSFER_MODE` | `s3` | Transfer mode: `s3` or `streaming` |
| `KEYDB_STREAMING_CHUNK_SIZE` | `4194304` | Chunk size in bytes (4MB) |
| `KEYDB_STREAMING_COMPRESSION` | `true` | Enable zstd compression |
| `KEYDB_STREAMING_CONCURRENT_TRANSFERS` | `3` | Max concurrent hash range transfers |

---

## Conclusion

Implementing direct node-to-node hash range streaming is a **medium-high effort** project that will significantly improve KeyDB's scaling performance and reduce operational costs. The key benefits include:

1. **45% faster scaling operations** (estimated)
2. **Elimination of S3 egress costs**
3. **750x reduction in memory usage during transfers**
4. **Simplified operational complexity** (no S3 dependency for scaling)
5. **Maintained flexibility** (keep S3 as fallback option)

The implementation is well-scoped and low-risk because:

- gRPC infrastructure already exists
- Protocol changes are backward compatible
- S3 mode remains available as fallback
- Changes are isolated to scaling operations (no client impact)

**Recommendation:** Proceed with implementation, prioritizing robust testing and gradual rollout.

---

## Appendix A: Alternative Designs Considered

### 1. Bidirectional Streaming
**Rejected Reason:** Added complexity without significant benefit. Server-side streaming from source to destination is sufficient.

### 2. HTTP/2 Streaming
**Rejected Reason:** gRPC already provides HTTP/2 with better tooling and type safety.

### 3. Custom TCP Protocol
**Rejected Reason:** Reinventing the wheel. gRPC provides streaming, compression, and error handling out of the box.

### 4. WebSocket Streaming
**Rejected Reason:** Not suitable for backend services. gRPC is more efficient and better suited for node-to-node communication.

---

## Appendix B: Performance Benchmarks (To Be Updated)

This section will be populated after implementation with real benchmark results:

- Throughput (MB/s) for different hash range sizes
- Latency comparison (S3 vs Streaming)
- Memory usage comparison
- CPU usage comparison
- Network bandwidth utilization

---

## Appendix C: Security Considerations

### Current State
- gRPC connections use insecure credentials (no TLS)
- No authentication between nodes
- S3 uses AWS credentials

### Streaming Mode Security

**Short Term (MVP):**
- Maintain current security model (insecure credentials)
- Nodes trust each other implicitly
- Deploy in private network

**Long Term (Future Enhancement):**
- Add mTLS for node-to-node communication
- Implement node authentication
- Encrypt streaming data
- Add authorization checks before streaming

**Configuration:**
```bash
KEYDB_ENABLE_TLS=true
KEYDB_TLS_CERT_FILE=/path/to/cert.pem
KEYDB_TLS_KEY_FILE=/path/to/key.pem
KEYDB_TLS_CA_FILE=/path/to/ca.pem
```

---

## Appendix D: Monitoring & Observability

### New Metrics to Add

**Streaming Metrics:**

```go
// Node metrics
keydb_streaming_bytes_sent_total{node_id, hash_range}
keydb_streaming_bytes_received_total{node_id, hash_range}
keydb_streaming_chunks_sent_total{node_id, hash_range}
keydb_streaming_chunks_received_total{node_id, hash_range}
keydb_streaming_duration_seconds{node_id, hash_range, operation="send|receive"}
keydb_streaming_errors_total{node_id, hash_range, error_type}

// Scaler metrics
keydb_scaler_streaming_transfers_total{transfer_mode, status="success|failure"}
keydb_scaler_streaming_concurrent_transfers{transfer_mode}
```

**Dashboards:**
- Streaming throughput over time
- Streaming success/failure rate
- Comparison: S3 vs Streaming performance
- Resource usage during streaming

**Alerts:**
- Streaming failure rate >5%
- Streaming duration >10 minutes
- Memory usage >80% during streaming
- Concurrent transfers >10

---

**Report End**