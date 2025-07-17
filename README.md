# KeyDB

KeyDB is a distributed key store (not a key-value store) designed to be fast, scalable, and eventually consistent. 
It provides a simple API for checking the existence of keys with TTL (Time To Live) support.

## Summary

KeyDB is a distributed system that allows you to:
- Store keys (no values) with configurable TTL
- Check for key existence
- Scale horizontally by adding or removing nodes
- Persist data through snapshots to cloud storage
  * Snapshots are used to scale the cluster since new nodes will download the hash ranges they need to manage directly
    from cloud storage

### Key features

- **Distributed Architecture**: Supports multiple nodes with automatic key distribution
- **Scalability**: Dynamically scale the cluster by adding or removing nodes
- **Eventual Consistency**: Changes propagate through the system over time
- **TTL Support**: Keys automatically expire after their time-to-live
- **Persistence**: Snapshots are stored in cloud storage for scaling the system without needing nodes to communicate
  with each other

## Client

The KeyDB client provides a simple interface to interact with the KeyDB cluster.

### Installation

```go
import "github.com/rudderlabs/keydb/client"
```

### Creating a Client

```go
config := client.Config{
    Addresses:       []string{"localhost:50051", "localhost:50052"}, // List of node addresses
    TotalHashRanges: 128,                                           // Optional, defaults to 128
    RetryCount:      3,                                             // Optional, defaults to 3
    RetryDelay:      100 * time.Millisecond,                        // Optional, defaults to 100ms
}

keydbClient, err := client.NewClient(config)
if err != nil {
    // Handle error
}
defer keydbClient.Close()
```

### Adding Keys

To add keys with TTL:

```go
keys := []string{"key1", "key2", "key3"}

// Put the keys
err := keydbClient.Put(context.Background(), keys, 24*time.Hour)
if err != nil {
    // Handle error
}
```

### Checking Key Existence

To check if keys exist:

```go
// Keys to check
keys := []string{"user:123", "session:456", "unknown:key"}

// Get existence status
exists, err := keydbClient.Get(context.Background(), keys)
if err != nil {
    // Handle error
}

// Process results
for i, key := range keys {
    if exists[i] {
        fmt.Printf("Key %s exists\n", key)
    } else {
        fmt.Printf("Key %s does not exist\n", key)
    }
}
```

### Client Features

- **Automatic Retries**: The client automatically retries operations on transient errors
- **Cluster Awareness**: Automatically adapts to cluster size changes
- **Key Distribution**: Automatically routes keys to the correct node based on hash
- **Parallel Operations**: Sends requests to multiple nodes in parallel for better performance

## Operator

The Operator is responsible for managing the KeyDB cluster, including scaling operations.

### Starting a Node

To start a KeyDB node:

```bash
# Configure node with environment variables
export KEYDB_NODE_ID=0
export KEYDB_CLUSTER_SIZE=3
export KEYDB_NODE_ADDRESSES='["localhost:50051","localhost:50052","localhost:50053"]'
export KEYDB_PORT=50051
export KEYDB_SNAPSHOT_INTERVAL=60s
export KEYDB_TOTAL_HASH_RANGES=128

# Storage configuration
export KEYDB_STORAGE_BUCKET="my-keydb-bucket"
export KEYDB_STORAGE_REGION="us-east-1"
export KEYDB_STORAGE_ACCESSKEYID="your-access-key-id"
export KEYDB_STORAGE_ACCESSKEY="your-secret-access-key"

# Start the node
go run cmd/node/main.go
```

### Scaling the Cluster

To scale the KeyDB cluster, use the client's `CreateSnapshots`, `Scale` and `ScaleComplete` methods:
Before scaling the cluster you should call `/createSnapshots` on the Operator HTTP API to force all nodes to create
snapshots of the hash ranges that need moving.
If the `CreateSnapshots` operation is skipped, new nodes added to the cluster during a scale operation, won't be
able to get the data for the hash ranges that they are going to serve, leading to missing data.

```go
// Create a client for operator operations
existingNodes := []string{
    "localhost:50051",
    "localhost:50052",
    "localhost:50053",
}
operatorClient, err := client.NewClient(client.Config{
    Addresses: existingNodes, // Connect to any existing node
})
if err != nil {
    // Handle error
}
defer operatorClient.Close()

// Let's force a snapshot creation on the old nodes first
err = operatorClient.CreateSnapshots(context.Background())
if err != nil {
    // Handle error
}

// New node address
newNode := "localhost:50054", // New node
existingNodes = append(existingNodes, newNode)

// Scale the cluster
err = operatorClient.Scale(context.Background(), existingNodes...)
if err != nil {
    // Handle error
}

// Notify all nodes that scaling is complete
err = operatorClient.ScaleComplete(context.Background())
if err != nil {
    // Handle error
}
```

### Monitoring

To get information about a specific node:

```go
nodeInfo, err := operatorClient.GetNodeInfo(context.Background(), 0) // Node ID 0
if err != nil {
    // Handle error
}

fmt.Printf("Node ID: %d\n", nodeInfo.NodeId)
fmt.Printf("Cluster Size: %d\n", nodeInfo.ClusterSize)
fmt.Printf("Keys Count: %d\n", nodeInfo.KeysCount)
fmt.Printf("Last Snapshot: %s\n", time.Unix(int64(nodeInfo.LastSnapshotTimestamp), 0))
```

### Scaling Best Practices

1. **Prepare New Nodes**: Ensure new nodes are running before scaling
2. **Gradual Scaling**: Scale by small increments for large clusters
3. **Monitor During Scaling**: Watch for errors and performance during scaling
4. **Complete the Scaling**: Always call ScaleComplete after scaling
5. **Create Snapshots**: Create snapshots before and after scaling operations

# Known issues

* During scaling operations the nodes might not be available
* New nodes are to be created first before scaling
* Creating and uploading very big snapshots can become a performance issue
  * A possible solution is that we could snapshot on local disk every 10s and work only on the head and tail of the
    file (i.e. remove expired from head and append new entries at the end of the file).
    This would allow us to hold the lock for a much smaller amount of time.
    Then once a minute we can upload the whole file to S3. The file that we upload could be compressed, 
    for example by using zstd with dictionaries.
* Missing metrics and logs (observability is poor)
* Missing Kubernetes health probes
* Missing linters
* Missing Continuos Integration

