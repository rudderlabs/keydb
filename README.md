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
- **TTL Support**: Keys automatically expire after their time-to-live
- **Persistence**: Snapshots can be stored in cloud storage for scaling the system or backing up the data without 
  needing nodes to communicate with each other

## Client

The KeyDB client provides a simple interface to interact with the KeyDB cluster.

### Installation

```go
import "github.com/rudderlabs/keydb/client"
```

### Creating a Client

```go
import (
    "github.com/rudderlabs/keydb/client"
    "github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

config := client.Config{
    Addresses:          []string{"localhost:50051", "localhost:50052"}, // List of node addresses
    TotalHashRanges:    271,                                            // Optional, defaults to 271
    ConnectionPoolSize: 10,                                             // Optional, defaults to 10
    RetryPolicy:        client.RetryPolicy{
        Disabled:        false,
        InitialInterval: 100 * time.Millisecond, // Optional, defaults to 100ms
        Multiplier:      1.5,                    // Optional, defaults to 1.5
        MaxInterval:     30 * time.Second,       // Optional, defaults to 30s
    },
}

// Logger is required
logFactory := logger.NewFactory(conf)
log := logFactory.NewLogger()

keydbClient, err := client.NewClient(config, log, client.WithStats(stats.NOP))
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

- **Connection Pooling**: Each node connection uses a pool of gRPC connections (default 10) for improved throughput
  - Connections are distributed using round-robin load balancing
  - Pool size is configurable via `ConnectionPoolSize` in client config
  - Metrics track operations per connection: `keydb_client_connection_ops_total`
- **Automatic Retries**: The client automatically retries operations on transient errors
- **Cluster Awareness**: Automatically adapts to cluster size changes (including degraded mode)
  - The cluster size is dynamically calculated based on active (non-degraded) nodes
  - When a node is marked as degraded, it's excluded from the effective cluster size
- **Key Distribution**: Automatically routes keys to the correct node based on hash
- **Parallel Operations**: Sends requests to multiple nodes in parallel for better performance
- **Automatic retries**: Automatically retries requests that fail for a configured number of times
  - The client tries again if the cluster size was changed and it hits a node not managing a requested key,
    although before trying again it will update its internal metadata with the new cluster node addresses first
    - Cluster changes trigger retries via recursion so there is no limit on the retries here
    - The client will retry as soon as possible right after trying to establish connections to new nodes (if any)
  - Normal errors trigger retries with an exponential backoff configured with a `RetryPolicy`
  - For the retries client configuration please refer to the client [Config](./client/client.go) `struct`

## Node Configuration

### Starting a Node

To start a KeyDB node, configure it with environment variables (all prefixed with `KEYDB_`):

```bash
# Configure node with environment variables
export KEYDB_NODE_ID=0
export KEYDB_NODE_ADDRESSES="localhost:50051,localhost:50052,localhost:50053"
export KEYDB_PORT=50051
export KEYDB_SNAPSHOT_INTERVAL=60s
export KEYDB_TOTAL_HASH_RANGES=271

# Optional: Configure degraded nodes (comma-separated boolean values)
# Nodes marked as degraded will not serve read/write traffic
export KEYDB_DEGRADED_NODES="false,false,false"

# Storage configuration
export KEYDB_STORAGE_BUCKET="my-keydb-bucket"
export KEYDB_STORAGE_REGION="us-east-1"
export KEYDB_STORAGE_ACCESSKEYID="your-access-key-id"
export KEYDB_STORAGE_ACCESSKEY="your-secret-access-key"

# Start the node
go run cmd/node/main.go
```

### Dynamic Cluster Configuration

Node addresses can be updated dynamically without restarting the node. The `KEYDB_NODE_ADDRESSES` environment variable
is reloadable, meaning changes to the configuration will be picked up by running nodes.

In order to do that, update the `nodeAddresses` via the configmap (i.e. `/etc/rudderstack/config.yaml`).

This is useful during scaling operations along with `degradedNodes`.

## Scaling the Cluster

To scale the KeyDB cluster, you can leverage the `Scaler` HTTP API.

Internally the `Scaler` API uses the `internal/scaler` gRPC client to manage the cluster.

### Scaling via HTTP API

Here is a `SCALE UP` example of how to scale the cluster via the HTTP API.

The below example will depict the scenario of scaling from 1 node to 3 nodes. 

1. Use the `/hashRangeMovements` to preview the number of hash ranges that will be moved when scaling
   * see [Previewing a Scaling Operation](#previewing-a-scaling-operation) for more details
2. Merge a `rudder-devops` PR what will:
   * Increase the CPU and memory of the nodes to give more power for the creation of the snapshots
     * Consider increasing IOPS and disk throughput as well if necessary
   * Increase the number of replicas (e.g. from 1 to 3) i.e. `replicaCount: 3`
   * Set the `degradedNodes` 
     * i.e. `false,true,true` which means that the 1st node will continue to receive traffic as usual whilst the 2
       new nodes will not receive traffic but will be available to receive new snapshots
3. If necessary call `/hashRangeMovements` again with `upload=true,full_sync=true` to start uploading the snapshots to
   the cloud storage
   * Pre-uploads and pre-download can be useful for several reasons:
     * To measure how long snapshots creation and loading might take without actually having to scale the cluster
     * To do a full sync before scaling (full syncs delete old data that might be expired from S3 so that then nodes
       won't have to download expired data, making the scaling process faster later)
     * You can still create snapshots during `/autoScale` but then they won't have to be full sync snapshots, meaning
       they can just be small files that contain only the most recent data (see `since` in 
       [node/node.go](./node/node.go))
4. Call `/autoScale`
   * You can do this if you haven't already moved the data via `/hashRangeMovements`
   * You can call it with `skip_create_snapshots=true` if you already created the snapshots in the previous operation
5. Merge another `rudder-devops` PR to set `degradedNodes` to either empty or `false,false,false`
   * If necessary you should reduce the CPU and memory if you ended up increasing them in point 2

Here is a `SCALE DOWN` example of how to scale the cluster via the HTTP API.

The below example will depict the scenario of scaling from 3 to 1 node.

1. Use the `/hashRangeMovements` to preview the number of hash ranges that will be moved when scaling
   * see [Previewing a Scaling Operation](#previewing-a-scaling-operation) for more details
2. Call `/hashRangeMovements` again with `upload=true,full_sync=true` to move the data
3. Merge a `rudder-devops` PR to decrease the `replicaCount`

### Previewing a Scaling Operation

Why previewing a scaling operation?
* It helps you understand the impact of the scaling operation on the cluster
* It will help decrease the length of the `/autoScale` operation in case you called `/hashRangeMovements` 
  with `upload=true`
* It will help you consider the impact of snapshots creation before actually scaling the cluster

To preview a scaling operation you can call `/hashRangeMovements` like in the example below:

```bash
curl --location 'localhost:8080/hashRangeMovements' \
--header 'Content-Type: application/json' \
--data '{
    "old_cluster_size": 4,
    "new_cluster_size": 5,
    "total_hash_ranges": 271
}'
```

In the above example we are previewing a scaling operation from 4 to 5 nodes (i.e. scale-up).
We're not going to tell the nodes to do a pre-upload of the snapshots (i.e. `upload != true`).

What the `scaler` might tell us in that case specifically is that 100 hash ranges will have to be moved:

```json
{
    "total": 100,
    "movements": [
        {
            "hash_range": 5,
            "from": 1,
            "to": 0
        },
        {
            "hash_range": 9,
            "from": 1,
            "to": 4
        },
        ...
    ]
}
```

You can try different combinations:

| op         | old_cluster_size | new_cluster_size | total_hash_ranges | no_of_moved_hash_ranges |
|------------|------------------|------------------|-------------------|-------------------------|
| scale_up   | 1                | 2                | 271               | 102                     |
| scale_up   | 1                | 3                | 271               | 167                     |
| scale_up   | 1                | 4                | 271               | 200                     |
| scale_up   | 2                | 3                | 271               | 87                      |
| scale_up   | 2                | 4                | 271               | 137                     |
| scale_up   | 3                | 4                | 271               | 84                      |
| scale_up   | 4                | 5                | 271               | 68                      |

Supported options:

```go
type HashRangeMovementsRequest struct {
	OldClusterSize                     int64 `json:"old_cluster_size"`
	NewClusterSize                     int64 `json:"new_cluster_size"`
	TotalHashRanges                    int64 `json:"total_hash_ranges"`
	Upload                             bool  `json:"upload,omitempty"`
	Download                           bool  `json:"download,omitempty"`
	FullSync                           bool  `json:"full_sync,omitempty"`
	CreateSnapshotsMaxConcurrency      int   `json:"create_snapshots_max_concurrency,omitempty"`
	LoadSnapshotsMaxConcurrency        int   `json:"load_snapshots_max_concurrency,omitempty"`
	DisableCreateSnapshotsSequentially bool  `json:"disable_create_snapshots_sequentially,omitempty"`
	Streaming                          bool  `json:"streaming,omitempty"`
}
```

Consider using `CreateSnapshotsMaxConcurrency` and `LoadSnapshotsMaxConcurrency` to control resource usage:
- `CreateSnapshotsMaxConcurrency`: Limits how many snapshots can be created concurrently (default: 10)
- `LoadSnapshotsMaxConcurrency`: Limits how many snapshots can be loaded concurrently from S3 (default: 10)

This is useful if a node has to handle a large number of big snapshots to avoid OOM kills.

### Streaming Mode

When `streaming=true` is set, data is transferred directly between nodes without using cloud storage as an
intermediary. This can significantly reduce scaling operation time by eliminating the upload/download steps to S3.

**How streaming works:**
1. The scaler instructs source nodes to send hash ranges directly to destination nodes
2. Source nodes connect to destination nodes via gRPC
3. Destination nodes inform source nodes of their last known timestamp (for incremental sync)
4. Source nodes create snapshots and stream them in chunks to destinations
5. Destination nodes load the received data and store the timestamp for future incremental syncs

**Streaming constraints:**
- One source node can only send one hash range at a time (prevents resource exhaustion)
- Multiple source nodes can send in parallel (maximizes throughput)
- Incremental syncs are supported via timestamp negotiation

**Example streaming request:**
```bash
curl --location 'localhost:8080/hashRangeMovements' \
--header 'Content-Type: application/json' \
--data '{
    "old_cluster_size": 2,
    "new_cluster_size": 3,
    "total_hash_ranges": 271,
    "streaming": true
}'
```

Alternatively you can give the nodes more memory, although a balance of the two is usually a good idea.
Snapshots are usually compressed and uploaded to S3, so the download and decompression
of big snapshots could take a significant portion of memory.

### Backup

The `/backup` endpoint allows you to create and/or load snapshots for all nodes (or specific nodes) in the cluster
without performing any scaling operations. This is useful for:
- Creating periodic backups to cloud storage
- Restoring data from cloud storage after a disaster
- Pre-populating nodes with data before they go live
- Resizing PVCs

**Request schema:**
```go
type BackupRequest struct {
    Upload                             bool    `json:"upload,omitempty"`
    Download                           bool    `json:"download,omitempty"`
    FullSync                           bool    `json:"full_sync,omitempty"`
    LoadSnapshotsMaxConcurrency        int     `json:"load_snapshots_max_concurrency,omitempty"`
    DisableCreateSnapshotsSequentially bool    `json:"disable_create_snapshots_sequentially,omitempty"`
    Nodes                              []int64 `json:"nodes,omitempty"`
}
```

**Parameters:**
- `upload`: Create snapshots and upload them to cloud storage
- `download`: Download and load snapshots from cloud storage
- `full_sync`: When true, performs a full sync that deletes old snapshot files on S3 before uploading
- `load_snapshots_max_concurrency`: Limits how many snapshots can be loaded concurrently (default: 10)
- `disable_create_snapshots_sequentially`: When true, creates snapshots in parallel instead of sequentially
- `nodes`: Optional list of specific node IDs to backup/restore. If omitted, all nodes are processed

**Note:** At least one of `upload` or `download` must be true. You cannot upload and download in the same request.

**Example: Backup all nodes to cloud storage**
```bash
curl --location 'localhost:8080/backup' \
--header 'Content-Type: application/json' \
--data '{
    "upload": true,
    "full_sync": true
}'
```

**Example: Restore all nodes from cloud storage**
```bash
curl --location 'localhost:8080/backup' \
--header 'Content-Type: application/json' \
--data '{
    "download": true,
    "load_snapshots_max_concurrency": 5
}'
```

**Example: Backup specific nodes**
```bash
curl --location 'localhost:8080/backup' \
--header 'Content-Type: application/json' \
--data '{
    "upload": true,
    "nodes": [0, 2]
}'
```

**Response:**
```json
{
    "success": true,
    "total": 271
}
```

Where `total` is the number of hash ranges that were processed (either uploaded or downloaded).

### AutoScale

The `AutoScale` scaler procedure can be used by sending an HTTP POST request to `/autoScale` with a JSON payload that
follows this schema:

```json
{
    "old_nodes_addresses": [
        "keydb-0.keydb-headless.loveholidays.svc.cluster.local:50051",
        "keydb-1.keydb-headless.loveholidays.svc.cluster.local:50051"
    ],
    "new_nodes_addresses": [
        "keydb-0.keydb-headless.loveholidays.svc.cluster.local:50051",
        "keydb-1.keydb-headless.loveholidays.svc.cluster.local:50051",
        "keydb-2.keydb-headless.loveholidays.svc.cluster.local:50051",
        "keydb-3.keydb-headless.loveholidays.svc.cluster.local:50051"
    ],
    "full_sync": false,
    "skip_create_snapshots": false,
    "create_snapshots_max_concurrency": 10,
    "load_snapshots_max_concurrency": 3,
    "disable_create_snapshots_sequentially": false,
    "streaming": false
}
```

Usage:
* if `old_nodes_addresses` length < `new_nodes_addresses` length → triggers **SCALE UP**
* if `old_nodes_addresses` length > `new_nodes_addresses` length → triggers **SCALE DOWN**
* if `old_nodes_addresses` length == `new_nodes_addresses` length → triggers **AUTO-HEALING**
  * it updates the scaler internal addresses of all the nodes and tells the nodes what is the desired cluster size
    without creating nor loading any snapshot
* if `full_sync` == `true` → triggers a full sync, deleting all the old files on S3 for the selected hash ranges
  * useful to avoid having nodes download data from S3 that might be too old (thus containing expired data)
* if `skip_create_snapshots` == `true` → it does not ask nodes to create snapshots
  * useful if you did a pre-upload via `/hashRangeMovements`
* if `create_snapshots_max_concurrency` > 0 → it limits how many snapshots can be created concurrently (default: 10)
* if `load_snapshots_max_concurrency` > 0 → it limits how many snapshots can be loaded concurrently from S3 (default: 10)
* if `disable_create_snapshots_sequentially` == `true` → snapshots are created in parallel instead of sequentially
  * By default, snapshots are created sequentially to reduce resource pressure on nodes
* if the operation does not succeed after all the retries, then a rollback to the "last operation" is triggered
  * to see what was the "last recorded operation" you can call `/lastOperation`

### Degraded Mode

KeyDB supports a "degraded mode" feature that allows nodes to be temporarily excluded from serving traffic during 
scaling operations. This helps prevent data inconsistencies when nodes restart during a scale operation.

**How it works:**
- Nodes can be marked as degraded via the `KEYDB_DEGRADED_NODES` environment variable (comma-separated boolean values)
- Degraded nodes reject all read/write requests with an `ERROR_SCALING` response code
- The effective cluster size excludes degraded nodes, ensuring correct hash range calculations
- Degraded nodes can still perform administrative operations (e.g., `LoadSnapshots`)

**Usage during scaling:**
1. **First devops PR**: Add new nodes in degraded mode without changing the config of old nodes
   ```bash
   # Old nodes (node 0, 1)
   export KEYDB_DEGRADED_NODES="false,false"

   # New nodes (node 2, 3) - start in degraded mode
   export KEYDB_DEGRADED_NODES="false,false,true,true"
   ```
2. **Run `/autoScale`**: The scaler moves the data between the old and new nodes and makes sure all nodes have all the
   nodes' addresses updated.
3. **Second devops PR**: Update persistent config to remove degraded mode permanently

This approach ensures that if any old node restarts during scaling, it won't use incorrect cluster size configurations.

**WARNING:** 
* Degrading an active node will redistribute the hash range that it was previously responsible for.
* Marking a new node as non-degraded before it could download the relevant snapshots, will make the node serve requests
  without the relevant data. This could lead to data inconsistencies. Whereas it could be a strategy to start serving
  traffic to a new node before it has downloaded all the relevant snapshots, it could lead to inconsistencies.

### Alternative Scaling Methods
* You can simply merge a devops PR with the desired cluster size and restart the nodes
  * In this case data won't be moved between nodes so it will lead to data loss
  * If you don't want to restart the nodes you can use `/autoScale` in auto-healing mode (i.e. with `old_cluster_size`
    equal to `new_cluster_size`)
* You can do everything manually by calling the single endpoints yourself, although it might be burdensome with a lot
  of hash ranges to move, so this would mean calling `/createSnapshots`, `/loadSnapshots`, and `/updateClusterData`
  manually (which is what the `/autoScale` endpoint does under the hood)
 
### Postman collection

[Here](./postman_collection.json) you can access the Postman collection for the `Scaler` HTTP API.

### Available HTTP Endpoints

- `POST /get` - Check key existence
- `POST /put` - Add keys with TTL
- `POST /info` - Get node information (e.g. node ID, cluster size, addresses, hash ranges managed by that node, etc...)
- `POST /createSnapshots` - Create snapshots and upload them to cloud storage
- `POST /loadSnapshots` - Downloads snapshots from cloud storage and loads them into BadgerDB
- `POST /updateClusterData` - Update the scaler internal information with the addresses of all the nodes that comprise
  the cluster
- `POST /autoScale` - Automatic scaling with retry and rollback logic
- `POST /hashRangeMovements` - Get hash range movement information (supports snapshot creation and loading)
- `POST /backup` - Create and/or load snapshots for all (or specific) nodes without scaling
- `GET /lastOperation` - Get last scaling operation status

## Performance

### Increased latency

It is possible to experience increased latencies when getting and setting keys.
One possible culprit could be insufficient CPU. Another possible culprit could be compactions triggering too often,
which also leads to CPU exhaustion.

Usually keydb can operate with decent latencies while using the default settings. However, it is important to understand
them in case some tuning might be needed to optimize compactions.

For a comprehensive list of available settings you can refer to the constructor in 
[badger.go](./internal/cache/badger/badger.go).

* `MemTableSize` default `64MB`
  * When too small, you might not be able to buffer enough writes before flushing to L0 (e.g. a bigger size can reduce
    the frequency of flushes)
* `NumLevelZeroTables` default to `10`
  * L0 flushing would start after ~640MB (64MB `MemTableSize` times 10)
  * Reducing this value might lead to flushing to L0 (disk) more frequently
* `BaseLevelSize` default `1GB` (base level = L1, i.e. the primary destination of compaction from L0)
  * Badger uses an LSM tree: immutable sorted tables (SSTables) grouped into levels
  * Level 0 (L0): contains newly flushed SSTables from MemTables, possibly overlapping in key ranges
  * Level 1 and beyond (L1, L2, ...): contain non-overlapping SSTables
  * Each successive level can hold more data than the previous one, usually growing by a size multiplier (default 5)
  * The base level size determines how big Level 1 (the base level) should be (consider that the base level is the first
    level of compaction where the overlapping key ranges from L0 are compacted down in L1 first and then down in higher
    levels as data keeps on growing)
  * TL;DR: More SSTables can accumulate in L1 before being compacted to L2
    * This means fewer compactions overall (good for write throughput)
    * Higher read amplification (since L1 can grow larger and contains overlapping SSTables, lookups may need to check 
      more files)
    * Larger total on-disk data before compactions
* `NumCompactors` default `4`
  * This increases the parallelism for compaction jobs, helping improve overall throughput with compactions
* `BlockCacheSize` (default to `1GB`) - Cache for SSTable data blocks (actual value/key payloads read from disk)
  * During compaction Badger must read data blocks from SSTables in input levels, merge them, and write new SSTables to 
    the next level
  * If the block cache is too small, compaction threads can cause a lot of block cache evictions
  * Reads during compaction will go to disk instead of RAM → higher disk I/O, slower compactions
  * User queries may suffer, since compaction and queries compete for cache
* `IndexCacheSize` (default to `512MB`) - Cache for SSTable index blocks and Bloom filters (metadata needed to locate 
  keys inside SSTables)
  * During compaction, the compactor repeatedly queries SSTable indexes to decide which keys to merge or drop
  * If index cache is too small, index blocks and bloom filters are constantly evicted
  * Each lookup during compaction requires re-reading index blocks from disk

## Known Issues and Considerations for Horizontal Scalability

* Creating and loading snapshots can take a lot of time for customers like `loveholidays`
  * For this reason we can do pre-uploads with `/hashRangeMovements` but it is a manual step that requires some
    understanding of the internal workings of `keydb` and the `scaler`
  * We could take automatic daily snapshots, but they would quite probably create spikes in CPU with a consequent
    increase in latencies
* Pre-downloads are not supported yet
  * We might need to create a `checkpoints` table to store what snapshots file a node has already loaded, so that we can
    skip them during a scaling operation
* Scaling operations require additional resources
  * For deployments with VPA enabled, resources can be automatically adjusted in-place (though restarts may still occur 
    depending on configuration)
  * For deployments without VPA, resources must be manually increased before scaling, which will cause a restart
