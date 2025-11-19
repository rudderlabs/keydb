package main

import (
	"github.com/rudderlabs/keydb/internal/scaler"
)

// GetRequest represents a request to get keys
type GetRequest struct {
	Keys []string `json:"keys"`
}

// PutRequest represents a request to put keys
type PutRequest struct {
	Keys []string `json:"keys"`
	TTL  string   `json:"ttl"`
}

// InfoRequest represents a request to get node info
type InfoRequest struct {
	NodeID uint32 `json:"node_id"`
}

// CreateSnapshotsRequest represents a request to create snapshots
type CreateSnapshotsRequest struct {
	NodeID                             uint32   `json:"node_id"`
	FullSync                           bool     `json:"full_sync"`
	HashRanges                         []uint32 `json:"hash_ranges,omitempty"`
	DisableCreateSnapshotsSequentially bool     `json:"disable_create_snapshots_sequentially,omitempty"`
}

// LoadSnapshotsRequest represents a request to load snapshots
type LoadSnapshotsRequest struct {
	NodeID         uint32   `json:"node_id"`
	HashRanges     []uint32 `json:"hash_ranges,omitempty"`
	MaxConcurrency uint32   `json:"max_concurrency"`
}

// ScaleRequest represents a request to scale the cluster
type ScaleRequest struct {
	NodeIDs []uint32 `json:"node_ids"`
}

// UpdateClusterDataRequest represents a request to update the cluster size
type UpdateClusterDataRequest struct {
	Addresses []string `json:"addresses"`
}

// AutoScaleRequest represents a request to scale the cluster automatically.
//
// ScaleDown example:
//
//	OldNodesAddresses: [10.0.0.1, 10.0.0.2, 10.0.0.3]
//	NewNodesAddresses: [10.0.0.1, 10.0.0.2]
//
// ScaleUp example:
//
//	OldNodesAddresses: [10.0.0.1, 10.0.0.2]
//	NewNodesAddresses: [10.0.0.1, 10.0.0.2, 10.0.0.3]
type AutoScaleRequest struct {
	// OldNodesAddresses should contain the addresses of all the nodes in the cluster before any scale operation.
	OldNodesAddresses []string `json:"old_nodes_addresses"`
	// NewNodesAddresses should contain the addresses of all the nodes after the scale operation.
	NewNodesAddresses []string `json:"new_nodes_addresses"`
	// FullSync indicates whether to perform a full synchronization during snapshot creation.
	// When true, all data will be included in snapshots regardless of incremental changes.
	FullSync                           bool   `json:"full_sync,omitempty"`
	SkipCreateSnapshots                bool   `json:"skip_create_snapshots,omitempty"`
	LoadSnapshotsMaxConcurrency        uint32 `json:"load_snapshots_max_concurrency,omitempty"`
	DisableCreateSnapshotsSequentially bool   `json:"disable_create_snapshots_sequentially,omitempty"`
}

// HashRangeMovementsRequest represents a request to preview hash range movements
type HashRangeMovementsRequest struct {
	OldClusterSize                     uint32 `json:"old_cluster_size"`
	NewClusterSize                     uint32 `json:"new_cluster_size"`
	TotalHashRanges                    uint32 `json:"total_hash_ranges"`
	Upload                             bool   `json:"upload,omitempty"`
	Download                           bool   `json:"download,omitempty"`
	FullSync                           bool   `json:"full_sync,omitempty"`
	LoadSnapshotsMaxConcurrency        uint32 `json:"load_snapshots_max_concurrency,omitempty"`
	DisableCreateSnapshotsSequentially bool   `json:"disable_create_snapshots_sequentially,omitempty"`
}

type HashRangeMovementsResponse struct {
	Total     int                 `json:"total"`
	Movements []HashRangeMovement `json:"movements"`
}

// HashRangeMovement represents a single hash range movement
type HashRangeMovement struct {
	HashRange uint32 `json:"hash_range"`
	From      uint32 `json:"from"`
	To        uint32 `json:"to"`
}

// LastOperationResponse represents the response containing the last operation
type LastOperationResponse struct {
	Operation *scaler.ScalingOperation `json:"operation"`
}
