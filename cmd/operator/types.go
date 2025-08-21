package main

import "github.com/rudderlabs/keydb/internal/operator"

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
	NodeID uint32 `json:"nodeID"`
}

// CreateSnapshotsRequest represents a request to create snapshots
type CreateSnapshotsRequest struct {
	NodeID     uint32   `json:"nodeID"`
	FullSync   bool     `json:"fullSync"`
	HashRanges []uint32 `json:"hashRanges,omitempty"`
}

// LoadSnapshotsRequest represents a request to load snapshots
type LoadSnapshotsRequest struct {
	NodeID     uint32   `json:"nodeID"`
	HashRanges []uint32 `json:"hashRanges,omitempty"`
}

// ScaleRequest represents a request to scale the cluster
type ScaleRequest struct {
	NodeIDs []uint32 `json:"node_ids"`
}

// ScaleCompleteRequest represents a request to scale the cluster
type ScaleCompleteRequest struct {
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
	FullSync bool `json:"full_sync,omitempty"`
}

// HashRangeMovementsRequest represents a request to preview hash range movements
type HashRangeMovementsRequest struct {
	OldClusterSize  uint32 `json:"oldClusterSize"`
	NewClusterSize  uint32 `json:"newClusterSize"`
	TotalHashRanges uint32 `json:"totalHashRanges"`
	Upload          bool   `json:"upload,omitempty"`
	Download        bool   `json:"download,omitempty"`
	FullSync        bool   `json:"fullSync,omitempty"`
}

// HashRangeMovement represents a single hash range movement
type HashRangeMovement struct {
	HashRange uint32 `json:"hashRange"`
	From      uint32 `json:"from"`
	To        uint32 `json:"to"`
}

// LastOperationResponse represents the response containing the last operation
type LastOperationResponse struct {
	Operation *operator.ScalingOperation `json:"operation"`
}
