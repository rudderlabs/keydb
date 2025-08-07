package main

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
