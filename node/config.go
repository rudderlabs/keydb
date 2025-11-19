package node

import "time"

// Config holds the configuration for a node
type Config struct {
	// NodeID is the ID of this node (0-based)
	NodeID int64

	// TotalHashRanges is the total number of hash ranges
	TotalHashRanges int64

	// MaxFilesToList specifies the maximum number of files that can be listed in a single operation.
	MaxFilesToList int64

	// SnapshotInterval is the interval for creating snapshots (in seconds)
	SnapshotInterval time.Duration

	// GarbageCollectionInterval defines the duration between automatic GC operation per cache
	GarbageCollectionInterval time.Duration

	// Addresses is a list of node addresses that this node will advertise to clients
	Addresses []string

	// DegradedNodes is a list of nodes that are considered degraded and should not be used for reads and writes.
	DegradedNodes func() []bool

	// logTableStructureDuration defines the duration for which the table structure is logged
	LogTableStructureDuration time.Duration

	// backupFolderName is the name of the folder in the S3 bucket where snapshots are stored
	BackupFolderName string
}

func (c *Config) getClusterSize() int64 {
	l := int64(len(c.Addresses))
	if c.DegradedNodes == nil {
		return l
	}
	degradedNodes := c.DegradedNodes()
	for _, degraded := range degradedNodes {
		if degraded {
			l--
		}
	}
	return l
}
