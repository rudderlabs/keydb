package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/internal/operator"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type httpServer struct {
	client   *client.Client
	operator *operator.Client
	server   *http.Server
}

// newHTTPServer creates a new HTTP server
func newHTTPServer(client *client.Client, operator *operator.Client, addr string, log logger.Logger) *httpServer {
	s := &httpServer{
		client:   client,
		operator: operator,
	}

	mux := chi.NewRouter()
	mux.Use(middleware.RequestID)
	mux.Use(middleware.RealIP)
	mux.Use(middleware.RequestLogger(&middleware.DefaultLogFormatter{
		Logger:  &loggerAdapter{logger: log},
		NoColor: true,
	}))
	mux.Use(middleware.Recoverer)

	mux.Post("/get", s.handleGet)
	mux.Post("/put", s.handlePut)
	mux.Post("/info", s.handleInfo)
	mux.Post("/createSnapshots", s.handleCreateSnapshots)
	mux.Post("/loadSnapshots", s.handleLoadSnapshots)
	mux.Post("/scale", s.handleScale)
	mux.Post("/scaleComplete", s.handleScaleComplete)
	mux.Post("/updateClusterData", s.handleUpdateClusterData)
	mux.Post("/autoScale", s.handleAutoScale)
	mux.Post("/hashRangeMovements", s.handleHashRangeMovements)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Minute, // using very long timeouts for now to allow for long operator ops
		WriteTimeout: 10 * time.Minute,
		IdleTimeout:  10 * time.Minute,
	}

	return s
}

// Start starts the HTTP server
func (s *httpServer) Start() error { return s.server.ListenAndServe() }

// Stop stops the HTTP server
func (s *httpServer) Stop(ctx context.Context) error { return s.server.Shutdown(ctx) }

// handleGet handles POST /get requests
func (s *httpServer) handleGet(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req GetRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Keys) == 0 {
		http.Error(w, "No keys provided", http.StatusBadRequest)
		return
	}

	// Get values for keys
	exists, err := s.client.Get(r.Context(), req.Keys)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting keys: %v", err), http.StatusInternalServerError)
		return
	}

	// Create response
	response := make(map[string]bool)
	for i, key := range req.Keys {
		response[key] = exists[i]
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := jsonrs.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handlePut handles POST /put requests
func (s *httpServer) handlePut(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req PutRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Keys) == 0 {
		http.Error(w, "No keys provided", http.StatusBadRequest)
		return
	}

	// Put keys
	ttl, err := time.ParseDuration(req.TTL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing TTL: %v", err), http.StatusBadRequest)
		return
	}
	if ttl <= 0 {
		http.Error(w, "Invalid TTL", http.StatusBadRequest)
	}
	if err := s.client.Put(r.Context(), req.Keys, ttl); err != nil {
		http.Error(w, fmt.Sprintf("Error putting keys: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleInfo handles POST /info requests
func (s *httpServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req InfoRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Get node info
	info, err := s.operator.GetNodeInfo(r.Context(), req.NodeID)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("Error getting info for node %d: %v", req.NodeID, err),
			http.StatusInternalServerError,
		)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := jsonrs.NewEncoder(w).Encode(info); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleCreateSnapshots handles POST /createSnapshots requests
func (s *httpServer) handleCreateSnapshots(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req CreateSnapshotsRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Create snapshot
	if err := s.operator.CreateSnapshots(r.Context(), req.NodeID, req.FullSync, req.HashRanges...); err != nil {
		http.Error(w, fmt.Sprintf("Error creating snapshot: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleLoadSnapshots handles POST /loadSnapshots requests
func (s *httpServer) handleLoadSnapshots(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req LoadSnapshotsRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Load snapshots from cloud storage
	if err := s.operator.LoadSnapshots(r.Context(), req.NodeID, req.HashRanges...); err != nil {
		http.Error(w, fmt.Sprintf("Error loading snapshots: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleScale handles POST /scale requests
func (s *httpServer) handleScale(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req ScaleRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.NodeIDs) == 0 {
		http.Error(w, "No node IDs provided", http.StatusBadRequest)
		return
	}

	// Scale cluster
	if err := s.operator.Scale(r.Context(), req.NodeIDs); err != nil {
		http.Error(w, fmt.Sprintf("Error scaling cluster: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleScaleComplete handles POST /scaleComplete requests
func (s *httpServer) handleScaleComplete(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req ScaleCompleteRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.NodeIDs) == 0 {
		http.Error(w, "No node IDs provided", http.StatusBadRequest)
		return
	}

	// Complete scale operation
	if err := s.operator.ScaleComplete(r.Context(), req.NodeIDs); err != nil {
		http.Error(w, fmt.Sprintf("Error completing scale operation: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleUpdateClusterData handles POST /updateClusterData requests
func (s *httpServer) handleUpdateClusterData(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req UpdateClusterDataRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Addresses) == 0 {
		http.Error(w, "No node IDs provided", http.StatusBadRequest)
		return
	}

	// Complete scale operation
	if err := s.operator.UpdateClusterData(req.Addresses...); err != nil {
		http.Error(w, fmt.Sprintf("Error completing scale operation: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

func (s *httpServer) handleAutoScale(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req AutoScaleRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.OldNodesAddresses) == 0 {
		http.Error(w, "No old node addresses provided", http.StatusBadRequest)
		return
	}
	if len(req.NewNodesAddresses) == 0 {
		http.Error(w, "No new node addresses provided", http.StatusBadRequest)
		return
	}

	oldClusterSize := uint32(len(req.OldNodesAddresses))
	newClusterSize := uint32(len(req.NewNodesAddresses))

	var err error
	if newClusterSize > oldClusterSize {
		err = s.handleScaleUp(r.Context(), req.OldNodesAddresses, req.NewNodesAddresses, req.FullSync)
	} else if newClusterSize < oldClusterSize {
		err = s.handleScaleDown(r.Context(), req.OldNodesAddresses, req.NewNodesAddresses, req.FullSync)
	} else {
		// Auto-healing: propagate cluster addresses to all nodes for consistency
		err = s.handleAutoHealing(r.Context(), req.NewNodesAddresses)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Error during auto scale operation: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleScaleUp implements the scale up logic based on TestScaleUpAndDown
func (s *httpServer) handleScaleUp(ctx context.Context, oldAddresses, newAddresses []string, fullSync bool) error {
	oldClusterSize := uint32(len(oldAddresses))
	newClusterSize := uint32(len(newAddresses))

	// Step 1: Update cluster data with new addresses
	if err := s.operator.UpdateClusterData(newAddresses...); err != nil {
		return fmt.Errorf("updating cluster data: %w", err)
	}

	// Step 2: Determine which hash ranges need to be moved and get ready-to-use maps
	sourceNodeMovements, destinationNodeMovements := hash.GetHashRangeMovements(
		oldClusterSize, newClusterSize, s.operator.TotalHashRanges(),
	)

	// Step 3: Create snapshots from source nodes for hash ranges that will be moved
	group, gCtx := errgroup.WithContext(ctx)
	for sourceNodeID, hashRanges := range sourceNodeMovements {
		if len(hashRanges) == 0 {
			continue
		}
		group.Go(func() error {
			if err := s.operator.CreateSnapshots(gCtx, sourceNodeID, fullSync, hashRanges...); err != nil {
				return fmt.Errorf("creating snapshots from node %d for hash ranges %v: %w",
					sourceNodeID, hashRanges, err,
				)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("waiting for snapshot creation: %w", err)
	}

	// Step 4: Load snapshots to destination nodes
	group, gCtx = errgroup.WithContext(ctx)
	for nodeID, hashRanges := range destinationNodeMovements {
		if len(hashRanges) == 0 {
			continue
		}
		group.Go(func() error {
			if err := s.operator.LoadSnapshots(gCtx, nodeID, hashRanges...); err != nil {
				return fmt.Errorf("loading snapshots to node %d for hash ranges %v: %w",
					nodeID, hashRanges, err,
				)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("waiting for snapshot loading: %w", err)
	}

	// Step 5: Scale all nodes
	return s.completeScaleOperation(ctx, newClusterSize)
}

// handleScaleDown implements the scale down logic based on TestScaleUpAndDown
func (s *httpServer) handleScaleDown(ctx context.Context, oldAddresses, newAddresses []string, fullSync bool) error {
	oldClusterSize := uint32(len(oldAddresses))
	newClusterSize := uint32(len(newAddresses))

	sourceNodeMovements, destinationNodeMovements := hash.GetHashRangeMovements(
		oldClusterSize, newClusterSize, s.operator.TotalHashRanges(),
	)

	// Step 1: Create snapshots from source nodes for hash ranges that will be moved
	group, gCtx := errgroup.WithContext(ctx)
	for sourceNodeID, hashRanges := range sourceNodeMovements {
		if len(hashRanges) == 0 {
			continue
		}
		group.Go(func() error {
			if err := s.operator.CreateSnapshots(gCtx, sourceNodeID, fullSync, hashRanges...); err != nil {
				return fmt.Errorf("creating snapshots from node %d for hash ranges %v: %w",
					sourceNodeID, hashRanges, err,
				)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("waiting for snapshot creation: %w", err)
	}

	// Step 2: Load snapshots to destination nodes
	group, gCtx = errgroup.WithContext(ctx)
	for nodeID, hashRanges := range destinationNodeMovements {
		if len(hashRanges) == 0 {
			continue
		}
		group.Go(func() error {
			if err := s.operator.LoadSnapshots(gCtx, nodeID, hashRanges...); err != nil {
				return fmt.Errorf("loading snapshots to node %d for hash ranges %v: %w",
					nodeID, hashRanges, err,
				)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("waiting for snapshot loading: %w", err)
	}

	// Step 3: Update cluster data with new addresses
	if err := s.operator.UpdateClusterData(newAddresses...); err != nil {
		return fmt.Errorf("updating cluster data: %w", err)
	}

	// Step 4: Scale remaining nodes
	return s.completeScaleOperation(ctx, newClusterSize)
}

// handleAutoHealing implements auto-healing by propagating cluster addresses to all nodes
// This ensures all nodes have consistent cluster topology information
func (s *httpServer) handleAutoHealing(ctx context.Context, addresses []string) error {
	clusterSize := uint32(len(addresses))

	// Step 1: Update cluster data with current addresses to ensure consistency
	if err := s.operator.UpdateClusterData(addresses...); err != nil {
		return fmt.Errorf("updating cluster data for auto-healing: %w", err)
	}

	// Step 2: Scale all nodes to refresh their cluster information
	return s.completeScaleOperation(ctx, clusterSize)
}

func (s *httpServer) completeScaleOperation(ctx context.Context, clusterSize uint32) error {
	nodeIDs := make([]uint32, clusterSize)
	for i := uint32(0); i < clusterSize; i++ {
		nodeIDs[i] = i
	}
	if err := s.operator.Scale(ctx, nodeIDs); err != nil {
		return fmt.Errorf("scaling nodes: %w", err)
	}

	if err := s.operator.ScaleComplete(ctx, nodeIDs); err != nil {
		return fmt.Errorf("completing scale operation: %w", err)
	}

	return nil
}

// handleHashRangeMovements handles POST /hashRangeMovements requests
func (s *httpServer) handleHashRangeMovements(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req HashRangeMovementsRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if req.OldClusterSize == 0 {
		http.Error(w, "oldClusterSize must be greater than 0", http.StatusBadRequest)
		return
	}
	if req.NewClusterSize == 0 {
		http.Error(w, "newClusterSize must be greater than 0", http.StatusBadRequest)
		return
	}
	if req.TotalHashRanges == 0 {
		http.Error(w, "totalHashRanges must be greater than 0", http.StatusBadRequest)
		return
	}
	if req.TotalHashRanges < req.OldClusterSize {
		http.Error(w, "totalHashRanges must be greater than or equal to oldClusterSize", http.StatusBadRequest)
		return
	}
	if req.TotalHashRanges < req.NewClusterSize {
		http.Error(w, "totalHashRanges must be greater than or equal to newClusterSize", http.StatusBadRequest)
		return
	}

	// Get hash range movements
	sourceNodeMovements, destinationNodeMovements := hash.GetHashRangeMovements(
		req.OldClusterSize, req.NewClusterSize, req.TotalHashRanges,
	)

	destinationMap := make(map[uint32]uint32)
	for nodeID, hashRanges := range destinationNodeMovements {
		for _, hashRange := range hashRanges {
			destinationMap[hashRange] = nodeID
		}
	}

	// Convert to response format
	var movements []HashRangeMovement
	for sourceNodeID, hashRanges := range sourceNodeMovements {
		for _, hashRange := range hashRanges {
			movements = append(movements, HashRangeMovement{
				HashRange: hashRange,
				From:      sourceNodeID,
				To:        destinationMap[hashRange],
			})
		}
	}

	// If upload=true, send CreateSnapshots requests to old nodes that are losing hash ranges
	if req.Upload {
		ctx := r.Context()
		group, gCtx := errgroup.WithContext(ctx)
		for sourceNodeID, hashRanges := range sourceNodeMovements {
			// Only send CreateSnapshots to nodes that exist in the old cluster
			if sourceNodeID < req.OldClusterSize {
				if req.SplitUploads {
					// Call CreateSnapshots once per hash range
					for _, hashRange := range hashRanges {
						group.Go(func() error {
							err := s.operator.CreateSnapshots(ctx, sourceNodeID, req.FullSync, hashRange)
							if err != nil {
								return fmt.Errorf("creating snapshots for node %d, hash range %d: %w",
									sourceNodeID, hashRange, err)
							}
							return nil
						})
					}
				} else {
					// Call CreateSnapshots once per node with all hash ranges
					group.Go(func() error {
						err := s.operator.CreateSnapshots(gCtx, sourceNodeID, req.FullSync, hashRanges...)
						if err != nil {
							return fmt.Errorf("creating snapshots for node %d: %w", sourceNodeID, err)
						}
						return nil
					})
				}
			}
		}
		if err := group.Wait(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := jsonrs.NewEncoder(w).Encode(movements); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}

type loggerAdapter struct {
	logger logger.Logger
}

func (l loggerAdapter) Print(v ...any) {
	l.logger.Infow("", v...) // nolint:forbidigo
}
