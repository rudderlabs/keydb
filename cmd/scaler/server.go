package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/internal/hash"
	"github.com/rudderlabs/keydb/internal/scaler"
	pb "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	totalSnapshotsToLoadMetricName     = "scaler_total_snapshots_to_load"
	currentSnapshotsToLoadMetricName   = "scaler_current_snapshots_loaded"
	totalSnapshotsToCreateMetricName   = "scaler_total_snapshots_to_create"
	currentSnapshotsToCreateMetricName = "scaler_current_snapshots_created"
)

type scalerClient interface {
	Scale(ctx context.Context, nodeIDs []int64) error
	UpdateClusterData(addresses ...string) error
	CreateSnapshots(ctx context.Context, nodeID int64, fullSync bool, hashRanges ...int64) error
	LoadSnapshots(ctx context.Context, nodeID, maxConcurrency int64, hashRanges ...int64) error
	ExecuteScalingWithRollback(opType scaler.ScalingOperationType, oldAddresses, newAddresses []string,
		fn func() error) error
	GetLastOperation() *scaler.ScalingOperation
	TotalHashRanges() int64
	GetNodeInfo(ctx context.Context, id int64) (*pb.GetNodeInfoResponse, error)
}

type httpServer struct {
	client *client.Client
	scaler scalerClient
	server *http.Server
	stat   stats.Stats
	logger logger.Logger
}

// newHTTPServer creates a new HTTP server
func newHTTPServer(
	client *client.Client, scaler *scaler.Client, addr string, stat stats.Stats, log logger.Logger,
) *httpServer {
	s := &httpServer{
		client: client,
		scaler: scaler,
		stat:   stat,
		logger: log,
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
	mux.Post("/updateClusterData", s.handleUpdateClusterData)
	mux.Post("/autoScale", s.handleAutoScale)
	mux.Post("/hashRangeMovements", s.handleHashRangeMovements)
	mux.Get("/lastOperation", s.handleLastOperation)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Minute, // using very long timeouts for now to allow for long scaler ops
		WriteTimeout: 10 * time.Minute,
		IdleTimeout:  10 * time.Minute,
	}

	return s
}

// Start starts the HTTP server
func (s *httpServer) Start(ctx context.Context) error { return httputil.ListenAndServe(ctx, s.server) }

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
	info, err := s.scaler.GetNodeInfo(r.Context(), req.NodeID)
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
	err := s.createSnapshotsWithProgress(
		r.Context(), req.NodeID, req.FullSync, req.DisableCreateSnapshotsSequentially, req.HashRanges,
	)
	if err != nil {
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

	// Initialize metrics
	nodeIDStr := strconv.FormatInt(req.NodeID, 10)
	totalSnapshotsToLoad := s.stat.NewTaggedStat(totalSnapshotsToLoadMetricName, stats.GaugeType, stats.Tags{
		"nodeId": nodeIDStr,
	})
	totalSnapshotsToLoad.Observe(float64(len(req.HashRanges)))
	defer totalSnapshotsToLoad.Observe(0)

	currentSnapshotsLoaded := s.stat.NewTaggedStat(currentSnapshotsToLoadMetricName, stats.GaugeType, stats.Tags{
		"nodeId": nodeIDStr,
	})
	currentSnapshotsLoaded.Observe(0)

	// Load snapshots from cloud storage
	if err := s.scaler.LoadSnapshots(r.Context(), req.NodeID, req.MaxConcurrency, req.HashRanges...); err != nil {
		http.Error(w, fmt.Sprintf("Error loading snapshots: %v", err), http.StatusInternalServerError)
		return
	}

	// Update metrics after successful load
	currentSnapshotsLoaded.Observe(float64(len(req.HashRanges)))

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
	if err := s.scaler.Scale(r.Context(), req.NodeIDs); err != nil {
		http.Error(w, fmt.Sprintf("Error scaling cluster: %v", err), http.StatusInternalServerError)
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
	if err := s.scaler.UpdateClusterData(req.Addresses...); err != nil {
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

	oldClusterSize := int64(len(req.OldNodesAddresses))
	newClusterSize := int64(len(req.NewNodesAddresses))
	createSnapshotsMaxConcurrency := req.CreateSnapshotsMaxConcurrency
	loadSnapshotsMaxConcurrency := req.LoadSnapshotsMaxConcurrency

	var err error
	if newClusterSize > oldClusterSize {
		err = s.handleScaleUp(
			r.Context(), req.OldNodesAddresses, req.NewNodesAddresses,
			req.FullSync, req.SkipCreateSnapshots, createSnapshotsMaxConcurrency, loadSnapshotsMaxConcurrency,
			req.DisableCreateSnapshotsSequentially,
		)
	} else if newClusterSize < oldClusterSize {
		err = s.handleScaleDown(
			r.Context(), req.OldNodesAddresses, req.NewNodesAddresses,
			req.FullSync, req.SkipCreateSnapshots, createSnapshotsMaxConcurrency, loadSnapshotsMaxConcurrency,
			req.DisableCreateSnapshotsSequentially,
		)
	} else {
		// Auto-healing: propagate cluster addresses to all nodes for consistency
		err = s.handleAutoHealing(r.Context(), req.OldNodesAddresses, req.NewNodesAddresses)
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
func (s *httpServer) handleScaleUp(
	ctx context.Context, oldAddresses, newAddresses []string,
	fullSync, skipCreateSnapshots bool,
	createSnapshotsMaxConcurrency, loadSnapshotsMaxConcurrency int,
	disableCreateSnapshotsSequentially bool,
) error {
	oldClusterSize := int64(len(oldAddresses))
	newClusterSize := int64(len(newAddresses))

	log := s.logger.Withn(
		logger.NewStringField("oldAddresses", strings.Join(oldAddresses, ",")),
		logger.NewStringField("newAddresses", strings.Join(newAddresses, ",")),
	)
	log.Infon("Starting scale up")

	start := time.Now()
	defer func() {
		log.Infon("Scale up completed",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	return s.scaler.ExecuteScalingWithRollback(scaler.ScaleUp, oldAddresses, newAddresses, func() error {
		// Step 1: Update cluster data with new addresses
		if err := s.scaler.UpdateClusterData(newAddresses...); err != nil {
			log.Errorn("Cannot update cluster data", obskit.Error(err))
			return fmt.Errorf("updating cluster data: %w", err)
		}

		log.Infon("Cluster data updated")

		// Step 2: Determine which hash ranges need to be moved and get ready-to-use maps
		movements := hash.GetHashRangeMovementsByRange(
			oldClusterSize, newClusterSize, s.scaler.TotalHashRanges(),
		)

		// Step 3: Create and load snapshots for hash ranges that will be moved
		// Use pipelined approach: start loading snapshots as they are created
		if skipCreateSnapshots {
			log.Infon("Skipping snapshots creation, only loading",
				logger.NewIntField("totalMovements", int64(len(movements))),
			)
		} else {
			log.Infon("Scale up to start creating and loading snapshots",
				logger.NewIntField("totalMovements", int64(len(movements))),
			)
		}

		err := s.processHashRangeMovements(
			ctx, movements, fullSync, skipCreateSnapshots, createSnapshotsMaxConcurrency,
			loadSnapshotsMaxConcurrency, disableCreateSnapshotsSequentially,
		)
		if err != nil {
			return err
		}

		// Step 5: Scale all nodes
		return s.completeScaleOperation(ctx, newClusterSize)
	})
}

// handleScaleDown implements the scale down logic based on TestScaleUpAndDown
func (s *httpServer) handleScaleDown(
	ctx context.Context, oldAddresses, newAddresses []string,
	fullSync, skipCreateSnapshots bool, createSnapshotsMaxConcurrency, loadSnapshotsMaxConcurrency int,
	disableCreateSnapshotsSequentially bool,
) error {
	oldClusterSize := int64(len(oldAddresses))
	newClusterSize := int64(len(newAddresses))

	log := s.logger.Withn(
		logger.NewStringField("oldAddresses", strings.Join(oldAddresses, ",")),
		logger.NewStringField("newAddresses", strings.Join(newAddresses, ",")),
	)
	log.Infon("Starting scale down")

	start := time.Now()
	defer func() {
		log.Infon("Scale down completed",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	return s.scaler.ExecuteScalingWithRollback(scaler.ScaleDown, oldAddresses, newAddresses, func() error {
		// Step 1: Determine which hash ranges need to be moved
		movements := hash.GetHashRangeMovementsByRange(
			oldClusterSize, newClusterSize, s.scaler.TotalHashRanges(),
		)

		// Step 2: Create and load snapshots for hash ranges that will be moved
		// Use pipelined approach: start loading snapshots as they are created
		if skipCreateSnapshots {
			log.Infon("Skipping snapshots creation, only loading",
				logger.NewIntField("totalMovements", int64(len(movements))),
			)
		} else {
			log.Infon("Scale down to start creating and loading snapshots",
				logger.NewIntField("totalMovements", int64(len(movements))),
			)
		}

		err := s.processHashRangeMovements(
			ctx, movements, fullSync, skipCreateSnapshots, createSnapshotsMaxConcurrency,
			loadSnapshotsMaxConcurrency, disableCreateSnapshotsSequentially,
		)
		if err != nil {
			return err
		}

		// Step 3: Update cluster data with new addresses
		if err := s.scaler.UpdateClusterData(newAddresses...); err != nil {
			log.Errorn("Cannot update cluster data", obskit.Error(err))
			return fmt.Errorf("updating cluster data: %w", err)
		}

		log.Infon("Cluster data updated")

		// Step 4: Scale remaining nodes
		return s.completeScaleOperation(ctx, newClusterSize)
	})
}

// handleAutoHealing implements auto-healing by propagating cluster addresses to all nodes
// This ensures all nodes have consistent cluster topology information
func (s *httpServer) handleAutoHealing(ctx context.Context, oldAddresses, newAddresses []string) error {
	log := s.logger.Withn(
		logger.NewStringField("oldAddresses", strings.Join(oldAddresses, ",")),
		logger.NewStringField("newAddresses", strings.Join(newAddresses, ",")),
	)
	log.Infon("Starting auto-healing")

	start := time.Now()
	defer func() {
		s.logger.Infon("Auto-healing completed",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	return s.scaler.ExecuteScalingWithRollback(scaler.AutoHealing, oldAddresses, newAddresses, func() error {
		// Step 1: Update cluster data with current addresses to ensure consistency
		if err := s.scaler.UpdateClusterData(newAddresses...); err != nil {
			log.Errorn("Cannot update cluster data", obskit.Error(err))
			return fmt.Errorf("updating cluster data for auto-healing: %w", err)
		}

		// Step 2: Scale all nodes to refresh their cluster information
		return s.completeScaleOperation(ctx, int64(len(newAddresses)))
	})
}

func (s *httpServer) completeScaleOperation(ctx context.Context, clusterSize int64) error {
	nodeIDs := make([]int64, clusterSize)
	for i := int64(0); i < clusterSize; i++ {
		nodeIDs[i] = i
	}

	s.logger.Infon("Starting scale operation")

	if err := s.scaler.Scale(ctx, nodeIDs); err != nil {
		return fmt.Errorf("scaling nodes: %w", err)
	}

	s.logger.Infon("Scale command sent to all nodes")

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

	log := s.logger.Withn(
		logger.NewIntField("oldClusterSize", req.OldClusterSize),
		logger.NewIntField("newClusterSize", req.NewClusterSize),
		logger.NewIntField("totalHashRanges", req.TotalHashRanges),
		logger.NewBoolField("upload", req.Upload),
		logger.NewBoolField("download", req.Download),
		logger.NewBoolField("fullSync", req.FullSync),
		logger.NewIntField("loadSnapshotsMaxConcurrency", int64(req.LoadSnapshotsMaxConcurrency)),
	)
	log.Infon("Received hash range movements request")

	start := time.Now()
	defer func() {
		log.Infon("Hash range movements request completed",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	// Get hash range movements
	movements := hash.GetHashRangeMovementsByRange(
		req.OldClusterSize, req.NewClusterSize, req.TotalHashRanges,
	)

	// Convert to response format
	var response HashRangeMovementsResponse
	response.Total = len(movements)
	response.Movements = make([]HashRangeMovement, 0, len(movements))
	for hashRange, movement := range movements {
		response.Movements = append(response.Movements, HashRangeMovement{
			HashRange: hashRange,
			From:      movement.SourceNodeID,
			To:        movement.DestinationNodeID,
		})
	}

	// If upload=true and download=true, use pipelined approach
	// Otherwise use the old sequential approach for backward compatibility
	if req.Upload && req.Download {
		log.Infon("Upload and download of hash ranges are enabled (using pipelined approach)",
			logger.NewIntField("totalHashRanges", req.TotalHashRanges),
		)

		ctx := r.Context()
		err := s.processHashRangeMovements(
			ctx, movements, req.FullSync, false, req.CreateSnapshotsMaxConcurrency,
			req.LoadSnapshotsMaxConcurrency, req.DisableCreateSnapshotsSequentially,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else if req.Upload {
		log.Infon("Upload of hash ranges is enabled",
			logger.NewIntField("totalHashRanges", req.TotalHashRanges),
		)

		// Group movements by source node
		sourceNodeMovements := make(map[int64][]int64)
		for hashRange, movement := range movements {
			sourceNodeMovements[movement.SourceNodeID] = append(
				sourceNodeMovements[movement.SourceNodeID], hashRange,
			)
		}

		ctx := r.Context()
		group, gCtx := errgroup.WithContext(ctx)
		for sourceNodeID, hashRanges := range sourceNodeMovements {
			// Only send CreateSnapshots to nodes that exist in the old cluster
			if sourceNodeID < req.OldClusterSize {
				// Call CreateSnapshots once per node with all hash ranges
				group.Go(func() error {
					start := time.Now()
					err := s.createSnapshotsWithProgress(
						gCtx, sourceNodeID, req.FullSync, req.DisableCreateSnapshotsSequentially, hashRanges,
					)
					if err != nil {
						return fmt.Errorf("creating snapshots for node %d: %w", sourceNodeID, err)
					}
					log.Infon("Node snapshots created",
						logger.NewIntField("nodeId", sourceNodeID),
						logger.NewIntField("hashRangesCount", int64(len(hashRanges))),
						logger.NewStringField("duration", time.Since(start).String()),
					)
					return nil
				})
			}
		}
		if err := group.Wait(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Infon("All snapshots created")
	} else if req.Download {
		log.Infon("Download of hash ranges is enabled",
			logger.NewIntField("totalHashRanges", req.TotalHashRanges),
		)

		// Group movements by destination node
		destinationNodeMovements := make(map[int64][]int64)
		for hashRange, movement := range movements {
			destinationNodeMovements[movement.DestinationNodeID] = append(
				destinationNodeMovements[movement.DestinationNodeID], hashRange,
			)
		}

		ctx := r.Context()
		group, gCtx := errgroup.WithContext(ctx)
		for destinationNodeID, hashRanges := range destinationNodeMovements {
			group.Go(func() error {
				start := time.Now()
				err := s.scaler.LoadSnapshots(
					gCtx, destinationNodeID, int64(req.LoadSnapshotsMaxConcurrency), hashRanges...,
				)
				if err != nil {
					return fmt.Errorf("loading snapshots for node %d: %w", destinationNodeID, err)
				}
				log.Infon("Node snapshots loaded",
					logger.NewIntField("nodeId", destinationNodeID),
					logger.NewIntField("hashRangesCount", int64(len(hashRanges))),
					logger.NewStringField("duration", time.Since(start).String()),
				)
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Infon("All snapshots loaded")
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := jsonrs.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}

// processHashRangeMovements processes hash range movements by creating and loading snapshots in a pipelined manner.
// Instead of creating all snapshots first and then loading them, it starts loading snapshots as they are created.
func (s *httpServer) processHashRangeMovements(
	ctx context.Context,
	movements map[int64]hash.Movement,
	fullSync bool,
	skipCreateSnapshots bool,
	createSnapshotsMaxConcurrency int,
	loadSnapshotsMaxConcurrency int,
	disableCreateSnapshotsSequentially bool,
) error {
	if len(movements) == 0 {
		return nil
	}

	// Set default concurrency limits if not specified
	if createSnapshotsMaxConcurrency <= 0 {
		createSnapshotsMaxConcurrency = 10 // Default to 10 concurrent creates
	}
	if loadSnapshotsMaxConcurrency <= 0 {
		loadSnapshotsMaxConcurrency = 10 // Default to 10 concurrent loads
	}

	// Group movements by source node to track metrics
	sourceNodeMovements := make(map[int64][]int64)
	destinationNodeMovements := make(map[int64][]int64)
	for hashRange, movement := range movements {
		sourceNodeMovements[movement.SourceNodeID] = append(
			sourceNodeMovements[movement.SourceNodeID], hashRange,
		)
		destinationNodeMovements[movement.DestinationNodeID] = append(
			destinationNodeMovements[movement.DestinationNodeID], hashRange,
		)
	}

	// Initialize metrics for tracking progress
	totalSnapshotsToCreate := s.stat.NewStat(totalSnapshotsToCreateMetricName, stats.GaugeType)
	totalSnapshotsToCreate.Observe(float64(len(movements)))
	defer totalSnapshotsToCreate.Observe(0)

	currentSnapshotsCreated := s.stat.NewStat(currentSnapshotsToCreateMetricName, stats.GaugeType)
	currentSnapshotsCreated.Observe(0)

	totalSnapshotsToLoad := s.stat.NewStat(totalSnapshotsToLoadMetricName, stats.GaugeType)
	totalSnapshotsToLoad.Observe(float64(len(movements)))
	defer totalSnapshotsToLoad.Observe(0)

	currentSnapshotsLoaded := s.stat.NewStat(currentSnapshotsToLoadMetricName, stats.GaugeType)
	currentSnapshotsLoaded.Observe(0)

	start := time.Now()
	defer func() {
		s.logger.Infon("All snapshots created and loaded",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	// Channel to coordinate between snapshot creation and loading
	type completedSnapshot struct {
		hashRange         int64
		sourceNodeID      int64
		destinationNodeID int64
	}
	snapshotQueue := make(chan completedSnapshot, len(movements))

	group, gCtx := errgroup.WithContext(ctx)

	// Use atomic counters for thread-safe progress tracking
	var createdCount, loadedCount atomic.Int64

	// Semaphores to limit concurrent operations
	createSemaphore := make(chan struct{}, createSnapshotsMaxConcurrency)
	loadSemaphore := make(chan struct{}, loadSnapshotsMaxConcurrency)

	// Start goroutine to create snapshots (or just queue them if skipCreateSnapshots is true)
	group.Go(func() error {
		defer close(snapshotQueue)

		if skipCreateSnapshots {
			// If skipping creation, just queue all snapshots for loading
			for hashRange, movement := range movements {
				select {
				case snapshotQueue <- completedSnapshot{
					hashRange:         hashRange,
					sourceNodeID:      movement.SourceNodeID,
					destinationNodeID: movement.DestinationNodeID,
				}:
				case <-gCtx.Done():
					return gCtx.Err()
				}
			}
			return nil
		}

		// Normal path: create snapshots
		createGroup, createCtx := errgroup.WithContext(gCtx)

		for hashRange, movement := range movements {
			hashRange := hashRange
			movement := movement

			createGroup.Go(func() error {
				// Acquire semaphore
				select {
				case createSemaphore <- struct{}{}:
					defer func() { <-createSemaphore }()
				case <-createCtx.Done():
					return createCtx.Err()
				}

				createStart := time.Now()
				err := s.scaler.CreateSnapshots(
					createCtx, movement.SourceNodeID, fullSync, hashRange,
				)
				if err != nil {
					return fmt.Errorf("creating snapshot for hash range %d from node %d: %w",
						hashRange, movement.SourceNodeID, err,
					)
				}

				s.logger.Infon("Snapshot created",
					logger.NewIntField("hashRange", hashRange),
					logger.NewIntField("sourceNodeId", movement.SourceNodeID),
					logger.NewIntField("destinationNodeId", movement.DestinationNodeID),
					logger.NewStringField("duration", time.Since(createStart).String()),
				)

				// Update progress
				count := createdCount.Add(1)
				currentSnapshotsCreated.Observe(float64(count))

				// Send to queue for loading
				select {
				case snapshotQueue <- completedSnapshot{
					hashRange:         hashRange,
					sourceNodeID:      movement.SourceNodeID,
					destinationNodeID: movement.DestinationNodeID,
				}:
				case <-createCtx.Done():
					return createCtx.Err()
				}

				return nil
			})
		}

		return createGroup.Wait()
	})

	// Start goroutines to load snapshots
	for snapshot := range snapshotQueue {
		snapshot := snapshot

		group.Go(func() error {
			// Acquire semaphore
			select {
			case loadSemaphore <- struct{}{}:
				defer func() { <-loadSemaphore }()
			case <-gCtx.Done():
				return gCtx.Err()
			}

			loadStart := time.Now()
			err := s.scaler.LoadSnapshots(
				gCtx, snapshot.destinationNodeID, int64(loadSnapshotsMaxConcurrency), snapshot.hashRange,
			)
			if err != nil {
				return fmt.Errorf("loading snapshot for hash range %d to node %d: %w",
					snapshot.hashRange, snapshot.destinationNodeID, err,
				)
			}

			s.logger.Infon("Snapshot loaded",
				logger.NewIntField("hashRange", snapshot.hashRange),
				logger.NewIntField("destinationNodeId", snapshot.destinationNodeID),
				logger.NewStringField("duration", time.Since(loadStart).String()),
			)

			// Update progress
			count := loadedCount.Add(1)
			currentSnapshotsLoaded.Observe(float64(count))

			return nil
		})
	}

	return group.Wait()
}

// createSnapshotsWithProgress creates snapshots either sequentially (one at a time) or in batch
// depending on the disableSequential flag
func (s *httpServer) createSnapshotsWithProgress(
	ctx context.Context, nodeID int64, fullSync, disableSequential bool, hashRanges []int64,
) error {
	// Initialize metrics
	nodeIDStr := strconv.FormatInt(nodeID, 10)
	totalSnapshotsToCreate := s.stat.NewTaggedStat(totalSnapshotsToCreateMetricName, stats.GaugeType, stats.Tags{
		"nodeId": nodeIDStr,
	})
	totalSnapshotsToCreate.Observe(float64(len(hashRanges)))
	defer totalSnapshotsToCreate.Observe(0)

	currentSnapshotsCreated := s.stat.NewTaggedStat(currentSnapshotsToCreateMetricName, stats.GaugeType, stats.Tags{
		"nodeId": nodeIDStr,
	})
	currentSnapshotsCreated.Observe(0)

	if disableSequential || len(hashRanges) == 0 {
		// Call with all hash ranges at once (existing behavior)
		err := s.scaler.CreateSnapshots(ctx, nodeID, fullSync, hashRanges...)
		if err != nil {
			return err
		}
		currentSnapshotsCreated.Observe(float64(len(hashRanges)))
		return nil
	}

	// Call CreateSnapshots once for each hash range
	for i, hashRange := range hashRanges {
		s.logger.Infon("Creating snapshot",
			logger.NewIntField("nodeId", nodeID),
			logger.NewIntField("hashRange", hashRange),
			logger.NewIntField("progress", int64(i+1)),
			logger.NewIntField("total", int64(len(hashRanges))),
		)

		if err := s.scaler.CreateSnapshots(ctx, nodeID, fullSync, hashRange); err != nil {
			return fmt.Errorf("creating snapshot for hash range %d: %w", hashRange, err)
		}

		// Update progress metric
		currentSnapshotsCreated.Observe(float64(i + 1))
	}

	return nil
}

// handleLastOperation handles GET /lastOperation requests
func (s *httpServer) handleLastOperation(w http.ResponseWriter, r *http.Request) {
	operation := s.scaler.GetLastOperation()
	if operation == nil {
		http.Error(w, "No operation found", http.StatusNotFound)
		return
	}

	response := LastOperationResponse{
		Operation: operation,
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := jsonrs.NewEncoder(w).Encode(response); err != nil {
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
