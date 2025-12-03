package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
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
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	totalSnapshotsToLoadMetricName       = "scaler_total_snapshots_to_load"
	currentSnapshotsToLoadMetricName     = "scaler_current_snapshots_loaded"
	totalSnapshotsToCreateMetricName     = "scaler_total_snapshots_to_create"
	currentSnapshotsToCreateMetricName   = "scaler_current_snapshots_created"
	defaultCreateSnapshotsMaxConcurrency = 10
	defaultLoadSnapshotsMaxConcurrency   = 10
)

type scalerClient interface {
	UpdateClusterData(ctx context.Context, addresses ...string) error
	CreateSnapshots(ctx context.Context, nodeID int64, fullSync bool, hashRanges ...int64) error
	LoadSnapshots(ctx context.Context, nodeID, maxConcurrency int64, hashRanges ...int64) error
	SendSnapshot(ctx context.Context, sourceNodeID int64, destinationAddress string, hashRange int64) error
	GetNodeAddress(nodeID int64) (string, error)
	TotalHashRanges() int64
	ClusterSize() int
	GetNodeInfo(ctx context.Context, id int64) (*pb.GetNodeInfoResponse, error)
	Close() error
}

type httpServer struct {
	client       *client.Client
	clientConfig client.Config
	scaler       scalerClient
	server       *http.Server
	stat         stats.Stats
	logger       logger.Logger
}

// newHTTPServer creates a new HTTP server
func newHTTPServer(
	clientConfig client.Config, scaler *scaler.Client, addr string, stat stats.Stats, log logger.Logger,
) (*httpServer, error) {
	s := &httpServer{
		clientConfig: clientConfig,
		scaler:       scaler,
		stat:         stat,
		logger:       log,
	}
	var err error
	s.client, err = client.NewClient(clientConfig, log.Child("client"), client.WithStats(stat))
	if err != nil {
		return nil, err
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
	mux.Post("/updateClusterData", s.handleUpdateClusterData)
	mux.Post("/hashRangeMovements", s.handleHashRangeMovements)
	mux.Post("/backup", s.handleBackup)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Minute, // using very long timeouts for now to allow for long scaler ops
		WriteTimeout: 10 * time.Minute,
		IdleTimeout:  10 * time.Minute,
	}

	return s, nil
}

// Start starts the HTTP server
func (s *httpServer) Start(ctx context.Context) error { return httputil.ListenAndServe(ctx, s.server) }

// Stop stops the HTTP server
func (s *httpServer) Stop(ctx context.Context) error { return s.server.Shutdown(ctx) }

func (s *httpServer) Close() error {
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	if err := s.scaler.Close(); err != nil {
		return fmt.Errorf("failed to close scaler: %w", err)
	}
	if err := s.server.Close(); err != nil {
		return fmt.Errorf("failed to close http server: %w", err)
	}
	return nil
}

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
	totalSnapshotsToLoad.Gauge(float64(len(req.HashRanges)))
	defer totalSnapshotsToLoad.Gauge(0)

	currentSnapshotsLoaded := s.stat.NewTaggedStat(currentSnapshotsToLoadMetricName, stats.GaugeType, stats.Tags{
		"nodeId": nodeIDStr,
	})
	currentSnapshotsLoaded.Gauge(0)

	// Load snapshots from cloud storage
	if err := s.scaler.LoadSnapshots(r.Context(), req.NodeID, req.MaxConcurrency, req.HashRanges...); err != nil {
		http.Error(w, fmt.Sprintf("Error loading snapshots: %v", err), http.StatusInternalServerError)
		return
	}

	// Update metrics after successful load
	currentSnapshotsLoaded.Gauge(float64(len(req.HashRanges)))

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
	if err := s.scaler.UpdateClusterData(r.Context(), req.Addresses...); err != nil {
		http.Error(w, fmt.Sprintf("Error completing scale operation: %v", err), http.StatusInternalServerError)
		return
	}

	// Recreate client
	err := s.client.Close()
	if err != nil {
		s.logger.Warnn("Cannot close client", obskit.Error(err))
	}
	s.clientConfig.Addresses = req.Addresses
	s.client, err = client.NewClient(s.clientConfig, s.logger.Child("client"), client.WithStats(s.stat))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error creating client: %v", err), http.StatusInternalServerError)
	}
	s.logger.Infon("Client successfully recreated")

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleBackup handles POST /backup requests
// It creates and/or loads snapshots for all nodes in the cluster without scaling operations.
func (s *httpServer) handleBackup(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req BackupRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if !req.Upload && !req.Download {
		http.Error(w, "at least one of upload or download must be true", http.StatusBadRequest)
		return
	}
	if req.Upload && req.Download {
		http.Error(w, "at most one of upload or download must be true", http.StatusBadRequest)
		return
	}

	clusterSize := s.scaler.ClusterSize()
	if clusterSize == 0 {
		http.Error(w, "cluster size is 0", http.StatusBadRequest)
		return
	}

	// Determine target nodes
	var targetNodes []int64
	if len(req.Nodes) > 0 {
		for _, nodeID := range req.Nodes {
			if nodeID < 0 || nodeID >= int64(clusterSize) {
				http.Error(w,
					fmt.Sprintf("invalid node ID %d: must be in range [0, %d)", nodeID, clusterSize),
					http.StatusBadRequest,
				)
				return
			}
		}
		targetNodes = req.Nodes
	} else {
		targetNodes = make([]int64, clusterSize)
		for i := int64(0); i < int64(clusterSize); i++ {
			targetNodes[i] = i
		}
	}

	totalHashRanges := s.scaler.TotalHashRanges()
	log := s.logger.Withn(
		logger.NewIntField("clusterSize", int64(clusterSize)),
		logger.NewIntField("totalHashRanges", totalHashRanges),
		logger.NewIntField("targetNodesCount", int64(len(targetNodes))),
		logger.NewBoolField("upload", req.Upload),
		logger.NewBoolField("download", req.Download),
		logger.NewBoolField("fullSync", req.FullSync),
	)
	log.Infon("Received backup request")

	start := time.Now()
	defer func() {
		log.Infon("Backup request completed",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	// Set default concurrency limit for loading snapshots
	loadSnapshotsMaxConcurrency := req.LoadSnapshotsMaxConcurrency
	if loadSnapshotsMaxConcurrency <= 0 {
		loadSnapshotsMaxConcurrency = defaultLoadSnapshotsMaxConcurrency
	}

	var (
		ctx                 = r.Context()
		hasher              = hash.New(int64(clusterSize), totalHashRanges)
		processedHashRanges int
	)
	group, gCtx := kitsync.NewEagerGroup(ctx, len(targetNodes))
	for _, nodeID := range targetNodes {
		hashRanges := hasher.GetNodeHashRangesList(nodeID)
		processedHashRanges += len(hashRanges)

		group.Go(func() error {
			if req.Upload {
				nodeStart := time.Now()
				err := s.createSnapshotsWithProgress(
					gCtx, nodeID, req.FullSync, req.DisableCreateSnapshotsSequentially, hashRanges,
				)
				if err != nil {
					return fmt.Errorf("creating snapshots for node %d: %w", nodeID, err)
				}
				log.Infon("Node snapshots created",
					logger.NewIntField("nodeId", nodeID),
					logger.NewIntField("hashRangesCount", int64(len(hashRanges))),
					logger.NewStringField("duration", time.Since(nodeStart).String()),
				)
			} else if req.Download {
				nodeStart := time.Now()
				err := s.scaler.LoadSnapshots(gCtx, nodeID, int64(loadSnapshotsMaxConcurrency), hashRanges...)
				if err != nil {
					return fmt.Errorf("loading snapshots for node %d: %w", nodeID, err)
				}
				log.Infon("Node snapshots loaded",
					logger.NewIntField("nodeId", nodeID),
					logger.NewIntField("hashRangesCount", int64(len(hashRanges))),
					logger.NewStringField("duration", time.Since(nodeStart).String()),
				)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := jsonrs.NewEncoder(w).Encode(BackupResponse{
		Success: true,
		Total:   processedHashRanges,
	}); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
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
		logger.NewBoolField("streaming", req.Streaming),
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

	// Handle streaming mode: node-to-node direct transfer
	if req.Streaming {
		log.Infon("Streaming mode enabled (node-to-node transfer)",
			logger.NewIntField("totalMovements", int64(len(movements))),
		)

		// Get current node addresses for streaming
		// We need to know all node addresses to send data to correct destinations
		addresses := make([]string, req.NewClusterSize)
		for i := int64(0); i < req.NewClusterSize; i++ {
			addr, err := s.scaler.GetNodeAddress(i)
			if err != nil {
				http.Error(w, fmt.Sprintf("getting address for node %d: %v", i, err), http.StatusInternalServerError)
				return
			}
			addresses[i] = addr
		}

		ctx := r.Context()
		err := s.processHashRangeMovementsStreaming(ctx, movements, addresses)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else if req.Upload && req.Download {
		// If upload=true and download=true, use pipelined approach
		// Otherwise use the old sequential approach for backward compatibility
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
		createSnapshotsMaxConcurrency = defaultCreateSnapshotsMaxConcurrency
	}
	if loadSnapshotsMaxConcurrency <= 0 {
		loadSnapshotsMaxConcurrency = defaultLoadSnapshotsMaxConcurrency
	}

	// Initialize metrics for tracking progress
	totalSnapshotsToCreate := s.stat.NewStat(totalSnapshotsToCreateMetricName, stats.GaugeType)
	totalSnapshotsToCreate.Gauge(float64(len(movements)))
	defer totalSnapshotsToCreate.Gauge(0)

	currentSnapshotsCreated := s.stat.NewStat(currentSnapshotsToCreateMetricName, stats.GaugeType)
	currentSnapshotsCreated.Gauge(0)

	totalSnapshotsToLoad := s.stat.NewStat(totalSnapshotsToLoadMetricName, stats.GaugeType)
	totalSnapshotsToLoad.Gauge(float64(len(movements)))
	defer totalSnapshotsToLoad.Gauge(0)

	currentSnapshotsLoaded := s.stat.NewStat(currentSnapshotsToLoadMetricName, stats.GaugeType)
	currentSnapshotsLoaded.Gauge(0)

	log := s.logger.Withn(
		logger.NewIntField("totalHashRanges", int64(len(movements))),
		logger.NewBoolField("fullSync", fullSync),
		logger.NewBoolField("skipCreateSnapshots", skipCreateSnapshots),
		logger.NewIntField("createSnapshotsMaxConcurrency", int64(createSnapshotsMaxConcurrency)),
		logger.NewIntField("loadSnapshotsMaxConcurrency", int64(loadSnapshotsMaxConcurrency)),
		logger.NewBoolField("disableCreateSnapshotsSequentially", disableCreateSnapshotsSequentially),
	)

	start := time.Now()
	defer func() {
		log.Infon("All snapshots created and loaded",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	// Channel to coordinate between snapshot creation and loading
	if !disableCreateSnapshotsSequentially {
		createSnapshotsMaxConcurrency = 1
		log.Warnn("Disabling concurrent snapshot creation due to disableCreateSnapshotsSequentially=true")
	}
	type snapshotCreated struct {
		hashRange         int64
		sourceNodeID      int64
		destinationNodeID int64
	}
	var (
		createdCount, loadedCount  atomic.Int64
		loadingDone                = make(chan error, 1)
		snapshotsQueue             = make(chan snapshotCreated, len(movements))
		sharedCtx, sharedCtxCancel = context.WithCancel(ctx)
		creatingGroup, creatingCtx = kitsync.NewEagerGroup(sharedCtx, createSnapshotsMaxConcurrency)
	)
	defer sharedCtxCancel()

	// Start goroutine to load snapshots in parallel
	go func() {
		defer close(loadingDone)
		loadingGroup, loadingCtx := kitsync.NewEagerGroup(sharedCtx, loadSnapshotsMaxConcurrency)
		for snapshot := range snapshotsQueue {
			loadingGroup.Go(func() error {
				log.Infon("Received snapshot queued for loading",
					logger.NewIntField("hashRange", snapshot.hashRange),
					logger.NewIntField("sourceNodeId", snapshot.sourceNodeID),
					logger.NewIntField("destinationNodeId", snapshot.destinationNodeID),
				)

				loadStart := time.Now()
				err := s.scaler.LoadSnapshots(
					loadingCtx, snapshot.destinationNodeID, int64(loadSnapshotsMaxConcurrency), snapshot.hashRange,
				)
				if err != nil {
					sharedCtxCancel()
					return fmt.Errorf("loading snapshot for hash range %d to node %d: %w",
						snapshot.hashRange, snapshot.destinationNodeID, err,
					)
				}

				log.Infon("Snapshot loaded",
					logger.NewIntField("hashRange", snapshot.hashRange),
					logger.NewIntField("destinationNodeId", snapshot.destinationNodeID),
					logger.NewStringField("duration", time.Since(loadStart).String()),
				)

				// Update progress
				currentSnapshotsLoaded.Gauge(float64(loadedCount.Add(1)))

				return nil
			})
		}
		loadingDone <- loadingGroup.Wait()
	}()

	// Start goroutine to create snapshots (or just queue them if skipCreateSnapshots is true)
	if skipCreateSnapshots {
		// If skipping creation, just queue all snapshots for loading
		for hashRange, movement := range movements {
			snapshotsQueue <- snapshotCreated{ // buffer is exactly the number of movements, no need to check the ctx
				hashRange:         hashRange,
				sourceNodeID:      movement.SourceNodeID,
				destinationNodeID: movement.DestinationNodeID,
			}
		}
	} else {
		for hashRange, movement := range movements {
			creatingGroup.Go(func() error {
				createStart := time.Now()
				err := s.scaler.CreateSnapshots(creatingCtx, movement.SourceNodeID, fullSync, hashRange)
				if err != nil {
					sharedCtxCancel()
					return fmt.Errorf("creating snapshot for hash range %d from node %d: %w",
						hashRange, movement.SourceNodeID, err,
					)
				}

				log.Infon("Snapshot created",
					logger.NewIntField("hashRange", hashRange),
					logger.NewIntField("sourceNodeId", movement.SourceNodeID),
					logger.NewIntField("destinationNodeId", movement.DestinationNodeID),
					logger.NewDurationField("duration", time.Since(createStart)),
				)

				// Update progress
				currentSnapshotsCreated.Gauge(float64(createdCount.Add(1)))

				// Send to queue for loading
				select {
				case <-creatingCtx.Done():
					return creatingCtx.Err()
				case snapshotsQueue <- snapshotCreated{
					hashRange:         hashRange,
					sourceNodeID:      movement.SourceNodeID,
					destinationNodeID: movement.DestinationNodeID,
				}:
					log.Infon("Snapshot queued for loading",
						logger.NewIntField("hashRange", hashRange),
						logger.NewIntField("sourceNodeId", movement.SourceNodeID),
						logger.NewIntField("destinationNodeId", movement.DestinationNodeID),
					)
				}
				return nil
			})
		}
	}

	creatingErr := creatingGroup.Wait()
	close(snapshotsQueue)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-loadingDone:
		if err != nil {
			return fmt.Errorf("error waiting for snapshot loading goroutine: %w", err)
		}
	}

	if creatingErr != nil {
		return fmt.Errorf("error waiting for snapshot creation goroutine: %w", creatingErr)
	}
	return nil
}

// processHashRangeMovementsStreaming processes hash range movements using node-to-node streaming.
// Each source node sends its hash ranges directly to destination nodes, bypassing cloud storage.
// Constraint: One source node can only send one hash range at a time, but different source nodes
// can send in parallel.
func (s *httpServer) processHashRangeMovementsStreaming(
	ctx context.Context,
	movements map[int64]hash.Movement,
	nodeAddresses []string,
) error {
	if len(movements) == 0 {
		return nil
	}

	log := s.logger.Withn(
		logger.NewIntField("totalMovements", int64(len(movements))),
	)
	log.Infon("Starting streaming hash range movements")

	start := time.Now()
	defer func() {
		log.Infon("Streaming hash range movements completed",
			logger.NewStringField("duration", time.Since(start).String()),
		)
	}()

	// Group movements by source node
	// map[sourceNodeID][]movement
	bySource := make(map[int64][]movementWithRange)
	for hashRange, movement := range movements {
		bySource[movement.SourceNodeID] = append(bySource[movement.SourceNodeID], movementWithRange{
			hashRange:   hashRange,
			destination: movement.DestinationNodeID,
		})
	}

	// Process each source node in parallel, but process movements sequentially within each source
	group, gCtx := kitsync.NewEagerGroup(ctx, len(bySource))
	for sourceNodeID, nodeMovements := range bySource {
		group.Go(func() error {
			// Process this source node's movements sequentially
			for _, mv := range nodeMovements {
				// Get destination address
				if mv.destination >= int64(len(nodeAddresses)) {
					return fmt.Errorf("destination node %d address not found", mv.destination)
				}

				destAddr := nodeAddresses[mv.destination]

				log.Infon("Streaming hash range",
					logger.NewIntField("hashRange", mv.hashRange),
					logger.NewIntField("sourceNodeId", sourceNodeID),
					logger.NewIntField("destinationNodeId", mv.destination),
					logger.NewStringField("destinationAddress", destAddr),
				)

				mvStart := time.Now()
				err := s.scaler.SendSnapshot(gCtx, sourceNodeID, destAddr, mv.hashRange)
				if err != nil {
					return fmt.Errorf("streaming hash range %d from node %d to node %d: %w",
						mv.hashRange, sourceNodeID, mv.destination, err)
				}

				log.Infon("Streamed hash range",
					logger.NewIntField("hashRange", mv.hashRange),
					logger.NewIntField("sourceNodeId", sourceNodeID),
					logger.NewIntField("destinationNodeId", mv.destination),
					logger.NewStringField("duration", time.Since(mvStart).String()),
				)
			}
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
	totalSnapshotsToCreate.Gauge(float64(len(hashRanges)))
	defer totalSnapshotsToCreate.Gauge(0)

	currentSnapshotsCreated := s.stat.NewTaggedStat(currentSnapshotsToCreateMetricName, stats.GaugeType, stats.Tags{
		"nodeId": nodeIDStr,
	})
	currentSnapshotsCreated.Gauge(0)

	if disableSequential || len(hashRanges) == 0 {
		// Call with all hash ranges at once (existing behavior)
		err := s.scaler.CreateSnapshots(ctx, nodeID, fullSync, hashRanges...)
		if err != nil {
			return err
		}
		currentSnapshotsCreated.Gauge(float64(len(hashRanges)))
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
		currentSnapshotsCreated.Gauge(float64(i + 1))
	}

	return nil
}

// movementWithRange represents a hash range movement with its hash range ID
type movementWithRange struct {
	hashRange   int64
	destination int64
}

type loggerAdapter struct {
	logger logger.Logger
}

func (l loggerAdapter) Print(v ...any) {
	l.logger.Infow("", v...) // nolint:forbidigo
}
