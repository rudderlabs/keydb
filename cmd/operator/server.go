package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type httpServer struct {
	client *client.Client
	server *http.Server
}

// newHTTPServer creates a new HTTP server
func newHTTPServer(client *client.Client, addr string, log logger.Logger) *httpServer {
	s := &httpServer{
		client: client,
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleInfo handles POST /info requests
func (s *httpServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req InfoRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Get node info
	info, err := s.client.GetNodeInfo(r.Context(), req.NodeID)
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req CreateSnapshotsRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Create snapshot
	if err := s.client.CreateSnapshots(r.Context(), req.NodeID, req.FullSync, req.HashRanges...); err != nil {
		http.Error(w, fmt.Sprintf("Error creating snapshot: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleLoadSnapshots handles POST /loadSnapshots requests
func (s *httpServer) handleLoadSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req LoadSnapshotsRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Load snapshots from cloud storage
	if err := s.client.LoadSnapshots(r.Context(), req.NodeID, req.HashRanges...); err != nil {
		http.Error(w, fmt.Sprintf("Error loading snapshots: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleScale handles POST /scale requests
func (s *httpServer) handleScale(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req ScaleRequest
	if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Addresses) == 0 {
		http.Error(w, "No addresses provided", http.StatusBadRequest)
		return
	}

	// Scale cluster
	if err := s.client.Scale(r.Context(), req.Addresses...); err != nil {
		http.Error(w, fmt.Sprintf("Error scaling cluster: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// handleScaleComplete handles POST /scaleComplete requests
func (s *httpServer) handleScaleComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Complete scale operation
	if err := s.client.ScaleComplete(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("Error completing scale operation: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

type loggerAdapter struct {
	logger logger.Logger
}

func (l loggerAdapter) Print(v ...any) {
	l.logger.Infow("", v...) // nolint:forbidigo
}
