package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rudderlabs/keydb/client"
)

type httpServer struct {
	client *client.Client
	server *http.Server
}

// newHTTPServer creates a new HTTP server
func newHTTPServer(client *client.Client, addr string) *httpServer {
	s := &httpServer{
		client: client,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/put", s.handlePut)
	mux.HandleFunc("/info", s.handleInfo)
	mux.HandleFunc("/snapshot", s.handleSnapshot)
	mux.HandleFunc("/scale", s.handleScale)
	mux.HandleFunc("/scaleComplete", s.handleScaleComplete)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

// Start starts the HTTP server
func (s *httpServer) Start() error { return s.server.ListenAndServe() }

// Stop stops the HTTP server
func (s *httpServer) Stop(ctx context.Context) error { return s.server.Shutdown(ctx) }

// handleGet handles GET /get requests
func (s *httpServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse keys from query parameters
	keys := r.URL.Query()["keys"]
	if len(keys) == 0 {
		http.Error(w, "No keys provided", http.StatusBadRequest)
		return
	}

	// Get values for keys
	exists, err := s.client.Get(r.Context(), keys)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting keys: %v", err), http.StatusInternalServerError)
		return
	}

	// Create response
	response := make(map[string]bool)
	for i, key := range keys {
		response[key] = exists[i]
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}

// PutRequest represents a request to put keys
type PutRequest struct {
	Keys []string      `json:"keys"`
	TTL  time.Duration `json:"ttl"` // TTL in seconds
}

// handlePut handles POST /put requests
func (s *httpServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Keys) == 0 {
		http.Error(w, "No keys provided", http.StatusBadRequest)
		return
	}

	// Put keys
	ttl, err := time.ParseDuration(fmt.Sprintf("%ds", req.TTL))
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

// handleInfo handles GET /info requests
func (s *httpServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse node ID from query parameters
	nodeIDStr := r.URL.Query().Get("nodeID")
	if nodeIDStr == "" {
		http.Error(w, "No nodeID provided", http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 32)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid nodeID: %v", err), http.StatusBadRequest)
		return
	}

	// Get node info
	info, err := s.client.GetNodeInfo(r.Context(), uint32(nodeID))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting info for node %d: %v", nodeID, err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleSnapshot handles POST /snapshot requests
func (s *httpServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Create snapshot
	if err := s.client.CreateSnapshot(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("Error creating snapshot: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"success":true}`))
}

// ScaleRequest represents a request to scale the cluster
type ScaleRequest struct {
	Addresses []string `json:"addresses"`
}

// handleScale handles POST /scale requests
func (s *httpServer) handleScale(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
