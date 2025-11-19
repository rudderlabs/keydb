package main

import (
	"testing"
)

func TestGetNodeID(t *testing.T) {
	tests := []struct {
		name        string
		podName     string
		expectedID  int64
		expectError bool
	}{
		// Multi-statefulset format tests (new format)
		{
			name:        "valid multi-statefulset pod name - node 0",
			podName:     "keydb-0-0",
			expectedID:  0,
			expectError: false,
		},
		{
			name:        "valid multi-statefulset pod name - node 1",
			podName:     "keydb-1-0",
			expectedID:  1,
			expectError: false,
		},
		{
			name:        "valid multi-statefulset pod name - node 99",
			podName:     "keydb-99-0",
			expectedID:  99,
			expectError: false,
		},
		{
			name:        "valid multi-statefulset pod name - node 1234",
			podName:     "keydb-1234-0",
			expectedID:  1234,
			expectError: false,
		},
		// Legacy format tests (backward compatibility)
		{
			name:        "valid legacy pod name - node 0",
			podName:     "keydb-0",
			expectedID:  0,
			expectError: false,
		},
		{
			name:        "valid legacy pod name - node 1",
			podName:     "keydb-1",
			expectedID:  1,
			expectError: false,
		},
		{
			name:        "valid legacy pod name - node 99",
			podName:     "keydb-99",
			expectedID:  99,
			expectError: false,
		},
		{
			name:        "valid legacy pod name - node 1234",
			podName:     "keydb-1234",
			expectedID:  1234,
			expectError: false,
		},
		// Invalid format tests
		{
			name:        "invalid multi-statefulset pod name - wrong suffix",
			podName:     "keydb-0-1",
			expectError: true,
		},
		{
			name:        "invalid multi-statefulset pod name - wrong suffix 2",
			podName:     "keydb-5-2",
			expectError: true,
		},
		{
			name:        "invalid pod name - no number",
			podName:     "keydb-",
			expectError: true,
		},
		{
			name:        "invalid pod name - wrong prefix",
			podName:     "redis-0",
			expectError: true,
		},
		{
			name:        "invalid pod name - empty string",
			podName:     "",
			expectError: true,
		},
		{
			name:        "invalid pod name - just number",
			podName:     "0",
			expectError: true,
		},
		{
			name:        "invalid pod name - random string",
			podName:     "invalid-name",
			expectError: true,
		},
		{
			name:        "invalid pod name - multiple hyphens",
			podName:     "keydb-1-0-extra",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeID, err := getNodeID(tt.podName)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error for pod name %q, but got none", tt.podName)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for pod name %q: %v", tt.podName, err)
					return
				}
				if nodeID != tt.expectedID {
					t.Errorf("expected node ID %d, got %d", tt.expectedID, nodeID)
				}
			}
		})
	}
}
