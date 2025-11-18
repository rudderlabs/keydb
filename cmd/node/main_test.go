package main

import (
	"testing"
)

func TestPodNameRegex(t *testing.T) {
	tests := []struct {
		name        string
		podName     string
		shouldMatch bool
		expectedID  string
	}{
		// Multi-statefulset format tests (new format)
		{
			name:        "valid multi-statefulset pod name - node 0",
			podName:     "keydb-0-0",
			shouldMatch: true,
			expectedID:  "0",
		},
		{
			name:        "valid multi-statefulset pod name - node 1",
			podName:     "keydb-1-0",
			shouldMatch: true,
			expectedID:  "1",
		},
		{
			name:        "valid multi-statefulset pod name - node 99",
			podName:     "keydb-99-0",
			shouldMatch: true,
			expectedID:  "99",
		},
		{
			name:        "valid multi-statefulset pod name - node 1234",
			podName:     "keydb-1234-0",
			shouldMatch: true,
			expectedID:  "1234",
		},
		// Invalid multi-statefulset format tests
		{
			name:        "invalid multi-statefulset pod name - wrong suffix",
			podName:     "keydb-0-1",
			shouldMatch: false,
		},
		{
			name:        "invalid multi-statefulset pod name - wrong suffix 2",
			podName:     "keydb-5-2",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := podNameRegex.FindStringSubmatch(tt.podName)
			if tt.shouldMatch {
				if matches == nil {
					t.Errorf("expected pod name %q to match podNameRegex, but it didn't", tt.podName)
					return
				}
				if len(matches) < 2 {
					t.Errorf("expected at least 2 matches, got %d", len(matches))
					return
				}
				if matches[1] != tt.expectedID {
					t.Errorf("expected node ID %q, got %q", tt.expectedID, matches[1])
				}
			} else {
				if matches != nil {
					t.Errorf("expected pod name %q not to match podNameRegex, but it did", tt.podName)
				}
			}
		})
	}
}

func TestLegacyPodNameRegex(t *testing.T) {
	tests := []struct {
		name        string
		podName     string
		shouldMatch bool
		expectedID  string
	}{
		// Legacy format tests (backward compatibility)
		{
			name:        "valid legacy pod name - node 0",
			podName:     "keydb-0",
			shouldMatch: true,
			expectedID:  "0",
		},
		{
			name:        "valid legacy pod name - node 1",
			podName:     "keydb-1",
			shouldMatch: true,
			expectedID:  "1",
		},
		{
			name:        "valid legacy pod name - node 99",
			podName:     "keydb-99",
			shouldMatch: true,
			expectedID:  "99",
		},
		{
			name:        "valid legacy pod name - node 1234",
			podName:     "keydb-1234",
			shouldMatch: true,
			expectedID:  "1234",
		},
		// Invalid legacy format tests
		{
			name:        "invalid pod name - no number",
			podName:     "keydb-",
			shouldMatch: false,
		},
		{
			name:        "invalid pod name - extra suffix",
			podName:     "keydb-0-extra",
			shouldMatch: false,
		},
		{
			name:        "invalid pod name - wrong prefix",
			podName:     "redis-0",
			shouldMatch: false,
		},
		{
			name:        "invalid pod name - empty string",
			podName:     "",
			shouldMatch: false,
		},
		{
			name:        "invalid pod name - just number",
			podName:     "0",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := legacyPodNameRegex.FindStringSubmatch(tt.podName)
			if tt.shouldMatch {
				if matches == nil {
					t.Errorf("expected pod name %q to match legacyPodNameRegex, but it didn't", tt.podName)
					return
				}
				if len(matches) < 2 {
					t.Errorf("expected at least 2 matches, got %d", len(matches))
					return
				}
				if matches[1] != tt.expectedID {
					t.Errorf("expected node ID %q, got %q", tt.expectedID, matches[1])
				}
			} else {
				if matches != nil {
					t.Errorf("expected pod name %q not to match legacyPodNameRegex, but it did", tt.podName)
				}
			}
		})
	}
}

func TestPodNameParsing(t *testing.T) {
	tests := []struct {
		name              string
		podName           string
		shouldMatchNew    bool
		shouldMatchLegacy bool
		expectedID        string
	}{
		{
			name:              "multi-statefulset format should match new regex",
			podName:           "keydb-5-0",
			shouldMatchNew:    true,
			shouldMatchLegacy: false,
			expectedID:        "5",
		},
		{
			name:              "legacy format should match legacy regex only",
			podName:           "keydb-7",
			shouldMatchNew:    false,
			shouldMatchLegacy: true,
			expectedID:        "7",
		},
		{
			name:              "invalid format should match neither",
			podName:           "invalid-name",
			shouldMatchNew:    false,
			shouldMatchLegacy: false,
		},
		{
			name:              "wrong suffix should match neither",
			podName:           "keydb-3-1",
			shouldMatchNew:    false,
			shouldMatchLegacy: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newMatches := podNameRegex.FindStringSubmatch(tt.podName)
			legacyMatches := legacyPodNameRegex.FindStringSubmatch(tt.podName)

			if tt.shouldMatchNew {
				if newMatches == nil {
					t.Errorf("expected pod name %q to match new podNameRegex", tt.podName)
				} else if newMatches[1] != tt.expectedID {
					t.Errorf("expected node ID %q, got %q", tt.expectedID, newMatches[1])
				}
			} else {
				if newMatches != nil {
					t.Errorf("expected pod name %q not to match new podNameRegex", tt.podName)
				}
			}

			if tt.shouldMatchLegacy {
				if legacyMatches == nil {
					t.Errorf("expected pod name %q to match legacy podNameRegex", tt.podName)
				} else if legacyMatches[1] != tt.expectedID {
					t.Errorf("expected node ID %q, got %q", tt.expectedID, legacyMatches[1])
				}
			} else {
				if legacyMatches != nil {
					t.Errorf("expected pod name %q not to match legacy podNameRegex", tt.podName)
				}
			}
		})
	}
}
