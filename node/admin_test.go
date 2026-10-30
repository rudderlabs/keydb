package node

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

func TestAdminReplicationStatus(t *testing.T) {
	s := newTestService(context.Background(), t, Config{NodeID: 0, Addresses: staticList("a:1")}, newTestConf(t))
	server := httptest.NewServer(s.AdminHandler())
	t.Cleanup(server.Close)

	t.Run("status", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/replication/status")
		require.NoError(t, err)
		defer func() { httputil.CloseResponse(resp) }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var status ReplicationStatus
		require.NoError(t, jsonrs.Unmarshal(body, &status))
		require.Equal(t, "ACTIVE", status.Role)
		require.Equal(t, "idle", status.State)
		require.False(t, status.Ready)
		require.EqualValues(t, -1, status.GapSeconds)
	})

	t.Run("wrong method", func(t *testing.T) {
		resp, err := http.Post(server.URL+"/replication/status", "application/json", nil)
		require.NoError(t, err)
		defer func() { httputil.CloseResponse(resp) }()
		require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	})

	t.Run("unknown path", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/nope")
		require.NoError(t, err)
		defer func() { httputil.CloseResponse(resp) }()
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

func TestAdminReplicationStatusWhileRunning(t *testing.T) {
	p := newReplicationPair(t, fastReplication(nil), stats.NOP)
	p.put(t, testKeys(100)...)
	p.shadows.set(p.shadowAddress)
	require.Eventually(t, func() bool { return p.source.ReplicationStatus().Ready }, 10*time.Second,
		20*time.Millisecond)
	server := httptest.NewServer(p.source.AdminHandler())
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/replication/status")
	require.NoError(t, err)
	defer func() { httputil.CloseResponse(resp) }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	// the field names are what the runbook tells people to read, so they are checked as JSON, not through the struct
	var status map[string]any
	require.NoError(t, jsonrs.Unmarshal(body, &status))
	require.Equal(t, "ACTIVE", status["role"])
	require.Equal(t, "running", status["state"])
	require.Equal(t, p.shadowAddress, status["shadowAddress"])
	require.Equal(t, true, status["ready"])
	require.Empty(t, status["notReadyReasons"])
	require.Equal(t, true, status["shadowAllRangesSynced"])
	for _, key := range []string{
		"gapSeconds", "worstCaseGapSeconds", "passesCompleted", "lastPassBytes", "budgetBytesPerSecond",
		"currentPassRangesDone", "currentPassRangesTotal", "currentPassBytes",
	} {
		require.IsType(t, float64(0), status[key], key)
	}
	require.GreaterOrEqual(t, status["gapSeconds"], float64(0))
	for _, key := range []string{"lastPassStartedAt", "lastPassEndedAt", "currentPassStartedAt", "lastPassDuration"} {
		require.IsType(t, "", status[key], key)
	}
}
