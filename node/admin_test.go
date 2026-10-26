package node

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
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
