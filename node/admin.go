package node

import (
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

// AdminHandler serves the node's admin endpoints:
//   - GET /replication/status returns the ReplicationStatus of the job filling this node's shadow
func (s *Service) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/replication/status", s.replicationStatusHandler())
	return mux
}

func (s *Service) replicationStatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := jsonrs.Marshal(s.ReplicationStatus())
		if err != nil {
			s.logger.Errorn("Marshalling the replication status", obskit.Error(err))
			http.Error(w, "marshalling the replication status", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(body); err != nil {
			s.logger.Warnn("Writing the replication status", obskit.Error(err))
		}
	})
}
