package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"domino_jc_project/pkg/consensus"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/gateway"
	"domino_jc_project/pkg/repository"
)

// StatsHandler exposes read-only career analytics and leaderboard endpoints.
type StatsHandler struct {
	stats repository.StatsRepository
}

// NewStatsHandler constructs HTTP handlers backed by the stats repository.
func NewStatsHandler(stats repository.StatsRepository) *StatsHandler {
	return &StatsHandler{stats: stats}
}

// Register mounts analytics routes on the provided mux.
func (h *StatsHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/leaderboard", h.handleLeaderboard)
	mux.HandleFunc("/api/players/", h.handlePlayerRoutes)
}

func (h *StatsHandler) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}

	entries, err := h.stats.ListLeaderboard(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"leaderboard": entries,
	})
}

func (h *StatsHandler) handlePlayerRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/players/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "player not found")
		return
	}

	playerID := parts[0]
	if len(parts) == 1 || parts[1] == "stats" {
		h.handlePlayerStats(w, r, playerID)
		return
	}
	if parts[1] == "matches" {
		h.handlePlayerMatches(w, r, playerID)
		return
	}

	writeError(w, http.StatusNotFound, "route not found")
}

func (h *StatsHandler) handlePlayerStats(w http.ResponseWriter, r *http.Request, playerID string) {
	limit := 20
	if raw := r.URL.Query().Get("recent"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid recent")
			return
		}
		limit = parsed
	}

	stats, err := h.stats.GetPlayerCareer(r.Context(), playerID, limit)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

func (h *StatsHandler) handlePlayerMatches(w http.ResponseWriter, r *http.Request, playerID string) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}

	cursor := r.URL.Query().Get("cursor")
	page, err := h.stats.ListPlayerMatchHistory(r.Context(), playerID, limit, cursor)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "invalid cursor") {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, page)
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// GatewayHandler exposes cluster leadership, session routing metadata, and the
// external match read/mutate API for game clients.
type GatewayHandler struct {
	raft        *consensus.RaftNode
	matchGateway *gateway.MatchGateway
}

// GatewayOption configures optional GatewayHandler behavior.
type GatewayOption func(*GatewayHandler)

// WithMatchGateway enables GET /match/read and POST /match/mutate on the gateway mux.
func WithMatchGateway(manager *engine.GameManager, opts ...gateway.MatchGatewayOption) GatewayOption {
	return func(h *GatewayHandler) {
		h.matchGateway = gateway.NewMatchGateway(h.raft, manager, opts...)
	}
}

// NewGatewayHandler constructs HTTP handlers for gateway failover and routing.
func NewGatewayHandler(raft *consensus.RaftNode, opts ...GatewayOption) *GatewayHandler {
	h := &GatewayHandler{raft: raft}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Register mounts gateway orchestration routes on the provided mux.
func (h *GatewayHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/gateway/leader", h.handleLeader)
	mux.HandleFunc("/api/gateway/session", h.handleSessionRoute)
	if h.matchGateway != nil {
		h.matchGateway.Register(mux)
	}
}

func (h *GatewayHandler) handleLeader(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.raft == nil {
		writeError(w, http.StatusServiceUnavailable, "raft node is not configured")
		return
	}

	leaderID, leaderAddress, err := h.raft.LeaderEndpoint()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":   err.Error(),
			"node_id": h.raft.NodeID,
			"state":   gatewayNodeState(h.raft),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id":         h.raft.NodeID,
		"leader_id":       leaderID,
		"leader_address":  leaderAddress,
		"is_local_leader": leaderID == h.raft.NodeID,
		"state":           gatewayNodeState(h.raft),
	})
}

func (h *GatewayHandler) handleSessionRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.raft == nil {
		writeError(w, http.StatusServiceUnavailable, "raft node is not configured")
		return
	}

	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		writeError(w, http.StatusBadRequest, "session_id is required")
		return
	}

	leaderID, leaderAddress, err := h.raft.LeaderEndpoint()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":      err.Error(),
			"session_id": sessionID,
			"node_id":    h.raft.NodeID,
			"redirect":   false,
		})
		return
	}

	redirectRequired := leaderID != h.raft.NodeID
	status := http.StatusOK
	if redirectRequired {
		status = http.StatusTemporaryRedirect
	}

	writeJSON(w, status, map[string]interface{}{
		"session_id":       sessionID,
		"node_id":          h.raft.NodeID,
		"leader_id":        leaderID,
		"leader_address":   leaderAddress,
		"redirect":         redirectRequired,
		"mutations_target": leaderAddress,
	})
}

func gatewayNodeState(raft *consensus.RaftNode) string {
	if raft == nil {
		return ""
	}
	if raft.IsLeader() {
		return consensus.StateLeader
	}
	return consensus.StateFollower
}
