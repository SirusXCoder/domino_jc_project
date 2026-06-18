package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"domino_jc_project/pkg/consensus"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/telemetry"
)

const (
	defaultForwardTimeout = 5 * time.Second
	defaultRetryAfterSecs = 2
	matchCreatePath       = "/match/create"
	matchMutatePath       = "/match/mutate"
	matchStatePath        = "/match/state"
	debugFillLogPath      = "/debug/fill-log"
	defaultCreatePlayers  = 2
	defaultFillLogCount   = 50
	fillLogTimeout        = 60 * time.Second
)

// MatchActionData carries a normalized player turn submitted through the HTTP gateway.
type MatchActionData struct {
	Kind       string            `json:"kind"`
	PlayerID   string            `json:"player_id"`
	Tile       models.DominoTile `json:"tile,omitempty"`
	PlayAtLeft bool              `json:"play_at_left,omitempty"`
}

// MatchCreateRequest is the JSON body for POST /match/create.
type MatchCreateRequest struct {
	MatchID      string   `json:"match_id,omitempty"`
	PlayerUIDs   []string `json:"player_uids,omitempty"`
	SetupGame    bool     `json:"setup_game,omitempty"`
	TilesPerHand int      `json:"tiles_per_hand,omitempty"`
}

// MatchCreateResponse is returned by POST /match/create.
type MatchCreateResponse struct {
	OK      bool   `json:"ok"`
	MatchID string `json:"match_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

// MatchMutateRequest is the JSON body for POST /match/mutate.
type MatchMutateRequest struct {
	ClientID       string          `json:"client_id"`
	SequenceNumber uint64          `json:"sequence_number"`
	MatchID        string          `json:"match_id"`
	PlayerID       string          `json:"player_id,omitempty"`
	ActionData     json.RawMessage `json:"action_data"`
}

// MatchMutateResponse is returned by POST /match/mutate.
type MatchMutateResponse struct {
	OK               bool                   `json:"ok"`
	IdempotentReplay bool                   `json:"idempotent_replay,omitempty"`
	Result           *consensus.ApplyResult `json:"result,omitempty"`
	Error            string                 `json:"error,omitempty"`
}

// MatchReadResponse is returned by GET /match/read.
type MatchReadResponse struct {
	MatchID string               `json:"match_id"`
	Found   bool                 `json:"found"`
	Session *models.GameSession  `json:"session,omitempty"`
	NodeID  string               `json:"node_id,omitempty"`
	State   string               `json:"state,omitempty"`
}

// MatchStateResponse is returned by GET /match/state with a player-scoped masked view.
type MatchStateResponse struct {
	MatchID  string              `json:"match_id"`
	PlayerID string              `json:"player_id"`
	Found    bool                `json:"found"`
	Session  *models.GameSession `json:"session,omitempty"`
	NodeID   string              `json:"node_id,omitempty"`
	State    string              `json:"state,omitempty"`
	Error    string              `json:"error,omitempty"`
}

// FillLogResponse is returned by POST /debug/fill-log.
type FillLogResponse struct {
	OK                  bool   `json:"ok"`
	Proposed            int    `json:"proposed"`
	SnapshotIndex       uint64 `json:"snapshot_index"`
	UncompactedEntries  uint64 `json:"uncompacted_entries"`
	NodeID              string `json:"node_id,omitempty"`
	Error               string `json:"error,omitempty"`
}

// MatchGateway exposes the external client API boundary with leader forwarding
// and idempotent mutation handling.
type MatchGateway struct {
	raft              *consensus.RaftNode
	manager           *engine.GameManager
	cache             *SessionCache
	httpPeerAddresses map[string]string
	forwardTimeout    time.Duration
	httpClient        *http.Client
	logger            *slog.Logger
}

// MatchGatewayOption configures optional MatchGateway behavior.
type MatchGatewayOption func(*MatchGateway)

// WithHTTPPeerAddresses maps Raft node IDs to reachable HTTP base URLs for proxying.
func WithHTTPPeerAddresses(addrs map[string]string) MatchGatewayOption {
	return func(g *MatchGateway) {
		g.httpPeerAddresses = addrs
	}
}

// WithSessionCache overrides the default idempotency cache.
func WithSessionCache(cache *SessionCache) MatchGatewayOption {
	return func(g *MatchGateway) {
		g.cache = cache
	}
}

// WithForwardTimeout sets the outbound leader-proxy deadline.
func WithForwardTimeout(timeout time.Duration) MatchGatewayOption {
	return func(g *MatchGateway) {
		if timeout > 0 {
			g.forwardTimeout = timeout
		}
	}
}

// NewMatchGateway constructs the client-facing match HTTP gateway.
func NewMatchGateway(
	raft *consensus.RaftNode,
	manager *engine.GameManager,
	opts ...MatchGatewayOption,
) *MatchGateway {
	g := &MatchGateway{
		raft:           raft,
		manager:        manager,
		cache:          NewSessionCache(defaultSessionCacheCapacity),
		forwardTimeout: defaultForwardTimeout,
		httpClient:     &http.Client{Timeout: defaultForwardTimeout},
		logger:         telemetry.DefaultLogger(),
	}
	for _, opt := range opts {
		opt(g)
	}
	if g.httpClient.Timeout != g.forwardTimeout {
		g.httpClient = &http.Client{Timeout: g.forwardTimeout}
	}
	return g
}

// Register mounts /match/read, /match/state, /match/create, /match/mutate, and /debug/fill-log on the provided mux.
func (g *MatchGateway) Register(mux *http.ServeMux) {
	mux.HandleFunc("/match/read", g.handleRead)
	mux.HandleFunc("/match/state", g.handleState)
	mux.HandleFunc("/match/create", g.handleCreate)
	mux.HandleFunc("/match/mutate", g.handleMutate)
	mux.HandleFunc("/debug/fill-log", g.handleFillLog)
}

func (g *MatchGateway) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeGatewayJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if g.manager == nil {
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "game manager is not configured"})
		return
	}

	matchID := strings.TrimSpace(r.URL.Query().Get("match_id"))
	if matchID == "" {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "match_id is required"})
		return
	}

	session, found := g.manager.GetSession(r.Context(), matchID)
	resp := MatchReadResponse{
		MatchID: matchID,
		Found:   found,
		Session: session,
	}
	if g.raft != nil {
		resp.NodeID = g.raft.NodeID
		resp.State = nodeState(g.raft)
	}

	writeGatewayJSON(w, http.StatusOK, resp)
}

func (g *MatchGateway) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeGatewayJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if g.raft == nil {
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "raft node is not configured"})
		return
	}
	if g.manager == nil {
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "game manager is not configured"})
		return
	}

	matchID := strings.TrimSpace(r.URL.Query().Get("match_id"))
	if matchID == "" {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "match_id is required"})
		return
	}
	playerID := strings.TrimSpace(r.URL.Query().Get("player_id"))
	if playerID == "" {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "player_id is required"})
		return
	}

	if !g.raft.IsLeader() {
		g.forwardStateToLeader(w, r, matchID, playerID)
		return
	}

	g.processLocalState(w, r.Context(), matchID, playerID)
}

func (g *MatchGateway) forwardStateToLeader(
	w http.ResponseWriter,
	r *http.Request,
	matchID, playerID string,
) {
	logger := telemetry.LoggerWithTrace(g.logger, r.Context())

	leaderID, leaderRaftAddr, err := g.raft.LeaderEndpoint()
	if err != nil {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":   "cluster leader is unavailable",
			"details": err.Error(),
			"node_id": g.raft.NodeID,
			"state":   nodeState(g.raft),
		})
		return
	}

	targetBase := g.leaderHTTPBase(leaderID, leaderRaftAddr)
	query := url.Values{}
	query.Set("match_id", matchID)
	query.Set("player_id", playerID)
	targetURL := strings.TrimRight(targetBase, "/") + matchStatePath + "?" + query.Encode()

	logger.Info("Forwarding state read to cluster leader",
		slog.String("match_id", matchID),
		slog.String("player_id", playerID),
		slog.String("leader_id", leaderID),
		slog.String("leader_url", targetURL),
		slog.String("node_id", g.raft.NodeID),
	)

	ctx, cancel := context.WithTimeout(r.Context(), g.forwardTimeout)
	defer cancel()

	proxyReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		writeGatewayJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build leader proxy request"})
		return
	}
	if traceID := telemetry.TraceIDFromContext(r.Context()); traceID != "" {
		proxyReq.Header.Set(telemetry.TraceIDHeader, traceID)
	}

	proxyResp, err := g.httpClient.Do(proxyReq)
	if err != nil {
		logger.Warn("Leader proxy state read failed",
			slog.String("leader_id", leaderID),
			slog.Any("error", err),
		)
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":      "leader proxy request failed",
			"leader_id":  leaderID,
			"forward_to": targetURL,
		})
		return
	}
	defer proxyResp.Body.Close()

	for key, values := range proxyResp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(proxyResp.StatusCode)
	_, _ = io.Copy(w, proxyResp.Body)
}

func (g *MatchGateway) processLocalState(w http.ResponseWriter, ctx context.Context, matchID, playerID string) {
	if err := g.raft.EnsureLinearizableRead(g.forwardTimeout); err != nil {
		g.writeProposalError(w, err)
		return
	}

	session, found := g.manager.GetSession(ctx, matchID)
	resp := MatchStateResponse{
		MatchID:  matchID,
		PlayerID: playerID,
		Found:    found,
		NodeID:   g.raft.NodeID,
		State:    nodeState(g.raft),
	}
	if found && session != nil {
		if !sessionPlayerKnown(session, playerID) {
			writeGatewayJSON(w, http.StatusBadRequest, MatchStateResponse{
				MatchID:  matchID,
				PlayerID: playerID,
				Error:    "player_id is not part of this match",
			})
			return
		}
		resp.Session = session.ViewForPlayer(playerID)
	}

	writeGatewayJSON(w, http.StatusOK, resp)
}

func sessionPlayerKnown(session *models.GameSession, playerID string) bool {
	for _, id := range session.Players {
		if id == playerID {
			return true
		}
	}
	return false
}

func (g *MatchGateway) validateMutatePlayerScope(ctx context.Context, req MatchMutateRequest) error {
	playerID := strings.TrimSpace(req.PlayerID)
	if playerID == "" || g.manager == nil {
		return nil
	}
	session, found := g.manager.GetSession(ctx, req.MatchID)
	if !found || session == nil {
		return nil
	}
	if !sessionPlayerKnown(session, playerID) {
		return fmt.Errorf("player_id is not part of this match")
	}
	return nil
}

func maskApplyResultForPlayer(result consensus.ApplyResult, playerID string) (consensus.ApplyResult, error) {
	playerID = strings.TrimSpace(playerID)
	if playerID == "" || result.Session == nil {
		return result, nil
	}
	if !sessionPlayerKnown(result.Session, playerID) {
		return result, fmt.Errorf("player_id is not part of this match")
	}
	masked := result.Session.ViewForPlayer(playerID)
	out := result
	out.Session = masked
	return out, nil
}

func (g *MatchGateway) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeGatewayJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if g.raft == nil {
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "raft node is not configured"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}

	var req MatchCreateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	if err := normalizeCreateRequest(&req); err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if !g.raft.IsLeader() {
		g.forwardCreateToLeader(w, r, body, req)
		return
	}

	g.processLocalCreate(w, r.Context(), req)
}

func (g *MatchGateway) forwardCreateToLeader(
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	req MatchCreateRequest,
) {
	logger := telemetry.LoggerWithTrace(g.logger, r.Context())

	leaderID, leaderRaftAddr, err := g.raft.LeaderEndpoint()
	if err != nil {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":   "cluster leader is unavailable",
			"details": err.Error(),
			"node_id": g.raft.NodeID,
			"state":   nodeState(g.raft),
		})
		return
	}

	targetBase := g.leaderHTTPBase(leaderID, leaderRaftAddr)
	targetURL := strings.TrimRight(targetBase, "/") + matchCreatePath

	logger.Info("Forwarding match create request to cluster leader",
		slog.String("match_id", req.MatchID),
		slog.String("leader_id", leaderID),
		slog.String("leader_url", targetURL),
		slog.String("node_id", g.raft.NodeID),
	)

	ctx, cancel := context.WithTimeout(r.Context(), g.forwardTimeout)
	defer cancel()

	proxyReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		writeGatewayJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build leader proxy request"})
		return
	}
	proxyReq.Header.Set("Content-Type", "application/json")
	if traceID := telemetry.TraceIDFromContext(r.Context()); traceID != "" {
		proxyReq.Header.Set(telemetry.TraceIDHeader, traceID)
	}

	proxyResp, err := g.httpClient.Do(proxyReq)
	if err != nil {
		logger.Warn("Leader proxy create request failed",
			slog.String("leader_id", leaderID),
			slog.Any("error", err),
		)
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":      "leader proxy request failed",
			"leader_id":  leaderID,
			"forward_to": targetURL,
		})
		return
	}
	defer proxyResp.Body.Close()

	for key, values := range proxyResp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(proxyResp.StatusCode)
	_, _ = io.Copy(w, proxyResp.Body)
}

func (g *MatchGateway) processLocalCreate(w http.ResponseWriter, ctx context.Context, req MatchCreateRequest) {
	matchID := strings.TrimSpace(req.MatchID)
	if matchID == "" {
		generated, err := generateMatchID()
		if err != nil {
			writeGatewayJSON(w, http.StatusInternalServerError, MatchCreateResponse{
				OK:    false,
				Error: "failed to generate match_id",
			})
			return
		}
		matchID = generated
	}

	command, err := consensus.EncodeCommandWithPayload(
		consensus.OpStartMatch,
		matchID,
		consensus.StartMatchPayload{
			PlayerUIDs:   req.PlayerUIDs,
			SetupGame:    req.SetupGame,
			TilesPerHand: req.TilesPerHand,
		},
	)
	if err != nil {
		writeGatewayJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to encode match create"})
		return
	}

	raw, err := g.raft.ProposeAndWaitTimeout(command, g.forwardTimeout)
	if err != nil {
		g.writeProposalError(w, err)
		return
	}
	if consensus.IsApplyError(raw) {
		if asErr, ok := raw.(error); ok {
			writeGatewayJSON(w, http.StatusConflict, MatchCreateResponse{
				OK:    false,
				Error: asErr.Error(),
			})
			return
		}
		writeGatewayJSON(w, http.StatusConflict, MatchCreateResponse{
			OK:    false,
			Error: fmt.Sprintf("match create rejected: %v", raw),
		})
		return
	}

	applyResult, ok := consensus.AsApplyResult(raw)
	if !ok || !applyResult.OK {
		writeGatewayJSON(w, http.StatusConflict, MatchCreateResponse{
			OK:    false,
			Error: "match create returned no confirmation",
		})
		return
	}

	writeGatewayJSON(w, http.StatusCreated, MatchCreateResponse{
		OK:      true,
		MatchID: applyResult.MatchID,
	})
}

func (g *MatchGateway) handleMutate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeGatewayJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if g.raft == nil {
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "raft node is not configured"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	var req MatchMutateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	if err := validateMutateRequest(req); err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := g.validateMutatePlayerScope(r.Context(), req); err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if !g.raft.IsLeader() {
		g.forwardMutateToLeader(w, r, body, req)
		return
	}

	g.processLocalMutate(w, r.Context(), req)
}

func (g *MatchGateway) forwardMutateToLeader(
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	req MatchMutateRequest,
) {
	logger := telemetry.LoggerWithTrace(g.logger, r.Context())

	leaderID, leaderRaftAddr, err := g.raft.LeaderEndpoint()
	if err != nil {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":   "cluster leader is unavailable",
			"details": err.Error(),
			"node_id": g.raft.NodeID,
			"state":   nodeState(g.raft),
		})
		return
	}

	targetBase := g.leaderHTTPBase(leaderID, leaderRaftAddr)
	targetURL := strings.TrimRight(targetBase, "/") + matchMutatePath

	logger.Info("Forwarding write request to cluster leader",
		slog.String("client_id", req.ClientID),
		slog.Uint64("seq", req.SequenceNumber),
		slog.String("match_id", req.MatchID),
		slog.String("leader_id", leaderID),
		slog.String("leader_url", targetURL),
		slog.String("node_id", g.raft.NodeID),
	)

	ctx, cancel := context.WithTimeout(r.Context(), g.forwardTimeout)
	defer cancel()

	proxyReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		writeGatewayJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build leader proxy request"})
		return
	}
	proxyReq.Header.Set("Content-Type", "application/json")
	if traceID := telemetry.TraceIDFromContext(r.Context()); traceID != "" {
		proxyReq.Header.Set(telemetry.TraceIDHeader, traceID)
	}

	proxyResp, err := g.httpClient.Do(proxyReq)
	if err != nil {
		logger.Warn("Leader proxy request failed",
			slog.String("leader_id", leaderID),
			slog.Any("error", err),
		)
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":      "leader proxy request failed",
			"leader_id":  leaderID,
			"forward_to": targetURL,
		})
		return
	}
	defer proxyResp.Body.Close()

	for key, values := range proxyResp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(proxyResp.StatusCode)
	_, _ = io.Copy(w, proxyResp.Body)
}

func (g *MatchGateway) processLocalMutate(w http.ResponseWriter, ctx context.Context, req MatchMutateRequest) {
	if cached, ok := g.cache.Get(req.ClientID, req.SequenceNumber); ok {
		cached.IdempotentReplay = true
		writeGatewayJSON(w, http.StatusOK, cached)
		return
	}

	action, err := decodeActionData(req.ActionData)
	if err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	command, err := consensus.EncodeCommandWithPayload(consensus.OpApplyTurn, req.MatchID, action)
	if err != nil {
		writeGatewayJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to encode mutation"})
		return
	}

	raw, err := g.raft.ProposeAndWaitTimeout(command, g.forwardTimeout)
	if err != nil {
		g.writeProposalError(w, err)
		return
	}
	if consensus.IsApplyError(raw) {
		if asErr, ok := raw.(error); ok {
			g.writeProposalError(w, asErr)
			return
		}
		writeGatewayJSON(w, http.StatusConflict, MatchMutateResponse{
			OK:    false,
			Error: fmt.Sprintf("mutation rejected: %v", raw),
		})
		return
	}

	applyResult, ok := consensus.AsApplyResult(raw)
	if !ok || !applyResult.OK {
		writeGatewayJSON(w, http.StatusConflict, MatchMutateResponse{
			OK:    false,
			Error: "mutation returned no confirmation",
		})
		return
	}

	maskedResult, err := maskApplyResultForPlayer(applyResult, req.PlayerID)
	if err != nil {
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	resp := MatchMutateResponse{
		OK:     true,
		Result: &maskedResult,
	}
	g.cache.Put(req.ClientID, req.SequenceNumber, resp)
	writeGatewayJSON(w, http.StatusOK, resp)
}

func (g *MatchGateway) handleFillLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeGatewayJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if g.raft == nil {
		writeGatewayJSON(w, http.StatusServiceUnavailable, FillLogResponse{
			OK:    false,
			Error: "raft node is not configured",
		})
		return
	}

	if !g.raft.IsLeader() {
		g.forwardFillLogToLeader(w, r)
		return
	}

	g.processLocalFillLog(w)
}

func (g *MatchGateway) forwardFillLogToLeader(w http.ResponseWriter, r *http.Request) {
	logger := telemetry.LoggerWithTrace(g.logger, r.Context())

	leaderID, leaderRaftAddr, err := g.raft.LeaderEndpoint()
	if err != nil {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":   "cluster leader is unavailable",
			"details": err.Error(),
			"node_id": g.raft.NodeID,
			"state":   nodeState(g.raft),
		})
		return
	}

	targetBase := g.leaderHTTPBase(leaderID, leaderRaftAddr)
	targetURL := strings.TrimRight(targetBase, "/") + debugFillLogPath

	logger.Info("Forwarding debug fill-log request to cluster leader",
		slog.String("leader_id", leaderID),
		slog.String("leader_url", targetURL),
		slog.String("node_id", g.raft.NodeID),
	)

	ctx, cancel := context.WithTimeout(r.Context(), fillLogTimeout)
	defer cancel()

	proxyReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, nil)
	if err != nil {
		writeGatewayJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build leader proxy request"})
		return
	}
	if traceID := telemetry.TraceIDFromContext(r.Context()); traceID != "" {
		proxyReq.Header.Set(telemetry.TraceIDHeader, traceID)
	}

	fillLogClient := &http.Client{Timeout: fillLogTimeout}
	proxyResp, err := fillLogClient.Do(proxyReq)
	if err != nil {
		logger.Warn("Leader proxy fill-log request failed",
			slog.String("leader_id", leaderID),
			slog.Any("error", err),
		)
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":      "leader proxy request failed",
			"leader_id":  leaderID,
			"forward_to": targetURL,
		})
		return
	}
	defer proxyResp.Body.Close()

	for key, values := range proxyResp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(proxyResp.StatusCode)
	_, _ = io.Copy(w, proxyResp.Body)
}

func (g *MatchGateway) processLocalFillLog(w http.ResponseWriter) {
	deadline := time.Now().Add(fillLogTimeout)
	proposed := 0

	for i := 0; i < defaultFillLogCount; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			writeGatewayJSON(w, http.StatusGatewayTimeout, FillLogResponse{
				OK:       false,
				Proposed: proposed,
				NodeID:   g.raft.NodeID,
				Error:    fmt.Sprintf("timed out after proposing %d entries", proposed),
			})
			return
		}

		command, err := consensus.EncodeDebugNoopCommand(fmt.Sprintf("debug-fill-%d", i+1))
		if err != nil {
			writeGatewayJSON(w, http.StatusInternalServerError, FillLogResponse{
				OK:       false,
				Proposed: proposed,
				NodeID:   g.raft.NodeID,
				Error:    "failed to encode debug noop entry",
			})
			return
		}

		raw, err := g.raft.ProposeAndWaitTimeout(command, remaining)
		if err != nil {
			g.writeProposalError(w, err)
			return
		}
		if consensus.IsApplyError(raw) {
			if asErr, ok := raw.(error); ok {
				writeGatewayJSON(w, http.StatusInternalServerError, FillLogResponse{
					OK:       false,
					Proposed: proposed,
					NodeID:   g.raft.NodeID,
					Error:    asErr.Error(),
				})
				return
			}
			writeGatewayJSON(w, http.StatusInternalServerError, FillLogResponse{
				OK:       false,
				Proposed: proposed,
				NodeID:   g.raft.NodeID,
				Error:    fmt.Sprintf("debug noop rejected: %v", raw),
			})
			return
		}
		proposed++
	}

	writeGatewayJSON(w, http.StatusOK, FillLogResponse{
		OK:                 true,
		Proposed:           proposed,
		SnapshotIndex:      g.raft.SnapshotIndex(),
		UncompactedEntries: g.raft.UncompactedLogEntries(),
		NodeID:             g.raft.NodeID,
	})
}

func (g *MatchGateway) writeProposalError(w http.ResponseWriter, err error) {
	var redirect *consensus.LeaderRedirectError
	if errors.As(err, &redirect) {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", defaultRetryAfterSecs))
		writeGatewayJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":          redirect.Error(),
			"leader_id":      redirect.LeaderID,
			"leader_address": redirect.LeaderAddress,
		})
		return
	}
	writeGatewayJSON(w, http.StatusInternalServerError, MatchMutateResponse{
		OK:    false,
		Error: err.Error(),
	})
}

func (g *MatchGateway) leaderHTTPBase(leaderID, raftAddr string) string {
	if g.httpPeerAddresses != nil {
		if base, ok := g.httpPeerAddresses[leaderID]; ok && base != "" {
			return normalizeHTTPBase(base)
		}
	}
	return normalizeHTTPBase(raftAddr)
}

func normalizeHTTPBase(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	host := addr
	if strings.HasPrefix(addr, ":") {
		host = "localhost" + addr
	}
	return "http://" + host
}

func normalizeCreateRequest(req *MatchCreateRequest) error {
	if req == nil {
		return fmt.Errorf("request is required")
	}

	if len(req.PlayerUIDs) == 0 {
		req.PlayerUIDs = make([]string, defaultCreatePlayers)
		for i := range req.PlayerUIDs {
			req.PlayerUIDs[i] = fmt.Sprintf("p%d", i+1)
		}
	}

	cleaned := make([]string, 0, len(req.PlayerUIDs))
	for _, uid := range req.PlayerUIDs {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			continue
		}
		cleaned = append(cleaned, uid)
	}
	if len(cleaned) == 0 {
		return fmt.Errorf("at least one player_uid is required")
	}
	req.PlayerUIDs = cleaned
	return nil
}

func generateMatchID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "match-" + hex.EncodeToString(entropy[:]), nil
}

func validateMutateRequest(req MatchMutateRequest) error {
	if strings.TrimSpace(req.ClientID) == "" {
		return fmt.Errorf("client_id is required")
	}
	if req.SequenceNumber == 0 {
		return fmt.Errorf("sequence_number must be greater than zero")
	}
	if strings.TrimSpace(req.MatchID) == "" {
		return fmt.Errorf("match_id is required")
	}
	if len(req.ActionData) == 0 {
		return fmt.Errorf("action_data is required")
	}
	return nil
}

func decodeActionData(raw json.RawMessage) (consensus.ApplyTurnPayload, error) {
	var action MatchActionData
	if err := json.Unmarshal(raw, &action); err != nil {
		return consensus.ApplyTurnPayload{}, fmt.Errorf("invalid action_data: %w", err)
	}
	if action.Kind == "" {
		return consensus.ApplyTurnPayload{}, fmt.Errorf("action_data.kind is required")
	}
	if action.PlayerID == "" {
		return consensus.ApplyTurnPayload{}, fmt.Errorf("action_data.player_id is required")
	}
	return consensus.ApplyTurnPayload{
		Kind:       action.Kind,
		PlayerID:   action.PlayerID,
		Tile:       action.Tile,
		PlayAtLeft: action.PlayAtLeft,
	}, nil
}

func nodeState(raft *consensus.RaftNode) string {
	if raft == nil {
		return ""
	}
	if raft.IsLeader() {
		return consensus.StateLeader
	}
	return consensus.StateFollower
}

func writeGatewayJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
