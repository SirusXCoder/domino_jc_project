package ws

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"domino_jc_project/pkg/models"
)

// MatchLedger enqueues immutable match records for async persistence.
type MatchLedger interface {
	Enqueue(record models.MatchRecord)
}

// InboundMessage carries a validated inbound payload from a connected client.
type InboundMessage struct {
	SessionID string
	PlayerID  string
	Payload   []byte
}

// abandonmentEvent is delivered when a disconnected player exceeds the grace period.
type abandonmentEvent struct {
	sessionID string
	playerID  string
}

// playerConnection tracks connection lifecycle independent of the live socket.
type playerConnection struct {
	status ConnectionStatus
	timer  *time.Timer
}

// Hub is the central connection registry. All map mutations happen inside Run()
// so callers never need to synchronize access to the clients map themselves.
type Hub struct {
	// clients is nested by sessionID -> playerID for per-session fan-out.
	clients map[string]map[string]*Client

	// playerStates persists connection lifecycle across socket drops.
	playerStates map[string]map[string]*playerConnection

	register   chan *Client
	unregister chan *Client
	inbound    chan *InboundMessage
	abandon    chan abandonmentEvent

	router              *EventRouter
	actions             GameActionHandler
	ledger              MatchLedger
	reconnectGracePeriod time.Duration

	mu sync.RWMutex
}

// HubOption configures optional Hub behavior (primarily for tests).
type HubOption func(*Hub)

// WithReconnectGracePeriod overrides the default 45-second reconnection window.
func WithReconnectGracePeriod(d time.Duration) HubOption {
	return func(h *Hub) {
		h.reconnectGracePeriod = d
	}
}

// WithMatchLedger attaches the async ledger worker used when matches terminate.
func WithMatchLedger(ledger MatchLedger) HubOption {
	return func(h *Hub) {
		h.ledger = ledger
	}
}

// NewHub constructs a Hub ready to process connection lifecycle events.
// Pass a non-nil GameActionHandler to enable inbound event routing and
// abandonment workflows; nil keeps the hub in connection-only mode.
func NewHub(actions GameActionHandler, opts ...HubOption) *Hub {
	h := &Hub{
		clients:              make(map[string]map[string]*Client),
		playerStates:         make(map[string]map[string]*playerConnection),
		register:             make(chan *Client),
		unregister:           make(chan *Client),
		inbound:              make(chan *InboundMessage, 256),
		abandon:              make(chan abandonmentEvent, 64),
		actions:              actions,
		reconnectGracePeriod: reconnectGracePeriod,
	}
	for _, opt := range opts {
		opt(h)
	}
	if actions != nil {
		h.router = NewEventRouter(h, actions)
	}
	return h
}

// Run processes registration, unregistration, inbound messages, and abandonment
// timers on a single goroutine to avoid data races on hub maps.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.registerClient(client)

		case client := <-h.unregister:
			h.unregisterClient(client)

		case msg := <-h.inbound:
			h.handleInbound(msg)

		case evt := <-h.abandon:
			h.handleAbandonment(evt)
		}
	}
}

func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	players, ok := h.clients[client.sessionID]
	if !ok {
		players = make(map[string]*Client)
		h.clients[client.sessionID] = players
	}

	// Replace an existing live socket for the same session/player pair.
	if existing, ok := players[client.playerID]; ok {
		close(existing.send)
		delete(players, client.playerID)
		log.Printf("ws: replaced stale connection session=%s player=%s", client.sessionID, client.playerID)
	}

	players[client.playerID] = client

	state := h.ensurePlayerStateLocked(client.sessionID, client.playerID)
	if state.status == ConnectionStatusDisconnected {
		h.cancelReconnectTimerLocked(state)
		log.Printf("ws: reconnected session=%s player=%s within grace period", client.sessionID, client.playerID)
	}
	state.status = ConnectionStatusConnected

	log.Printf("ws: registered session=%s player=%s (session_clients=%d)", client.sessionID, client.playerID, len(players))

	if h.actions != nil {
		h.sendStateSnapshotLocked(client.sessionID, client.playerID)
	}
}

func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	players, ok := h.clients[client.sessionID]
	if !ok {
		return
	}

	current, ok := players[client.playerID]
	if !ok || current != client {
		// A newer connection may have replaced this client; ignore stale unregister.
		return
	}

	delete(players, client.playerID)
	if len(players) == 0 {
		delete(h.clients, client.sessionID)
	}

	close(client.send)

	state := h.ensurePlayerStateLocked(client.sessionID, client.playerID)
	if state.status == ConnectionStatusAbandoned {
		return
	}

	state.status = ConnectionStatusDisconnected
	h.scheduleReconnectTimerLocked(client.sessionID, client.playerID, state)

	log.Printf("ws: unregistered session=%s player=%s (grace period started)", client.sessionID, client.playerID)
}

func (h *Hub) handleInbound(msg *InboundMessage) {
	if h.router == nil {
		log.Printf("ws: inbound session=%s player=%s bytes=%d (no router configured)", msg.SessionID, msg.PlayerID, len(msg.Payload))
		return
	}
	h.router.Route(context.Background(), msg)
}

func (h *Hub) handleAbandonment(evt abandonmentEvent) {
	h.mu.Lock()
	state, ok := h.playerStateLocked(evt.sessionID, evt.playerID)
	if !ok || state.status != ConnectionStatusDisconnected {
		h.mu.Unlock()
		return
	}

	state.status = ConnectionStatusAbandoned
	state.timer = nil
	h.mu.Unlock()

	log.Printf("ws: player abandoned session=%s player=%s after grace period", evt.sessionID, evt.playerID)

	if h.actions == nil {
		return
	}

	if err := h.actions.HandlePlayerAbandoned(context.Background(), evt.sessionID, evt.playerID); err != nil {
		log.Printf("ws: abandonment workflow failed session=%s player=%s: %v", evt.sessionID, evt.playerID, err)
	}
}

func (h *Hub) ensurePlayerStateLocked(sessionID, playerID string) *playerConnection {
	states, ok := h.playerStates[sessionID]
	if !ok {
		states = make(map[string]*playerConnection)
		h.playerStates[sessionID] = states
	}

	state, ok := states[playerID]
	if !ok {
		state = &playerConnection{status: ConnectionStatusConnected}
		states[playerID] = state
	}
	return state
}

func (h *Hub) playerStateLocked(sessionID, playerID string) (*playerConnection, bool) {
	states, ok := h.playerStates[sessionID]
	if !ok {
		return nil, false
	}
	state, ok := states[playerID]
	return state, ok
}

func (h *Hub) cancelReconnectTimerLocked(state *playerConnection) {
	if state.timer == nil {
		return
	}
	if !state.timer.Stop() {
		select {
		case <-state.timer.C:
		default:
		}
	}
	state.timer = nil
}

func (h *Hub) scheduleReconnectTimerLocked(sessionID, playerID string, state *playerConnection) {
	h.cancelReconnectTimerLocked(state)

	grace := h.reconnectGracePeriod
	state.timer = time.AfterFunc(grace, func() {
		select {
		case h.abandon <- abandonmentEvent{sessionID: sessionID, playerID: playerID}:
		default:
			log.Printf("ws: abandonment channel full session=%s player=%s", sessionID, playerID)
		}
	})
}

func (h *Hub) sendStateSnapshotLocked(sessionID, playerID string) {
	session, ok := h.actions.GetSession(context.Background(), sessionID)
	if !ok {
		log.Printf("ws: no session for state snapshot session=%s player=%s", sessionID, playerID)
		return
	}

	payload, err := json.Marshal(session)
	if err != nil {
		log.Printf("ws: failed to marshal state snapshot session=%s player=%s: %v", sessionID, playerID, err)
		return
	}

	envelope, err := json.Marshal(EventEnvelope{
		Type:      EventTypeStateSnapshot,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
	})
	if err != nil {
		log.Printf("ws: failed to marshal snapshot envelope session=%s player=%s: %v", sessionID, playerID, err)
		return
	}

	h.sendToPlayerLocked(sessionID, playerID, envelope)
}

// sendToPlayer enqueues an outbound frame for one connected peer. It must be
// called from the hub Run loop goroutine to avoid races on the clients map.
func (h *Hub) sendToPlayer(sessionID, playerID string, payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.sendToPlayerLocked(sessionID, playerID, payload)
}

func (h *Hub) sendToPlayerLocked(sessionID, playerID string, payload []byte) {
	players, ok := h.clients[sessionID]
	if !ok {
		return
	}
	client, ok := players[playerID]
	if !ok {
		return
	}

	select {
	case client.send <- payload:
	default:
		log.Printf("ws: outbound buffer full session=%s player=%s", sessionID, playerID)
	}
}

// ClientCount returns the total number of active WebSocket connections.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, players := range h.clients {
		total += len(players)
	}
	return total
}

// PlayerConnectionStatus returns the tracked lifecycle status for a session/player pair.
// Useful in tests; returns empty string when no state is recorded.
func (h *Hub) PlayerConnectionStatus(sessionID, playerID string) ConnectionStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()

	state, ok := h.playerStateLocked(sessionID, playerID)
	if !ok {
		return ""
	}
	return state.status
}

// TerminateMatch broadcasts the final match frame to all connected peers and
// enqueues an immutable snapshot for the ledger worker.
func (h *Hub) TerminateMatch(_ context.Context, sessionID string, outcome *models.MatchOutcome, session *models.GameSession) {
	if outcome == nil || session == nil {
		log.Printf("ws: TerminateMatch skipped session=%s (missing outcome or session)", sessionID)
		return
	}

	matchPayload, err := json.Marshal(MatchEndPayload{
		SessionID: sessionID,
		WinnerID:  outcome.WinnerID,
		Reason:    outcome.Reason,
		Scores:    outcome.Scores,
		Status:    session.Status,
	})
	if err != nil {
		log.Printf("ws: failed to marshal match end payload session=%s: %v", sessionID, err)
		return
	}

	matchEnvelope, err := json.Marshal(EventEnvelope{
		Type:      EventTypeMatchEnd,
		Timestamp: time.Now().UnixMilli(),
		Payload:   matchPayload,
	})
	if err != nil {
		log.Printf("ws: failed to marshal match end envelope session=%s: %v", sessionID, err)
		return
	}

	sessionPayload, err := json.Marshal(session)
	if err != nil {
		log.Printf("ws: failed to marshal final session snapshot session=%s: %v", sessionID, err)
		return
	}

	snapshotEnvelope, err := json.Marshal(EventEnvelope{
		Type:      EventTypeStateSnapshot,
		Timestamp: time.Now().UnixMilli(),
		Payload:   sessionPayload,
	})
	if err != nil {
		log.Printf("ws: failed to marshal final snapshot envelope session=%s: %v", sessionID, err)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	players, ok := h.clients[sessionID]
	if !ok {
		log.Printf("ws: TerminateMatch session=%s has no connected clients", sessionID)
	} else {
		for playerID := range players {
			h.sendToPlayerLocked(sessionID, playerID, matchEnvelope)
			h.sendToPlayerLocked(sessionID, playerID, snapshotEnvelope)
		}
	}

	if h.ledger != nil {
		players := make([]models.Player, len(session.Players))
		for i, playerID := range session.Players {
			players[i] = models.Player{
				PlayerID: playerID,
				DType:    []string{models.TypePlayer},
			}
		}
		record, err := models.NewMatchRecord(outcome.MatchID, outcome.WinnerID, outcome.Scores, players)
		if err != nil {
			log.Printf("ws: failed to build ledger record session=%s: %v", sessionID, err)
			return
		}
		h.ledger.Enqueue(record)
	}
}
