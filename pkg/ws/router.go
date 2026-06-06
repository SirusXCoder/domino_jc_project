package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"domino_jc_project/pkg/models"
)

// GameActionHandler is the domain surface the event router invokes after
// unmarshaling and validating an inbound envelope.
type GameActionHandler interface {
	ApplyPlayTile(ctx context.Context, sessionID, playerID string, tile models.DominoTile, playAtLeft bool) (bool, error)
	ApplyDrawFromBoneyard(ctx context.Context, sessionID, playerID string) (*models.DominoTile, error)
	ApplyPassTurn(ctx context.Context, sessionID, playerID string) error
	HandlePlayerAbandoned(ctx context.Context, sessionID, playerID string) error
	GetSession(ctx context.Context, sessionID string) (*models.GameSession, bool)
}

// EventRouter parses inbound JSON envelopes and dispatches them to game logic.
type EventRouter struct {
	hub     *Hub
	actions GameActionHandler
}

// NewEventRouter constructs a router bound to the given hub and action handler.
func NewEventRouter(hub *Hub, actions GameActionHandler) *EventRouter {
	return &EventRouter{
		hub:     hub,
		actions: actions,
	}
}

// Route unmarshals msg.Payload, dispatches on envelope type, and sends error
// feedback to the originating client when processing fails.
func (r *EventRouter) Route(ctx context.Context, msg *InboundMessage) {
	if r.actions == nil {
		log.Printf("ws: no game action handler configured; dropping inbound session=%s player=%s", msg.SessionID, msg.PlayerID)
		return
	}

	var envelope EventEnvelope
	if err := json.Unmarshal(msg.Payload, &envelope); err != nil {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidEnvelope, "message is not a valid event envelope")
		return
	}

	if envelope.Type == "" {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidEnvelope, "event type is required")
		return
	}

	switch envelope.Type {
	case EventTypePlayerMove:
		r.handlePlayerMove(ctx, msg, envelope.Payload)
	case EventTypePlayTile:
		r.handlePlayTile(ctx, msg, envelope.Payload)
	case EventTypeDrawFromBoneyard:
		r.handleDrawFromBoneyard(ctx, msg, envelope.Payload)
	case EventTypePassTurn:
		r.handlePassTurn(ctx, msg, envelope.Payload)
	case EventTypeJoin:
		r.handleJoin(ctx, msg, envelope.Payload)
	case EventTypeLeave:
		r.handleLeave(ctx, msg, envelope.Payload)
	default:
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeUnknownEventType, fmt.Sprintf("unsupported event type %q", envelope.Type))
	}
}

func (r *EventRouter) handlePlayerMove(ctx context.Context, msg *InboundMessage, raw json.RawMessage) {
	var payload MovePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "PLAYER_MOVE payload is malformed")
		return
	}

	if err := r.validateActor(msg, payload.SessionID, payload.PlayerID); err != nil {
		r.sendRouteError(msg, err)
		return
	}

	if payload.TileID == "" {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "tile_id is required for PLAYER_MOVE")
		return
	}

	tile, err := r.resolveTileFromHand(ctx, payload.SessionID, payload.PlayerID, payload.TileID)
	if err != nil {
		r.sendRouteError(msg, err)
		return
	}

	if _, err := r.actions.ApplyPlayTile(ctx, payload.SessionID, payload.PlayerID, tile, payload.PlayAtLeft); err != nil {
		code, message := mapActionError(err)
		r.sendError(msg.SessionID, msg.PlayerID, code, message)
	}
}

func (r *EventRouter) handlePlayTile(ctx context.Context, msg *InboundMessage, raw json.RawMessage) {
	var payload PlayTilePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "PLAY_TILE payload is malformed")
		return
	}

	if err := r.validateActor(msg, payload.SessionID, payload.PlayerID); err != nil {
		r.sendRouteError(msg, err)
		return
	}

	if payload.Tile.ID == "" {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "tile_id is required for PLAY_TILE")
		return
	}

	if _, err := r.actions.ApplyPlayTile(ctx, payload.SessionID, payload.PlayerID, payload.Tile, payload.PlayAtLeft); err != nil {
		code, message := mapActionError(err)
		r.sendError(msg.SessionID, msg.PlayerID, code, message)
	}
}

func (r *EventRouter) handleDrawFromBoneyard(ctx context.Context, msg *InboundMessage, raw json.RawMessage) {
	var payload DrawPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "DRAW_FROM_BONEYARD payload is malformed")
		return
	}

	if err := r.validateActor(msg, payload.SessionID, payload.PlayerID); err != nil {
		r.sendRouteError(msg, err)
		return
	}

	if _, err := r.actions.ApplyDrawFromBoneyard(ctx, payload.SessionID, payload.PlayerID); err != nil {
		code, message := mapActionError(err)
		r.sendError(msg.SessionID, msg.PlayerID, code, message)
	}
}

func (r *EventRouter) handlePassTurn(ctx context.Context, msg *InboundMessage, raw json.RawMessage) {
	var payload PassPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "PASS_TURN payload is malformed")
		return
	}

	if err := r.validateActor(msg, payload.SessionID, payload.PlayerID); err != nil {
		r.sendRouteError(msg, err)
		return
	}

	if err := r.actions.ApplyPassTurn(ctx, payload.SessionID, payload.PlayerID); err != nil {
		code, message := mapActionError(err)
		r.sendError(msg.SessionID, msg.PlayerID, code, message)
	}
}

func (r *EventRouter) handleJoin(ctx context.Context, msg *InboundMessage, raw json.RawMessage) {
	var payload JoinPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "JOIN payload is malformed")
		return
	}

	if err := r.validateActor(msg, payload.SessionID, payload.PlayerID); err != nil {
		r.sendRouteError(msg, err)
		return
	}

	if _, ok := r.actions.GetSession(ctx, payload.SessionID); !ok {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeSessionNotFound, fmt.Sprintf("session %q not found", payload.SessionID))
	}
}

func (r *EventRouter) handleLeave(ctx context.Context, msg *InboundMessage, raw json.RawMessage) {
	var payload LeavePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeInvalidPayload, "LEAVE payload is malformed")
		return
	}

	if err := r.validateActor(msg, payload.SessionID, payload.PlayerID); err != nil {
		r.sendRouteError(msg, err)
		return
	}

	// Disconnect lifecycle is handled by readPump/unregister; LEAVE is a no-op ack path.
	_ = ctx
}

type routeError struct {
	code    string
	message string
}

func (e *routeError) Error() string {
	return e.message
}

func (r *EventRouter) validateActor(msg *InboundMessage, sessionID, playerID string) error {
	if sessionID == "" {
		return &routeError{code: ErrCodeInvalidPayload, message: "session_id is required in payload"}
	}
	if playerID == "" {
		return &routeError{code: ErrCodeInvalidPayload, message: "player_id is required in payload"}
	}
	if sessionID != msg.SessionID {
		return &routeError{code: ErrCodeSessionMismatch, message: "payload session_id does not match connection"}
	}
	if playerID != msg.PlayerID {
		return &routeError{code: ErrCodePlayerMismatch, message: "payload player_id does not match connection"}
	}
	return nil
}

func (r *EventRouter) resolveTileFromHand(ctx context.Context, sessionID, playerID, tileID string) (models.DominoTile, error) {
	session, ok := r.actions.GetSession(ctx, sessionID)
	if !ok {
		return models.DominoTile{}, &routeError{
			code:    ErrCodeSessionNotFound,
			message: fmt.Sprintf("session %q not found", sessionID),
		}
	}

	for _, hand := range session.Hands {
		if hand.PlayerID != playerID {
			continue
		}
		for _, tile := range hand.Tiles {
			if tile.ID == tileID {
				return tile, nil
			}
		}
		break
	}

	return models.DominoTile{}, &routeError{
		code:    ErrCodeInvalidPayload,
		message: fmt.Sprintf("tile %q is not in player hand", tileID),
	}
}

func (r *EventRouter) sendRouteError(msg *InboundMessage, err error) {
	var re *routeError
	if errors.As(err, &re) {
		r.sendError(msg.SessionID, msg.PlayerID, re.code, re.message)
		return
	}

	if isSessionNotFound(err) {
		r.sendError(msg.SessionID, msg.PlayerID, ErrCodeSessionNotFound, err.Error())
		return
	}

	r.sendError(msg.SessionID, msg.PlayerID, ErrCodeMoveRejected, err.Error())
}

func isSessionNotFound(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

func mapActionError(err error) (code, message string) {
	if errors.Is(err, models.ErrMutationsLocked) {
		return ErrCodeSessionLocked, err.Error()
	}
	if isSessionNotFound(err) {
		return ErrCodeSessionNotFound, err.Error()
	}
	return ErrCodeMoveRejected, err.Error()
}

func (r *EventRouter) sendError(sessionID, playerID, code, message string) {
	payload, err := newErrorEnvelope(code, message)
	if err != nil {
		log.Printf("ws: failed to marshal error envelope session=%s player=%s: %v", sessionID, playerID, err)
		return
	}
	r.hub.sendToPlayer(sessionID, playerID, payload)
}
