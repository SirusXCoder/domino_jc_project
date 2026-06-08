package ws

import (
	"encoding/json"
	"time"

	"domino_jc_project/pkg/models"
)

// Wire event type constants.
const (
	EventTypePlayerMove       = "PLAYER_MOVE"
	EventTypePlayTile         = "PLAY_TILE"
	EventTypeDrawFromBoneyard = "DRAW_FROM_BONEYARD"
	EventTypePassTurn         = "PASS_TURN"
	EventTypeJoin             = "JOIN"
	EventTypeLeave            = "LEAVE"
	EventTypeStateSnapshot    = "STATE_SNAPSHOT"
	EventTypeMatchEnd         = "MATCH_END"
	EventTypePlayerStatsUpdated = "PLAYER_STATS_UPDATED"
	EventTypeError            = "ERROR"
)

// ConnectionStatus tracks a player's socket lifecycle within the hub.
type ConnectionStatus string

// Connection status values tracked by the hub for reconnection lifecycle.
const (
	ConnectionStatusConnected    ConnectionStatus = "CONNECTED"
	ConnectionStatusDisconnected ConnectionStatus = "DISCONNECTED"
	ConnectionStatusAbandoned    ConnectionStatus = "ABANDONED"
)

// Client-visible error codes returned in ErrorEnvelope.Error.
const (
	ErrCodeInvalidJSON      = "INVALID_JSON"
	ErrCodeInvalidEnvelope  = "INVALID_ENVELOPE"
	ErrCodeUnknownEventType = "UNKNOWN_EVENT_TYPE"
	ErrCodeInvalidPayload   = "INVALID_MOVE_FORMAT"
	ErrCodeSessionNotFound  = "SESSION_NOT_FOUND"
	ErrCodeSessionMismatch  = "SESSION_MISMATCH"
	ErrCodePlayerMismatch   = "PLAYER_MISMATCH"
	ErrCodeMoveRejected     = "MOVE_REJECTED"
	ErrCodeSessionLocked    = "SESSION_LOCKED"
	ErrCodeNotImplemented   = "ACTION_NOT_SUPPORTED"
)

// EventEnvelope is the standard inbound/outbound message wrapper.
type EventEnvelope struct {
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// ErrorEnvelope is sent to a client when an inbound event cannot be processed.
type ErrorEnvelope struct {
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Error     string `json:"error"`
	Message   string `json:"message,omitempty"`
}

// MovePayload carries coordinate hints and tile selection for PLAYER_MOVE.
type MovePayload struct {
	SessionID  string `json:"session_id"`
	PlayerID   string `json:"player_id"`
	TileID     string `json:"tile_id"`
	PlayAtLeft bool   `json:"play_at_left"`
	X          *int   `json:"x,omitempty"`
	Y          *int   `json:"y,omitempty"`
}

// PlayTilePayload carries an explicit tile definition for PLAY_TILE.
type PlayTilePayload struct {
	SessionID  string            `json:"session_id"`
	PlayerID   string            `json:"player_id"`
	Tile       models.DominoTile `json:"tile"`
	PlayAtLeft bool              `json:"play_at_left"`
}

// DrawPayload requests a draw from the boneyard.
type DrawPayload struct {
	SessionID string `json:"session_id"`
	PlayerID  string `json:"player_id"`
}

// PassPayload records that the active player passes their turn.
type PassPayload struct {
	SessionID string `json:"session_id"`
	PlayerID  string `json:"player_id"`
}

// JoinPayload acknowledges a player joining an existing session.
type JoinPayload struct {
	SessionID string `json:"session_id"`
	PlayerID  string `json:"player_id"`
}

// LeavePayload acknowledges a player leaving a session.
type LeavePayload struct {
	SessionID string `json:"session_id"`
	PlayerID  string `json:"player_id"`
}

// MatchEndPayload is broadcast when a match completes.
type MatchEndPayload struct {
	SessionID string        `json:"session_id"`
	WinnerID  string        `json:"winner_id"`
	Reason    string        `json:"reason"`
	Scores    models.Scores `json:"scores"`
	Status    string        `json:"status"`
}

// PlayerStatsUpdatedPayload is pushed after ELO/career recalculation completes.
type PlayerStatsUpdatedPayload struct {
	SessionID     string  `json:"session_id"`
	MatchID       string  `json:"match_id"`
	PlayerID      string  `json:"player_id"`
	ELO           float64 `json:"elo"`
	PeakELO       float64 `json:"peak_elo"`
	MatchesPlayed int     `json:"matches_played"`
	Wins          int     `json:"wins"`
	Losses        int     `json:"losses"`
	ELODelta      float64 `json:"elo_delta"`
	Won           bool    `json:"won"`
}

func newErrorEnvelope(code, message string) ([]byte, error) {
	env := ErrorEnvelope{
		Type:      EventTypeError,
		Timestamp: time.Now().UnixMilli(),
		Error:     code,
		Message:   message,
	}
	return json.Marshal(env)
}
