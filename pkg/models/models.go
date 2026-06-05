package models

import (
	"encoding/json"
	"time"
)

// Dgraph node type names.
const (
	TypePlayer      = "Player"
	TypeGameSession = "GameSession"
	TypeMatchRecord = "MatchRecord"
)

// Game session status values.
const (
	SessionStatusWaiting   = "WAITING"
	SessionStatusActive    = "ACTIVE"
	SessionStatusCompleted = "COMPLETED"
)

// Tile represents a discrete domino piece with integer values.
type Tile struct {
	ID    string `json:"id"`
	SideA int    `json:"side_a"`
	SideB int    `json:"side_b"`
}

// Match represents the in-memory state of an active domino game instance.
type Match struct {
	ID          string            `json:"id"`
	Status      string            `json:"status"` // "WAITING", "ACTIVE", "BLOCKED", "FINISHED"
	Players     []string          `json:"players"`
	CurrentTurn string            `json:"current_turn"`
	Boneyard    []Tile            `json:"boneyard"`
	Hands       map[string][]Tile `json:"hands"`
	Board       []Tile            `json:"board"`
	CreatedAt   time.Time         `json:"created_at"`
}

// Scores maps player UID to their final score for a completed match.
type Scores map[string]int

// Player is a registered platform user persisted in Dgraph.
type Player struct {
	UID          string        `json:"uid,omitempty"`
	DType        []string      `json:"dgraph.type,omitempty"`
	Username     string        `json:"player.username,omitempty"`
	Email        string        `json:"player.email,omitempty"`
	CreatedAt    time.Time     `json:"player.created_at,omitempty"`
	MatchHistory []MatchRecord `json:"player.match_history,omitempty"`
}

// NewPlayer returns a Player node ready for Dgraph insertion.
func NewPlayer(username, email string) Player {
	return Player{
		DType:     []string{TypePlayer},
		Username:  username,
		Email:     email,
		CreatedAt: time.Now().UTC(),
	}
}

// GameSession groups players in a lobby or active table before/during play.
type GameSession struct {
	UID         string    `json:"uid,omitempty"`
	DType       []string  `json:"dgraph.type,omitempty"`
	SessionID   string    `json:"game_session.session_id,omitempty"`
	Status      string    `json:"game_session.status,omitempty"`
	CurrentTurn string    `json:"game_session.current_turn,omitempty"`
	CreatedAt   time.Time `json:"game_session.created_at,omitempty"`
	Players     []Player  `json:"game_session.players,omitempty"`
}

// NewGameSession returns a GameSession node in WAITING status.
func NewGameSession(sessionID string) GameSession {
	return GameSession{
		DType:     []string{TypeGameSession},
		SessionID: sessionID,
		Status:    SessionStatusWaiting,
		CreatedAt: time.Now().UTC(),
	}
}

// MatchRecord captures the outcome of a completed game.
type MatchRecord struct {
	UID     string    `json:"uid,omitempty"`
	DType   []string  `json:"dgraph.type,omitempty"`
	MatchID string    `json:"match_record.match_id,omitempty"`
	Winner  string    `json:"match_record.winner,omitempty"`
	Scores  string    `json:"match_record.scores,omitempty"`
	EndTime time.Time `json:"match_record.end_time,omitempty"`
	Players []Player  `json:"match_record.players,omitempty"`
}

// NewMatchRecord returns a MatchRecord node with JSON-encoded scores.
func NewMatchRecord(matchID, winnerUID string, scores Scores, players []Player) (MatchRecord, error) {
	scoresJSON, err := json.Marshal(scores)
	if err != nil {
		return MatchRecord{}, err
	}

	return MatchRecord{
		DType:   []string{TypeMatchRecord},
		MatchID: matchID,
		Winner:  winnerUID,
		Scores:  string(scoresJSON),
		EndTime: time.Now().UTC(),
		Players: players,
	}, nil
}

// ParseScores decodes the JSON scores payload stored on the node.
func (m *MatchRecord) ParseScores() (Scores, error) {
	if m.Scores == "" {
		return Scores{}, nil
	}

	var scores Scores
	if err := json.Unmarshal([]byte(m.Scores), &scores); err != nil {
		return nil, err
	}
	return scores, nil
}
