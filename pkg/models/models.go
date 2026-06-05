package models

import (
	"encoding/json"
	"time"
)

// Dgraph node type names (dgraph.type facet values).
const (
	TypePlayer      = "Player"
	TypeGameSession = "GameSession"
	TypeMatchRecord = "MatchRecord"
)

// Game session status values stored in game_session.status.
const (
	SessionStatusWaiting   = "WAITING"
	SessionStatusActive    = "ACTIVE"
	SessionStatusCompleted = "COMPLETED"
)

// Tile represents a discrete domino piece with integer values (in-memory play state).
type Tile struct {
	ID    string `json:"id"`
	SideA int    `json:"side_a"`
	SideB int    `json:"side_b"`
}

// Match represents the in-memory state of an active domino game instance.
type Match struct {
	ID          string            `json:"id"`
	Status      string            `json:"status"` // WAITING, ACTIVE, BLOCKED, FINISHED
	Players     []string          `json:"players"`
	CurrentTurn string            `json:"current_turn"`
	Boneyard    []Tile            `json:"boneyard"`
	Hands       map[string][]Tile `json:"hands"`
	Board       []Tile            `json:"board"`
	CreatedAt   time.Time         `json:"created_at"`
}

// Scores maps player UID to their final pip count for a completed match.
type Scores map[string]int

// PlayerRef is a minimal Dgraph node reference used in mutations and queries
// when only the UID is known.
type PlayerRef struct {
	UID string `json:"uid,omitempty"`
}

// Player is a registered platform user persisted in Dgraph.
//
// UID is assigned by Dgraph after insert; PlayerID is the stable application ID.
// MatchHistory is populated via the reverse of match_record.players.
type Player struct {
	UID          string        `json:"uid,omitempty"`
	DType        []string      `json:"dgraph.type,omitempty"`
	PlayerID     string        `json:"player.id,omitempty"`
	Username     string        `json:"player.username,omitempty"`
	Email        string        `json:"player.email,omitempty"`
	CreatedAt    time.Time     `json:"player.created_at,omitempty"`
	MatchHistory []MatchRecord `json:"player.match_history,omitempty"`
}

// NewPlayer returns a Player node ready for Dgraph upsert by player.id.
func NewPlayer(playerID, username, email string) Player {
	return Player{
		DType:     []string{TypePlayer},
		PlayerID:  playerID,
		Username:  username,
		Email:     email,
		CreatedAt: time.Now().UTC(),
	}
}

// Ref returns a UID-only reference suitable for edge mutations.
func (p Player) Ref() PlayerRef {
	return PlayerRef{UID: p.UID}
}

// GameSession groups players in a lobby or active table.
//
// CurrentTurn holds the Dgraph UID of the player whose turn it is.
// Players lists full or partial Player nodes linked via game_session.players.
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

// WithPlayers attaches UID-only player stubs for a mutation payload.
func (s GameSession) WithPlayers(uids ...string) GameSession {
	s.Players = PlayersByUID(uids...)
	return s
}

// MatchRecord captures the outcome of a completed game.
//
// Winner is the Dgraph UID of the winning player.
// Scores is JSON-encoded map[uid]int (see Scores and ParseScores).
// Players should be set on insert; Player.match_history is filled via @reverse.
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

// PlayersByUID builds Player stubs containing only UID for relationship edges.
func PlayersByUID(uids ...string) []Player {
	out := make([]Player, len(uids))
	for i, uid := range uids {
		out[i] = Player{UID: uid, DType: []string{TypePlayer}}
	}
	return out
}
