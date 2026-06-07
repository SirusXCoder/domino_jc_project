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

// Match represents the in-memory state of an active domino game instance.
type Match struct {
	ID          string            `json:"id"`
	Status      string            `json:"status"` // WAITING, ACTIVE, BLOCKED, FINISHED
	Players     []string          `json:"players"`
	CurrentTurn string            `json:"current_turn"`
	Boneyard    []DominoTile      `json:"boneyard"`
	PlayerHands []PlayerHand      `json:"player_hands"`
	GameBoard   []DominoTile      `json:"game_board"`
	OpenLeft    int               `json:"open_left"`
	OpenRight   int               `json:"open_right"`
	CreatedAt   time.Time         `json:"created_at"`
}

// Scores maps player UID to their final pip count for a completed match.
type Scores map[string]int

// PlayerRef is a minimal Dgraph node reference used in mutations and queries
// when only the UID is known.
type PlayerRef struct {
	UID string `json:"uid,omitempty"`
}

// DefaultELO is the starting skill rating for players with no match history.
const DefaultELO = 1500.0

// Player is a registered platform user persisted in Dgraph.
//
// UID is assigned by Dgraph after insert; PlayerID is the stable application ID.
// MatchHistory is populated via the reverse of match_record.players.
type Player struct {
	UID           string        `json:"uid,omitempty"`
	DType         []string      `json:"dgraph.type,omitempty"`
	PlayerID      string        `json:"player.id,omitempty"`
	Username      string        `json:"player.username,omitempty"`
	Email         string        `json:"player.email,omitempty"`
	CreatedAt     time.Time     `json:"player.created_at,omitempty"`
	ELO           float64       `json:"player.elo,omitempty"`
	PeakELO       float64       `json:"player.peak_elo,omitempty"`
	MatchesPlayed int           `json:"player.matches_played,omitempty"`
	Wins          int           `json:"player.wins,omitempty"`
	Losses        int           `json:"player.losses,omitempty"`
	LastMatchAt   time.Time     `json:"player.last_match_at,omitempty"`
	MatchHistory  []MatchRecord `json:"~match_record.players,omitempty"`
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

// ELODeltas maps player.id to the rating change applied for a completed match.
type ELODeltas map[string]float64

// MatchRecord captures the outcome of a completed game.
//
// Winner references the winning player (player.id upsert key or Dgraph UID).
// Scores is JSON-encoded map[playerID]int (see Scores and ParseScores).
// Players should be set on insert; reverse edge ~match_record.players links career history.
type MatchRecord struct {
	UID             string    `json:"uid,omitempty"`
	DType           []string  `json:"dgraph.type,omitempty"`
	MatchID         string    `json:"match_record.match_id,omitempty"`
	Winner          string    `json:"match_record.winner,omitempty"`
	Scores          string    `json:"match_record.scores,omitempty"`
	EndTime         time.Time `json:"match_record.end_time,omitempty"`
	EndReason       string    `json:"match_record.end_reason,omitempty"`
	ELodDeltas      string    `json:"match_record.elo_deltas,omitempty"`
	RatingsApplied  bool      `json:"match_record.ratings_applied,omitempty"`
	Players         []Player  `json:"match_record.players,omitempty"`
}

// NewMatchRecord returns a MatchRecord node with JSON-encoded scores.
func NewMatchRecord(matchID, winnerID string, scores Scores, reason string, players []Player) (MatchRecord, error) {
	scoresJSON, err := json.Marshal(scores)
	if err != nil {
		return MatchRecord{}, err
	}

	return MatchRecord{
		DType:     []string{TypeMatchRecord},
		MatchID:   matchID,
		Winner:    winnerID,
		Scores:    string(scoresJSON),
		EndTime:   time.Now().UTC(),
		EndReason: reason,
		Players:   players,
	}, nil
}

// ParseELODeltas decodes the JSON elo delta payload stored on the node.
func (m *MatchRecord) ParseELODeltas() (ELODeltas, error) {
	if m.ELodDeltas == "" {
		return ELODeltas{}, nil
	}

	var deltas ELODeltas
	if err := json.Unmarshal([]byte(m.ELodDeltas), &deltas); err != nil {
		return nil, err
	}
	return deltas, nil
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
