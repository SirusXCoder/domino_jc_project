package models

import (
	"fmt"
)

// DominoTile represents a single domino piece.
type DominoTile struct {
	UID        string `json:"uid,omitempty"`
	ID         string `json:"tile_id,omitempty"`
	ValueLeft  int    `json:"value_left"`
	ValueRight int    `json:"value_right"`
}

// NewTile is a constructor that auto-generates the deterministic ID.
func NewTile(left, right int) DominoTile {
	return DominoTile{
		ID:         fmt.Sprintf("%d-%d", left, right),
		ValueLeft:  left,
		ValueRight: right,
	}
}

// IsDouble returns true if both sides of the domino have the same value.
func (t DominoTile) IsDouble() bool {
	return t.ValueLeft == t.ValueRight
}

// String provides a clean string representation for debugging or logging.
func (t DominoTile) String() string {
	return fmt.Sprintf("[%d|%d]", t.ValueLeft, t.ValueRight)
}

// PlayerHand represents the private state of a player's tiles during a game.
type PlayerHand struct {
	UID       string       `json:"uid,omitempty"`
	PlayerID  string       `json:"player_id"`
	Tiles     []DominoTile `json:"tiles"`
	HasPassed bool         `json:"has_passed"`
	IsReady   bool         `json:"is_ready"`
}

// GameSession represents the complete state of an active match.
type GameSession struct {
	UID            string       `json:"uid,omitempty"`
	SessionID      string       `json:"session_id,omitempty"`
	Status         string       `json:"status"`
	Players        []string     `json:"players"`
	Hands          []PlayerHand `json:"hands"`
	Boneyard       []DominoTile `json:"boneyard"`
	GameBoard      []DominoTile `json:"game_board"`
	LeftOpenValue  int          `json:"left_open_value"`
	RightOpenValue int          `json:"right_open_value"`
	CurrentTurn    string       `json:"current_turn"`

	// Dgraph-specific persistence blobs
	BoneyardRaw  string `json:"boneyard_raw,omitempty"`
	GameBoardRaw string `json:"game_board_raw,omitempty"`
}
