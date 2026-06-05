package models

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
)

// NewPlayerHand returns an empty hand for the given player.
func NewPlayerHand(playerID string) PlayerHand {
	return PlayerHand{
		PlayerID: playerID,
		Tiles:    []DominoTile{},
	}
}

// TileCount returns the number of tiles currently held.
func (h PlayerHand) TileCount() int {
	return len(h.Tiles)
}

// HasTile reports whether the hand contains a tile with the given ID.
func (h PlayerHand) HasTile(tileID string) bool {
	for _, tile := range h.Tiles {
		if tile.ID == tileID {
			return true
		}
	}
	return false
}

// MatchesValue reports whether either end of the tile equals value.
func (t DominoTile) MatchesValue(value int) bool {
	return t.ValueLeft == value || t.ValueRight == value
}

// OtherValue returns the opposite end when one side matches value.
// Returns value and false when value does not match either end.
func (t DominoTile) OtherValue(value int) (int, bool) {
	switch value {
	case t.ValueLeft:
		return t.ValueRight, true
	case t.ValueRight:
		return t.ValueLeft, true
	default:
		return value, false
	}
}

// BoardEnds tracks the open pip values on the left and right ends of the chain.
type BoardEnds struct {
	OpenLeft  int `json:"open_left"`
	OpenRight int `json:"open_right"`
}

// IsOpen reports whether the board has been started (at least one tile played).
func (e BoardEnds) IsOpen() bool {
	return e.OpenLeft >= 0 && e.OpenRight >= 0
}

// MatchesPlay reports whether value matches either open end of the board.
func (e BoardEnds) MatchesPlay(value int) bool {
	return e.OpenLeft == value || e.OpenRight == value
}

// StandardDoubleSixDeck returns the 28 tiles of a double-six domino set.
func StandardDoubleSixDeck() []DominoTile {
	deck := make([]DominoTile, 0, 28)
	for i := 0; i <= 6; i++ {
		for j := i; j <= 6; j++ {
			deck = append(deck, NewTile(i, j))
		}
	}
	return deck
}

// NewGameSession initializes a fresh game session state.
func NewGameSession(sessionID string, playerUIDs []string) *GameSession {
	session := &GameSession{
		SessionID:      sessionID,
		Status:         "waiting",
		Players:        playerUIDs,
		Hands:          make([]PlayerHand, 0, len(playerUIDs)),
		Boneyard:       make([]DominoTile, 0),
		GameBoard:      make([]DominoTile, 0),
		LeftOpenValue:  -1, // -1 signifies an empty board end
		RightOpenValue: -1,
	}

	for _, uid := range playerUIDs {
		session.Hands = append(session.Hands, PlayerHand{
			PlayerID: uid,
			Tiles:    make([]DominoTile, 0),
		})
	}

	return session
}

// GenerateStandardDeck builds a standard Double-Six set (28 tiles).
func (s *GameSession) GenerateStandardDeck() {
	s.Boneyard = StandardDoubleSixDeck()
}

// ShuffleBoneyard uses cryptographically secure random numbers to shuffle the pool.
func (s *GameSession) ShuffleBoneyard() error {
	n := len(s.Boneyard)
	for i := n - 1; i > 0; i-- {
		bg, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return err
		}
		j := bg.Int64()
		s.Boneyard[i], s.Boneyard[j] = s.Boneyard[j], s.Boneyard[i]
	}
	return nil
}

// BoardEnds returns the current open values on the game board ends.
func (s *GameSession) BoardEnds() BoardEnds {
	return BoardEnds{
		OpenLeft:  s.LeftOpenValue,
		OpenRight: s.RightOpenValue,
	}
}

// SetBoardEnds updates the O(1) move-validation cache trackers.
func (s *GameSession) SetBoardEnds(left, right int) {
	s.LeftOpenValue = left
	s.RightOpenValue = right
}

// FlushPlayState marshals game slices into raw JSON strings so Dgraph can store them as flat string predicates.
func (s *GameSession) FlushPlayState() {
	if len(s.Boneyard) > 0 {
		if bytes, err := json.Marshal(s.Boneyard); err == nil {
			s.BoneyardRaw = string(bytes)
		}
	} else {
		s.BoneyardRaw = "[]"
	}

	if len(s.GameBoard) > 0 {
		if bytes, err := json.Marshal(s.GameBoard); err == nil {
			s.GameBoardRaw = string(bytes)
		}
	} else {
		s.GameBoardRaw = "[]"
	}
}

// HydratePlayState unmarshals Dgraph's raw string fields back into rich Go structural slices.
func (s *GameSession) HydratePlayState() error {
	if s.BoneyardRaw != "" {
		if err := json.Unmarshal([]byte(s.BoneyardRaw), &s.Boneyard); err != nil {
			return fmt.Errorf("unmarshal boneyard_raw: %w", err)
		}
	}

	if s.GameBoardRaw != "" {
		if err := json.Unmarshal([]byte(s.GameBoardRaw), &s.GameBoard); err != nil {
			return fmt.Errorf("unmarshal game_board_raw: %w", err)
		}
	}

	return nil
}

// CanPlay checks if a given tile can legally be placed on either the Left or Right end of the board.
func (s *GameSession) CanPlay(tile DominoTile, playAtLeft bool) bool {
	// If the board is completely empty, any tile is valid
	if s.LeftOpenValue == -1 && s.RightOpenValue == -1 {
		return true
	}

	if playAtLeft {
		return tile.ValueLeft == s.LeftOpenValue || tile.ValueRight == s.LeftOpenValue
	}
	return tile.ValueLeft == s.RightOpenValue || tile.ValueRight == s.RightOpenValue
}

// PlayTile attempts to place a tile from a player's hand onto the board.
// It handles turn verification, validation, hand extraction, and board end caching.
func (s *GameSession) PlayTile(playerID string, tile DominoTile, playAtLeft bool) (bool, error) {
	if s.CurrentTurn != playerID {
		return false, fmt.Errorf("it is not player %s's turn", playerID)
	}

	if !s.CanPlay(tile, playAtLeft) {
		return false, fmt.Errorf("illegal move: tile %s cannot be played there", tile.String())
	}

	// Find player's hand
	var targetHand *PlayerHand
	for i := range s.Hands {
		if s.Hands[i].PlayerID == playerID {
			targetHand = &s.Hands[i]
			break
		}
	}
	if targetHand == nil {
		return false, fmt.Errorf("player hand not found for ID: %s", playerID)
	}

	// Remove tile from player's hand
	tileFound := false
	for i, t := range targetHand.Tiles {
		if t.ID == tile.ID {
			targetHand.Tiles = append(targetHand.Tiles[:i], targetHand.Tiles[i+1:]...)
			tileFound = true
			break
		}
	}
	if !tileFound {
		return false, fmt.Errorf("tile %s is not in player's hand", tile.String())
	}

	// Handle board placement and orient/flip the tile values if necessary
	if s.LeftOpenValue == -1 && s.RightOpenValue == -1 {
		// First tile on the board sets both open ends
		s.GameBoard = append(s.GameBoard, tile)
		s.SetBoardEnds(tile.ValueLeft, tile.ValueRight)
	} else if playAtLeft {
		// Playing on the Left side: the side matching LeftOpenValue must face inward (right)
		if tile.ValueRight != s.LeftOpenValue {
			// Flip tile orientation
			tile.ValueLeft, tile.ValueRight = tile.ValueRight, tile.ValueLeft
		}
		s.GameBoard = append([]DominoTile{tile}, s.GameBoard...)
		s.LeftOpenValue = tile.ValueLeft
	} else {
		// Playing on the Right side: the side matching RightOpenValue must face inward (left)
		if tile.ValueLeft != s.RightOpenValue {
			// Flip tile orientation
			tile.ValueLeft, tile.ValueRight = tile.ValueRight, tile.ValueLeft
		}
		s.GameBoard = append(s.GameBoard, tile)
		s.RightOpenValue = tile.ValueRight
	}

	// Reset their pass flag since they just successfully made a move
	targetHand.HasPassed = false

	return true, nil
}

// DealHands distributes a set number of starting tiles to every player from the Boneyard.
func (s *GameSession) DealHands(tilesPerPlayer int) error {
	totalNeeded := len(s.Players) * tilesPerPlayer
	if len(s.Boneyard) < totalNeeded {
		return fmt.Errorf("not enough tiles in the boneyard to deal %d tiles to %d players", tilesPerPlayer, len(s.Players))
	}

	for i := range s.Hands {
		// Slice off a chunk of tiles from the boneyard for the player
		s.Hands[i].Tiles = append(s.Hands[i].Tiles, s.Boneyard[:tilesPerPlayer]...)
		s.Boneyard = s.Boneyard[tilesPerPlayer:]
	}

	return nil
}

// DrawFromBoneyard pulls the top tile from the remaining pool and appends it to the player's hand.
func (s *GameSession) DrawFromBoneyard(playerID string) (*DominoTile, error) {
	if s.CurrentTurn != playerID {
		return nil, fmt.Errorf("it is not player %s's turn to draw", playerID)
	}

	if len(s.Boneyard) == 0 {
		return nil, fmt.Errorf("the boneyard is empty; cannot draw")
	}

	// Pop the first tile from the boneyard
	drawnTile := s.Boneyard[0]
	s.Boneyard = s.Boneyard[1:]

	// Locate player hand and append
	for i := range s.Hands {
		if s.Hands[i].PlayerID == playerID {
			s.Hands[i].Tiles = append(s.Hands[i].Tiles, drawnTile)
			// Reset pass flag since they just acquired a new action potential
			s.Hands[i].HasPassed = false
			return &drawnTile, nil
		}
	}

	return nil, fmt.Errorf("player hand not found for ID: %s", playerID)
}

// PassTurn records that the active player cannot or will not play, then advances turn.
func (s *GameSession) PassTurn(playerID string) error {
	if s.CurrentTurn != playerID {
		return fmt.Errorf("it is not player %s's turn to pass", playerID)
	}

	var targetHand *PlayerHand
	for i := range s.Hands {
		if s.Hands[i].PlayerID == playerID {
			targetHand = &s.Hands[i]
			break
		}
	}
	if targetHand == nil {
		return fmt.Errorf("player hand not found for ID: %s", playerID)
	}

	targetHand.HasPassed = true
	s.RotateTurn()
	return nil
}

// RotateTurn cycles the active turn state to the next structural player index.
func (s *GameSession) RotateTurn() {
	if len(s.Players) == 0 {
		return
	}

	currentIndex := -1
	for i, pID := range s.Players {
		if pID == s.CurrentTurn {
			currentIndex = i
			break
		}
	}

	// If current turn is blank or not found, default to the first player
	if currentIndex == -1 {
		s.CurrentTurn = s.Players[0]
		return
	}

	// Move to the next index contextually using modulo arithmetic for wrap-around loops
	nextIndex := (currentIndex + 1) % len(s.Players)
	s.CurrentTurn = s.Players[nextIndex]
}
