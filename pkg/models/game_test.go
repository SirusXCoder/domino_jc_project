package models

import (
	"testing"
)

func TestDominoEngineLifecycle(t *testing.T) {
	// 1. Initialization
	playerIDs := []string{"player-1", "player-2"}
	session := NewGameSession("test-session-123", playerIDs)

	if len(session.Hands) != 2 {
		t.Fatalf("expected 2 player hands, got %d", len(session.Hands))
	}

	// 2. Deck Generation & Shuffle
	session.GenerateStandardDeck()
	if len(session.Boneyard) != 28 {
		t.Fatalf("expected a fresh deck of 28 tiles, got %d", len(session.Boneyard))
	}

	if err := session.ShuffleBoneyard(); err != nil {
		t.Fatalf("failed to shuffle boneyard: %v", err)
	}

	// 3. Dealing Hands
	tilesPerPlayer := 7
	if err := session.DealHands(tilesPerPlayer); err != nil {
		t.Fatalf("failed to deal hands: %v", err)
	}

	if len(session.Hands[0].Tiles) != 7 || len(session.Hands[1].Tiles) != 7 {
		t.Errorf("expected each player to have 7 tiles, got %d and %d", len(session.Hands[0].Tiles), len(session.Hands[1].Tiles))
	}

	expectedBoneyard := 28 - (2 * tilesPerPlayer)
	if len(session.Boneyard) != expectedBoneyard {
		t.Errorf("expected boneyard to have %d tiles left, got %d", expectedBoneyard, len(session.Boneyard))
	}

	// 4. Set Initial Turn
	session.CurrentTurn = "player-1"

	// 5. Simulate First Play (Any tile is valid on empty board)
	p1Hand := &session.Hands[0]
	if len(p1Hand.Tiles) == 0 {
		t.Fatal("player-1 has an empty hand unexpectedly")
	}
	firstTile := p1Hand.Tiles[0]

	success, err := session.PlayTile("player-1", firstTile, true)
	if !success || err != nil {
		t.Fatalf("first tile play failed: %v", err)
	}

	if len(session.GameBoard) != 1 {
		t.Errorf("expected game board to have 1 tile, got %d", len(session.GameBoard))
	}

	if session.LeftOpenValue != firstTile.ValueLeft || session.RightOpenValue != firstTile.ValueRight {
		t.Errorf("board ends improperly cached. Got Left: %d, Right: %d", session.LeftOpenValue, session.RightOpenValue)
	}

	// 6. Turn Rotation
	session.RotateTurn()
	if session.CurrentTurn != "player-2" {
		t.Errorf("expected turn to rotate to player-2, got %s", session.CurrentTurn)
	}

	// 7. Verify Move Validation (Force an impossible tile play)
	impossibleTile := DominoTile{ID: "99-99", ValueLeft: 99, ValueRight: 99}
	session.Hands[1].Tiles = append(session.Hands[1].Tiles, impossibleTile)

	_, err = session.PlayTile("player-2", impossibleTile, true)
	if err == nil {
		t.Error("expected engine to reject an illegal tile play, but it passed")
	}
}
