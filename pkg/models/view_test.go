package models

import "testing"

func TestGameSession_ViewForPlayerRedactsOpponentHands(t *testing.T) {
	session := NewGameSession("match-1", []string{"p1", "p2"})
	session.Status = SessionStatusActive
	session.Hands[0].Tiles = []DominoTile{NewTile(1, 2), NewTile(3, 4)}
	session.Hands[1].Tiles = []DominoTile{NewTile(5, 6), NewTile(2, 2), NewTile(0, 1)}
	session.Boneyard = []DominoTile{NewTile(4, 4), NewTile(6, 6)}
	session.GameBoard = []DominoTile{NewTile(3, 3)}

	view := session.ViewForPlayer("p1")
	if view == nil {
		t.Fatal("expected non-nil view")
	}
	if len(view.Hands) != 2 {
		t.Fatalf("hands = %d, want 2", len(view.Hands))
	}
	if len(view.Hands[0].Tiles) != 2 {
		t.Fatalf("own hand tiles = %d, want 2", len(view.Hands[0].Tiles))
	}
	if len(view.Hands[1].Tiles) != 0 {
		t.Fatalf("opponent tiles should be hidden, got %d", len(view.Hands[1].Tiles))
	}
	if view.Hands[1].RedactedTileCount != 3 {
		t.Fatalf("opponent tile_count = %d, want 3", view.Hands[1].RedactedTileCount)
	}
	if len(view.Boneyard) != 0 {
		t.Fatalf("boneyard should be hidden during active play, got %d tiles", len(view.Boneyard))
	}
	if view.BoneyardCount != 2 {
		t.Fatalf("boneyard_count = %d, want 2", view.BoneyardCount)
	}
	if len(view.GameBoard) != 1 {
		t.Fatalf("board tiles = %d, want 1", len(view.GameBoard))
	}
}

func TestGameSession_ViewForPlayerRevealsBoneyardWhenCompleted(t *testing.T) {
	session := NewGameSession("match-2", []string{"p1", "p2"})
	session.Status = SessionStatusCompleted
	session.Hands[0].Tiles = []DominoTile{NewTile(1, 1)}
	session.Hands[1].Tiles = []DominoTile{NewTile(2, 3)}
	session.Boneyard = []DominoTile{NewTile(4, 5)}

	view := session.ViewForPlayer("p2")
	if len(view.Hands[1].Tiles) != 1 {
		t.Fatalf("own hand tiles = %d, want 1", len(view.Hands[1].Tiles))
	}
	if len(view.Hands[0].Tiles) != 0 || view.Hands[0].RedactedTileCount != 1 {
		t.Fatalf("opponent hand should expose count only: tiles=%d count=%d", len(view.Hands[0].Tiles), view.Hands[0].RedactedTileCount)
	}
	if len(view.Boneyard) != 1 {
		t.Fatalf("completed match should reveal boneyard, got %d tiles", len(view.Boneyard))
	}
}
