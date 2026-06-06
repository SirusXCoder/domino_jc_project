package models

import "testing"

func TestProcessGameTurn_BlockedGame(t *testing.T) {
	session := NewGameSession("s1", []string{"p1", "p2"})
	session.Status = SessionStatusActive
	session.CurrentTurn = "p1"
	session.Hands[0].Tiles = []DominoTile{NewTile(1, 2)}
	session.Hands[1].Tiles = []DominoTile{NewTile(3, 4)}
	session.Hands[0].HasPassed = true
	session.Hands[1].HasPassed = true

	result, err := session.ProcessGameTurn(TurnAction{Kind: TurnKindPass, PlayerID: "p1"})
	if err != nil {
		t.Fatalf("ProcessGameTurn: %v", err)
	}
	if !result.MatchEnded {
		t.Fatal("expected blocked game to end")
	}
	if result.Outcome.Reason != MatchEndBlocked {
		t.Fatalf("reason = %q, want %q", result.Outcome.Reason, MatchEndBlocked)
	}
	if !session.MutationsLocked {
		t.Fatal("expected session to be locked after match end")
	}
}

func TestProcessGameTurn_RejectsWhenLocked(t *testing.T) {
	session := NewGameSession("s1", []string{"p1"})
	session.MutationsLocked = true
	session.Status = SessionStatusCompleted

	_, err := session.ProcessGameTurn(TurnAction{Kind: TurnKindPass, PlayerID: "p1"})
	if err != ErrMutationsLocked {
		t.Fatalf("err = %v, want ErrMutationsLocked", err)
	}
}
