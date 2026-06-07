package rating

import (
	"math"
	"testing"
)

func TestComputeMatch_TwoPlayerEqualRatings(t *testing.T) {
	results := ComputeMatch([]Participant{
		{PlayerID: "p1", ELO: 1500},
		{PlayerID: "p2", ELO: 1500},
	}, "p1")

	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}

	var winner, loser Result
	for _, r := range results {
		if r.PlayerID == "p1" {
			winner = r
		} else {
			loser = r
		}
	}

	if !winner.Won {
		t.Fatal("p1 should be marked as winner")
	}
	if loser.Won {
		t.Fatal("p2 should be marked as loser")
	}
	if math.Abs(winner.Delta+loser.Delta) > 0.01 {
		t.Fatalf("zero-sum violation: winner=%f loser=%f", winner.Delta, loser.Delta)
	}
	if winner.NewELO <= winner.OldELO {
		t.Fatalf("winner elo should increase: old=%f new=%f", winner.OldELO, winner.NewELO)
	}
	if loser.NewELO >= loser.OldELO {
		t.Fatalf("loser elo should decrease: old=%f new=%f", loser.OldELO, loser.NewELO)
	}
}

func TestComputeMatch_ThreePlayerProducesTwoLosers(t *testing.T) {
	results := ComputeMatch([]Participant{
		{PlayerID: "p1", ELO: 1500},
		{PlayerID: "p2", ELO: 1500},
		{PlayerID: "p3", ELO: 1500},
	}, "p1")

	losers := 0
	for _, r := range results {
		if !r.Won {
			losers++
			if r.Delta >= 0 {
				t.Fatalf("loser %s should lose rating, delta=%f", r.PlayerID, r.Delta)
			}
		}
	}
	if losers != 2 {
		t.Fatalf("losers = %d, want 2", losers)
	}
}

func TestComputeMatch_ExperiencedPlayerUsesLowerK(t *testing.T) {
	novice := ComputeMatch([]Participant{
		{PlayerID: "p1", ELO: 1500, MatchesPlayed: 0},
		{PlayerID: "p2", ELO: 1500, MatchesPlayed: 0},
	}, "p1")
	veteran := ComputeMatch([]Participant{
		{PlayerID: "p1", ELO: 1500, MatchesPlayed: 40},
		{PlayerID: "p2", ELO: 1500, MatchesPlayed: 40},
	}, "p1")

	var noviceWin, veteranWin Result
	for _, r := range novice {
		if r.PlayerID == "p1" {
			noviceWin = r
		}
	}
	for _, r := range veteran {
		if r.PlayerID == "p1" {
			veteranWin = r
		}
	}

	if veteranWin.Delta >= noviceWin.Delta {
		t.Fatalf("veteran delta (%f) should be smaller than novice (%f)", veteranWin.Delta, noviceWin.Delta)
	}
}
