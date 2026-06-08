package engine

import (
	"context"
	"testing"

	"domino_jc_project/pkg/models"
)

type stubStatsRepo struct {
	match         *models.MatchRecord
	players       []models.Player
	updated       []models.Player
	appliedUID    string
	applied       models.ELODeltas
	matchWithErr  error
}

func (s *stubStatsRepo) GetPlayersByIDs(_ context.Context, _ []string) ([]models.Player, error) {
	return s.players, nil
}

func (s *stubStatsRepo) UpdatePlayerCareers(_ context.Context, players []models.Player) error {
	s.updated = players
	return nil
}

func (s *stubStatsRepo) ListLeaderboard(_ context.Context, _ int) ([]models.LeaderboardEntry, error) {
	return nil, nil
}

func (s *stubStatsRepo) GetPlayerCareer(_ context.Context, _ string, _ int) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (s *stubStatsRepo) GetMatchRecord(_ context.Context, _ string) (*models.MatchRecord, error) {
	return s.match, nil
}

func (s *stubStatsRepo) ApplyMatchRatings(_ context.Context, matchUID string, deltas models.ELODeltas) error {
	s.appliedUID = matchUID
	s.applied = deltas
	return nil
}

func (s *stubStatsRepo) GetPlayerProfile(_ context.Context, _ string) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (s *stubStatsRepo) ListPlayerMatchHistory(_ context.Context, _ string, _ int, _ string) (*models.MatchHistoryPage, error) {
	return nil, nil
}

func (s *stubStatsRepo) GetLedgerState(_ context.Context, _ string) (*models.LedgerState, error) {
	return nil, nil
}

func (s *stubStatsRepo) GetMatchWithPlayers(_ context.Context, _ string) (*models.MatchRecord, []models.Player, error) {
	if s.matchWithErr != nil {
		return nil, nil, s.matchWithErr
	}
	if s.match == nil {
		return nil, nil, nil
	}
	players := s.players
	if len(players) == 0 {
		players = s.match.Players
	}
	return s.match, players, nil
}

func TestRatingWorker_ProcessMatchUpdatesCareers(t *testing.T) {
	repo := &stubStatsRepo{
		match: &models.MatchRecord{
			UID:            "0xmatch",
			MatchID:        "s1",
			Winner:         "p1",
			RatingsApplied: false,
			Players: []models.Player{
				{PlayerID: "p1"},
				{PlayerID: "p2"},
			},
		},
		players: []models.Player{
			{PlayerID: "p1", ELO: 1500, MatchesPlayed: 4, Wins: 2, Losses: 2, PeakELO: 1510},
			{PlayerID: "p2", ELO: 1500, MatchesPlayed: 4, Wins: 2, Losses: 2, PeakELO: 1505},
		},
	}

	worker := NewRatingWorker(repo)
	if err := worker.ProcessMatch(context.Background(), models.MatchRecord{MatchID: "s1"}); err != nil {
		t.Fatalf("ProcessMatch: %v", err)
	}

	if len(repo.updated) != 2 {
		t.Fatalf("updated players = %d, want 2", len(repo.updated))
	}
	if repo.appliedUID != "0xmatch" {
		t.Fatalf("applied uid = %q, want 0xmatch", repo.appliedUID)
	}
	if repo.applied["p1"] <= 0 {
		t.Fatalf("winner delta = %f, want positive", repo.applied["p1"])
	}
	if repo.applied["p2"] >= 0 {
		t.Fatalf("loser delta = %f, want negative", repo.applied["p2"])
	}
}

func TestRatingWorker_ProcessMatchSkipsWhenAlreadyApplied(t *testing.T) {
	repo := &stubStatsRepo{
		match: &models.MatchRecord{
			MatchID:        "s1",
			RatingsApplied: true,
		},
	}

	worker := NewRatingWorker(repo)
	if err := worker.ProcessMatch(context.Background(), models.MatchRecord{MatchID: "s1"}); err != nil {
		t.Fatalf("ProcessMatch: %v", err)
	}
	if len(repo.updated) != 0 {
		t.Fatal("expected no career updates for already-rated match")
	}
}
