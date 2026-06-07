package repository

import (
	"context"

	"domino_jc_project/pkg/models"
)

// StatsRepository provides read models and career mutations for post-game analytics.
type StatsRepository interface {
	GetPlayersByIDs(ctx context.Context, playerIDs []string) ([]models.Player, error)
	UpdatePlayerCareers(ctx context.Context, players []models.Player) error
	ListLeaderboard(ctx context.Context, limit int) ([]models.LeaderboardEntry, error)
	GetPlayerCareer(ctx context.Context, playerID string, recentLimit int) (*models.PlayerCareerStats, error)
	GetMatchRecord(ctx context.Context, matchID string) (*models.MatchRecord, error)
	ApplyMatchRatings(ctx context.Context, matchUID string, deltas models.ELODeltas) error
}
