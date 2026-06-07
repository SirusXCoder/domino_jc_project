package models

import "time"

// LeaderboardEntry is a ranked row on the global skill leaderboard.
type LeaderboardEntry struct {
	Rank          int     `json:"rank"`
	PlayerID      string  `json:"player_id"`
	Username      string  `json:"username,omitempty"`
	ELO           float64 `json:"elo"`
	PeakELO       float64 `json:"peak_elo"`
	MatchesPlayed int     `json:"matches_played"`
	Wins          int     `json:"wins"`
	Losses        int     `json:"losses"`
	WinRate       float64 `json:"win_rate"`
}

// PlayerCareerStats aggregates a player's competitive profile and recent form.
type PlayerCareerStats struct {
	PlayerID      string              `json:"player_id"`
	Username      string              `json:"username,omitempty"`
	ELO           float64             `json:"elo"`
	PeakELO       float64             `json:"peak_elo"`
	MatchesPlayed int                 `json:"matches_played"`
	Wins          int                 `json:"wins"`
	Losses        int                 `json:"losses"`
	WinRate       float64             `json:"win_rate"`
	LastMatchAt   *time.Time          `json:"last_match_at,omitempty"`
	RecentMatches []MatchHistoryEntry `json:"recent_matches,omitempty"`
}

// MatchHistoryEntry is a condensed ledger row for career analytics views.
type MatchHistoryEntry struct {
	MatchID   string    `json:"match_id"`
	EndTime   time.Time `json:"end_time"`
	EndReason string    `json:"end_reason,omitempty"`
	WinnerID  string    `json:"winner_id"`
	Scores    Scores    `json:"scores"`
	Won       bool      `json:"won"`
	ELODelta  float64   `json:"elo_delta,omitempty"`
}

// WinRate returns wins / matches, or 0 when no matches have been played.
func WinRate(wins, matches int) float64 {
	if matches <= 0 {
		return 0
	}
	return float64(wins) / float64(matches)
}
