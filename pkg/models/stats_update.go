package models

// PlayerStatsUpdate is the domain payload emitted after rating recalculation.
type PlayerStatsUpdate struct {
	SessionID     string  `json:"session_id"`
	MatchID       string  `json:"match_id"`
	PlayerID      string  `json:"player_id"`
	ELO           float64 `json:"elo"`
	PeakELO       float64 `json:"peak_elo"`
	MatchesPlayed int     `json:"matches_played"`
	Wins          int     `json:"wins"`
	Losses        int     `json:"losses"`
	ELODelta      float64 `json:"elo_delta"`
	Won           bool    `json:"won"`
}
