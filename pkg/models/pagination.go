package models

import "domino_jc_project/pkg/pagination"

// MatchHistoryPage is a cursor-paginated slice of ledger rows for a player.
type MatchHistoryPage struct {
	PlayerID string                 `json:"player_id"`
	Matches  []MatchHistoryEntry    `json:"matches"`
	Page     pagination.PageMeta    `json:"page"`
}

// MatchParticipantRole describes a player's role in a completed match (edge facet).
type MatchParticipantRole struct {
	PlayerID         string  `json:"player_id"`
	Role             string  `json:"role,omitempty"`
	PlacementWeight  float64 `json:"placement_weight,omitempty"`
}

// LedgerState summarizes an immutable match row with participant facet metadata.
type LedgerState struct {
	MatchID      string                 `json:"match_id"`
	WinnerID     string                 `json:"winner_id"`
	EndTime      string                 `json:"end_time,omitempty"`
	EndReason    string                 `json:"end_reason,omitempty"`
	RatingsApplied bool                 `json:"ratings_applied"`
	Participants []MatchParticipantRole `json:"participants"`
	ELODeltas    ELODeltas              `json:"elo_deltas,omitempty"`
}
