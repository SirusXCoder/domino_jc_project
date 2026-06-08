package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/pagination"
)

// ListPlayerMatchHistory returns a cursor-paginated slice of match history for a player.
// Uses composite (end_time DESC, match_id DESC) ordering for stable pagination.
func (r *dgraphGameRepository) ListPlayerMatchHistory(
	ctx context.Context,
	playerID string,
	limit int,
	afterCursor string,
) (*models.MatchHistoryPage, error) {
	if playerID == "" {
		return nil, fmt.Errorf("player ID cannot be empty")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	fetchLimit := limit + 1

	var query string
	vars := map[string]string{
		"$playerID": playerID,
		"$limit":    fmt.Sprintf("%d", fetchLimit),
	}

	if afterCursor == "" {
		query = `
		query matchHistory($playerID: string, $limit: int) {
			player(func: eq(player.id, $playerID), first: 1) {
				player.id
				matches: ~match_record.players @facets(role, placement_weight) (
					orderdesc: match_record.end_time,
					orderdesc: match_record.match_id,
					first: $limit
				) {
					match_record.match_id
					match_record.end_time
					match_record.end_reason
					match_record.winner
					match_record.scores
					match_record.elo_deltas
				}
			}
		}`
	} else {
		cursor, err := pagination.Decode(afterCursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}
		vars["$cursorTime"] = cursor.EndTime.UTC().Format(time.RFC3339Nano)
		vars["$cursorMatchID"] = cursor.MatchID

		query = `
		query matchHistory($playerID: string, $limit: int, $cursorTime: string, $cursorMatchID: string) {
			player(func: eq(player.id, $playerID), first: 1) {
				player.id
				matches: ~match_record.players @facets(role, placement_weight) @filter(
					lt(match_record.end_time, $cursorTime) OR
					(eq(match_record.end_time, $cursorTime) AND lt(match_record.match_id, $cursorMatchID))
				) (
					orderdesc: match_record.end_time,
					orderdesc: match_record.match_id,
					first: $limit
				) {
					match_record.match_id
					match_record.end_time
					match_record.end_reason
					match_record.winner
					match_record.scores
					match_record.elo_deltas
				}
			}
		}`
	}

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("query match history: %w", err)
	}

	var result struct {
		Player []struct {
			PlayerID string               `json:"player.id"`
			Matches  []models.MatchRecord `json:"matches"`
		} `json:"player"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("parse match history response: %w", err)
	}

	if len(result.Player) == 0 {
		return nil, fmt.Errorf("player not found: %q", playerID)
	}

	row := result.Player[0]
	hasMore := len(row.Matches) > limit
	if hasMore {
		row.Matches = row.Matches[:limit]
	}

	entries := make([]models.MatchHistoryEntry, 0, len(row.Matches))
	for _, match := range row.Matches {
		entry, err := matchHistoryEntryFromRecord(playerID, match)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	page := &models.MatchHistoryPage{
		PlayerID: playerID,
		Matches:  entries,
		Page:     pagination.PageMeta{HasMore: hasMore},
	}

	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		token, err := pagination.Encode(pagination.MatchHistoryCursor{
			EndTime: last.EndTime,
			MatchID: last.MatchID,
		})
		if err != nil {
			return nil, fmt.Errorf("encode next cursor: %w", err)
		}
		page.Page.NextCursor = token
	}

	return page, nil
}

// GetPlayerProfile loads career fields in a single hop without match history.
func (r *dgraphGameRepository) GetPlayerProfile(ctx context.Context, playerID string) (*models.PlayerCareerStats, error) {
	if playerID == "" {
		return nil, fmt.Errorf("player ID cannot be empty")
	}

	const query = `
	query profile($playerID: string) {
		player(func: eq(player.id, $playerID), first: 1) {
			player.id
			player.username
			player.elo
			player.peak_elo
			player.matches_played
			player.wins
			player.losses
			player.last_match_at
		}
	}`

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	vars := map[string]string{"$playerID": playerID}
	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("query player profile: %w", err)
	}

	var result struct {
		Player []models.Player `json:"player"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("parse player profile response: %w", err)
	}

	if len(result.Player) == 0 {
		return nil, fmt.Errorf("player not found: %q", playerID)
	}

	p := result.Player[0]
	stats := &models.PlayerCareerStats{
		PlayerID:      p.PlayerID,
		Username:      p.Username,
		ELO:           normalizeELO(p.ELO),
		PeakELO:       normalizeELO(p.PeakELO),
		MatchesPlayed: p.MatchesPlayed,
		Wins:          p.Wins,
		Losses:        p.Losses,
		WinRate:       models.WinRate(p.Wins, p.MatchesPlayed),
	}
	if !p.LastMatchAt.IsZero() {
		last := p.LastMatchAt
		stats.LastMatchAt = &last
	}
	return stats, nil
}

// GetLedgerState loads match metadata and participant edge facets in one query.
func (r *dgraphGameRepository) GetLedgerState(ctx context.Context, matchID string) (*models.LedgerState, error) {
	if matchID == "" {
		return nil, fmt.Errorf("match ID cannot be empty")
	}

	const query = `
	query ledgerState($matchID: string) {
		match(func: eq(match_record.match_id, $matchID), first: 1) {
			match_record.match_id
			match_record.winner
			match_record.end_time
			match_record.end_reason
			match_record.ratings_applied
			match_record.elo_deltas
			participants: match_record.players @facets(role, placement_weight) {
				player.id
			}
		}
	}`

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	vars := map[string]string{"$matchID": matchID}
	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("query ledger state: %w", err)
	}

	var result struct {
		Matches []struct {
			MatchID        string         `json:"match_record.match_id"`
			Winner         string         `json:"match_record.winner"`
			EndTime        time.Time      `json:"match_record.end_time"`
			EndReason      string         `json:"match_record.end_reason"`
			RatingsApplied bool           `json:"match_record.ratings_applied"`
			ELodDeltas     string         `json:"match_record.elo_deltas"`
			Participants   []models.Player `json:"participants"`
		} `json:"match"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("parse ledger state response: %w", err)
	}

	if len(result.Matches) == 0 {
		return nil, fmt.Errorf("match record not found: %q", matchID)
	}

	row := result.Matches[0]
	state := &models.LedgerState{
		MatchID:        row.MatchID,
		WinnerID:       row.Winner,
		EndReason:      row.EndReason,
		RatingsApplied: row.RatingsApplied,
	}
	if !row.EndTime.IsZero() {
		state.EndTime = row.EndTime.UTC().Format(time.RFC3339Nano)
	}

	participants := make([]models.MatchParticipantRole, 0, len(row.Participants))
	for _, p := range row.Participants {
		participants = append(participants, models.MatchParticipantRole{
			PlayerID: p.PlayerID,
		})
	}
	state.Participants = participants

	if row.ELodDeltas != "" {
		var deltas models.ELODeltas
		if err := json.Unmarshal([]byte(row.ELodDeltas), &deltas); err != nil {
			return nil, fmt.Errorf("parse elo deltas: %w", err)
		}
		state.ELODeltas = deltas
	}

	return state, nil
}

// GetMatchWithPlayers loads a ledger row and participant career fields in one round trip.
func (r *dgraphGameRepository) GetMatchWithPlayers(ctx context.Context, matchID string) (*models.MatchRecord, []models.Player, error) {
	if matchID == "" {
		return nil, nil, fmt.Errorf("match ID cannot be empty")
	}

	const query = `
	query matchWithPlayers($matchID: string) {
		match(func: eq(match_record.match_id, $matchID), first: 1) {
			uid
			match_record.match_id
			match_record.winner
			match_record.scores
			match_record.end_time
			match_record.end_reason
			match_record.elo_deltas
			match_record.ratings_applied
			match_record.players @facets(role, placement_weight) {
				player.id
				player.elo
				player.peak_elo
				player.matches_played
				player.wins
				player.losses
				player.last_match_at
			}
		}
	}`

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	vars := map[string]string{"$matchID": matchID}
	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, nil, fmt.Errorf("query match with players: %w", err)
	}

	var result struct {
		Matches []models.MatchRecord `json:"match"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, nil, fmt.Errorf("parse match with players response: %w", err)
	}

	if len(result.Matches) == 0 {
		return nil, nil, fmt.Errorf("match record not found: %q", matchID)
	}

	record := result.Matches[0]
	return &record, record.Players, nil
}

func matchHistoryEntryFromRecord(playerID string, match models.MatchRecord) (models.MatchHistoryEntry, error) {
	scores, err := match.ParseScores()
	if err != nil {
		return models.MatchHistoryEntry{}, fmt.Errorf("parse scores for match %q: %w", match.MatchID, err)
	}
	deltas, err := match.ParseELODeltas()
	if err != nil {
		return models.MatchHistoryEntry{}, fmt.Errorf("parse elo deltas for match %q: %w", match.MatchID, err)
	}

	return models.MatchHistoryEntry{
		MatchID:   match.MatchID,
		EndTime:   match.EndTime,
		EndReason: match.EndReason,
		WinnerID:  match.Winner,
		Scores:    scores,
		Won:       match.Winner == playerID,
		ELODelta:  deltas[playerID],
	}, nil
}
