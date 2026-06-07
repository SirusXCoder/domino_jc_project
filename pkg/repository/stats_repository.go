package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"domino_jc_project/pkg/models"

	"github.com/dgraph-io/dgo/v210/protos/api"
)

// GetPlayersByIDs loads career fields for the given application player IDs.
func (r *dgraphGameRepository) GetPlayersByIDs(ctx context.Context, playerIDs []string) ([]models.Player, error) {
	if len(playerIDs) == 0 {
		return nil, nil
	}

	const query = `
	query playersByID($ids: string) {
		players(func: eq(player.id, $ids)) {
			uid
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

	idsJSON, err := json.Marshal(playerIDs)
	if err != nil {
		return nil, fmt.Errorf("marshal player ids: %w", err)
	}

	vars := map[string]string{"$ids": string(idsJSON)}
	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("query players by id: %w", err)
	}

	var result struct {
		Players []models.Player `json:"players"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("parse players response: %w", err)
	}

	return result.Players, nil
}

// UpdatePlayerCareers upserts career stat mutations keyed by player.id.
func (r *dgraphGameRepository) UpdatePlayerCareers(ctx context.Context, players []models.Player) error {
	if len(players) == 0 {
		return nil
	}

	for i := range players {
		players[i].DType = []string{models.TypePlayer}
	}

	payload, err := json.Marshal(players)
	if err != nil {
		return fmt.Errorf("marshal player careers: %w", err)
	}

	mu := &api.Mutation{CommitNow: true, SetJson: payload}
	txn := r.dg.NewTxn()
	defer txn.Discard(ctx)

	if _, err := txn.Mutate(ctx, mu); err != nil {
		return fmt.Errorf("update player careers: %w", err)
	}
	return nil
}

// ListLeaderboard returns the top players ordered by current ELO descending.
func (r *dgraphGameRepository) ListLeaderboard(ctx context.Context, limit int) ([]models.LeaderboardEntry, error) {
	if limit <= 0 {
		limit = 50
	}

	query := fmt.Sprintf(`
	query {
		leaderboard(func: has(player.elo), orderdesc: player.elo, first: %d) {
			player.id
			player.username
			player.elo
			player.peak_elo
			player.matches_played
			player.wins
			player.losses
		}
	}`, limit)

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	resp, err := txn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query leaderboard: %w", err)
	}

	var result struct {
		Leaderboard []models.Player `json:"leaderboard"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("parse leaderboard response: %w", err)
	}

	entries := make([]models.LeaderboardEntry, 0, len(result.Leaderboard))
	for i, p := range result.Leaderboard {
		entries = append(entries, models.LeaderboardEntry{
			Rank:          i + 1,
			PlayerID:      p.PlayerID,
			Username:      p.Username,
			ELO:           normalizeELO(p.ELO),
			PeakELO:       normalizeELO(p.PeakELO),
			MatchesPlayed: p.MatchesPlayed,
			Wins:          p.Wins,
			Losses:        p.Losses,
			WinRate:       models.WinRate(p.Wins, p.MatchesPlayed),
		})
	}
	return entries, nil
}

// GetPlayerCareer loads aggregate stats and recent immutable ledger rows.
func (r *dgraphGameRepository) GetPlayerCareer(ctx context.Context, playerID string, recentLimit int) (*models.PlayerCareerStats, error) {
	if playerID == "" {
		return nil, fmt.Errorf("player ID cannot be empty")
	}
	if recentLimit <= 0 {
		recentLimit = 20
	}

	query := fmt.Sprintf(`
	query career($playerID: string) {
		player(func: eq(player.id, $playerID), first: 1) {
			player.id
			player.username
			player.elo
			player.peak_elo
			player.matches_played
			player.wins
			player.losses
			player.last_match_at
			recent: ~match_record.players(orderdesc: match_record.end_time, first: %d) {
				uid
				match_record.match_id
				match_record.end_time
				match_record.end_reason
				match_record.winner
				match_record.scores
				match_record.elo_deltas
			}
		}
	}`, recentLimit)

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	vars := map[string]string{"$playerID": playerID}
	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("query player career: %w", err)
	}

	var result struct {
		Player []struct {
			models.Player
			Recent []models.MatchRecord `json:"recent"`
		} `json:"player"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("parse player career response: %w", err)
	}

	if len(result.Player) == 0 {
		return nil, fmt.Errorf("player not found: %q", playerID)
	}

	row := result.Player[0]
	stats := &models.PlayerCareerStats{
		PlayerID:      row.PlayerID,
		Username:      row.Username,
		ELO:           normalizeELO(row.ELO),
		PeakELO:       normalizeELO(row.PeakELO),
		MatchesPlayed: row.MatchesPlayed,
		Wins:          row.Wins,
		Losses:        row.Losses,
		WinRate:       models.WinRate(row.Wins, row.MatchesPlayed),
	}
	if !row.LastMatchAt.IsZero() {
		last := row.LastMatchAt
		stats.LastMatchAt = &last
	}

	stats.RecentMatches = make([]models.MatchHistoryEntry, 0, len(row.Recent))
	for _, match := range row.Recent {
		scores, err := match.ParseScores()
		if err != nil {
			return nil, fmt.Errorf("parse scores for match %q: %w", match.MatchID, err)
		}
		deltas, err := match.ParseELODeltas()
		if err != nil {
			return nil, fmt.Errorf("parse elo deltas for match %q: %w", match.MatchID, err)
		}

		entry := models.MatchHistoryEntry{
			MatchID:   match.MatchID,
			EndTime:   match.EndTime,
			EndReason: match.EndReason,
			WinnerID:  match.Winner,
			Scores:    scores,
			Won:       match.Winner == playerID,
			ELODelta:  deltas[playerID],
		}
		stats.RecentMatches = append(stats.RecentMatches, entry)
	}

	return stats, nil
}

// GetMatchRecord loads a persisted ledger row by application match ID.
func (r *dgraphGameRepository) GetMatchRecord(ctx context.Context, matchID string) (*models.MatchRecord, error) {
	if matchID == "" {
		return nil, fmt.Errorf("match ID cannot be empty")
	}

	const query = `
	query matchRecord($matchID: string) {
		match(func: eq(match_record.match_id, $matchID), first: 1) {
			uid
			match_record.match_id
			match_record.winner
			match_record.scores
			match_record.end_time
			match_record.end_reason
			match_record.elo_deltas
			match_record.ratings_applied
			match_record.players {
				player.id
			}
		}
	}`

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	vars := map[string]string{"$matchID": matchID}
	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("query match record: %w", err)
	}

	var result struct {
		Matches []models.MatchRecord `json:"match"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("parse match record response: %w", err)
	}

	if len(result.Matches) == 0 {
		return nil, fmt.Errorf("match record not found: %q", matchID)
	}

	record := result.Matches[0]
	return &record, nil
}

// ApplyMatchRatings stamps computed deltas onto the ledger row and marks it processed.
func (r *dgraphGameRepository) ApplyMatchRatings(ctx context.Context, matchUID string, deltas models.ELODeltas) error {
	if matchUID == "" {
		return fmt.Errorf("match UID cannot be empty")
	}

	deltasJSON, err := json.Marshal(deltas)
	if err != nil {
		return fmt.Errorf("marshal elo deltas: %w", err)
	}

	node := map[string]interface{}{
		"uid":                          matchUID,
		"match_record.elo_deltas":      string(deltasJSON),
		"match_record.ratings_applied":   true,
	}

	payload, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("marshal rating stamp: %w", err)
	}

	mu := &api.Mutation{CommitNow: true, SetJson: payload}
	txn := r.dg.NewTxn()
	defer txn.Discard(ctx)

	if _, err := txn.Mutate(ctx, mu); err != nil {
		return fmt.Errorf("apply match ratings: %w", err)
	}
	return nil
}

func normalizeELO(value float64) float64 {
	if value <= 0 {
		return models.DefaultELO
	}
	return value
}
