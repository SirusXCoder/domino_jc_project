package engine

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/rating"
	"domino_jc_project/pkg/repository"
)

// RatingWorker applies ELO adjustments after immutable ledger rows are persisted.
type RatingWorker struct {
	stats repository.StatsRepository
}

// NewRatingWorker constructs a post-ledger rating processor.
func NewRatingWorker(stats repository.StatsRepository) *RatingWorker {
	return &RatingWorker{stats: stats}
}

// ProcessMatch computes and persists career stats for a completed ledger row.
func (w *RatingWorker) ProcessMatch(ctx context.Context, record models.MatchRecord) error {
	stored, err := w.stats.GetMatchRecord(ctx, record.MatchID)
	if err != nil {
		return err
	}
	if stored.RatingsApplied {
		return nil
	}

	playerIDs := make([]string, 0, len(stored.Players))
	for _, player := range stored.Players {
		if player.PlayerID != "" {
			playerIDs = append(playerIDs, player.PlayerID)
		}
	}
	if len(playerIDs) == 0 {
		return nil
	}

	existing, err := w.stats.GetPlayersByIDs(ctx, playerIDs)
	if err != nil {
		return err
	}

	byID := make(map[string]models.Player, len(playerIDs))
	for _, player := range existing {
		byID[player.PlayerID] = player
	}

	participants := make([]rating.Participant, 0, len(playerIDs))
	for _, playerID := range playerIDs {
		player := byID[playerID]
		elo := player.ELO
		if elo <= 0 {
			elo = models.DefaultELO
		}
		participants = append(participants, rating.Participant{
			PlayerID:      playerID,
			ELO:           elo,
			MatchesPlayed: player.MatchesPlayed,
		})
	}

	winnerID := stored.Winner
	if winnerID == "" {
		return nil
	}

	results := rating.ComputeMatch(participants, winnerID)
	if len(results) == 0 {
		return nil
	}

	now := time.Now().UTC()
	updates := make([]models.Player, 0, len(results))
	deltas := make(models.ELODeltas, len(results))

	for _, result := range results {
		player := byID[result.PlayerID]
		peak := player.PeakELO
		if peak <= 0 {
			peak = result.OldELO
		}
		if result.NewELO > peak {
			peak = result.NewELO
		}

		wins := player.Wins
		losses := player.Losses
		if result.Won {
			wins++
		} else {
			losses++
		}

		updates = append(updates, models.Player{
			PlayerID:      result.PlayerID,
			ELO:           result.NewELO,
			PeakELO:       peak,
			MatchesPlayed: result.MatchesPlayed,
			Wins:          wins,
			Losses:        losses,
			LastMatchAt:   now,
		})
		deltas[result.PlayerID] = roundDelta(result.Delta)
	}

	if err := w.stats.UpdatePlayerCareers(ctx, updates); err != nil {
		return err
	}

	matchUID := stored.UID
	if matchUID == "" {
		matchUID = record.UID
	}
	if matchUID == "" {
		log.Printf("rating: match_id=%s persisted without uid; skipping ratings stamp", record.MatchID)
		return nil
	}

	if err := w.stats.ApplyMatchRatings(ctx, matchUID, deltas); err != nil {
		return err
	}

	deltasJSON, _ := json.Marshal(deltas)
	log.Printf("rating: applied elo deltas match_id=%s deltas=%s", record.MatchID, string(deltasJSON))
	return nil
}

func roundDelta(value float64) float64 {
	return float64(int(value*100+0.5)) / 100
}
