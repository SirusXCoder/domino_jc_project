package engine

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/rating"
	"domino_jc_project/pkg/repository"
	"domino_jc_project/pkg/resilience"
	"domino_jc_project/pkg/ws"
)

// RatingWorker applies ELO adjustments after immutable ledger rows are persisted.
type RatingWorker struct {
	stats       repository.StatsRepository
	broadcaster ws.StatsBroadcaster
	breaker     *resilience.Breaker
	retry       resilience.RetryConfig
}

// RatingWorkerOption configures optional rating worker behavior.
type RatingWorkerOption func(*RatingWorker)

// WithStatsBroadcaster attaches a WebSocket dispatcher for live stat updates.
func WithStatsBroadcaster(b ws.StatsBroadcaster) RatingWorkerOption {
	return func(w *RatingWorker) {
		w.broadcaster = b
	}
}

// WithRatingBreaker attaches a circuit breaker around rating persistence.
func WithRatingBreaker(b *resilience.Breaker) RatingWorkerOption {
	return func(w *RatingWorker) {
		w.breaker = b
	}
}

// WithRatingRetry configures retry behavior for transient rating errors.
func WithRatingRetry(cfg resilience.RetryConfig) RatingWorkerOption {
	return func(w *RatingWorker) {
		w.retry = cfg
	}
}

// NewRatingWorker constructs a post-ledger rating processor.
func NewRatingWorker(stats repository.StatsRepository, opts ...RatingWorkerOption) *RatingWorker {
	w := &RatingWorker{
		stats: stats,
		retry: resilience.DefaultRetryConfig(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// BreakerState exposes the current circuit breaker state for observability/tests.
func (w *RatingWorker) BreakerState() resilience.State {
	if w.breaker == nil {
		return resilience.StateClosed
	}
	return w.breaker.State()
}

// ProcessMatch computes and persists career stats for a completed ledger row.
func (w *RatingWorker) ProcessMatch(ctx context.Context, record models.MatchRecord) error {
	return w.runProtected(ctx, func() error {
		return w.processMatch(ctx, record)
	})
}

func (w *RatingWorker) runProtected(ctx context.Context, fn func() error) error {
	if w.breaker == nil {
		return resilience.Retry(ctx, w.retry, fn)
	}
	var err error
	_, execErr := w.breaker.Execute(func() (interface{}, error) {
		err = resilience.Retry(ctx, w.retry, fn)
		return nil, err
	})
	if execErr != nil {
		return execErr
	}
	return err
}

func (w *RatingWorker) processMatch(ctx context.Context, record models.MatchRecord) error {
	stored, players, err := w.stats.GetMatchWithPlayers(ctx, record.MatchID)
	if err != nil {
		return err
	}
	if stored.RatingsApplied {
		return nil
	}

	playerIDs := make([]string, 0, len(players))
	byID := make(map[string]models.Player, len(players))
	for _, player := range players {
		if player.PlayerID == "" {
			continue
		}
		playerIDs = append(playerIDs, player.PlayerID)
		byID[player.PlayerID] = player
	}
	if len(playerIDs) == 0 {
		return nil
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
	statUpdates := make([]models.PlayerStatsUpdate, 0, len(results))

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

		delta := roundDelta(result.Delta)
		updates = append(updates, models.Player{
			PlayerID:      result.PlayerID,
			ELO:           result.NewELO,
			PeakELO:       peak,
			MatchesPlayed: result.MatchesPlayed,
			Wins:          wins,
			Losses:        losses,
			LastMatchAt:   now,
		})
		deltas[result.PlayerID] = delta
		statUpdates = append(statUpdates, models.PlayerStatsUpdate{
			SessionID:     record.MatchID,
			MatchID:       record.MatchID,
			PlayerID:      result.PlayerID,
			ELO:           result.NewELO,
			PeakELO:       peak,
			MatchesPlayed: result.MatchesPlayed,
			Wins:          wins,
			Losses:        losses,
			ELODelta:      delta,
			Won:           result.Won,
		})
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

	if w.broadcaster != nil {
		w.broadcaster.BroadcastPlayerStatsUpdate(record.MatchID, statUpdates)
	}

	deltasJSON, _ := json.Marshal(deltas)
	log.Printf("rating: applied elo deltas match_id=%s deltas=%s", record.MatchID, string(deltasJSON))
	return nil
}

func roundDelta(value float64) float64 {
	return float64(int(value*100+0.5)) / 100
}
