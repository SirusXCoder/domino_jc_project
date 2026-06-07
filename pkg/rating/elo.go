package rating

import (
	"math"

	"domino_jc_project/pkg/models"
)

const (
	// DefaultELO mirrors models.DefaultELO for players entering the rating pool.
	DefaultELO = models.DefaultELO

	baseKFactor       = 32.0
	experiencedKFactor = 16.0
	experiencedMatchThreshold = 30
)

// Participant carries the pre-match rating state for one competitor.
type Participant struct {
	PlayerID      string
	ELO           float64
	MatchesPlayed int
}

// Result captures the post-match rating change for one player.
type Result struct {
	PlayerID   string
	OldELO     float64
	NewELO     float64
	Delta      float64
	Won        bool
	MatchesPlayed int
}

// ComputeMatch applies multiplayer ELO adjustments for a finished match.
//
// WinnerID is the player.id of the victor. All other participants are treated
// as losses. Ratings use the standard expected-score formula averaged across
// opponents, which collapses to classic two-player ELO for head-to-head games.
func ComputeMatch(participants []Participant, winnerID string) []Result {
	if len(participants) == 0 || winnerID == "" {
		return nil
	}

	byID := make(map[string]Participant, len(participants))
	for _, p := range participants {
		if p.PlayerID == "" {
			continue
		}
		if p.ELO <= 0 {
			p.ELO = DefaultELO
		}
		byID[p.PlayerID] = p
	}

	n := len(byID)
	if n == 0 {
		return nil
	}

	results := make([]Result, 0, n)
	for playerID, player := range byID {
		actual := 0.0
		if playerID == winnerID {
			actual = 1.0
		}

		expected := expectedScore(player.ELO, byID, playerID)
		k := kFactor(player.MatchesPlayed)
		delta := k * (actual - expected)
		newELO := player.ELO + delta

		results = append(results, Result{
			PlayerID:      playerID,
			OldELO:        player.ELO,
			NewELO:        newELO,
			Delta:         delta,
			Won:           playerID == winnerID,
			MatchesPlayed: player.MatchesPlayed + 1,
		})
	}

	return results
}

func expectedScore(rating float64, field map[string]Participant, selfID string) float64 {
	if len(field) <= 1 {
		return 0.5
	}

	total := 0.0
	opponents := 0
	for id, opponent := range field {
		if id == selfID {
			continue
		}
		total += 1.0 / (1.0 + math.Pow(10, (opponent.ELO-rating)/400.0))
		opponents++
	}
	return total / float64(opponents)
}

func kFactor(matchesPlayed int) float64 {
	if matchesPlayed >= experiencedMatchThreshold {
		return experiencedKFactor
	}
	return baseKFactor
}

