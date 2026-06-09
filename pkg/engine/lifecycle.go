package engine

import (
	"context"
	"fmt"
	"log"

	"domino_jc_project/pkg/game"
	"domino_jc_project/pkg/models"
)

// processGameTurn runs ProcessGameTurn under the session mutex, applies conditional
// gRPC upsert checks, and publishes integration events without blocking on consumers.
func (m *GameManager) processGameTurn(
	ctx context.Context,
	sessionID string,
	action models.TurnAction,
) (*models.TurnResult, error) {
	entry, err := m.lookupSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	result, err := entry.session.ProcessGameTurn(action)
	if err != nil {
		return nil, err
	}

	if shouldPersistSession(entry.session, result) {
		if err := m.persistState(ctx, entry.session); err != nil {
			return nil, err
		}
	}

	if result != nil && result.MatchEnded {
		if err := m.finalizeMatchLocked(ctx, sessionID, entry.session, result.Outcome); err != nil {
			return result, err
		}
	}

	m.publishTurnEvents(ctx, entry.session, result)
	return result, nil
}

func shouldPersistSession(session *models.GameSession, result *models.TurnResult) bool {
	if result == nil || !result.NeedsPersist {
		return false
	}
	if session.Status == models.SessionStatusCompleted {
		return true
	}
	if session.Status == models.SessionStatusActive {
		return true
	}
	return session.Status == models.SessionStatusWaiting
}

func (m *GameManager) finalizeMatchLocked(
	ctx context.Context,
	sessionID string,
	session *models.GameSession,
	outcome *models.MatchOutcome,
) error {
	if outcome == nil {
		return fmt.Errorf("cannot finalize session %q without a match outcome", sessionID)
	}

	if !session.MutationsLocked {
		session.MutationsLocked = true
		session.Status = models.SessionStatusCompleted
	}

	if err := m.persistState(ctx, session); err != nil {
		return fmt.Errorf("persist completed session %q: %w", sessionID, err)
	}

	return nil
}

func (m *GameManager) publishTurnEvents(
	ctx context.Context,
	session *models.GameSession,
	result *models.TurnResult,
) {
	if m.gameEngine == nil {
		return
	}
	if err := m.gameEngine.Tick(ctx, game.TickRequest{
		Session: session,
		Result:  result,
	}); err != nil {
		log.Printf("game engine publish: %v", err)
	}
}

// evaluateAndMaybeFinalize re-evaluates match completion and publishes broker events
// when a terminal state is reached outside the normal turn pipeline.
func (m *GameManager) evaluateAndMaybeFinalize(ctx context.Context, sessionID string) error {
	entry, err := m.lookupSession(ctx, sessionID)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.session.MutationsLocked {
		return nil
	}

	outcome, ended := models.EvaluateMatch(entry.session)
	if !ended {
		return nil
	}

	if err := m.finalizeMatchLocked(ctx, sessionID, entry.session, outcome); err != nil {
		return err
	}

	m.publishTurnEvents(ctx, entry.session, &models.TurnResult{
		MatchEnded: true,
		Outcome:    outcome,
	})
	return nil
}
