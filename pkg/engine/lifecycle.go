package engine

import (
	"context"
	"fmt"

	"domino_jc_project/pkg/models"
)

// MatchTerminator broadcasts final match frames and enqueues immutable ledger snapshots.
type MatchTerminator interface {
	TerminateMatch(ctx context.Context, sessionID string, outcome *models.MatchOutcome, session *models.GameSession)
}

// SetMatchTerminator wires the WebSocket hub (or test double) for match completion fan-out.
func (m *GameManager) SetMatchTerminator(terminator MatchTerminator) {
	m.matchTerminator = terminator
}

// processGameTurn runs ProcessGameTurn under the session mutex, applies conditional
// gRPC upsert checks, and triggers match termination when evaluation succeeds.
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

	if m.matchTerminator != nil {
		m.matchTerminator.TerminateMatch(ctx, sessionID, outcome, session)
	}

	return nil
}
// terminates the match when evaluation succeeds.
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

	return m.finalizeMatchLocked(ctx, sessionID, entry.session, outcome)
}
