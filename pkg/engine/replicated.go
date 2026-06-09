package engine

import (
	"context"
	"fmt"

	"domino_jc_project/pkg/models"
)

// ReplicatedMatchSetup configures session creation from a committed START_MATCH command.
type ReplicatedMatchSetup struct {
	PlayerUIDs   []string
	SetupGame    bool
	TilesPerHand int
}

// ApplyReplicatedStartMatch creates or updates a session from a committed log entry.
func (m *GameManager) ApplyReplicatedStartMatch(
	ctx context.Context,
	sessionID string,
	setup ReplicatedMatchSetup,
) (*models.GameSession, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("session id is required")
	}
	if len(setup.PlayerUIDs) == 0 {
		return nil, fmt.Errorf("at least one player is required for session %q", sessionID)
	}

	tilesPerHand := setup.TilesPerHand
	if tilesPerHand <= 0 {
		tilesPerHand = 7
	}

	entry, err := m.lookupSession(ctx, sessionID)
	if err != nil {
		if _, createErr := m.CreateSession(sessionID, setup.PlayerUIDs); createErr != nil {
			return nil, createErr
		}
		entry, err = m.lookupSession(ctx, sessionID)
		if err != nil {
			return nil, err
		}
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	session := entry.session
	if len(session.Players) == 0 {
		session.Players = append([]string(nil), setup.PlayerUIDs...)
	}
	if len(session.Hands) == 0 {
		for _, uid := range setup.PlayerUIDs {
			session.Hands = append(session.Hands, models.NewPlayerHand(uid))
		}
	}

	if setup.SetupGame {
		session.GenerateStandardDeck()
		if err := session.ShuffleBoneyard(); err != nil {
			return nil, fmt.Errorf("shuffle boneyard for session %q: %w", sessionID, err)
		}
		if err := session.DealHands(tilesPerHand); err != nil {
			return nil, fmt.Errorf("deal hands for session %q: %w", sessionID, err)
		}
		session.Status = models.SessionStatusActive
		if session.CurrentTurn == "" && len(session.Players) > 0 {
			session.CurrentTurn = session.Players[0]
		}
	} else if session.Status == "" {
		session.Status = models.SessionStatusWaiting
	}

	if err := m.persistState(ctx, session); err != nil {
		return nil, err
	}
	return session, nil
}

// ApplyReplicatedTurn applies a committed player turn to the in-memory session.
func (m *GameManager) ApplyReplicatedTurn(
	ctx context.Context,
	sessionID string,
	action models.TurnAction,
) (*models.TurnResult, error) {
	return m.processGameTurn(ctx, sessionID, action)
}

// ApplyReplicatedLedgerBalance persists a committed player career balance update.
func (m *GameManager) ApplyReplicatedLedgerBalance(
	ctx context.Context,
	update models.PlayerStatsUpdate,
) (*models.PlayerStatsUpdate, error) {
	if update.PlayerID == "" {
		return nil, fmt.Errorf("player_id is required")
	}

	player := models.Player{
		PlayerID:      update.PlayerID,
		ELO:           update.ELO,
		PeakELO:       update.PeakELO,
		MatchesPlayed: update.MatchesPlayed,
		Wins:          update.Wins,
		Losses:        update.Losses,
	}
	if player.PeakELO <= 0 {
		player.PeakELO = update.ELO
	}

	if err := m.repo.UpdatePlayerCareers(ctx, []models.Player{player}); err != nil {
		return nil, fmt.Errorf("update player career for %q: %w", update.PlayerID, err)
	}

	m.mu.Lock()
	if m.ledgerProfiles == nil {
		m.ledgerProfiles = make(map[string]models.PlayerStatsUpdate)
	}
	m.ledgerProfiles[update.PlayerID] = update
	m.mu.Unlock()

	return &update, nil
}

// SnapshotSessions returns a deep copy of all in-memory sessions for FSM snapshots.
func (m *GameManager) SnapshotSessions() map[string]*models.GameSession {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]*models.GameSession, len(m.sessions))
	for id, entry := range m.sessions {
		entry.mu.Lock()
		copySession := *entry.session
		entry.mu.Unlock()
		out[id] = &copySession
	}
	return out
}

// RestoreSessions replaces the in-memory session registry from an FSM snapshot.
func (m *GameManager) RestoreSessions(sessions map[string]*models.GameSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessions = make(map[string]*sessionEntry, len(sessions))
	m.recoveredIDs = make(map[string]bool)
	for id, session := range sessions {
		if session == nil || id == "" {
			continue
		}
		copySession := *session
		m.sessions[id] = &sessionEntry{session: &copySession}
	}
	return nil
}

// LedgerProfiles returns committed in-memory ledger profile updates.
func (m *GameManager) LedgerProfiles() map[string]models.PlayerStatsUpdate {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]models.PlayerStatsUpdate, len(m.ledgerProfiles))
	for id, profile := range m.ledgerProfiles {
		out[id] = profile
	}
	return out
}

// RestoreLedgerProfiles replaces committed ledger profile cache from a snapshot.
func (m *GameManager) RestoreLedgerProfiles(profiles map[string]models.PlayerStatsUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ledgerProfiles = make(map[string]models.PlayerStatsUpdate, len(profiles))
	for id, profile := range profiles {
		m.ledgerProfiles[id] = profile
	}
}
