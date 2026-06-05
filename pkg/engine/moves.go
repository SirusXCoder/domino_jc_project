package engine

import (
	"context"
	"fmt"

	"domino_jc_project/pkg/models"
)

// ApplyPlayTile places a domino tile on the board for the active player.
// On success the updated session state is persisted before returning.
func (m *GameManager) ApplyPlayTile(
	ctx context.Context,
	sessionID, playerID string,
	tile models.DominoTile,
	playAtLeft bool,
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return false, fmt.Errorf("session %q not found", sessionID)
	}

	success, err := session.PlayTile(playerID, tile, playAtLeft)
	if err != nil || !success {
		return success, err
	}

	if err := m.persistState(ctx, session); err != nil {
		return false, err
	}

	return true, nil
}

// ApplyDrawFromBoneyard draws the top tile from the boneyard into the active player's hand.
// On success the updated session state is persisted before returning.
func (m *GameManager) ApplyDrawFromBoneyard(
	ctx context.Context,
	sessionID, playerID string,
) (*models.DominoTile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	drawnTile, err := session.DrawFromBoneyard(playerID)
	if err != nil {
		return nil, err
	}

	if err := m.persistState(ctx, session); err != nil {
		return nil, err
	}

	return drawnTile, nil
}

// ApplyPassTurn marks the active player's turn as passed and rotates to the next player.
// On success the updated session state is persisted before returning.
func (m *GameManager) ApplyPassTurn(
	ctx context.Context,
	sessionID, playerID string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}

	if err := session.PassTurn(playerID); err != nil {
		return err
	}

	return m.persistState(ctx, session)
}
