package engine

import (
	"context"

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
	entry, err := m.lookupSession(ctx, sessionID)
	if err != nil {
		return false, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	success, err := entry.session.PlayTile(playerID, tile, playAtLeft)
	if err != nil || !success {
		return success, err
	}

	if err := m.persistState(ctx, entry.session); err != nil {
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
	var drawnTile *models.DominoTile
	err := m.withSessionWrite(ctx, sessionID, func(session *models.GameSession) error {
		tile, drawErr := session.DrawFromBoneyard(playerID)
		if drawErr != nil {
			return drawErr
		}
		drawnTile = tile
		return nil
	})
	if err != nil {
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
	return m.withSessionWrite(ctx, sessionID, func(session *models.GameSession) error {
		return session.PassTurn(playerID)
	})
}
