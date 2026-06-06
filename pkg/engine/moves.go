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
	result, err := m.processGameTurn(ctx, sessionID, models.TurnAction{
		Kind:       models.TurnKindPlayTile,
		PlayerID:   playerID,
		Tile:       tile,
		PlayAtLeft: playAtLeft,
	})
	if err != nil {
		return false, err
	}
	return result.Applied, nil
}

// ApplyDrawFromBoneyard draws the top tile from the boneyard into the active player's hand.
// On success the updated session state is persisted before returning.
func (m *GameManager) ApplyDrawFromBoneyard(
	ctx context.Context,
	sessionID, playerID string,
) (*models.DominoTile, error) {
	result, err := m.processGameTurn(ctx, sessionID, models.TurnAction{
		Kind:     models.TurnKindDraw,
		PlayerID: playerID,
	})
	if err != nil {
		return nil, err
	}
	return result.DrawnTile, nil
}

// ApplyPassTurn marks the active player's turn as passed and rotates to the next player.
// On success the updated session state is persisted before returning.
func (m *GameManager) ApplyPassTurn(
	ctx context.Context,
	sessionID, playerID string,
) error {
	_, err := m.processGameTurn(ctx, sessionID, models.TurnAction{
		Kind:     models.TurnKindPass,
		PlayerID: playerID,
	})
	return err
}
