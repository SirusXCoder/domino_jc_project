package engine

import (
	"context"
	"fmt"

	"domino_jc_project/pkg/models"
)

// HandlePlayerAbandoned marks a player as forfeited after the reconnection grace
// period expires. The in-memory session is retained; if it was the abandoned
// player's turn, play advances automatically before persisting.
func (m *GameManager) HandlePlayerAbandoned(ctx context.Context, sessionID, playerID string) error {
	return m.withSessionWrite(ctx, sessionID, func(session *models.GameSession) error {
		var handFound bool
		for i := range session.Hands {
			if session.Hands[i].PlayerID == playerID {
				session.Hands[i].IsAbandoned = true
				handFound = true
				break
			}
		}
		if !handFound {
			return fmt.Errorf("player %q not found in session %q", playerID, sessionID)
		}

		if session.CurrentTurn == playerID {
			if err := session.PassTurn(playerID); err != nil {
				session.RotateTurn()
			}
		}

		return nil
	})
}
