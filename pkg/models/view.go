package models

// ViewForPlayer returns a cheat-proof copy of the session visible to viewerPlayerID.
// The requesting player's hand is fully visible; opponent hands expose only tile counts.
// While the match is in progress, boneyard tile values are hidden.
func (s *GameSession) ViewForPlayer(viewerPlayerID string) *GameSession {
	if s == nil {
		return nil
	}

	view := *s
	view.Hands = make([]PlayerHand, len(s.Hands))
	for i, hand := range s.Hands {
		view.Hands[i] = PlayerHand{
			PlayerID:    hand.PlayerID,
			HasPassed:   hand.HasPassed,
			IsReady:     hand.IsReady,
			IsAbandoned: hand.IsAbandoned,
		}
		if hand.PlayerID == viewerPlayerID {
			view.Hands[i].Tiles = append([]DominoTile(nil), hand.Tiles...)
		} else {
			view.Hands[i].RedactedTileCount = len(hand.Tiles)
		}
	}

	if s.Status == SessionStatusCompleted {
		view.Boneyard = append([]DominoTile(nil), s.Boneyard...)
	} else {
		view.BoneyardCount = len(s.Boneyard)
		view.Boneyard = nil
	}

	view.GameBoard = append([]DominoTile(nil), s.GameBoard...)
	view.Players = append([]string(nil), s.Players...)
	view.BoneyardRaw = ""
	view.GameBoardRaw = ""
	return &view
}
