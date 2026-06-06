package models

// EvaluateMatch inspects session state and returns an outcome when the game is over.
func EvaluateMatch(session *GameSession) (*MatchOutcome, bool) {
	for _, hand := range session.Hands {
		if hand.IsAbandoned {
			continue
		}
		if len(hand.Tiles) == 0 {
			return &MatchOutcome{
				MatchID:  session.SessionID,
				WinnerID: hand.PlayerID,
				Scores:   computeHandScores(session),
				Reason:   MatchEndEmptyHand,
			}, true
		}
	}

	active := activeNonAbandonedPlayers(session)
	if len(active) == 1 {
		return &MatchOutcome{
			MatchID:  session.SessionID,
			WinnerID: active[0],
			Scores:   computeHandScores(session),
			Reason:   MatchEndForfeit,
		}, true
	}

	if len(session.Boneyard) == 0 && allActivePlayersPassed(session) {
		return &MatchOutcome{
			MatchID:  session.SessionID,
			WinnerID: lowestPipPlayer(session),
			Scores:   computeHandScores(session),
			Reason:   MatchEndBlocked,
		}, true
	}

	return nil, false
}

func computeHandScores(session *GameSession) Scores {
	scores := make(Scores, len(session.Hands))
	for _, hand := range session.Hands {
		total := 0
		for _, tile := range hand.Tiles {
			total += tile.ValueLeft + tile.ValueRight
		}
		scores[hand.PlayerID] = total
	}
	return scores
}

func activeNonAbandonedPlayers(session *GameSession) []string {
	out := make([]string, 0, len(session.Hands))
	for _, hand := range session.Hands {
		if !hand.IsAbandoned {
			out = append(out, hand.PlayerID)
		}
	}
	return out
}

func allActivePlayersPassed(session *GameSession) bool {
	active := 0
	passed := 0
	for _, hand := range session.Hands {
		if hand.IsAbandoned {
			continue
		}
		active++
		if hand.HasPassed {
			passed++
		}
	}
	return active > 0 && passed == active
}

func lowestPipPlayer(session *GameSession) string {
	scores := computeHandScores(session)
	winner := ""
	lowest := -1
	for _, hand := range session.Hands {
		if hand.IsAbandoned {
			continue
		}
		pips := scores[hand.PlayerID]
		if winner == "" || pips < lowest {
			winner = hand.PlayerID
			lowest = pips
		}
	}
	return winner
}
