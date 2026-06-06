package models

import "errors"

// TurnKind identifies the type of player action processed in a turn.
type TurnKind string

const (
	TurnKindPlayTile TurnKind = "PLAY_TILE"
	TurnKindDraw     TurnKind = "DRAW"
	TurnKindPass     TurnKind = "PASS"
)

// Match end reason constants recorded on the immutable ledger snapshot.
const (
	MatchEndEmptyHand = "empty_hand"
	MatchEndBlocked   = "blocked"
	MatchEndForfeit   = "forfeit"
)

// ErrMutationsLocked is returned when a client attempts to mutate a completed session.
var ErrMutationsLocked = errors.New("session mutations are locked")

// TurnAction is the normalized input for GameSession.ProcessGameTurn.
type TurnAction struct {
	Kind       TurnKind
	PlayerID   string
	Tile       DominoTile
	PlayAtLeft bool
}

// MatchOutcome captures the evaluated result of a finished match.
type MatchOutcome struct {
	MatchID  string `json:"match_id"`
	WinnerID string `json:"winner_id"`
	Scores   Scores `json:"scores"`
	Reason   string `json:"reason"`
}

// TurnResult reports whether state changed, persistence is required, and if the match ended.
type TurnResult struct {
	Applied      bool
	DrawnTile    *DominoTile
	MatchEnded   bool
	Outcome      *MatchOutcome
	NeedsPersist bool
}
