package consensus

import (
	"encoding/json"
	"fmt"

	"domino_jc_project/pkg/models"
)

const (
	OpStartMatch    = "START_MATCH"
	OpApplyTurn     = "APPLY_TURN"
	OpLedgerBalance = "LEDGER_BALANCE"
)

// StartMatchPayload configures session creation and optional game setup.
type StartMatchPayload struct {
	PlayerUIDs   []string `json:"player_uids"`
	SetupGame    bool     `json:"setup_game,omitempty"`
	TilesPerHand int      `json:"tiles_per_hand,omitempty"`
}

// ApplyTurnPayload carries a normalized player turn action.
type ApplyTurnPayload struct {
	Kind       string            `json:"kind"`
	PlayerID   string            `json:"player_id"`
	Tile       models.DominoTile `json:"tile,omitempty"`
	PlayAtLeft bool              `json:"play_at_left,omitempty"`
}

// LedgerBalancePayload records a committed player career balance adjustment.
type LedgerBalancePayload struct {
	PlayerID      string  `json:"player_id"`
	ELO           float64 `json:"elo"`
	PeakELO       float64 `json:"peak_elo"`
	ELODelta      float64 `json:"elo_delta"`
	MatchesPlayed int     `json:"matches_played"`
	Wins          int     `json:"wins"`
	Losses        int     `json:"losses"`
}

// ApplyResult confirms a committed command or carries structured evaluation data.
type ApplyResult struct {
	OK       bool                 `json:"ok"`
	Op       string               `json:"op"`
	MatchID  string               `json:"match_id"`
	Applied  bool                 `json:"applied,omitempty"`
	Turn     *models.TurnResult   `json:"turn,omitempty"`
	Session  *models.GameSession  `json:"session,omitempty"`
	Ledger   *models.PlayerStatsUpdate `json:"ledger,omitempty"`
}

// ToTurnAction converts the wire payload into the domain turn action struct.
func (p ApplyTurnPayload) ToTurnAction() models.TurnAction {
	return models.TurnAction{
		Kind:       models.TurnKind(p.Kind),
		PlayerID:   p.PlayerID,
		Tile:       p.Tile,
		PlayAtLeft: p.PlayAtLeft,
	}
}

// ToStatsUpdate converts the wire payload into the domain stats update struct.
func (p LedgerBalancePayload) ToStatsUpdate(matchID string) models.PlayerStatsUpdate {
	return models.PlayerStatsUpdate{
		SessionID:     matchID,
		MatchID:       matchID,
		PlayerID:      p.PlayerID,
		ELO:           p.ELO,
		PeakELO:       p.PeakELO,
		ELODelta:      p.ELODelta,
		MatchesPlayed: p.MatchesPlayed,
		Wins:          p.Wins,
		Losses:        p.Losses,
	}
}

// DecodeCommand unmarshals a replicated log entry into a Command.
func DecodeCommand(logEntry []byte) (Command, error) {
	var cmd Command
	if err := json.Unmarshal(logEntry, &cmd); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}
	if cmd.Op == "" {
		return Command{}, fmt.Errorf("command op is required")
	}
	if cmd.MatchID == "" {
		return Command{}, fmt.Errorf("command match_id is required")
	}
	return cmd, nil
}

// DecodeStartMatchPayload unmarshals the START_MATCH payload bytes.
func DecodeStartMatchPayload(payload []byte) (StartMatchPayload, error) {
	if len(payload) == 0 {
		return StartMatchPayload{}, nil
	}
	var out StartMatchPayload
	if err := json.Unmarshal(payload, &out); err != nil {
		return StartMatchPayload{}, fmt.Errorf("decode start match payload: %w", err)
	}
	return out, nil
}

// DecodeApplyTurnPayload unmarshals the APPLY_TURN payload bytes.
func DecodeApplyTurnPayload(payload []byte) (ApplyTurnPayload, error) {
	if len(payload) == 0 {
		return ApplyTurnPayload{}, fmt.Errorf("apply turn payload is required")
	}
	var out ApplyTurnPayload
	if err := json.Unmarshal(payload, &out); err != nil {
		return ApplyTurnPayload{}, fmt.Errorf("decode apply turn payload: %w", err)
	}
	if out.Kind == "" {
		return ApplyTurnPayload{}, fmt.Errorf("turn kind is required")
	}
	if out.PlayerID == "" {
		return ApplyTurnPayload{}, fmt.Errorf("player_id is required")
	}
	return out, nil
}

// DecodeLedgerBalancePayload unmarshals the LEDGER_BALANCE payload bytes.
func DecodeLedgerBalancePayload(payload []byte) (LedgerBalancePayload, error) {
	if len(payload) == 0 {
		return LedgerBalancePayload{}, fmt.Errorf("ledger balance payload is required")
	}
	var out LedgerBalancePayload
	if err := json.Unmarshal(payload, &out); err != nil {
		return LedgerBalancePayload{}, fmt.Errorf("decode ledger balance payload: %w", err)
	}
	if out.PlayerID == "" {
		return LedgerBalancePayload{}, fmt.Errorf("player_id is required")
	}
	return out, nil
}

// EncodeCommandWithPayload marshals a Command with an optional typed payload.
func EncodeCommandWithPayload(op, matchID string, payload interface{}) ([]byte, error) {
	cmd := Command{Op: op, MatchID: matchID}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		cmd.Payload = raw
	}
	return json.Marshal(cmd)
}
