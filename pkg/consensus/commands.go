package consensus

import (
	"encoding/json"
	"fmt"

	"domino_jc_project/pkg/models"
)

const (
	OpStartMatch     = "START_MATCH"
	OpApplyTurn      = "APPLY_TURN"
	OpLedgerBalance  = "LEDGER_BALANCE"
	OpAddNode        = "ADD_NODE"
	OpRemoveNode     = "REMOVE_NODE"
	OpCommitConfig   = "COMMIT_CONFIG"
	ClusterMatchID   = "_cluster"
)

// AddNodeCommand enters joint consensus by adding a peer to the new configuration.
type AddNodeCommand struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// RemoveNodeCommand enters joint consensus by removing a peer from the new configuration.
type RemoveNodeCommand struct {
	NodeID string `json:"node_id"`
}

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

// IsMembershipOp reports whether op mutates cluster configuration instead of game state.
func IsMembershipOp(op string) bool {
	switch op {
	case OpAddNode, OpRemoveNode, OpCommitConfig:
		return true
	default:
		return false
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
	if cmd.MatchID == "" && !IsMembershipOp(cmd.Op) {
		return Command{}, fmt.Errorf("command match_id is required")
	}
	return cmd, nil
}

// DecodeAddNodeCommand unmarshals the ADD_NODE payload bytes.
func DecodeAddNodeCommand(payload []byte) (AddNodeCommand, error) {
	if len(payload) == 0 {
		return AddNodeCommand{}, fmt.Errorf("add node payload is required")
	}
	var out AddNodeCommand
	if err := json.Unmarshal(payload, &out); err != nil {
		return AddNodeCommand{}, fmt.Errorf("decode add node payload: %w", err)
	}
	if out.NodeID == "" {
		return AddNodeCommand{}, fmt.Errorf("node_id is required")
	}
	if out.Address == "" {
		return AddNodeCommand{}, fmt.Errorf("address is required")
	}
	return out, nil
}

// DecodeRemoveNodeCommand unmarshals the REMOVE_NODE payload bytes.
func DecodeRemoveNodeCommand(payload []byte) (RemoveNodeCommand, error) {
	if len(payload) == 0 {
		return RemoveNodeCommand{}, fmt.Errorf("remove node payload is required")
	}
	var out RemoveNodeCommand
	if err := json.Unmarshal(payload, &out); err != nil {
		return RemoveNodeCommand{}, fmt.Errorf("decode remove node payload: %w", err)
	}
	if out.NodeID == "" {
		return RemoveNodeCommand{}, fmt.Errorf("node_id is required")
	}
	return out, nil
}

// EncodeAddNodeCommand marshals an ADD_NODE configuration change.
func EncodeAddNodeCommand(nodeID, address string) ([]byte, error) {
	return EncodeCommandWithPayload(OpAddNode, ClusterMatchID, AddNodeCommand{
		NodeID:  nodeID,
		Address: address,
	})
}

// EncodeRemoveNodeCommand marshals a REMOVE_NODE configuration change.
func EncodeRemoveNodeCommand(nodeID string) ([]byte, error) {
	return EncodeCommandWithPayload(OpRemoveNode, ClusterMatchID, RemoveNodeCommand{
		NodeID: nodeID,
	})
}

// EncodeCommitConfigCommand marshals the joint-consensus exit step.
func EncodeCommitConfigCommand() ([]byte, error) {
	return EncodeCommandWithPayload(OpCommitConfig, ClusterMatchID, nil)
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
		switch typed := payload.(type) {
		case []byte:
			cmd.Payload = append([]byte(nil), typed...)
		default:
			raw, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("marshal payload: %w", err)
			}
			cmd.Payload = raw
		}
	}
	return json.Marshal(cmd)
}
