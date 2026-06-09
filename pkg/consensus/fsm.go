package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
)

// Command is a replicated log entry payload executed deterministically by every node.
type Command struct {
	Op      string `json:"op"`
	MatchID string `json:"match_id"`
	Payload []byte `json:"payload,omitempty"`
}

// GameFSM mirrors the HashiCorp Raft FSM contract for deterministic game replication.
type GameFSM interface {
	// Apply executes a committed command against local in-memory game state.
	Apply(logEntry []byte) interface{}
	// Snapshot serializes active match memory for log compaction.
	Snapshot() ([]byte, error)
	// Restore rebuilds game state from a snapshot during bootstrap or sync.
	Restore(snapshot []byte) error
}

// matchState tracks deterministic in-memory state for a single active match.
type matchState struct {
	Counter int    `json:"counter"`
	Status  string `json:"status"`
}

type fsmSnapshot struct {
	Matches map[string]matchState `json:"matches"`
}

type managedFSMSnapshot struct {
	Sessions map[string]*models.GameSession       `json:"sessions"`
	Ledger   map[string]models.PlayerStatsUpdate  `json:"ledger,omitempty"`
}

// LocalGameFSM is a mock FSM that tracks per-match counters so identical
// command sequences produce identical state across replicated nodes.
type LocalGameFSM struct {
	mu      sync.RWMutex
	matches map[string]matchState
}

// NewLocalGameFSM returns an empty mock FSM ready to accept replicated commands.
func NewLocalGameFSM() *LocalGameFSM {
	return &LocalGameFSM{
		matches: make(map[string]matchState),
	}
}

// Apply decodes logEntry as a Command and mutates local state deterministically.
func (f *LocalGameFSM) Apply(logEntry []byte) interface{} {
	cmd, err := DecodeCommand(logEntry)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Op {
	case OpStartMatch:
		f.matches[cmd.MatchID] = matchState{Counter: 0, Status: "active"}
		return ApplyResult{
			OK:      true,
			Op:      cmd.Op,
			MatchID: cmd.MatchID,
		}
	case OpApplyTurn:
		state, ok := f.matches[cmd.MatchID]
		if !ok {
			return fmt.Errorf("match %q not found", cmd.MatchID)
		}
		state.Counter++
		f.matches[cmd.MatchID] = state
		return ApplyResult{
			OK:      true,
			Op:      cmd.Op,
			MatchID: cmd.MatchID,
			Applied: true,
		}
	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
}

// Snapshot serializes the current match map for Raft log compaction.
func (f *LocalGameFSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snap := fsmSnapshot{Matches: make(map[string]matchState, len(f.matches))}
	for id, state := range f.matches {
		snap.Matches[id] = state
	}
	return json.Marshal(snap)
}

// Restore replaces local state from a snapshot produced by Snapshot.
func (f *LocalGameFSM) Restore(snapshot []byte) error {
	var snap fsmSnapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.matches = make(map[string]matchState, len(snap.Matches))
	for id, state := range snap.Matches {
		f.matches[id] = state
	}
	return nil
}

// Matches returns a deep copy of the current match map for cross-node comparison.
func (f *LocalGameFSM) Matches() map[string]matchState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out := make(map[string]matchState, len(f.matches))
	for id, state := range f.matches {
		out[id] = state
	}
	return out
}

// ManagedGameFSM applies committed commands through the live GameManager.
type ManagedGameFSM struct {
	ctx     context.Context
	manager *engine.GameManager
}

// NewManagedGameFSM wires a GameManager-backed FSM for production replication.
func NewManagedGameFSM(ctx context.Context, manager *engine.GameManager) *ManagedGameFSM {
	return &ManagedGameFSM{
		ctx:     ctx,
		manager: manager,
	}
}

// Apply decodes the command, unpacks payload bytes, and dispatches to GameManager.
func (f *ManagedGameFSM) Apply(logEntry []byte) interface{} {
	if f.manager == nil {
		return fmt.Errorf("game manager is not configured")
	}

	cmd, err := DecodeCommand(logEntry)
	if err != nil {
		return err
	}

	switch cmd.Op {
	case OpStartMatch:
		payload, err := DecodeStartMatchPayload(cmd.Payload)
		if err != nil {
			return err
		}
		session, err := f.manager.ApplyReplicatedStartMatch(f.ctx, cmd.MatchID, engine.ReplicatedMatchSetup{
			PlayerUIDs:   payload.PlayerUIDs,
			SetupGame:    payload.SetupGame,
			TilesPerHand: payload.TilesPerHand,
		})
		if err != nil {
			return err
		}
		return ApplyResult{
			OK:      true,
			Op:      cmd.Op,
			MatchID: cmd.MatchID,
			Session: session,
		}

	case OpApplyTurn:
		payload, err := DecodeApplyTurnPayload(cmd.Payload)
		if err != nil {
			return err
		}
		turn, err := f.manager.ApplyReplicatedTurn(f.ctx, cmd.MatchID, payload.ToTurnAction())
		if err != nil {
			return err
		}
		return ApplyResult{
			OK:      true,
			Op:      cmd.Op,
			MatchID: cmd.MatchID,
			Applied: turn != nil && turn.Applied,
			Turn:    turn,
		}

	case OpLedgerBalance:
		payload, err := DecodeLedgerBalancePayload(cmd.Payload)
		if err != nil {
			return err
		}
		update, err := f.manager.ApplyReplicatedLedgerBalance(f.ctx, payload.ToStatsUpdate(cmd.MatchID))
		if err != nil {
			return err
		}
		return ApplyResult{
			OK:      true,
			Op:      cmd.Op,
			MatchID: cmd.MatchID,
			Ledger:  update,
		}

	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
}

// Snapshot serializes active sessions and ledger profile cache for compaction.
func (f *ManagedGameFSM) Snapshot() ([]byte, error) {
	snap := managedFSMSnapshot{
		Sessions: f.manager.SnapshotSessions(),
		Ledger:   f.manager.LedgerProfiles(),
	}
	return json.Marshal(snap)
}

// Restore rebuilds GameManager state from a snapshot.
func (f *ManagedGameFSM) Restore(snapshot []byte) error {
	var snap managedFSMSnapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if err := f.manager.RestoreSessions(snap.Sessions); err != nil {
		return err
	}
	f.manager.RestoreLedgerProfiles(snap.Ledger)
	return nil
}

// EncodeCommand marshals a Command into the byte form expected by Apply.
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// ReplayCommands applies the same ordered log entries to a fresh FSM instance.
// Two nodes replaying identical entries will converge to identical state.
func ReplayCommands(entries [][]byte) (*LocalGameFSM, error) {
	fsm := NewLocalGameFSM()
	for i, entry := range entries {
		if result := fsm.Apply(entry); isApplyError(result) {
			return nil, fmt.Errorf("entry %d: %v", i, result)
		}
	}
	return fsm, nil
}

func isApplyError(result interface{}) bool {
	_, ok := result.(error)
	return ok
}

// IsApplyError reports whether Apply returned a structural failure.
func IsApplyError(result interface{}) bool {
	return isApplyError(result)
}

// AsApplyResult extracts a successful ApplyResult when Apply did not fail.
func AsApplyResult(result interface{}) (ApplyResult, bool) {
	if isApplyError(result) {
		return ApplyResult{}, false
	}
	applyResult, ok := result.(ApplyResult)
	return applyResult, ok
}
