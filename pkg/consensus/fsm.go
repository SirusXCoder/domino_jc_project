package consensus

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"sync"

	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
)

const (
	fsmSnapshotMagic   uint32 = 0x4746534D // "GFSM"
	fsmSnapshotVersion uint8  = 1
)

func init() {
	gob.Register(managedFSMSnapshot{})
	gob.Register(map[string]*models.GameSession{})
	gob.Register(map[string]models.PlayerStatsUpdate{})
	gob.Register(&models.GameSession{})
	gob.Register(models.PlayerStatsUpdate{})
}

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
	// CreateSnapshot serializes active match memory into a minimized binary payload.
	CreateSnapshot() ([]byte, error)
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

// CreateSnapshot serializes the current match map into a minimized binary payload.
func (f *LocalGameFSM) CreateSnapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return encodeLocalFSMSnapshot(f.matches)
}

// Snapshot serializes the current match map for Raft log compaction.
func (f *LocalGameFSM) Snapshot() ([]byte, error) {
	return f.CreateSnapshot()
}

// Restore replaces local state from a snapshot produced by Snapshot.
func (f *LocalGameFSM) Restore(snapshot []byte) error {
	matches, err := decodeLocalFSMSnapshot(snapshot)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.matches = make(map[string]matchState, len(matches))
	for id, state := range matches {
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

// CreateSnapshot serializes active sessions and ledger profile cache for compaction.
func (f *ManagedGameFSM) CreateSnapshot() ([]byte, error) {
	snap := managedFSMSnapshot{
		Sessions: f.manager.SnapshotSessions(),
		Ledger:   f.manager.LedgerProfiles(),
	}
	return encodeManagedFSMSnapshot(snap)
}

// Snapshot serializes active sessions and ledger profile cache for compaction.
func (f *ManagedGameFSM) Snapshot() ([]byte, error) {
	return f.CreateSnapshot()
}

// Restore rebuilds GameManager state from a snapshot.
func (f *ManagedGameFSM) Restore(snapshot []byte) error {
	snap, err := decodeManagedFSMSnapshot(snapshot)
	if err != nil {
		return err
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

func encodeLocalFSMSnapshot(matches map[string]matchState) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, fsmSnapshotMagic); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(fsmSnapshotVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(matches))); err != nil {
		return nil, err
	}

	for id, state := range matches {
		if len(id) > int(^uint16(0)) {
			return nil, fmt.Errorf("match id %q exceeds maximum encoded length", id)
		}
		if err := binary.Write(&buf, binary.BigEndian, uint16(len(id))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(id); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, int32(state.Counter)); err != nil {
			return nil, err
		}
		status := []byte(state.Status)
		if len(status) > int(^uint16(0)) {
			return nil, fmt.Errorf("match status for %q exceeds maximum encoded length", id)
		}
		if err := binary.Write(&buf, binary.BigEndian, uint16(len(status))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(status); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeLocalFSMSnapshot(snapshot []byte) (map[string]matchState, error) {
	if len(snapshot) < 9 {
		return nil, fmt.Errorf("decode snapshot: payload too short")
	}

	reader := bytes.NewReader(snapshot)
	var magic uint32
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("decode snapshot magic: %w", err)
	}
	if magic != fsmSnapshotMagic {
		return nil, fmt.Errorf("decode snapshot: invalid magic")
	}

	version, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("decode snapshot version: %w", err)
	}
	if version != fsmSnapshotVersion {
		return nil, fmt.Errorf("decode snapshot: unsupported version %d", version)
	}

	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("decode snapshot count: %w", err)
	}

	matches := make(map[string]matchState, count)
	for i := uint32(0); i < count; i++ {
		var idLen uint16
		if err := binary.Read(reader, binary.BigEndian, &idLen); err != nil {
			return nil, fmt.Errorf("decode snapshot match id length: %w", err)
		}
		idBytes := make([]byte, idLen)
		if _, err := ioReadFull(reader, idBytes); err != nil {
			return nil, fmt.Errorf("decode snapshot match id: %w", err)
		}

		var counter int32
		if err := binary.Read(reader, binary.BigEndian, &counter); err != nil {
			return nil, fmt.Errorf("decode snapshot counter: %w", err)
		}

		var statusLen uint16
		if err := binary.Read(reader, binary.BigEndian, &statusLen); err != nil {
			return nil, fmt.Errorf("decode snapshot status length: %w", err)
		}
		statusBytes := make([]byte, statusLen)
		if _, err := ioReadFull(reader, statusBytes); err != nil {
			return nil, fmt.Errorf("decode snapshot status: %w", err)
		}

		matches[string(idBytes)] = matchState{
			Counter: int(counter),
			Status:  string(statusBytes),
		}
	}
	return matches, nil
}

func encodeManagedFSMSnapshot(snap managedFSMSnapshot) ([]byte, error) {
	var payload bytes.Buffer
	encoder := gob.NewEncoder(&payload)
	if err := encoder.Encode(snap); err != nil {
		return nil, fmt.Errorf("encode managed snapshot: %w", err)
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, fsmSnapshotMagic); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(fsmSnapshotVersion + 1); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(payload.Len())); err != nil {
		return nil, err
	}
	if _, err := buf.Write(payload.Bytes()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeManagedFSMSnapshot(snapshot []byte) (managedFSMSnapshot, error) {
	if len(snapshot) < 9 {
		return managedFSMSnapshot{}, fmt.Errorf("decode snapshot: payload too short")
	}

	reader := bytes.NewReader(snapshot)
	var magic uint32
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return managedFSMSnapshot{}, fmt.Errorf("decode snapshot magic: %w", err)
	}
	if magic != fsmSnapshotMagic {
		return managedFSMSnapshot{}, fmt.Errorf("decode snapshot: invalid magic")
	}

	version, err := reader.ReadByte()
	if err != nil {
		return managedFSMSnapshot{}, fmt.Errorf("decode snapshot version: %w", err)
	}
	if version != fsmSnapshotVersion+1 {
		return managedFSMSnapshot{}, fmt.Errorf("decode snapshot: unsupported version %d", version)
	}

	var payloadLen uint32
	if err := binary.Read(reader, binary.BigEndian, &payloadLen); err != nil {
		return managedFSMSnapshot{}, fmt.Errorf("decode snapshot payload length: %w", err)
	}
	payload := make([]byte, payloadLen)
	if _, err := ioReadFull(reader, payload); err != nil {
		return managedFSMSnapshot{}, fmt.Errorf("decode snapshot payload: %w", err)
	}

	var snap managedFSMSnapshot
	decoder := gob.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&snap); err != nil {
		return managedFSMSnapshot{}, fmt.Errorf("decode managed snapshot: %w", err)
	}
	return snap, nil
}

func ioReadFull(r interface {
	Read([]byte) (int, error)
}, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// AsApplyResult extracts a successful ApplyResult when Apply did not fail.
func AsApplyResult(result interface{}) (ApplyResult, bool) {
	if isApplyError(result) {
		return ApplyResult{}, false
	}
	applyResult, ok := result.(ApplyResult)
	return applyResult, ok
}
