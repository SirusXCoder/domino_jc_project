package database

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	snapshotFileMagic   uint32 = 0x474D534E // "GMSN"
	snapshotFileVersion uint8  = 1
)

// PersistedLogEntry is the on-disk representation of a replicated Raft log command.
type PersistedLogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command"`
}

// SnapshotRecord captures a persisted FSM snapshot and its trailing log coordinates.
type SnapshotRecord struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// RaftMeta stores durable Raft metadata that survives process restarts.
type RaftMeta struct {
	SnapshotIndex uint64 `json:"snapshot_index"`
	SnapshotTerm  uint64 `json:"snapshot_term"`
	CommitIndex   uint64 `json:"commit_index"`
	LastApplied   uint64 `json:"last_applied"`
	CurrentTerm   uint64 `json:"current_term"`
	VotedFor      string `json:"voted_for"`
}

// RaftStorage persists Raft snapshots, log segments, and metadata atomically to disk.
type RaftStorage struct {
	mu      sync.Mutex
	dataDir string
}

// NewRaftStorage prepares a directory for durable Raft state.
func NewRaftStorage(dataDir string) (*RaftStorage, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("raft storage data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create raft storage directory: %w", err)
	}
	return &RaftStorage{dataDir: dataDir}, nil
}

// DataDir returns the configured storage root.
func (s *RaftStorage) DataDir() string {
	return s.dataDir
}

// HasSnapshot reports whether a snapshot file exists on disk.
func (s *RaftStorage) HasSnapshot() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := os.Stat(s.snapshotPath())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// LoadSnapshot reads the latest persisted snapshot, if present.
func (s *RaftStorage) LoadSnapshot() (*SnapshotRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.snapshotPath()
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	defer file.Close()

	record, err := decodeSnapshotFile(file)
	if err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return record, nil
}

// PersistSnapshot atomically writes an FSM snapshot and its trailing index to disk.
func (s *RaftStorage) PersistSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload, err := encodeSnapshotFile(lastIncludedIndex, lastIncludedTerm, data)
	if err != nil {
		return err
	}

	tmpPath := s.snapshotPath() + ".tmp"
	if err := writeFileAtomic(tmpPath, s.snapshotPath(), payload); err != nil {
		return fmt.Errorf("persist snapshot: %w", err)
	}
	return nil
}

// LoadLog returns uncompacted log entries and durable Raft metadata.
func (s *RaftStorage) LoadLog() ([]PersistedLogEntry, *RaftMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := s.loadLogLocked()
	if err != nil {
		return nil, nil, err
	}

	meta, err := s.loadMetaLocked()
	if err != nil {
		return nil, nil, err
	}
	return entries, meta, nil
}

// PersistLog atomically replaces the uncompacted log segment on disk.
func (s *RaftStorage) PersistLog(entries []PersistedLogEntry, meta RaftMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logPayload, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal log entries: %w", err)
	}
	metaPayload, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal raft meta: %w", err)
	}

	if err := writeFileAtomic(s.logPath()+".tmp", s.logPath(), logPayload); err != nil {
		return fmt.Errorf("persist log: %w", err)
	}
	if err := writeFileAtomic(s.metaPath()+".tmp", s.metaPath(), metaPayload); err != nil {
		return fmt.Errorf("persist raft meta: %w", err)
	}
	return nil
}

// TruncateLog atomically persists a truncated uncompacted log segment and metadata.
func (s *RaftStorage) TruncateLog(entries []PersistedLogEntry, meta RaftMeta) error {
	return s.PersistLog(entries, meta)
}

func (s *RaftStorage) loadLogLocked() ([]PersistedLogEntry, error) {
	raw, err := os.ReadFile(s.logPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read log: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}

	var entries []PersistedLogEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("decode log: %w", err)
	}
	return entries, nil
}

func (s *RaftStorage) loadMetaLocked() (*RaftMeta, error) {
	raw, err := os.ReadFile(s.metaPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read raft meta: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}

	var meta RaftMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("decode raft meta: %w", err)
	}
	return &meta, nil
}

func (s *RaftStorage) snapshotPath() string {
	return filepath.Join(s.dataDir, "raft.snapshot")
}

func (s *RaftStorage) logPath() string {
	return filepath.Join(s.dataDir, "raft.log")
}

func (s *RaftStorage) metaPath() string {
	return filepath.Join(s.dataDir, "raft.meta")
}

func encodeSnapshotFile(lastIncludedIndex, lastIncludedTerm uint64, data []byte) ([]byte, error) {
	if len(data) > int(^uint32(0)) {
		return nil, fmt.Errorf("snapshot payload exceeds maximum size")
	}

	out := make([]byte, 4+1+8+8+4+len(data))
	binary.BigEndian.PutUint32(out[0:4], snapshotFileMagic)
	out[4] = snapshotFileVersion
	binary.BigEndian.PutUint64(out[5:13], lastIncludedIndex)
	binary.BigEndian.PutUint64(out[13:21], lastIncludedTerm)
	binary.BigEndian.PutUint32(out[21:25], uint32(len(data)))
	copy(out[25:], data)
	return out, nil
}

func decodeSnapshotFile(r io.Reader) (*SnapshotRecord, error) {
	header := make([]byte, 25)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("read snapshot header: %w", err)
	}

	if binary.BigEndian.Uint32(header[0:4]) != snapshotFileMagic {
		return nil, fmt.Errorf("invalid snapshot magic")
	}
	if header[4] != snapshotFileVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", header[4])
	}

	record := &SnapshotRecord{
		LastIncludedIndex: binary.BigEndian.Uint64(header[5:13]),
		LastIncludedTerm:  binary.BigEndian.Uint64(header[13:21]),
	}
	dataLen := binary.BigEndian.Uint32(header[21:25])
	if dataLen == 0 {
		return record, nil
	}

	record.Data = make([]byte, dataLen)
	if _, err := io.ReadFull(r, record.Data); err != nil {
		return nil, fmt.Errorf("read snapshot payload: %w", err)
	}
	return record, nil
}

func writeFileAtomic(tmpPath, finalPath string, payload []byte) error {
	if err := os.WriteFile(tmpPath, payload, 0o640); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}
