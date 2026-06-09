package consensus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"domino_jc_project/pkg/database"
)

const snapshotTestPortBase = "127.0.0.1:951"

func TestLocalFSM_BinarySnapshotRoundTrip(t *testing.T) {
	fsm := NewLocalGameFSM()
	startCmd, err := EncodeCommand(Command{Op: OpStartMatch, MatchID: "match-a"})
	if err != nil {
		t.Fatalf("encode start: %v", err)
	}
	turnCmd, err := EncodeCommand(Command{Op: OpApplyTurn, MatchID: "match-a"})
	if err != nil {
		t.Fatalf("encode turn: %v", err)
	}

	if result := fsm.Apply(startCmd); IsApplyError(result) {
		t.Fatalf("apply start: %v", result)
	}
	if result := fsm.Apply(turnCmd); IsApplyError(result) {
		t.Fatalf("apply turn: %v", result)
	}
	if result := fsm.Apply(turnCmd); IsApplyError(result) {
		t.Fatalf("apply second turn: %v", result)
	}

	snap, err := fsm.CreateSnapshot()
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	restored := NewLocalGameFSM()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	matches := restored.Matches()
	state, ok := matches["match-a"]
	if !ok {
		t.Fatal("restored snapshot missing match-a")
	}
	if state.Counter != 2 || state.Status != "active" {
		t.Fatalf("restored state = %+v, want counter 2 active", state)
	}
}

func TestRaftStorage_PersistSnapshotAndLog(t *testing.T) {
	dir := t.TempDir()
	storage, err := database.NewRaftStorage(dir)
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}

	payload := []byte("snapshot-payload")
	if err := storage.PersistSnapshot(42, 7, payload); err != nil {
		t.Fatalf("persist snapshot: %v", err)
	}

	hasSnap, err := storage.HasSnapshot()
	if err != nil || !hasSnap {
		t.Fatalf("HasSnapshot = %v, err = %v, want true", hasSnap, err)
	}

	record, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if record.LastIncludedIndex != 42 || record.LastIncludedTerm != 7 {
		t.Fatalf("snapshot metadata = %+v, want index 42 term 7", record)
	}
	if string(record.Data) != string(payload) {
		t.Fatalf("snapshot data = %q, want %q", record.Data, payload)
	}

	entries := []database.PersistedLogEntry{
		{Index: 43, Term: 7, Command: []byte("cmd-43")},
		{Index: 44, Term: 8, Command: []byte("cmd-44")},
	}
	meta := database.RaftMeta{
		SnapshotIndex: 42,
		SnapshotTerm:  7,
		CommitIndex:   44,
		LastApplied:   44,
		CurrentTerm:   8,
	}
	if err := storage.TruncateLog(entries, meta); err != nil {
		t.Fatalf("truncate log: %v", err)
	}

	loadedEntries, loadedMeta, err := storage.LoadLog()
	if err != nil {
		t.Fatalf("load log: %v", err)
	}
	if len(loadedEntries) != 2 {
		t.Fatalf("loaded entries = %d, want 2", len(loadedEntries))
	}
	if loadedMeta == nil || loadedMeta.CommitIndex != 44 {
		t.Fatalf("loaded meta = %+v, want commit index 44", loadedMeta)
	}
}

func TestSnapshot_AutoCompactionOnThreshold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, storage, _, cleanup := startSnapshotTestNode(t, ctx, "node-1", snapshotTestPortBase+"1", "")
	defer cleanup()

	leader.SetCompactThreshold(25)
	proposeMatchTurns(t, leader, "compact-match", 30)

	waitUntilSnapshot(t, 3*time.Second, func() bool {
		return leader.SnapshotIndex() >= 25 && leader.UncompactedLogEntries() < 25
	})

	if leader.LogLength() > 10 {
		t.Fatalf("log length = %d after compaction, want a truncated tail", leader.LogLength())
	}

	hasSnap, err := storage.HasSnapshot()
	if err != nil || !hasSnap {
		t.Fatalf("expected persisted snapshot, has=%v err=%v", hasSnap, err)
	}
}

func TestSnapshot_StatePreservedAfterCompaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, _, _, cleanup := startSnapshotTestNode(t, ctx, "node-1", snapshotTestPortBase+"2", "")
	defer cleanup()

	leader.SetCompactThreshold(10)
	proposeMatchTurns(t, leader, "state-match", 15)

	waitUntilSnapshot(t, 3*time.Second, func() bool {
		return leader.SnapshotIndex() >= 10
	})

	matches := readLocalMatches(leader)
	state, ok := matches["state-match"]
	if !ok {
		t.Fatal("missing compacted match state")
	}
	if state.Counter != 14 {
		t.Fatalf("counter = %d, want 14 turns after 15 proposals (1 start + 14 turns)", state.Counter)
	}
}

func TestSnapshot_ColdBootRestoresSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	nodeDir := filepath.Join(dir, "node-1")

	leader, storage, transport, cleanup := startSnapshotTestNode(t, ctx, "node-1", snapshotTestPortBase+"3", nodeDir)
	leader.SetCompactThreshold(8)
	proposeMatchTurns(t, leader, "boot-match", 12)

	waitUntilSnapshot(t, 3*time.Second, func() bool {
		return leader.SnapshotIndex() >= 8
	})

	snapIdx := leader.SnapshotIndex()
	matches := readLocalMatches(leader)
	wantState := matches["boot-match"]

	if err := leader.FlushStorage(); err != nil {
		t.Fatalf("flush storage: %v", err)
	}

	transport.Shutdown()
	transport.Wait()
	cleanup()

	restoredFSM := NewLocalGameFSM()
	restored, err := NewRaftNodeWithStorage("node-1", nil, restoredFSM, storage)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	if restored.SnapshotIndex() != snapIdx {
		t.Fatalf("restored snapshot index = %d, want %d", restored.SnapshotIndex(), snapIdx)
	}

	restoredMatches := readLocalMatches(restored)
	gotState, ok := restoredMatches["boot-match"]
	if !ok {
		t.Fatal("cold boot missing restored match")
	}
	if gotState != wantState {
		t.Fatalf("cold boot state = %+v, want %+v", gotState, wantState)
	}
}

func TestSnapshot_CrashRecoveryZeroDataLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	nodeDir := filepath.Join(dir, "node-1")

	leader, storage, transport, cleanup := startSnapshotTestNode(t, ctx, "node-1", snapshotTestPortBase+"4", nodeDir)
	leader.SetCompactThreshold(6)

	proposeMatchTurns(t, leader, "crash-match", 10)
	waitUntilSnapshot(t, 3*time.Second, func() bool {
		return leader.SnapshotIndex() >= 6
	})

	proposeMatchTurns(t, leader, "crash-match", 4)
	waitUntilSnapshot(t, 3*time.Second, func() bool {
		return leader.SnapshotIndex() >= 10
	})

	wantMatches := readLocalMatches(leader)
	wantSnapIdx := leader.SnapshotIndex()

	if err := leader.FlushStorage(); err != nil {
		t.Fatalf("flush storage: %v", err)
	}

	transport.Shutdown()
	transport.Wait()
	cleanup()

	recovered, err := NewRaftNodeWithStorage("node-1", nil, NewLocalGameFSM(), storage)
	if err != nil {
		t.Fatalf("recover node: %v", err)
	}

	if recovered.SnapshotIndex() != wantSnapIdx {
		t.Fatalf("recovered snapshot index = %d, want %d", recovered.SnapshotIndex(), wantSnapIdx)
	}

	gotMatches := readLocalMatches(recovered)
	if len(gotMatches) != len(wantMatches) {
		t.Fatalf("recovered matches = %+v, want %+v", gotMatches, wantMatches)
	}
	for id, want := range wantMatches {
		got, ok := gotMatches[id]
		if !ok || got != want {
			t.Fatalf("match %s = %+v, want %+v", id, got, want)
		}
	}
}

func TestSnapshot_InstallSnapshotForLaggingFollower(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": snapshotTestPortBase + "7",
		"node-2": snapshotTestPortBase + "8",
	}

	leader := NewRaftNode("node-1", peers, NewLocalGameFSM())
	follower := NewRaftNode("node-2", peers, NewLocalGameFSM())
	leader.SetCompactThreshold(12)
	follower.SetCompactThreshold(12)

	transports := []*NetworkTransport{
		NewNetworkTransport(ctx, leader),
		NewNetworkTransport(ctx, follower),
	}
	for i, node := range []*RaftNode{leader, follower} {
		if err := transports[i].StartServer(peers[node.NodeID]); err != nil {
			t.Fatalf("start %s: %v", node.NodeID, err)
		}
		go node.runElectionTicker(ctx)
	}
	defer func() {
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	proposeMatchTurns(t, leader, "lag-match", 20)
	waitUntilSnapshot(t, 3*time.Second, func() bool {
		return leader.SnapshotIndex() >= 12 && leader.UncompactedLogEntries() < 12
	})

	leader.mu.RLock()
	args := InstallSnapshotArgs{
		Term:              leader.CurrentTerm,
		LeaderID:          "node-1",
		LastIncludedIndex: leader.snapshotIndex,
		LastIncludedTerm:  leader.snapshotTerm,
		Data:              append([]byte(nil), leader.lastSnapshot...),
	}
	prevLogIndex := leader.snapshotIndex
	prevLogTerm := leader.snapshotTerm
	leaderCommit := leader.commitIndex
	tailStart := leader.logOffsetForIndex(leader.snapshotIndex + 1)
	var tailEntries []LogEntry
	if tailStart > 0 && tailStart < len(leader.log) {
		tailEntries = append([]LogEntry(nil), leader.log[tailStart:]...)
	}
	leader.mu.RUnlock()

	var installReply InstallSnapshotReply
	if err := follower.InstallSnapshot(args, &installReply); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	appendArgs := AppendEntriesArgs{
		Term:         args.Term,
		LeaderID:     "node-1",
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      tailEntries,
		LeaderCommit: leaderCommit,
	}
	var appendReply AppendEntriesReply
	if err := follower.AppendEntries(appendArgs, &appendReply); err != nil {
		t.Fatalf("append entries after snapshot: %v", err)
	}
	if !appendReply.Success {
		t.Fatal("append entries after snapshot was rejected")
	}

	leaderState := readLocalMatches(leader)["lag-match"]
	followerState := readLocalMatches(follower)["lag-match"]
	if leaderState.Counter != 19 {
		t.Fatalf("leader counter = %d, want 19", leaderState.Counter)
	}
	if followerState != leaderState {
		t.Fatalf("follower state = %+v, want %+v", followerState, leaderState)
	}
}

func TestSnapshot_NetworkInstallSnapshotForLaggingFollower(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": snapshotTestPortBase + "1",
		"node-2": snapshotTestPortBase + "2",
	}

	leader := NewRaftNode("node-1", peers, NewLocalGameFSM())
	follower := NewRaftNode("node-2", peers, NewLocalGameFSM())
	leader.SetCompactThreshold(12)
	follower.SetCompactThreshold(12)

	transports := []*NetworkTransport{
		NewNetworkTransport(ctx, leader),
		NewNetworkTransport(ctx, follower),
	}
	for i, node := range []*RaftNode{leader, follower} {
		if err := transports[i].StartServer(peers[node.NodeID]); err != nil {
			t.Fatalf("start %s: %v", node.NodeID, err)
		}
		go node.runElectionTicker(ctx)
	}
	defer func() {
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	proposeMatchTurns(t, leader, "lag-match", 20)
	waitUntilSnapshot(t, 3*time.Second, func() bool {
		return leader.SnapshotIndex() >= 12 && leader.UncompactedLogEntries() < 12
	})

	leader.mu.RLock()
	installArgs := InstallSnapshotArgs{
		Term:              leader.CurrentTerm,
		LeaderID:          "node-1",
		LastIncludedIndex: leader.snapshotIndex,
		LastIncludedTerm:  leader.snapshotTerm,
		Data:              append([]byte(nil), leader.lastSnapshot...),
	}
	appendArgs := AppendEntriesArgs{
		Term:         leader.CurrentTerm,
		LeaderID:     "node-1",
		PrevLogIndex: leader.snapshotIndex,
		PrevLogTerm:  leader.snapshotTerm,
		LeaderCommit: leader.commitIndex,
	}
	tailStart := leader.logOffsetForIndex(leader.snapshotIndex + 1)
	if tailStart > 0 && tailStart < len(leader.log) {
		appendArgs.Entries = append([]LogEntry(nil), leader.log[tailStart:]...)
	}
	leader.mu.RUnlock()

	var installReply InstallSnapshotReply
	if err := SendRPC(peers["node-2"], "node-2.InstallSnapshot", installArgs, &installReply); err != nil {
		t.Fatalf("network install snapshot: %v", err)
	}

	var appendReply AppendEntriesReply
	if err := SendRPC(peers["node-2"], "node-2.AppendEntries", appendArgs, &appendReply); err != nil {
		t.Fatalf("network append entries: %v", err)
	}
	if !appendReply.Success {
		t.Fatal("network append entries after snapshot was rejected")
	}

	leaderState := readLocalMatches(leader)["lag-match"]
	followerState := readLocalMatches(follower)["lag-match"]
	if followerState != leaderState {
		t.Fatalf("network follower state = %+v, want %+v", followerState, leaderState)
	}
}

func TestSnapshot_RapidLogGrowthAndRepeatedCompaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	leader, storage, transport, cleanup := startSnapshotTestNode(t, ctx, "node-1", snapshotTestPortBase+"6", filepath.Join(dir, "node-1"))
	defer func() {
		transport.Shutdown()
		transport.Wait()
		cleanup()
		cancel()
	}()

	leader.SetCompactThreshold(5)

	for round := 0; round < 4; round++ {
		matchID := fmt.Sprintf("rapid-match-%d", round)
		proposeMatchTurns(t, leader, matchID, 8)
		waitUntilSnapshot(t, 3*time.Second, func() bool {
			return leader.SnapshotIndex() >= uint64((round+1)*5)
		})
	}

	hasSnap, err := storage.HasSnapshot()
	if err != nil || !hasSnap {
		t.Fatalf("expected snapshot after rapid growth, has=%v err=%v", hasSnap, err)
	}

	matches := readLocalMatches(leader)
	for round := 0; round < 4; round++ {
		matchID := fmt.Sprintf("rapid-match-%d", round)
		state, ok := matches[matchID]
		if !ok {
			t.Fatalf("missing %s after repeated compaction", matchID)
		}
		if state.Counter != 7 {
			t.Fatalf("%s counter = %d, want 7", matchID, state.Counter)
		}
	}
}

func startSnapshotTestNode(t *testing.T, ctx context.Context, nodeID, bindAddr string, dataDir string) (*RaftNode, *database.RaftStorage, *NetworkTransport, func()) {
	t.Helper()

	if dataDir == "" {
		dataDir = filepath.Join(t.TempDir(), nodeID)
	} else if err := os.MkdirAll(dataDir, 0o750); err != nil {
		t.Fatalf("mkdir storage dir: %v", err)
	}

	storage, err := database.NewRaftStorage(dataDir)
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}

	node, err := NewRaftNodeWithStorage(nodeID, nil, NewLocalGameFSM(), storage)
	if err != nil {
		t.Fatalf("bootstrap node: %v", err)
	}

	transport := NewNetworkTransport(ctx, node)
	if err := transport.StartServer(bindAddr); err != nil {
		t.Fatalf("start transport: %v", err)
	}
	go node.runElectionTicker(ctx)

	cleanup := func() {}
	return node, storage, transport, cleanup
}

func proposeMatchTurns(t *testing.T, leader *RaftNode, matchID string, totalEntries int) {
	t.Helper()

	leader.mu.Lock()
	leader.State = StateLeader
	leader.CurrentTerm = 1
	leader.mu.Unlock()

	startCmd, err := EncodeCommand(Command{Op: OpStartMatch, MatchID: matchID})
	if err != nil {
		t.Fatalf("encode start command: %v", err)
	}
	turnCmd, err := EncodeCommand(Command{Op: OpApplyTurn, MatchID: matchID})
	if err != nil {
		t.Fatalf("encode turn command: %v", err)
	}

	for i := 0; i < totalEntries; i++ {
		cmd := startCmd
		if i > 0 {
			cmd = turnCmd
		}
		index, err := leader.Propose(cmd)
		if err != nil {
			t.Fatalf("propose entry %d: %v", i, err)
		}

		leader.mu.Lock()
		if index > leader.commitIndex {
			leader.commitIndex = index
		}
		leader.applyCommittedLocked()
		leader.mu.Unlock()
	}
}

func waitUntilSnapshot(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("snapshot condition not met before timeout")
}
