package consensus

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	phase13PortBase = "127.0.0.1:920"
)

func TestPhase13_LogReplicationAndLinearizableReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": phase13PortBase + "1",
		"node-2": phase13PortBase + "2",
		"node-3": phase13PortBase + "3",
	}

	nodes := []*RaftNode{
		NewRaftNode("node-1", peers, NewLocalGameFSM()),
		NewRaftNode("node-2", peers, NewLocalGameFSM()),
		NewRaftNode("node-3", peers, NewLocalGameFSM()),
	}

	bindAddresses := []string{
		phase13PortBase + "1",
		phase13PortBase + "2",
		phase13PortBase + "3",
	}

	transports := make([]*NetworkTransport, len(nodes))
	for i, node := range nodes {
		transport := NewNetworkTransport(ctx, node)
		transports[i] = transport

		if err := transport.StartServer(bindAddresses[i]); err != nil {
			t.Fatalf("start %s server: %v", node.NodeID, err)
		}

		go node.runElectionTicker(ctx)
	}

	leader := waitForStableLeader(t, nodes)
	followers := followerNodes(nodes, leader)

	releaseFollowers := blockFollowerReplication(followers...)
	released := false
	releaseOnce := func() {
		if released {
			return
		}
		released = true
		releaseFollowers()
	}

	defer func() {
		releaseOnce()
		testRPCObserver = nil
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	startCmd, err := EncodeCommand(Command{
		Op:      OpStartMatch,
		MatchID: "match-phase13",
	})
	if err != nil {
		t.Fatalf("encode start command: %v", err)
	}

	turnCmd, err := EncodeCommand(Command{
		Op:      OpApplyTurn,
		MatchID: "match-phase13",
	})
	if err != nil {
		t.Fatalf("encode turn command: %v", err)
	}

	// Hold replication on every follower so the leader can append locally but
	// cannot reach a commit quorum until at least one follower is released.
	proposedIndex, err := leader.Propose(startCmd)
	if err != nil {
		t.Fatalf("propose start match: %v", err)
	}
	if proposedIndex != 1 {
		t.Fatalf("proposed index = %d, want 1", proposedIndex)
	}

	waitUntilPhase13(t, time.Second, func() bool {
		return nodeLastLogIndex(leader) == proposedIndex
	})

	leader.mu.RLock()
	preCommitIndex := leader.commitIndex
	preApplied := leader.lastApplied
	leader.mu.RUnlock()

	if preCommitIndex != 0 {
		t.Fatalf("commitIndex = %d before quorum, want 0", preCommitIndex)
	}
	if preApplied != 0 {
		t.Fatalf("lastApplied = %d before quorum, want 0", preApplied)
	}
	if len(readLocalMatches(leader)) != 0 {
		t.Fatal("leader GameFSM mutated before quorum commit")
	}

	// Release followers so the leader can reach majority (self + one peer).
	releaseOnce()

	waitUntilPhase13(t, 2*time.Second, func() bool {
		return nodeCommitIndex(leader) >= proposedIndex
	})

	for _, node := range nodes {
		waitUntilPhase13(t, 2*time.Second, func() bool {
			return nodeCommitIndex(node) >= proposedIndex
		})

		matches := readLocalMatches(node)
		state, ok := matches["match-phase13"]
		if !ok {
			t.Fatalf("%s missing match after commit", node.NodeID)
		}
		if state.Status != "active" || state.Counter != 0 {
			t.Fatalf("%s start match state = %+v, want active counter 0", node.NodeID, state)
		}
	}

	turnIndex, err := leader.Propose(turnCmd)
	if err != nil {
		t.Fatalf("propose turn: %v", err)
	}

	waitUntilPhase13(t, 2*time.Second, func() bool {
		return nodeCommitIndex(leader) >= turnIndex
	})

	var appendEntriesDuringRead int32
	testRPCObserver = func(method string) func() {
		if strings.HasSuffix(method, ".AppendEntries") {
			atomic.AddInt32(&appendEntriesDuringRead, 1)
		}
		return nil
	}

	matches, err := leader.ReadLinearizableState()
	if err != nil {
		t.Fatalf("linearizable read: %v", err)
	}

	if got := atomic.LoadInt32(&appendEntriesDuringRead); got < 1 {
		t.Fatalf("VerifyLeader AppendEntries count = %d, want at least 1 follower heartbeat", got)
	}

	state, ok := matches["match-phase13"]
	if !ok {
		t.Fatal("linearizable read missing match-phase13")
	}
	if state.Counter != 1 {
		t.Fatalf("linearizable read counter = %d, want 1", state.Counter)
	}
	if state.Status != "active" {
		t.Fatalf("linearizable read status = %q, want active", state.Status)
	}
}

func waitForStableLeader(t *testing.T, nodes []*RaftNode) *RaftNode {
	t.Helper()

	var leader *RaftNode
	waitUntilPhase13(t, 2*time.Second, func() bool {
		leaderCount, followerCount, leaderID, _ := clusterRoleSnapshot(nodes)
		if leaderCount != 1 || followerCount != 2 || leaderID == "" {
			return false
		}
		for _, node := range nodes {
			if node.NodeID == leaderID {
				leader = node
				return true
			}
		}
		return false
	})

	if leader == nil {
		t.Fatal("expected a stable leader")
	}
	return leader
}

func followerNodes(nodes []*RaftNode, leader *RaftNode) []*RaftNode {
	out := make([]*RaftNode, 0, len(nodes)-1)
	for _, node := range nodes {
		if node != leader {
			out = append(out, node)
		}
	}
	return out
}

func blockFollowerReplication(followers ...*RaftNode) func() {
	gates := make(map[string]chan struct{}, len(followers))
	for _, follower := range followers {
		ch := make(chan struct{})
		gates[follower.NodeID] = ch
		testAppendEntriesGate.Store(follower.NodeID, ch)
	}

	return func() {
		for id, ch := range gates {
			close(ch)
			testAppendEntriesGate.Delete(id)
		}
	}
}

func waitUntilPhase13(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func nodeLastLogIndex(node *RaftNode) uint64 {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.lastLogIndexLocked()
}

func nodeCommitIndex(node *RaftNode) uint64 {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.commitIndex
}

func readLocalMatches(node *RaftNode) map[string]matchState {
	fsm, ok := node.MatchFSM.(*LocalGameFSM)
	if !ok {
		return nil
	}
	return fsm.Matches()
}
