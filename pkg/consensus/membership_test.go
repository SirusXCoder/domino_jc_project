package consensus

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	phase17PortBase     = "127.0.0.1:940"
	phase17ReadPortBase = "127.0.0.1:941"
	phase17SnapPortBase = "127.0.0.1:942"
)

func TestPhase17_UnresponsiveNewPeerDoesNotBlockCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": phase17PortBase + "1",
		"node-2": phase17PortBase + "2",
		"node-3": phase17PortBase + "3",
	}
	nodes := []*RaftNode{
		NewRaftNode("node-1", clonePeerMap(peers), NewLocalGameFSM()),
		NewRaftNode("node-2", clonePeerMap(peers), NewLocalGameFSM()),
		NewRaftNode("node-3", clonePeerMap(peers), NewLocalGameFSM()),
	}
	transports := make([]*NetworkTransport, len(nodes))
	for i, node := range nodes {
		transports[i] = NewNetworkTransport(ctx, node)
		if err := transports[i].StartServer(phase17PortBase + fmt.Sprintf("%d", i+1)); err != nil {
			t.Fatalf("start %s: %v", node.NodeID, err)
		}
		go node.runElectionTicker(ctx)
		assertPeerStarted(t, node.NodeID)
	}
	defer func() {
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	leader := waitForStableLeader(t, nodes)

	done := make(chan error, 1)
	go func() {
		// node-4 is in config but intentionally not started; commit must not hang.
		done <- leader.ProposeAddNode("node-4", phase17PortBase+"4")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("add unresponsive peer: %v", err)
		}
	case <-time.After(defaultProposeTimeout + time.Second):
		t.Fatal("ProposeAddNode blocked on unresponsive new peer")
	}

	waitUntilPhase17(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			node.mu.RLock()
			ok := len(node.PeerAddresses) == 4 && node.joint == nil
			node.mu.RUnlock()
			if !ok {
				return false
			}
		}
		return true
	})
}

func TestPhase17_AddNodeReplicatesToExistingMembers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": phase17PortBase + "1",
		"node-2": phase17PortBase + "2",
		"node-3": phase17PortBase + "3",
	}
	nodes := []*RaftNode{
		NewRaftNode("node-1", clonePeerMap(peers), NewLocalGameFSM()),
		NewRaftNode("node-2", clonePeerMap(peers), NewLocalGameFSM()),
		NewRaftNode("node-3", clonePeerMap(peers), NewLocalGameFSM()),
	}
	transports := make([]*NetworkTransport, len(nodes))
	for i, node := range nodes {
		transports[i] = NewNetworkTransport(ctx, node)
		if err := transports[i].StartServer(phase17PortBase + fmt.Sprintf("%d", i+1)); err != nil {
			t.Fatalf("start %s: %v", node.NodeID, err)
		}
		go node.runElectionTicker(ctx)
		assertPeerStarted(t, node.NodeID)
	}
	defer func() {
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	leader := waitForStableLeader(t, nodes)
	// node-4 is added to config but deliberately not started in this test.
	if err := leader.ProposeAddNode("node-4", phase17PortBase+"4"); err != nil {
		t.Fatalf("add node: %v", err)
	}

	leader.mu.RLock()
	targetCommit := leader.commitIndex
	term := leader.CurrentTerm
	leader.mu.RUnlock()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		leader.broadcastHeartbeats(term)
		ready := true
		for _, node := range nodes {
			node.mu.RLock()
			ok := len(node.PeerAddresses) == 4 && node.joint == nil && node.commitIndex >= targetCommit
			node.mu.RUnlock()
			if !ok {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, node := range nodes {
		node.mu.RLock()
		t.Errorf("%s peers=%d joint=%v commit=%d lastLog=%d lastApplied=%d",
			node.NodeID, len(node.PeerAddresses), node.joint != nil,
			node.commitIndex, node.lastLogIndexLocked(), node.lastApplied)
		node.mu.RUnlock()
	}
	t.Fatal("followers did not converge on membership change")
}

func TestPhase17_ClusterExpandsFromThreeToFiveUnderLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initialPeers := map[string]string{
		"node-1": phase17PortBase + "1",
		"node-2": phase17PortBase + "2",
		"node-3": phase17PortBase + "3",
	}

	nodes := []*RaftNode{
		NewRaftNode("node-1", clonePeerMap(initialPeers), NewLocalGameFSM()),
		NewRaftNode("node-2", clonePeerMap(initialPeers), NewLocalGameFSM()),
		NewRaftNode("node-3", clonePeerMap(initialPeers), NewLocalGameFSM()),
	}

	transports := make([]*NetworkTransport, 0, 5)
	startNode := func(node *RaftNode, addr string) {
		transport := NewNetworkTransport(ctx, node)
		transports = append(transports, transport)
		if err := transport.StartServer(addr); err != nil {
			t.Fatalf("start %s: %v", node.NodeID, err)
		}
		go node.runElectionTicker(ctx)
	}

	for i, node := range nodes {
		startNode(node, phase17PortBase+fmt.Sprintf("%d", i+1))
		assertPeerStarted(t, node.NodeID)
	}

	defer func() {
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	leader := waitForStableLeader(t, nodes)

	startCmd, err := EncodeCommand(Command{Op: OpStartMatch, MatchID: "membership-match"})
	if err != nil {
		t.Fatalf("encode start: %v", err)
	}
	turnCmd, err := EncodeCommand(Command{Op: OpApplyTurn, MatchID: "membership-match"})
	if err != nil {
		t.Fatalf("encode turn: %v", err)
	}

	if _, err := leader.ProposeAndWait(startCmd); err != nil {
		t.Fatalf("start match: %v", err)
	}

	loadStop := make(chan struct{})
	loadErr := make(chan error, 1)
	var proposedTurns atomic.Int32

	go func() {
		ticker := time.NewTicker(15 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-loadStop:
				return
			case <-ticker.C:
				currentLeader := findLeader(nodes)
				if currentLeader == nil {
					continue
				}
				if _, err := currentLeader.Propose(turnCmd); err != nil {
					if err == ErrNotLeader {
						continue
					}
					if _, ok := err.(*LeaderRedirectError); ok {
						continue
					}
					loadErr <- err
					return
				}
				proposedTurns.Add(1)
			}
		}
	}()

	additions := []struct {
		id   string
		port string
	}{
		{"node-4", phase17PortBase + "4"},
		{"node-5", phase17PortBase + "5"},
	}

	for _, addition := range additions {
		currentLeader := findLeader(nodes)
		if currentLeader == nil {
			t.Fatal("lost leader before membership change")
		}

		if err := currentLeader.ProposeAddNode(addition.id, addition.port); err != nil {
			t.Fatalf("add %s: %v", addition.id, err)
		}

		existingCount := len(nodes)
		waitUntilPhase17(t, 8*time.Second, func() bool {
			currentLeader = findLeader(nodes)
			if currentLeader == nil {
				return false
			}
			currentLeader.mu.RLock()
			targetCommit := currentLeader.commitIndex
			term := currentLeader.CurrentTerm
			currentLeader.mu.RUnlock()
			currentLeader.broadcastHeartbeats(term)

			for _, node := range nodes[:existingCount] {
				node.mu.RLock()
				_, ok := node.PeerAddresses[addition.id]
				inJoint := node.joint != nil
				synced := node.commitIndex >= targetCommit
				node.mu.RUnlock()
				if !ok || inJoint || !synced {
					return false
				}
			}
			return true
		})

		peers := clonePeerMap(currentLeader.PeerAddresses)
		newNode := NewRaftNode(addition.id, peers, NewLocalGameFSM())
		nodes = append(nodes, newNode)
		startNode(newNode, addition.port)
		assertPeerStarted(t, addition.id)

		waitUntilPhase17(t, 5*time.Second, func() bool {
			currentLeader = findLeader(nodes)
			if currentLeader == nil {
				return false
			}
			currentLeader.mu.RLock()
			targetCommit := currentLeader.commitIndex
			currentLeader.mu.RUnlock()

			newNode.mu.RLock()
			synced := newNode.commitIndex >= targetCommit && !newNode.IsInJointConsensus()
			newNode.mu.RUnlock()
			return synced
		})
	}

	close(loadStop)

	select {
	case err := <-loadErr:
		t.Fatalf("transaction load failed: %v", err)
	default:
	}

	waitUntilPhase17(t, 5*time.Second, func() bool {
		leader = findLeader(nodes)
		if leader == nil {
			return false
		}
		leader.mu.RLock()
		size := len(leader.PeerAddresses)
		leader.mu.RUnlock()
		return size == 5
	})

	if leader == nil {
		t.Fatal("expected leader after expansion")
	}

	waitUntilPhase17(t, 5*time.Second, func() bool {
		leader.mu.RLock()
		commit := leader.commitIndex
		leader.mu.RUnlock()
		return commit > 1
	})

	for _, node := range nodes {
		waitUntilPhase17(t, 5*time.Second, func() bool {
			node.mu.RLock()
			defer node.mu.RUnlock()
			return node.commitIndex >= leader.commitIndex
		})

		matches := readLocalMatches(node)
		state, ok := matches["membership-match"]
		if !ok {
			t.Fatalf("%s missing membership-match after expansion", node.NodeID)
		}
		if state.Status != "active" {
			t.Fatalf("%s status = %q, want active", node.NodeID, state.Status)
		}
		if state.Counter < 1 {
			t.Fatalf("%s counter = %d, want at least 1 applied turn under load", node.NodeID, state.Counter)
		}
	}

	leaderMatches := readLocalMatches(leader)
	for _, node := range nodes {
		if node.NodeID == leader.NodeID {
			continue
		}
		followerMatches := readLocalMatches(node)
		if len(followerMatches) != len(leaderMatches) {
			t.Fatalf("%s match map size = %d, leader = %d", node.NodeID, len(followerMatches), len(leaderMatches))
		}
		for id, want := range leaderMatches {
			got, ok := followerMatches[id]
			if !ok || got != want {
				t.Fatalf("%s state for %s = %+v, want %+v", node.NodeID, id, got, want)
			}
		}
	}
}

func TestPhase17_HighVelocityReadsAreLinearizableWithoutLogAppends(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": phase17ReadPortBase + "1",
		"node-2": phase17ReadPortBase + "2",
		"node-3": phase17ReadPortBase + "3",
	}

	nodes := []*RaftNode{
		NewRaftNode("node-1", peers, NewLocalGameFSM()),
		NewRaftNode("node-2", peers, NewLocalGameFSM()),
		NewRaftNode("node-3", peers, NewLocalGameFSM()),
	}

	bindAddresses := []string{
		phase17ReadPortBase + "1",
		phase17ReadPortBase + "2",
		phase17ReadPortBase + "3",
	}

	transports := make([]*NetworkTransport, len(nodes))
	for i, node := range nodes {
		transport := NewNetworkTransport(ctx, node)
		transports[i] = transport
		if err := transport.StartServer(bindAddresses[i]); err != nil {
			t.Fatalf("start %s: %v", node.NodeID, err)
		}
		go node.runElectionTicker(ctx)
	}

	defer func() {
		testRPCObserver = nil
		testPersistObserver = nil
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	leader := waitForStableLeader(t, nodes)

	startCmd, _ := EncodeCommand(Command{Op: OpStartMatch, MatchID: "read-match"})
	turnCmd, _ := EncodeCommand(Command{Op: OpApplyTurn, MatchID: "read-match"})

	if _, err := leader.ProposeAndWait(startCmd); err != nil {
		t.Fatalf("start match: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := leader.ProposeAndWait(turnCmd); err != nil {
			t.Fatalf("apply turn %d: %v", i, err)
		}
	}

	writeStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-writeStop:
				return
			case <-ticker.C:
				currentLeader := findLeader(nodes)
				if currentLeader == nil || !currentLeader.IsLeader() {
					continue
				}
				_, _ = currentLeader.Propose(turnCmd)
			}
		}
	}()
	close(writeStop)
	time.Sleep(30 * time.Millisecond)

	baselineLogLen := nodeLastLogIndex(leader)
	var appendEntriesDuringRead int32
	var persistDuringRead int32

	testRPCObserver = func(method string) func() {
		if strings.HasSuffix(method, ".AppendEntries") {
			atomic.AddInt32(&appendEntriesDuringRead, 1)
		}
		return nil
	}
	testPersistObserver = func() {
		atomic.AddInt32(&persistDuringRead, 1)
	}

	const readRounds = 40
	var wg sync.WaitGroup
	errCh := make(chan error, readRounds)

	for i := 0; i < readRounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			currentLeader := findLeader(nodes)
			if currentLeader == nil {
				errCh <- fmt.Errorf("leader unavailable for read")
				return
			}
			matches, err := currentLeader.ReadLinearizableState()
			if err != nil {
				errCh <- err
				return
			}
			state, ok := matches["read-match"]
			if !ok {
				errCh <- fmt.Errorf("read missing read-match")
				return
			}
			if state.Status != "active" {
				errCh <- fmt.Errorf("read status = %q, want active", state.Status)
				return
			}
			if state.Counter < 5 {
				errCh <- fmt.Errorf("stale read counter = %d, want at least 5", state.Counter)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := atomic.LoadInt32(&appendEntriesDuringRead); got < 1 {
		t.Fatalf("ReadIndex heartbeats = %d, want at least 1 AppendEntries confirmation", got)
	}
	if got := atomic.LoadInt32(&persistDuringRead); got != 0 {
		t.Fatalf("linearizable reads triggered %d disk persist cycles, want 0", got)
	}

	afterLogLen := nodeLastLogIndex(leader)
	if afterLogLen != baselineLogLen {
		t.Fatalf("log grew during reads: before=%d after=%d", baselineLogLen, afterLogLen)
	}

	finalMatches, err := leader.ReadLinearizableState()
	if err != nil {
		t.Fatalf("final linearizable read: %v", err)
	}
	finalState := finalMatches["read-match"]
	if finalState.Counter < 5 {
		t.Fatalf("final counter = %d, want at least 5", finalState.Counter)
	}
}

func TestPhase17_NewNodeReceivesSnapshotWhenLogCompacted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": phase17SnapPortBase + "1",
		"node-2": phase17SnapPortBase + "2",
		"node-3": phase17SnapPortBase + "3",
	}

	nodes := []*RaftNode{
		NewRaftNode("node-1", clonePeerMap(peers), NewLocalGameFSM()),
		NewRaftNode("node-2", clonePeerMap(peers), NewLocalGameFSM()),
		NewRaftNode("node-3", clonePeerMap(peers), NewLocalGameFSM()),
	}
	nodes[0].SetCompactThreshold(8)

	transports := make([]*NetworkTransport, len(nodes))
	for i, node := range nodes {
		transport := NewNetworkTransport(ctx, node)
		transports[i] = transport
		addr := phase17SnapPortBase + fmt.Sprintf("%d", i+1)
		if err := transport.StartServer(addr); err != nil {
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

	leader := waitForStableLeader(t, nodes)
	leader.SetCompactThreshold(8)

	startCmd, _ := EncodeCommand(Command{Op: OpStartMatch, MatchID: "snap-join-match"})
	turnCmd, _ := EncodeCommand(Command{Op: OpApplyTurn, MatchID: "snap-join-match"})
	if _, err := leader.ProposeAndWait(startCmd); err != nil {
		t.Fatalf("start match: %v", err)
	}
	for i := 0; i < 11; i++ {
		if _, err := leader.ProposeAndWait(turnCmd); err != nil {
			t.Fatalf("apply turn %d: %v", i, err)
		}
	}
	waitUntilSnapshot(t, 5*time.Second, func() bool {
		return leader.SnapshotIndex() >= 8 && len(leader.lastSnapshot) > 0
	})

	if err := leader.ProposeAddNode("node-4", phase17SnapPortBase+"4"); err != nil {
		t.Fatalf("add node: %v", err)
	}

	expandedPeers := clonePeerMap(leader.PeerAddresses)
	joiner := NewRaftNode("node-4", expandedPeers, NewLocalGameFSM())
	joinTransport := NewNetworkTransport(ctx, joiner)
	if err := joinTransport.StartServer(phase17SnapPortBase + "4"); err != nil {
		t.Fatalf("start joiner: %v", err)
	}
	go joiner.runElectionTicker(ctx)
	assertPeerStarted(t, "node-4")
	defer func() {
		joinTransport.Shutdown()
		joinTransport.Wait()
	}()

	waitUntilPhase17(t, 8*time.Second, func() bool {
		leader.mu.RLock()
		targetCommit := leader.commitIndex
		snapIdx := leader.SnapshotIndex()
		term := leader.CurrentTerm
		leader.mu.RUnlock()
		leader.broadcastHeartbeats(term)

		joiner.mu.RLock()
		synced := joiner.snapshotIndex >= snapIdx &&
			joiner.commitIndex >= targetCommit &&
			joiner.joint == nil
		joiner.mu.RUnlock()
		return synced
	})

	leaderState := readLocalMatches(leader)["snap-join-match"]
	joinerState := readLocalMatches(joiner)["snap-join-match"]
	if joinerState != leaderState {
		t.Fatalf("snapshot-joined node = %+v, want %+v", joinerState, leaderState)
	}
	if joiner.SnapshotIndex() < leader.SnapshotIndex() {
		t.Fatalf("joiner snapshot index = %d, want >= %d", joiner.SnapshotIndex(), leader.SnapshotIndex())
	}
}

func assertPeerStarted(t *testing.T, nodeID string) {
	t.Helper()
	waitUntilPhase17(t, time.Second, func() bool {
		return PeerIsActive(nodeID)
	})
}

func findLeader(nodes []*RaftNode) *RaftNode {
	for _, node := range nodes {
		if node.IsLeader() {
			return node
		}
	}
	return nil
}

func waitUntilPhase17(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	waitUntilPhase13(t, timeout, cond)
}
