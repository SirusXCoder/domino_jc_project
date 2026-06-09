package consensus

import (
	"context"
	"testing"
	"time"
)

func TestCluster_AutomatedLeaderElection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := map[string]string{
		"node-1": "127.0.0.1:9001",
		"node-2": "127.0.0.1:9002",
		"node-3": "127.0.0.1:9003",
	}

	nodes := []*RaftNode{
		NewRaftNode("node-1", peers, NewLocalGameFSM()),
		NewRaftNode("node-2", peers, NewLocalGameFSM()),
		NewRaftNode("node-3", peers, NewLocalGameFSM()),
	}

	bindAddresses := []string{
		"127.0.0.1:9001",
		"127.0.0.1:9002",
		"127.0.0.1:9003",
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

	defer func() {
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
		cancel()
	}()

	time.Sleep(500 * time.Millisecond)

	leaderCount, followerCount, leaderID, clusterTerm := clusterRoleSnapshot(nodes)
	if leaderCount != 1 {
		t.Fatalf("leader count = %d, want exactly 1", leaderCount)
	}
	if followerCount != 2 {
		t.Fatalf("follower count = %d, want exactly 2", followerCount)
	}
	if leaderID == "" {
		t.Fatal("expected a leader to be elected")
	}

	// Wait beyond the max election timeout to confirm heartbeats prevent re-election.
	time.Sleep(400 * time.Millisecond)

	leaderCount, followerCount, leaderIDAfter, clusterTermAfter := clusterRoleSnapshot(nodes)
	if leaderCount != 1 {
		t.Fatalf("leader count after stabilization = %d, want exactly 1", leaderCount)
	}
	if followerCount != 2 {
		t.Fatalf("follower count after stabilization = %d, want exactly 2", followerCount)
	}
	if leaderIDAfter != leaderID {
		t.Fatalf("leader changed from %q to %q during stabilization", leaderID, leaderIDAfter)
	}
	if clusterTermAfter != clusterTerm {
		t.Fatalf("cluster term drifted from %d to %d; secondary election likely occurred", clusterTerm, clusterTermAfter)
	}

	for _, node := range nodes {
		node.mu.RLock()
		state := node.State
		term := node.CurrentTerm
		node.mu.RUnlock()

		if state == StateCandidate {
			t.Fatalf("%s remained in %q after stabilization", node.NodeID, StateCandidate)
		}
		if term != clusterTerm {
			t.Fatalf("%s term = %d, want stable cluster term %d", node.NodeID, term, clusterTerm)
		}
	}
}

func clusterRoleSnapshot(nodes []*RaftNode) (leaderCount, followerCount int, leaderID string, clusterTerm uint64) {
	for _, node := range nodes {
		node.mu.RLock()
		state := node.State
		term := node.CurrentTerm
		id := node.NodeID
		node.mu.RUnlock()

		switch state {
		case StateLeader:
			leaderCount++
			leaderID = id
			clusterTerm = term
		case StateFollower:
			followerCount++
		}
	}

	return leaderCount, followerCount, leaderID, clusterTerm
}
