package consensus

import (
	"context"
	"testing"
)

func TestTransport_ClusterRoundTrip(t *testing.T) {
	ctx := context.Background()

	peers := map[string]string{
		"node-1": "127.0.0.1:8001",
		"node-2": "127.0.0.1:8002",
	}

	node1 := NewRaftNode("node-1", peers, NewLocalGameFSM())
	node2 := NewRaftNode("node-2", peers, NewLocalGameFSM())

	transport1 := NewNetworkTransport(ctx, node1)
	transport2 := NewNetworkTransport(ctx, node2)

	if err := transport1.StartServer("127.0.0.1:8001"); err != nil {
		t.Fatalf("start node-1 server: %v", err)
	}
	if err := transport2.StartServer("127.0.0.1:8002"); err != nil {
		t.Fatalf("start node-2 server: %v", err)
	}

	defer func() {
		transport1.Shutdown()
		transport1.Wait()
		transport2.Shutdown()
		transport2.Wait()
	}()

	args := RequestVoteArgs{
		Term:         1,
		CandidateID:  "node-1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	var reply RequestVoteReply

	if err := SendRPC("127.0.0.1:8002", "node-2.RequestVote", args, &reply); err != nil {
		t.Fatalf("SendRPC: %v", err)
	}

	if !reply.VoteGranted {
		t.Fatal("expected node-2 to grant vote for valid higher-term request")
	}
	if reply.Term != 1 {
		t.Fatalf("reply.Term = %d, want 1", reply.Term)
	}

	node2.mu.RLock()
	defer node2.mu.RUnlock()

	if node2.CurrentTerm != 1 {
		t.Fatalf("node2.CurrentTerm = %d, want 1", node2.CurrentTerm)
	}
	if node2.VotedFor != "node-1" {
		t.Fatalf("node2.VotedFor = %q, want node-1", node2.VotedFor)
	}
	if node2.State != StateFollower {
		t.Fatalf("node2.State = %q, want %q", node2.State, StateFollower)
	}
}
