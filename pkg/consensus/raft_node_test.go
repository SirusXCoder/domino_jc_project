package consensus

import "testing"

func TestRaftNode_RequestVote_StaleTerm(t *testing.T) {
	node := NewRaftNode("node-1", nil, NewLocalGameFSM())

	node.mu.Lock()
	node.CurrentTerm = 5
	node.VotedFor = ""
	node.State = StateFollower
	node.mu.Unlock()

	args := RequestVoteArgs{
		Term:         3,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	var reply RequestVoteReply

	if err := node.RequestVote(args, &reply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected for stale candidate term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.CurrentTerm != 5 {
		t.Fatalf("CurrentTerm = %d, want 5", node.CurrentTerm)
	}
	if node.VotedFor != "" {
		t.Fatalf("VotedFor = %q, want empty", node.VotedFor)
	}
	if node.State != StateFollower {
		t.Fatalf("State = %q, want %q", node.State, StateFollower)
	}
}

func TestRaftNode_RequestVote_Grant(t *testing.T) {
	node := NewRaftNode("node-1", nil, NewLocalGameFSM())

	node.mu.Lock()
	node.CurrentTerm = 2
	node.VotedFor = ""
	node.State = StateFollower
	node.mu.Unlock()

	args := RequestVoteArgs{
		Term:         5,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	var reply RequestVoteReply

	if err := node.RequestVote(args, &reply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted for valid higher-term candidate")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.CurrentTerm != 5 {
		t.Fatalf("CurrentTerm = %d, want 5", node.CurrentTerm)
	}
	if node.VotedFor != "node-2" {
		t.Fatalf("VotedFor = %q, want node-2", node.VotedFor)
	}
	if node.State != StateFollower {
		t.Fatalf("State = %q, want %q", node.State, StateFollower)
	}
}

func TestRaftNode_AppendEntries_StepDown(t *testing.T) {
	cases := []struct {
		name       string
		initialState string
	}{
		{name: "candidate steps down", initialState: StateCandidate},
		{name: "leader steps down", initialState: StateLeader},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := NewRaftNode("node-1", nil, NewLocalGameFSM())

			node.mu.Lock()
			node.CurrentTerm = 3
			node.VotedFor = "node-1"
			node.State = tc.initialState
			node.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         5,
				LeaderID:     "node-2",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
			}
			var reply AppendEntriesReply

			if err := node.AppendEntries(args, &reply); err != nil {
				t.Fatalf("AppendEntries: %v", err)
			}

			if reply.Term != 5 {
				t.Fatalf("reply.Term = %d, want 5", reply.Term)
			}

			node.mu.RLock()
			defer node.mu.RUnlock()

			if node.CurrentTerm != 5 {
				t.Fatalf("CurrentTerm = %d, want 5", node.CurrentTerm)
			}
			if node.State != StateFollower {
				t.Fatalf("State = %q, want %q", node.State, StateFollower)
			}
			if node.VotedFor != "" {
				t.Fatalf("VotedFor = %q, want empty after step down", node.VotedFor)
			}
		})
	}
}
