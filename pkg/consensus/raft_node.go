package consensus

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ErrNotLeader indicates the local node cannot accept replicated proposals.
var ErrNotLeader = errors.New("not leader")

// LeaderRedirectError carries leader metadata so gateway clients can re-route upstream.
type LeaderRedirectError struct {
	LeaderID      string
	LeaderAddress string
	Reason        string
}

func (e *LeaderRedirectError) Error() string {
	if e == nil {
		return "leader redirect required"
	}
	if e.Reason != "" {
		return e.Reason
	}
	return fmt.Sprintf("redirect to leader %s at %s", e.LeaderID, e.LeaderAddress)
}

// ApplyNotifier receives confirmed FSM apply results for gateway fan-out.
type ApplyNotifier func(result ApplyResult)

const defaultProposeTimeout = 2 * time.Second

// Raft role constants.
const (
	StateLeader    = "Leader"
	StateCandidate = "Candidate"
	StateFollower  = "Follower"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// RequestVoteArgs is the RPC payload for leader election.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RPC response for leader election.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// LogEntry is a single replicated command in the Raft log.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// AppendEntriesArgs is the RPC payload for log replication and heartbeats.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the RPC response for log replication.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// RaftNode encapsulates cluster metadata, election state, and the match FSM.
type RaftNode struct {
	mu sync.RWMutex

	NodeID        string
	PeerAddresses map[string]string
	CurrentTerm   uint64
	VotedFor      string
	State         string
	MatchFSM      GameFSM
	ElectionTimeout time.Duration

	log         []LogEntry
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	knownLeaderID   string
	knownLeaderTerm uint64

	applyResults map[uint64]interface{}
	applyWaiters map[uint64][]chan interface{}
	applyNotify  ApplyNotifier

	electionReset chan struct{}
	heartbeatStop chan struct{}
}

// NewRaftNode initializes a follower node wired to the provided match FSM.
func NewRaftNode(nodeID string, peers map[string]string, fsm GameFSM) *RaftNode {
	peerCopy := make(map[string]string, len(peers))
	for id, addr := range peers {
		peerCopy[id] = addr
	}

	return &RaftNode{
		NodeID:          nodeID,
		PeerAddresses:   peerCopy,
		CurrentTerm:     0,
		VotedFor:        "",
		State:           StateFollower,
		MatchFSM:        fsm,
		ElectionTimeout: randomElectionTimeout(),
		log:             []LogEntry{{Index: 0, Term: 0}},
		applyResults:    make(map[uint64]interface{}),
		applyWaiters:    make(map[uint64][]chan interface{}),
		electionReset:   make(chan struct{}, 1),
	}
}

// Start launches the background election ticker loop for the node.
func (n *RaftNode) Start(ctx context.Context) {
	go n.runElectionTicker(ctx)
}

// SetApplyNotifier registers a callback invoked after each committed FSM apply.
func (n *RaftNode) SetApplyNotifier(notifier ApplyNotifier) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyNotify = notifier
}

// RequestVote handles incoming vote requests during leader election.
func (n *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if reply == nil {
		return fmt.Errorf("request vote reply is nil")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.CurrentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	reply.Term = n.CurrentTerm
	reply.VoteGranted = false

	if args.Term < n.CurrentTerm {
		return nil
	}

	if n.VotedFor != "" && n.VotedFor != args.CandidateID {
		return nil
	}

	if !n.logIsUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		return nil
	}

	n.VotedFor = args.CandidateID
	reply.VoteGranted = true
	return nil
}

// AppendEntries handles log replication and leader heartbeats from the cluster leader.
func (n *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if reply == nil {
		return fmt.Errorf("append entries reply is nil")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Success = false

	if args.Term < n.CurrentTerm {
		reply.Term = n.CurrentTerm
		return nil
	}
	defer n.notifyHeartbeat()

	if args.Term > n.CurrentTerm || n.State != StateFollower {
		n.becomeFollowerLocked(args.Term)
	} else {
		n.CurrentTerm = args.Term
		n.VotedFor = ""
	}
	n.knownLeaderID = args.LeaderID
	n.knownLeaderTerm = args.Term

	if args.PrevLogIndex >= uint64(len(n.log)) {
		reply.Term = n.CurrentTerm
		return nil
	}

	prev := n.log[args.PrevLogIndex]
	if prev.Term != args.PrevLogTerm {
		reply.Term = n.CurrentTerm
		return nil
	}

	if len(args.Entries) > 0 {
		n.log = append(n.log[:args.PrevLogIndex+1], n.entriesFromArgs(args)...)
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := n.lastLogIndexLocked()
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
	}

	n.applyCommittedLocked()

	reply.Term = n.CurrentTerm
	reply.Success = true
	return nil
}

func (n *RaftNode) becomeFollowerLocked(term uint64) {
	n.stopHeartbeatTickerLocked()
	n.CurrentTerm = term
	n.VotedFor = ""
	n.State = StateFollower
	n.nextIndex = nil
	n.matchIndex = nil
}

func (n *RaftNode) runElectionTicker(ctx context.Context) {
	for {
		timeout := randomElectionTimeout()

		n.mu.Lock()
		n.ElectionTimeout = timeout
		n.mu.Unlock()

		timer := time.NewTimer(timeout)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-n.electionReset:
			timer.Stop()
		case <-timer.C:
			n.startElection(ctx)
		}
	}
}

func (n *RaftNode) startHeartbeatTicker(ctx context.Context) {
	n.mu.Lock()
	if n.heartbeatStop != nil {
		close(n.heartbeatStop)
	}
	stop := make(chan struct{})
	n.heartbeatStop = stop
	n.mu.Unlock()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			n.mu.RLock()
			isLeader := n.State == StateLeader
			term := n.CurrentTerm
			n.mu.RUnlock()

			if !isLeader {
				return
			}
			n.broadcastHeartbeats(term)
		}
	}
}

func (n *RaftNode) startElection(ctx context.Context) {
	n.mu.Lock()
	if n.State == StateLeader {
		n.mu.Unlock()
		return
	}

	n.CurrentTerm++
	term := n.CurrentTerm
	n.State = StateCandidate
	n.VotedFor = n.NodeID
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()

	peers := make(map[string]string, len(n.PeerAddresses))
	for peerID, addr := range n.PeerAddresses {
		if peerID != n.NodeID {
			peers[peerID] = addr
		}
	}
	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1
	n.mu.Unlock()

	votes := 1
	var voteMu sync.Mutex

	if votes >= majority {
		n.promoteToLeader(ctx, term)
		return
	}

	var wg sync.WaitGroup
	for peerID, addr := range peers {
		wg.Add(1)
		go func(peerID, addr string) {
			defer wg.Done()

			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.NodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if err := SendRPC(addr, peerID+".RequestVote", args, &reply); err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.CurrentTerm {
				n.becomeFollowerLocked(reply.Term)
				n.mu.Unlock()
				return
			}
			granted := reply.VoteGranted &&
				reply.Term == term &&
				n.State == StateCandidate &&
				n.CurrentTerm == term
			n.mu.Unlock()
			if !granted {
				return
			}

			voteMu.Lock()
			votes++
			reached := votes >= majority
			voteMu.Unlock()

			if reached {
				n.promoteToLeader(ctx, term)
			}
		}(peerID, addr)
	}
	wg.Wait()
}

func (n *RaftNode) promoteToLeader(ctx context.Context, term uint64) {
	n.mu.Lock()
	if n.State != StateCandidate || n.CurrentTerm != term {
		n.mu.Unlock()
		return
	}
	n.State = StateLeader
	n.initLeaderReplicationLocked()
	n.mu.Unlock()

	go n.startHeartbeatTicker(ctx)
}

func (n *RaftNode) initLeaderReplicationLocked() {
	lastIdx := n.lastLogIndexLocked()
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	for peerID := range n.PeerAddresses {
		if peerID == n.NodeID {
			continue
		}
		n.nextIndex[peerID] = lastIdx + 1
		n.matchIndex[peerID] = 0
	}
}

func (n *RaftNode) broadcastHeartbeats(term uint64) {
	type peerReplication struct {
		peerID       string
		addr         string
		prevLogIndex uint64
		prevLogTerm  uint64
		entries      []LogEntry
		leaderCommit uint64
	}

	n.mu.Lock()
	if n.State != StateLeader || n.CurrentTerm != term {
		n.mu.Unlock()
		return
	}

	leaderID := n.NodeID
	tasks := make([]peerReplication, 0, len(n.PeerAddresses))
	for peerID, addr := range n.PeerAddresses {
		if peerID == n.NodeID {
			continue
		}
		nextIdx := n.nextIndex[peerID]
		prevLogIndex := nextIdx - 1
		prevLogTerm := n.log[prevLogIndex].Term

		var entries []LogEntry
		if nextIdx < uint64(len(n.log)) {
			entries = append([]LogEntry(nil), n.log[nextIdx:]...)
		}

		tasks = append(tasks, peerReplication{
			peerID:       peerID,
			addr:         addr,
			prevLogIndex: prevLogIndex,
			prevLogTerm:  prevLogTerm,
			entries:      entries,
			leaderCommit: n.commitIndex,
		})
	}
	n.mu.Unlock()

	for _, task := range tasks {
		go func(task peerReplication) {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: task.prevLogIndex,
				PrevLogTerm:  task.prevLogTerm,
				Entries:      task.entries,
				LeaderCommit: task.leaderCommit,
			}
			var reply AppendEntriesReply
			if err := SendRPC(task.addr, task.peerID+".AppendEntries", args, &reply); err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.CurrentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if n.State != StateLeader || n.CurrentTerm != term {
				return
			}

			if reply.Success {
				n.matchIndex[task.peerID] = args.PrevLogIndex + uint64(len(args.Entries))
				n.nextIndex[task.peerID] = n.matchIndex[task.peerID] + 1
				n.advanceCommitIndexLocked()
				return
			}

			if n.nextIndex[task.peerID] > 1 {
				n.nextIndex[task.peerID]--
			}
		}(task)
	}
}

func (n *RaftNode) advanceCommitIndexLocked() {
	majority := len(n.PeerAddresses)/2 + 1
	lastIdx := n.lastLogIndexLocked()

	for idx := lastIdx; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.CurrentTerm {
			continue
		}

		replicated := 1
		for peerID := range n.matchIndex {
			if n.matchIndex[peerID] >= idx {
				replicated++
			}
		}
		if replicated >= majority {
			n.commitIndex = idx
			n.applyCommittedLocked()
			return
		}
	}
}

// Propose appends a command to the leader log and immediately replicates it to followers.
func (n *RaftNode) Propose(command []byte) (uint64, error) {
	n.mu.Lock()
	if n.State != StateLeader {
		redirect := n.leaderRedirectLocked()
		n.mu.Unlock()
		if redirect != nil {
			return 0, redirect
		}
		return 0, ErrNotLeader
	}

	index := n.lastLogIndexLocked() + 1
	n.log = append(n.log, LogEntry{
		Index:   index,
		Term:    n.CurrentTerm,
		Command: append([]byte(nil), command...),
	})
	term := n.CurrentTerm
	n.mu.Unlock()

	n.broadcastHeartbeats(term)
	return index, nil
}

// ProposeAndWait appends a command on the leader, waits for quorum commit and FSM apply,
// and returns the deterministic Apply result.
func (n *RaftNode) ProposeAndWait(command []byte) (interface{}, error) {
	return n.ProposeAndWaitTimeout(command, defaultProposeTimeout)
}

// ProposeAndWaitTimeout is ProposeAndWait with an explicit commit/apply deadline.
func (n *RaftNode) ProposeAndWaitTimeout(command []byte, timeout time.Duration) (interface{}, error) {
	index, err := n.Propose(command)
	if err != nil {
		return nil, err
	}
	if err := n.waitForCommit(index, timeout); err != nil {
		return nil, err
	}
	return n.waitForApply(index, timeout)
}

// LeaderEndpoint returns the current cluster leader identity and routable address.
func (n *RaftNode) LeaderEndpoint() (leaderID, leaderAddress string, err error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.State == StateLeader {
		addr, ok := n.PeerAddresses[n.NodeID]
		if !ok {
			return n.NodeID, "", fmt.Errorf("leader address for %q is not configured", n.NodeID)
		}
		return n.NodeID, addr, nil
	}

	if n.knownLeaderID == "" {
		return "", "", fmt.Errorf("cluster leader is unknown")
	}
	addr, ok := n.PeerAddresses[n.knownLeaderID]
	if !ok {
		return n.knownLeaderID, "", fmt.Errorf("leader address for %q is not configured", n.knownLeaderID)
	}
	return n.knownLeaderID, addr, nil
}

// IsLeader reports whether this node is the active Raft leader.
func (n *RaftNode) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State == StateLeader
}

func (n *RaftNode) leaderRedirectLocked() *LeaderRedirectError {
	if n.State == StateLeader {
		return nil
	}
	leaderID := n.knownLeaderID
	if leaderID == "" {
		return &LeaderRedirectError{Reason: "cluster leader is unknown"}
	}
	addr, ok := n.PeerAddresses[leaderID]
	if !ok {
		return &LeaderRedirectError{
			LeaderID: leaderID,
			Reason:   fmt.Sprintf("leader address for %q is not configured", leaderID),
		}
	}
	return &LeaderRedirectError{
		LeaderID:      leaderID,
		LeaderAddress: addr,
		Reason:        "submit in-game actions to the cluster leader",
	}
}

func (n *RaftNode) waitForCommit(index uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		n.mu.RLock()
		committed := n.commitIndex >= index
		term := n.CurrentTerm
		n.mu.RUnlock()
		if committed {
			return nil
		}
		n.broadcastHeartbeats(term)
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for commit at index %d", index)
}

func (n *RaftNode) waitForApply(index uint64, timeout time.Duration) (interface{}, error) {
	waiter := make(chan interface{}, 1)

	n.mu.Lock()
	if result, ok := n.applyResults[index]; ok {
		n.mu.Unlock()
		return result, nil
	}
	n.applyWaiters[index] = append(n.applyWaiters[index], waiter)
	n.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-waiter:
		return result, nil
	case <-timer.C:
		return nil, fmt.Errorf("timed out waiting for replicated apply at index %d", index)
	}
}

// ReadLinearizableState confirms leadership with a quorum, then reads from the local GameFSM.
func (n *RaftNode) ReadLinearizableState() (map[string]matchState, error) {
	if err := n.VerifyLeader(); err != nil {
		return nil, err
	}

	fsm, ok := n.MatchFSM.(*LocalGameFSM)
	if !ok {
		return nil, fmt.Errorf("MatchFSM is not LocalGameFSM")
	}
	return fsm.Matches(), nil
}

// VerifyLeader confirms leadership with a quorum via rapid AppendEntries heartbeats.
// Callers may read directly from the GameFSM after a successful verification.
func (n *RaftNode) VerifyLeader() error {
	n.mu.Lock()
	if n.State != StateLeader {
		n.mu.Unlock()
		return fmt.Errorf("not leader")
	}
	term := n.CurrentTerm
	majority := len(n.PeerAddresses)/2 + 1

	peers := make(map[string]string, len(n.PeerAddresses))
	for peerID, addr := range n.PeerAddresses {
		if peerID != n.NodeID {
			peers[peerID] = addr
		}
	}
	n.mu.Unlock()

	if len(peers) == 0 {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.State != StateLeader || n.CurrentTerm != term {
			return fmt.Errorf("leadership lost")
		}
		return nil
	}

	confirmations := 1
	var confirmMu sync.Mutex
	var wg sync.WaitGroup

	for peerID, addr := range peers {
		wg.Add(1)
		go func(peerID, addr string) {
			defer wg.Done()

			n.mu.Lock()
			if n.State != StateLeader || n.CurrentTerm != term {
				n.mu.Unlock()
				return
			}
			prevLogIndex := n.lastLogIndexLocked()
			prevLogTerm := n.lastLogTermLocked()
			leaderCommit := n.commitIndex
			leaderID := n.NodeID
			n.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: leaderCommit,
			}
			var reply AppendEntriesReply
			if err := SendRPC(addr, peerID+".AppendEntries", args, &reply); err != nil {
				return
			}

			if reply.Term > term {
				n.mu.Lock()
				if reply.Term > n.CurrentTerm {
					n.becomeFollowerLocked(reply.Term)
				}
				n.mu.Unlock()
				return
			}

			if reply.Success && reply.Term == term {
				confirmMu.Lock()
				confirmations++
				confirmMu.Unlock()
			}
		}(peerID, addr)
	}

	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != StateLeader || n.CurrentTerm != term {
		return fmt.Errorf("leadership lost during verification")
	}
	if confirmations >= majority {
		return nil
	}
	return fmt.Errorf("failed to confirm leadership with quorum")
}

func (n *RaftNode) notifyHeartbeat() {
	select {
	case n.electionReset <- struct{}{}:
	default:
	}
}

func (n *RaftNode) stopHeartbeatTickerLocked() {
	if n.heartbeatStop != nil {
		close(n.heartbeatStop)
		n.heartbeatStop = nil
	}
}

func randomElectionTimeout() time.Duration {
	delta := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(delta)+1))
}

func (n *RaftNode) lastLogIndexLocked() uint64 {
	return uint64(len(n.log) - 1)
}

func (n *RaftNode) lastLogTermLocked() uint64 {
	return n.log[len(n.log)-1].Term
}

func (n *RaftNode) logIsUpToDateLocked(lastLogIndex, lastLogTerm uint64) bool {
	localIndex := n.lastLogIndexLocked()
	localTerm := n.lastLogTermLocked()

	if lastLogTerm != localTerm {
		return lastLogTerm > localTerm
	}
	return lastLogIndex >= localIndex
}

func (n *RaftNode) entriesFromArgs(args AppendEntriesArgs) []LogEntry {
	out := make([]LogEntry, len(args.Entries))
	for i, entry := range args.Entries {
		index := entry.Index
		if index == 0 {
			index = args.PrevLogIndex + 1 + uint64(i)
		}
		term := entry.Term
		if term == 0 {
			term = args.Term
		}
		out[i] = LogEntry{
			Index:   index,
			Term:    term,
			Command: append([]byte(nil), entry.Command...),
		}
	}
	return out
}

func (n *RaftNode) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		index := n.lastApplied
		result := n.MatchFSM.Apply(n.log[index].Command)
		n.recordApplyResultLocked(index, result)
		n.dispatchApplyNotification(result)
	}
}

func (n *RaftNode) recordApplyResultLocked(index uint64, result interface{}) {
	n.applyResults[index] = result
	waiters := n.applyWaiters[index]
	delete(n.applyWaiters, index)
	for _, waiter := range waiters {
		select {
		case waiter <- result:
		default:
		}
	}
}

func (n *RaftNode) dispatchApplyNotification(result interface{}) {
	if n.applyNotify == nil {
		return
	}
	applyResult, ok := AsApplyResult(result)
	if !ok || !applyResult.OK {
		return
	}
	n.applyNotify(applyResult)
}
