package consensus

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"domino_jc_project/pkg/database"
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
	// defaultReplicationRoundTimeout bounds a full leader fan-out replication cycle.
	defaultReplicationRoundTimeout = 500 * time.Millisecond
)

type replicationTask struct {
	peerID       string
	addr         string
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []LogEntry
	leaderCommit uint64
	installSnap  bool
}

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

// InstallSnapshotArgs ships a compacted FSM image to a lagging follower.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotReply is the RPC response for snapshot installation.
type InstallSnapshotReply struct {
	Term uint64
}

const defaultCompactThreshold uint64 = 1000

// jointConfig holds old-new membership during a Raft configuration transition.
type jointConfig struct {
	old map[string]string
	new map[string]string
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

	joint *jointConfig

	knownLeaderID   string
	knownLeaderTerm uint64

	applyResults map[uint64]interface{}
	applyWaiters map[uint64][]chan interface{}
	applyNotify  ApplyNotifier

	storage          *database.RaftStorage
	compactThreshold uint64
	snapshotIndex    uint64
	snapshotTerm     uint64
	lastSnapshot     []byte

	electionReset chan struct{}
	heartbeatStop chan struct{}
}

// testPersistObserver counts durable log writes during integration tests.
var testPersistObserver func()

// NewRaftNode initializes a follower node wired to the provided match FSM.
func NewRaftNode(nodeID string, peers map[string]string, fsm GameFSM) *RaftNode {
	peerCopy := make(map[string]string, len(peers))
	for id, addr := range peers {
		peerCopy[id] = addr
	}

	return &RaftNode{
		NodeID:           nodeID,
		PeerAddresses:    peerCopy,
		CurrentTerm:      0,
		VotedFor:         "",
		State:            StateFollower,
		MatchFSM:         fsm,
		ElectionTimeout:  randomElectionTimeout(),
		log:              []LogEntry{{Index: 0, Term: 0}},
		applyResults:     make(map[uint64]interface{}),
		applyWaiters:     make(map[uint64][]chan interface{}),
		compactThreshold: defaultCompactThreshold,
		electionReset:    make(chan struct{}, 1),
	}
}

// NewRaftNodeWithStorage bootstraps a node from durable storage when present.
func NewRaftNodeWithStorage(nodeID string, peers map[string]string, fsm GameFSM, storage *database.RaftStorage) (*RaftNode, error) {
	node := NewRaftNode(nodeID, peers, fsm)
	node.storage = storage
	if storage == nil {
		return node, nil
	}
	if err := node.bootstrapFromStorage(); err != nil {
		return nil, err
	}
	return node, nil
}

// SetCompactThreshold configures how many uncompacted log entries trigger compaction.
func (n *RaftNode) SetCompactThreshold(threshold uint64) {
	if threshold == 0 {
		threshold = defaultCompactThreshold
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.compactThreshold = threshold
}

// SnapshotIndex returns the trailing index of the latest installed snapshot.
func (n *RaftNode) SnapshotIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.snapshotIndex
}

// UncompactedLogEntries returns the number of log entries after the latest snapshot.
func (n *RaftNode) UncompactedLogEntries() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.uncompactedLogEntriesLocked()
}

// LogLength returns the in-memory Raft log length including the sentinel entry.
func (n *RaftNode) LogLength() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.log)
}

// FlushStorage persists the current in-memory log and metadata to disk.
func (n *RaftNode) FlushStorage() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.persistDurableStateLocked()
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

	if args.PrevLogIndex < n.snapshotIndex {
		reply.Term = n.CurrentTerm
		return nil
	}

	prevOffset := n.logOffsetForIndex(args.PrevLogIndex)
	if prevOffset >= len(n.log) {
		reply.Term = n.CurrentTerm
		return nil
	}

	prev := n.log[prevOffset]
	if prev.Term != args.PrevLogTerm {
		reply.Term = n.CurrentTerm
		return nil
	}

	if len(args.Entries) > 0 {
		n.log = append(n.log[:prevOffset+1], n.entriesFromArgs(args)...)
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
	if n.storage != nil {
		_ = n.persistDurableStateLocked()
	}

	reply.Term = n.CurrentTerm
	reply.Success = true
	return nil
}

// InstallSnapshot rebuilds local FSM state from a leader snapshot when log entries were compacted away.
func (n *RaftNode) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if reply == nil {
		return fmt.Errorf("install snapshot reply is nil")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.CurrentTerm

	if args.Term < n.CurrentTerm {
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

	if args.LastIncludedIndex <= n.snapshotIndex {
		return nil
	}

	if err := n.MatchFSM.Restore(args.Data); err != nil {
		return fmt.Errorf("restore snapshot at index %d: %w", args.LastIncludedIndex, err)
	}

	tailOffset := n.logOffsetForIndex(args.LastIncludedIndex) + 1
	var tail []LogEntry
	if tailOffset > 0 && tailOffset < len(n.log) {
		tail = append([]LogEntry(nil), n.log[tailOffset:]...)
	}

	n.snapshotIndex = args.LastIncludedIndex
	n.snapshotTerm = args.LastIncludedTerm
	n.lastSnapshot = append([]byte(nil), args.Data...)
	n.lastApplied = args.LastIncludedIndex
	n.log = append([]LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}, tail...)

	if args.LastIncludedIndex < n.commitIndex {
		n.commitIndex = args.LastIncludedIndex
	}

	if n.storage != nil {
		if err := n.storage.PersistSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data); err != nil {
			return fmt.Errorf("persist installed snapshot: %w", err)
		}
		if err := n.persistDurableStateLocked(); err != nil {
			return err
		}
	}

	reply.Term = n.CurrentTerm
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
	majority := n.electionMajorityLocked()
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
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultReplicationRoundTimeout)
		defer cancel()
		_ = n.replicateAndCommit(ctx, term)
	}()
}

// replicateAndCommit fans out AppendEntries or InstallSnapshot RPCs to peers and
// waits for the round to finish or the context to expire. Unresponsive peers are
// skipped without blocking the caller indefinitely.
func (n *RaftNode) replicateAndCommit(ctx context.Context, term uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}

	tasks, leaderID, ok := n.collectReplicationTasks(term)
	if !ok || len(tasks) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(task replicationTask) {
			defer wg.Done()
			n.replicateToPeer(ctx, term, leaderID, task)
		}(task)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (n *RaftNode) collectReplicationTasks(term uint64) (tasks []replicationTask, leaderID string, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != StateLeader || n.CurrentTerm != term {
		return nil, "", false
	}

	leaderID = n.NodeID
	tasks = make([]replicationTask, 0, len(n.PeerAddresses))
	for peerID, addr := range n.PeerAddresses {
		if peerID == n.NodeID {
			continue
		}
		nextIdx := n.nextIndex[peerID]
		if nextIdx <= n.snapshotIndex {
			tasks = append(tasks, replicationTask{
				peerID:      peerID,
				addr:        addr,
				installSnap: true,
			})
			continue
		}

		prevLogIndex := nextIdx - 1
		prevOffset := n.logOffsetForIndex(prevLogIndex)
		prevLogTerm := n.log[prevOffset].Term

		var entries []LogEntry
		startOffset := n.logOffsetForIndex(nextIdx)
		if startOffset < len(n.log) {
			entries = append([]LogEntry(nil), n.log[startOffset:]...)
		}

		tasks = append(tasks, replicationTask{
			peerID:       peerID,
			addr:         addr,
			prevLogIndex: prevLogIndex,
			prevLogTerm:  prevLogTerm,
			entries:      entries,
			leaderCommit: n.commitIndex,
		})
	}
	return tasks, leaderID, true
}

func (n *RaftNode) replicateToPeer(ctx context.Context, term uint64, leaderID string, task replicationTask) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	if task.installSnap {
		n.sendInstallSnapshotWithContext(ctx, task.peerID, task.addr, term)
		return
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: task.prevLogIndex,
		PrevLogTerm:  task.prevLogTerm,
		Entries:      task.entries,
		LeaderCommit: task.leaderCommit,
	}
	var reply AppendEntriesReply
	if err := SendRPCWithContext(ctx, task.addr, task.peerID+".AppendEntries", args, &reply); err != nil {
		return
	}
	n.handleAppendEntriesReply(term, task.peerID, task.addr, args, reply)
}

func (n *RaftNode) handleAppendEntriesReply(term uint64, peerID, addr string, args AppendEntriesArgs, reply AppendEntriesReply) {
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
		n.matchIndex[peerID] = args.PrevLogIndex + uint64(len(args.Entries))
		n.nextIndex[peerID] = n.matchIndex[peerID] + 1
		n.advanceCommitIndexLocked()
		return
	}

	if n.nextIndex[peerID] <= n.snapshotIndex+1 {
		go n.sendInstallSnapshot(peerID, addr, term)
		return
	}

	if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
}

func (n *RaftNode) advanceCommitIndexLocked() {
	lastIdx := n.lastLogIndexLocked()

	for idx := lastIdx; idx > n.commitIndex; idx-- {
		offset := n.logOffsetForIndex(idx)
		if offset < 0 || offset >= len(n.log) || n.log[offset].Term != n.CurrentTerm {
			continue
		}

		if n.quorumMetForIndexLocked(idx) {
			n.commitIndex = idx
			n.applyCommittedLocked()
			return
		}
	}
}

func (n *RaftNode) quorumMetForIndexLocked(index uint64) bool {
	if n.joint != nil {
		return n.quorumMetInConfigLocked(index, n.joint.old) &&
			n.quorumMetInConfigLocked(index, n.joint.new)
	}
	return n.quorumMetInConfigLocked(index, n.PeerAddresses)
}

func (n *RaftNode) quorumMetInConfigLocked(index uint64, config map[string]string) bool {
	voters := n.activeVotersInConfigLocked(config)
	if voters == 0 {
		return true
	}
	majority := voters/2 + 1
	return n.replicationCountInConfigLocked(index, config) >= majority
}

func (n *RaftNode) activeVotersInConfigLocked(config map[string]string) int {
	count := 0
	for peerID := range config {
		if n.isPendingNewPeerLocked(peerID) {
			continue
		}
		count++
	}
	return count
}

func (n *RaftNode) isPendingNewPeerLocked(peerID string) bool {
	if n.joint == nil {
		return false
	}
	if _, inOld := n.joint.old[peerID]; inOld {
		return false
	}
	if n.matchIndex == nil {
		return true
	}
	return n.matchIndex[peerID] == 0
}

func (n *RaftNode) replicationCountInConfigLocked(index uint64, config map[string]string) int {
	count := 0
	for peerID := range config {
		if n.isPendingNewPeerLocked(peerID) {
			continue
		}
		if peerID == n.NodeID {
			count++
			continue
		}
		if n.matchIndex != nil && n.matchIndex[peerID] >= index {
			count++
		}
	}
	return count
}

func (n *RaftNode) electionMajorityLocked() int {
	config := n.PeerAddresses
	if n.joint != nil {
		config = n.joint.old
	}
	return len(config)/2 + 1
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
	if n.storage != nil {
		_ = n.persistDurableStateLocked()
	}
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

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		roundTimeout := defaultReplicationRoundTimeout
		if remaining < roundTimeout {
			roundTimeout = remaining
		}
		ctx, cancel := context.WithTimeout(context.Background(), roundTimeout)
		_ = n.replicateAndCommit(ctx, term)
		cancel()

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

// ReadLinearizableState performs a ReadIndex linearizable read without appending to the log.
func (n *RaftNode) ReadLinearizableState() (map[string]matchState, error) {
	readIndex, term, err := n.beginReadIndex()
	if err != nil {
		return nil, err
	}
	if err := n.confirmLeadershipForRead(term, readIndex); err != nil {
		return nil, err
	}
	if err := n.waitForReadIndex(readIndex, defaultProposeTimeout); err != nil {
		return nil, err
	}

	fsm, ok := n.MatchFSM.(*LocalGameFSM)
	if !ok {
		return nil, fmt.Errorf("MatchFSM is not LocalGameFSM")
	}
	return fsm.Matches(), nil
}

// beginReadIndex records the current commit index for a linearizable read.
func (n *RaftNode) beginReadIndex() (readIndex, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != StateLeader {
		if redirect := n.leaderRedirectLocked(); redirect != nil {
			return 0, 0, redirect
		}
		return 0, 0, ErrNotLeader
	}
	return n.commitIndex, n.CurrentTerm, nil
}

// waitForReadIndex blocks until the state machine has applied through readIndex.
func (n *RaftNode) waitForReadIndex(readIndex uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n.mu.RLock()
		ready := n.lastApplied >= readIndex
		n.mu.RUnlock()
		if ready {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for read index %d", readIndex)
}

// VerifyLeader confirms leadership with a quorum via rapid AppendEntries heartbeats.
func (n *RaftNode) VerifyLeader() error {
	readIndex, term, err := n.beginReadIndex()
	if err != nil {
		return err
	}
	return n.confirmLeadershipForRead(term, readIndex)
}

func (n *RaftNode) confirmLeadershipForRead(term, readIndex uint64) error {
	n.mu.Lock()
	if n.State != StateLeader || n.CurrentTerm != term {
		n.mu.Unlock()
		return fmt.Errorf("leadership lost")
	}
	majority := n.electionMajorityLocked()
	peers := n.remotePeersLocked()
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
			if leaderCommit < readIndex {
				leaderCommit = readIndex
			}
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

func (n *RaftNode) remotePeersLocked() map[string]string {
	peers := make(map[string]string, len(n.PeerAddresses))
	for peerID, addr := range n.PeerAddresses {
		if peerID != n.NodeID {
			peers[peerID] = addr
		}
	}
	return peers
}

// IsInJointConsensus reports whether the node is in an old-new configuration transition.
func (n *RaftNode) IsInJointConsensus() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.joint != nil
}

// ProposeAddNode replicates a joint-consensus add-node transition and finalizes the new config.
func (n *RaftNode) ProposeAddNode(nodeID, address string) error {
	cmd, err := EncodeAddNodeCommand(nodeID, address)
	if err != nil {
		return err
	}
	if _, err := n.ProposeAndWait(cmd); err != nil {
		return err
	}
	commitCmd, err := EncodeCommitConfigCommand()
	if err != nil {
		return err
	}
	_, err = n.ProposeAndWait(commitCmd)
	return err
}

// ProposeRemoveNode replicates a joint-consensus remove-node transition and finalizes the new config.
func (n *RaftNode) ProposeRemoveNode(nodeID string) error {
	cmd, err := EncodeRemoveNodeCommand(nodeID)
	if err != nil {
		return err
	}
	if _, err := n.ProposeAndWait(cmd); err != nil {
		return err
	}
	commitCmd, err := EncodeCommitConfigCommand()
	if err != nil {
		return err
	}
	_, err = n.ProposeAndWait(commitCmd)
	return err
}

func (n *RaftNode) applyMembershipLocked(cmd Command) error {
	switch cmd.Op {
	case OpAddNode:
		add, err := DecodeAddNodeCommand(cmd.Payload)
		if err != nil {
			return err
		}
		if _, ok := n.PeerAddresses[add.NodeID]; ok {
			return fmt.Errorf("node %q already in cluster", add.NodeID)
		}
		if n.joint != nil {
			return fmt.Errorf("already in joint consensus")
		}
		newPeers := clonePeerMap(n.PeerAddresses)
		newPeers[add.NodeID] = add.Address
		n.joint = &jointConfig{
			old: clonePeerMap(n.PeerAddresses),
			new: newPeers,
		}
		n.PeerAddresses = clonePeerMap(newPeers)
		n.trackNewPeerLocked(add.NodeID)
		return nil

	case OpRemoveNode:
		remove, err := DecodeRemoveNodeCommand(cmd.Payload)
		if err != nil {
			return err
		}
		if _, ok := n.PeerAddresses[remove.NodeID]; !ok {
			return fmt.Errorf("node %q not in cluster", remove.NodeID)
		}
		if n.joint != nil {
			return fmt.Errorf("already in joint consensus")
		}
		newPeers := clonePeerMap(n.PeerAddresses)
		delete(newPeers, remove.NodeID)
		n.joint = &jointConfig{
			old: clonePeerMap(n.PeerAddresses),
			new: newPeers,
		}
		n.PeerAddresses = clonePeerMap(newPeers)
		n.dropPeerLocked(remove.NodeID)
		return nil

	case OpCommitConfig:
		if n.joint == nil {
			return fmt.Errorf("not in joint consensus")
		}
		n.PeerAddresses = clonePeerMap(n.joint.new)
		n.joint = nil
		n.pruneReplicationStateLocked()
		return nil

	default:
		return fmt.Errorf("unknown membership op %q", cmd.Op)
	}
}

func (n *RaftNode) trackNewPeerLocked(peerID string) {
	if n.State != StateLeader || n.nextIndex == nil {
		return
	}
	lastIdx := n.lastLogIndexLocked()
	n.nextIndex[peerID] = lastIdx + 1
	n.matchIndex[peerID] = 0
}

func (n *RaftNode) dropPeerLocked(peerID string) {
	if n.nextIndex != nil {
		delete(n.nextIndex, peerID)
	}
	if n.matchIndex != nil {
		delete(n.matchIndex, peerID)
	}
}

func (n *RaftNode) pruneReplicationStateLocked() {
	if n.nextIndex == nil {
		return
	}
	for peerID := range n.nextIndex {
		if _, ok := n.PeerAddresses[peerID]; !ok {
			delete(n.nextIndex, peerID)
			delete(n.matchIndex, peerID)
		}
	}
}

func clonePeerMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for id, addr := range in {
		out[id] = addr
	}
	return out
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
	if len(n.log) == 0 {
		return n.snapshotIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return n.snapshotTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *RaftNode) logOffsetForIndex(index uint64) int {
	if index < n.snapshotIndex {
		return -1
	}
	return int(index - n.snapshotIndex)
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
		offset := n.logOffsetForIndex(index)
		if offset < 0 || offset >= len(n.log) {
			n.lastApplied--
			break
		}
		entry := n.log[offset]
		cmd, err := DecodeCommand(entry.Command)
		if err != nil {
			n.recordApplyResultLocked(index, err)
			continue
		}

		var result interface{}
		if IsMembershipOp(cmd.Op) {
			result = n.applyMembershipLocked(cmd)
		} else {
			result = n.MatchFSM.Apply(entry.Command)
			n.dispatchApplyNotification(result)
		}
		n.recordApplyResultLocked(index, result)
	}
	_ = n.maybeCompactLocked()
}

func (n *RaftNode) bootstrapFromStorage() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	record, err := n.storage.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	if record != nil {
		if err := n.MatchFSM.Restore(record.Data); err != nil {
			return fmt.Errorf("restore snapshot at index %d: %w", record.LastIncludedIndex, err)
		}
		n.snapshotIndex = record.LastIncludedIndex
		n.snapshotTerm = record.LastIncludedTerm
		n.lastSnapshot = append([]byte(nil), record.Data...)
		n.lastApplied = record.LastIncludedIndex
	}

	entries, meta, err := n.storage.LoadLog()
	if err != nil {
		return fmt.Errorf("load log: %w", err)
	}

	n.log = []LogEntry{{Index: n.snapshotIndex, Term: n.snapshotTerm}}
	for _, entry := range entries {
		if entry.Index <= n.snapshotIndex {
			continue
		}
		n.log = append(n.log, LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		})
	}

	if meta != nil {
		if meta.SnapshotIndex > n.snapshotIndex {
			n.snapshotIndex = meta.SnapshotIndex
			n.snapshotTerm = meta.SnapshotTerm
		}
		if meta.CommitIndex > n.commitIndex {
			n.commitIndex = meta.CommitIndex
		}
		if meta.CurrentTerm > n.CurrentTerm {
			n.CurrentTerm = meta.CurrentTerm
		}
		if meta.VotedFor != "" {
			n.VotedFor = meta.VotedFor
		}
	}

	lastIdx := n.lastLogIndexLocked()
	if lastIdx > n.commitIndex {
		n.commitIndex = lastIdx
	}

	if n.lastApplied < n.snapshotIndex {
		n.lastApplied = n.snapshotIndex
	}
	if n.commitIndex < n.snapshotIndex {
		n.commitIndex = n.snapshotIndex
	}

	n.replayUncompactedLogLocked()
	return nil
}

func (n *RaftNode) replayUncompactedLogLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		index := n.lastApplied
		offset := n.logOffsetForIndex(index)
		if offset < 0 || offset >= len(n.log) {
			n.lastApplied = index - 1
			break
		}
		entry := n.log[offset]
		cmd, err := DecodeCommand(entry.Command)
		if err != nil {
			n.recordApplyResultLocked(index, err)
			continue
		}
		var result interface{}
		if IsMembershipOp(cmd.Op) {
			result = n.applyMembershipLocked(cmd)
		} else {
			result = n.MatchFSM.Apply(entry.Command)
		}
		n.recordApplyResultLocked(index, result)
	}
}

func (n *RaftNode) maybeCompactLocked() error {
	if n.uncompactedLogEntriesLocked() < n.compactThreshold {
		return nil
	}

	compactUpTo := n.lastApplied
	if compactUpTo <= n.snapshotIndex {
		return nil
	}

	data, err := n.MatchFSM.CreateSnapshot()
	if err != nil {
		return fmt.Errorf("create snapshot at index %d: %w", compactUpTo, err)
	}

	term := n.log[n.logOffsetForIndex(compactUpTo)].Term
	if n.storage != nil {
		if err := n.storage.PersistSnapshot(compactUpTo, term, data); err != nil {
			return fmt.Errorf("persist snapshot at index %d: %w", compactUpTo, err)
		}
	}

	tailOffset := n.logOffsetForIndex(compactUpTo) + 1
	var tail []LogEntry
	if tailOffset > 0 && tailOffset < len(n.log) {
		tail = append([]LogEntry(nil), n.log[tailOffset:]...)
	}

	n.snapshotIndex = compactUpTo
	n.snapshotTerm = term
	n.lastSnapshot = append([]byte(nil), data...)
	n.log = append([]LogEntry{{Index: compactUpTo, Term: term}}, tail...)

	if n.storage != nil {
		if err := n.persistDurableStateLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (n *RaftNode) truncateLogInMemoryLocked(upToIndex uint64) {
	term := n.snapshotTerm
	offset := n.logOffsetForIndex(upToIndex)
	if offset >= 0 && offset < len(n.log) {
		term = n.log[offset].Term
	}

	tailOffset := offset + 1
	if tailOffset >= len(n.log) {
		n.log = []LogEntry{{Index: upToIndex, Term: term}}
		return
	}
	n.log = append([]LogEntry{{Index: upToIndex, Term: term}}, n.log[tailOffset:]...)
}

func (n *RaftNode) persistDurableStateLocked() error {
	if testPersistObserver != nil {
		testPersistObserver()
	}
	if n.storage == nil {
		return nil
	}

	entries := make([]database.PersistedLogEntry, 0, len(n.log)-1)
	for i := 1; i < len(n.log); i++ {
		entry := n.log[i]
		if entry.Index <= n.snapshotIndex {
			continue
		}
		entries = append(entries, database.PersistedLogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		})
	}

	meta := database.RaftMeta{
		SnapshotIndex: n.snapshotIndex,
		SnapshotTerm:  n.snapshotTerm,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		CurrentTerm:   n.CurrentTerm,
		VotedFor:      n.VotedFor,
	}
	return n.storage.TruncateLog(entries, meta)
}

func (n *RaftNode) uncompactedLogEntriesLocked() uint64 {
	lastIdx := n.lastLogIndexLocked()
	if lastIdx <= n.snapshotIndex {
		return 0
	}
	return lastIdx - n.snapshotIndex
}

func (n *RaftNode) sendInstallSnapshot(peerID, addr string, term uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()
	n.sendInstallSnapshotWithContext(ctx, peerID, addr, term)
}

func (n *RaftNode) sendInstallSnapshotWithContext(ctx context.Context, peerID, addr string, term uint64) {
	n.mu.RLock()
	if n.State != StateLeader || n.CurrentTerm != term || len(n.lastSnapshot) == 0 {
		n.mu.RUnlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.NodeID,
		LastIncludedIndex: n.snapshotIndex,
		LastIncludedTerm:  n.snapshotTerm,
		Data:              append([]byte(nil), n.lastSnapshot...),
	}
	n.mu.RUnlock()

	var reply InstallSnapshotReply
	_ = SendRPCWithContext(ctx, addr, peerID+".InstallSnapshot", args, &reply)
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
