package consensus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const defaultRPCTimeout = 200 * time.Millisecond

var (
	activeRaftNodes    sync.Map
	registeredRPCNodes sync.Map

	// testAppendEntriesGate blocks follower AppendEntries handling until released.
	testAppendEntriesGate sync.Map
	// testRPCObserver wraps outbound Raft RPC calls during integration tests.
	testRPCObserver func(method string) (done func())
)

type raftRPCBridge struct {
	nodeID string
}

func (b *raftRPCBridge) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	node, err := lookupActiveRaftNode(b.nodeID)
	if err != nil {
		return err
	}
	return node.RequestVote(args, reply)
}

func (b *raftRPCBridge) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if ch, ok := testAppendEntriesGate.Load(b.nodeID); ok {
		<-ch.(chan struct{})
	}

	node, err := lookupActiveRaftNode(b.nodeID)
	if err != nil {
		return err
	}
	return node.AppendEntries(args, reply)
}

func (b *raftRPCBridge) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	node, err := lookupActiveRaftNode(b.nodeID)
	if err != nil {
		return err
	}
	return node.InstallSnapshot(args, reply)
}

func lookupActiveRaftNode(nodeID string) (*RaftNode, error) {
	value, ok := activeRaftNodes.Load(nodeID)
	if !ok {
		return nil, fmt.Errorf("raft node %q is not active", nodeID)
	}
	node, ok := value.(*RaftNode)
	if !ok {
		return nil, fmt.Errorf("raft node %q has invalid registry entry", nodeID)
	}
	return node, nil
}

func registerRaftRPCService(nodeID string) error {
	if _, loaded := registeredRPCNodes.LoadOrStore(nodeID, true); loaded {
		return nil
	}

	if err := rpc.RegisterName(nodeID, &raftRPCBridge{nodeID: nodeID}); err != nil {
		registeredRPCNodes.Delete(nodeID)
		return err
	}
	return nil
}

// NetworkTransport serves inbound Raft RPCs for a local node and dials peers outbound.
type NetworkTransport struct {
	ctx    context.Context
	cancel context.CancelFunc
	node   *RaftNode

	mu     sync.Mutex
	ln     net.Listener
	closed bool
	wg     sync.WaitGroup
}

// NewNetworkTransport wires a local RaftNode to the cluster network layer.
// The provided context controls server lifetime; cancel it or call Shutdown to tear down.
func NewNetworkTransport(ctx context.Context, node *RaftNode) *NetworkTransport {
	childCtx, cancel := context.WithCancel(ctx)
	return &NetworkTransport{
		ctx:    childCtx,
		cancel: cancel,
		node:   node,
	}
}

// StartServer registers the local RaftNode with net/rpc, binds TCP, and accepts
// inbound cluster RPC connections in a background goroutine.
func (t *NetworkTransport) StartServer(bindAddress string) error {
	if t.node == nil {
		return fmt.Errorf("raft node is nil")
	}

	activeRaftNodes.Store(t.node.NodeID, t.node)

	if err := registerRaftRPCService(t.node.NodeID); err != nil {
		activeRaftNodes.Delete(t.node.NodeID)
		return fmt.Errorf("register raft rpc service: %w", err)
	}

	ln, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", bindAddress, err)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		ln.Close()
		return fmt.Errorf("transport already shut down")
	}
	t.ln = ln
	t.mu.Unlock()

	t.wg.Add(1)
	go t.serveLoop(ln)

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		<-t.ctx.Done()
		t.shutdown()
	}()

	return nil
}

func (t *NetworkTransport) serveLoop(ln net.Listener) {
	defer t.wg.Done()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if t.isShuttingDown() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}

		go rpc.ServeConn(conn)
	}
}

func (t *NetworkTransport) isShuttingDown() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *NetworkTransport) shutdown() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}
	t.closed = true

	if t.ln != nil {
		_ = t.ln.Close()
	}
}

// Shutdown closes the listener and cancels the transport context.
func (t *NetworkTransport) Shutdown() {
	if t.node != nil {
		activeRaftNodes.Delete(t.node.NodeID)
	}
	t.shutdown()
	if t.cancel != nil {
		t.cancel()
	}
}

// Wait blocks until the transport accept loop has exited after shutdown.
func (t *NetworkTransport) Wait() {
	t.wg.Wait()
}

// SendRPC dials a remote cluster node and dispatches a net/rpc call with a strict
// 200ms deadline so an unreachable peer cannot block the state engine.
func SendRPC(peerAddress string, method string, args interface{}, reply interface{}) error {
	if testRPCObserver != nil {
		if done := testRPCObserver(method); done != nil {
			defer done()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	type rpcResult struct {
		err error
	}
	done := make(chan rpcResult, 1)

	go func() {
		done <- rpcResult{err: sendRPC(peerAddress, method, args, reply)}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("rpc call %s to %s: %w", method, peerAddress, ctx.Err())
	case result := <-done:
		return result.err
	}
}

func sendRPC(peerAddress string, method string, args interface{}, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", peerAddress, defaultRPCTimeout)
	if err != nil {
		return fmt.Errorf("dial peer %s: %w", peerAddress, err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(defaultRPCTimeout)); err != nil {
		return fmt.Errorf("set rpc deadline: %w", err)
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	if err := client.Call(method, args, reply); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("rpc call %s: connection closed: %w", method, err)
		}
		return fmt.Errorf("rpc call %s: %w", method, err)
	}
	return nil
}
