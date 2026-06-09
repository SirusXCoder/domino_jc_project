package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"domino_jc_project/pkg/api"
	"domino_jc_project/pkg/consensus"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/gateway"
	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/ws"
)

var sessionReplicationPortSeq int

type sessionReplicationRepo struct {
	stubGameRepo
}

type sessionGatewayNode struct {
	id        string
	raft      *consensus.RaftNode
	manager   *engine.GameManager
	hub       *ws.Hub
	handler   *gateway.ReplicatedGameHandler
	transport *consensus.NetworkTransport
}

func startSessionReplicationCluster(t *testing.T, parent context.Context, managed bool) ([]*sessionGatewayNode, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(parent)

	sessionReplicationPortSeq++
	base := 9300 + sessionReplicationPortSeq*10
	peers := map[string]string{
		"node-1": fmt.Sprintf("127.0.0.1:%d", base+1),
		"node-2": fmt.Sprintf("127.0.0.1:%d", base+2),
		"node-3": fmt.Sprintf("127.0.0.1:%d", base+3),
	}

	nodes := make([]*sessionGatewayNode, 0, 3)
	transports := make([]*consensus.NetworkTransport, 0, 3)

	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("node-%d", i)
		repo := &sessionReplicationRepo{}
		manager := engine.NewGameManager(repo)

		var fsm consensus.GameFSM
		if managed {
			fsm = consensus.NewManagedGameFSM(ctx, manager)
		} else {
			fsm = consensus.NewLocalGameFSM()
		}

		raft := consensus.NewRaftNode(id, peers, fsm)
		handler := gateway.NewReplicatedGameHandler(raft, manager)
		hub := ws.NewHub(handler)
		gateway.WireSessionReplication(raft, hub)

		go hub.Run()

		transport := consensus.NewNetworkTransport(ctx, raft)
		addr := peers[id]
		if err := transport.StartServer(addr); err != nil {
			t.Fatalf("start %s transport: %v", id, err)
		}
		raft.Start(ctx)

		nodes = append(nodes, &sessionGatewayNode{
			id:        id,
			raft:      raft,
			manager:   manager,
			hub:       hub,
			handler:   handler,
			transport: transport,
		})
		transports = append(transports, transport)
	}

	cleanup := func() {
		cancel()
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
	}

	return nodes, cleanup
}

func proposeLocalMatchCommand(t *testing.T, leader *sessionGatewayNode, matchID, op string) {
	t.Helper()

	entry, err := consensus.EncodeCommand(consensus.Command{
		Op:      op,
		MatchID: matchID,
	})
	if err != nil {
		t.Fatalf("encode %s command: %v", op, err)
	}
	if _, err := leader.raft.ProposeAndWait(entry); err != nil {
		t.Fatalf("propose %s: %v", op, err)
	}
}

func waitForLocalMatchOnAllNodes(t *testing.T, nodes []*sessionGatewayNode, matchID string, wantCounter int) {
	t.Helper()

	waitUntil(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			fsm, ok := node.raft.MatchFSM.(*consensus.LocalGameFSM)
			if !ok {
				return false
			}
			state, ok := fsm.Matches()[matchID]
			if !ok || state.Counter != wantCounter {
				return false
			}
		}
		return true
	})
}

func waitForMatchActiveOnAllNodes(t *testing.T, nodes []*sessionGatewayNode, matchID string) {
	t.Helper()

	waitUntil(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			session, ok := node.manager.GetSession(context.Background(), matchID)
			if !ok || session.Status != models.SessionStatusActive {
				return false
			}
		}
		return true
	})
}

func waitForSessionLeader(t *testing.T, nodes []*sessionGatewayNode) *sessionGatewayNode {
	t.Helper()

	var leader *sessionGatewayNode
	waitUntil(t, 3*time.Second, func() bool {
		leaderCount := 0
		for _, node := range nodes {
			if node.raft.IsLeader() {
				leaderCount++
				leader = node
			}
		}
		return leaderCount == 1 && leader != nil
	})
	if leader == nil {
		t.Fatal("expected exactly one cluster leader")
	}
	return leader
}

func followerSessionNodes(nodes []*sessionGatewayNode, leader *sessionGatewayNode) []*sessionGatewayNode {
	out := make([]*sessionGatewayNode, 0, len(nodes)-1)
	for _, node := range nodes {
		if node != leader {
			out = append(out, node)
		}
	}
	return out
}

func proposeStartMatch(t *testing.T, leader *sessionGatewayNode, matchID string, players []string) *models.GameSession {
	t.Helper()

	entry, err := consensus.EncodeCommandWithPayload(
		consensus.OpStartMatch,
		matchID,
		consensus.StartMatchPayload{
			PlayerUIDs: players,
			SetupGame:  true,
		},
	)
	if err != nil {
		t.Fatalf("encode start match: %v", err)
	}

	raw, err := leader.raft.ProposeAndWait(entry)
	if err != nil {
		t.Fatalf("propose start match: %v", err)
	}
	if consensus.IsApplyError(raw) {
		t.Fatalf("apply start match: %v", raw)
	}
	result, ok := consensus.AsApplyResult(raw)
	if !ok || !result.OK || result.Session == nil {
		t.Fatalf("unexpected start match result: %+v", raw)
	}
	return result.Session
}

func TestSessionReplication_ConsensusBroadcastAndGatewayRedirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes, cleanup := startSessionReplicationCluster(t, ctx, false)
	defer cleanup()

	leader := waitForSessionLeader(t, nodes)
	followers := followerSessionNodes(nodes, leader)
	if len(followers) == 0 {
		t.Fatal("expected at least one follower")
	}
	follower := followers[0]

	const matchID = "session-repl-1"
	proposeLocalMatchCommand(t, leader, matchID, consensus.OpStartMatch)
	waitForLocalMatchOnAllNodes(t, nodes, matchID, 0)

	leaderSend := make(chan []byte, 8)
	followerSend := make(chan []byte, 8)
	leader.hub.RegisterTestClient(matchID, "p1", leaderSend)
	follower.hub.RegisterTestClient(matchID, "p2", followerSend)
	waitUntil(t, time.Second, func() bool {
		return leader.hub.ClientCount() == 1 && follower.hub.ClientCount() == 1
	})

	turnCmd, err := consensus.EncodeCommand(consensus.Command{
		Op:      consensus.OpApplyTurn,
		MatchID: matchID,
	})
	if err != nil {
		t.Fatalf("encode turn command: %v", err)
	}
	_, err = follower.raft.ProposeAndWait(turnCmd)
	var redirectErr *consensus.LeaderRedirectError
	if !errors.As(err, &redirectErr) {
		t.Fatalf("follower propose err = %v, want LeaderRedirectError", err)
	}
	if redirectErr.LeaderID != leader.id {
		t.Fatalf("redirect leader_id = %q, want %q", redirectErr.LeaderID, leader.id)
	}
	if redirectErr.LeaderAddress == "" {
		t.Fatal("redirect leader_address is empty")
	}

	if _, err := leader.raft.ProposeAndWait(turnCmd); err != nil {
		t.Fatalf("leader propose turn: %v", err)
	}
	waitForLocalMatchOnAllNodes(t, nodes, matchID, 1)

	waitForSessionDelta(t, leaderSend, matchID, consensus.OpApplyTurn, 5*time.Second)
	waitForSessionDelta(t, followerSend, matchID, consensus.OpApplyTurn, 5*time.Second)

	mux := http.NewServeMux()
	api.NewGatewayHandler(follower.raft).Register(mux)

	leaderReq := httptest.NewRequest(http.MethodGet, "/api/gateway/leader", nil)
	leaderRec := httptest.NewRecorder()
	mux.ServeHTTP(leaderRec, leaderReq)

	var leaderPayload map[string]interface{}
	if err := json.Unmarshal(leaderRec.Body.Bytes(), &leaderPayload); err != nil {
		t.Fatalf("unmarshal leader response: %v", err)
	}
	if leaderPayload["leader_id"] != leader.id {
		t.Fatalf("gateway leader_id = %v, want %q", leaderPayload["leader_id"], leader.id)
	}
	if leaderPayload["is_local_leader"] == true {
		t.Fatal("follower gateway should not report local leadership")
	}

	sessionReq := httptest.NewRequest(http.MethodGet, "/api/gateway/session?session_id="+matchID, nil)
	sessionRec := httptest.NewRecorder()
	mux.ServeHTTP(sessionRec, sessionReq)

	if sessionRec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("session route status = %d, want %d", sessionRec.Code, http.StatusTemporaryRedirect)
	}

	var routePayload map[string]interface{}
	if err := json.Unmarshal(sessionRec.Body.Bytes(), &routePayload); err != nil {
		t.Fatalf("unmarshal session route response: %v", err)
	}
	if routePayload["redirect"] != true {
		t.Fatalf("session route redirect = %v, want true", routePayload["redirect"])
	}
	if routePayload["leader_id"] != leader.id {
		t.Fatalf("session route leader_id = %v, want %q", routePayload["leader_id"], leader.id)
	}
}

func TestSessionReplication_WSActionProposesThroughRaft(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes, cleanup := startSessionReplicationCluster(t, ctx, true)
	defer cleanup()

	leader := waitForSessionLeader(t, nodes)
	const matchID = "session-repl-ws"
	session := proposeStartMatch(t, leader, matchID, []string{"p1", "p2"})
	waitForMatchActiveOnAllNodes(t, nodes, matchID)
	firstTile := session.Hands[0].Tiles[0]

	send := make(chan []byte, 4)
	leader.hub.RegisterTestClient(matchID, "p1", send)
	waitUntil(t, time.Second, func() bool { return leader.hub.ClientCount() == 1 })

	payload, err := json.Marshal(ws.PlayTilePayload{
		SessionID:  matchID,
		PlayerID:   "p1",
		Tile:       firstTile,
		PlayAtLeft: true,
	})
	if err != nil {
		t.Fatalf("marshal play tile payload: %v", err)
	}
	envelope, err := json.Marshal(ws.EventEnvelope{
		Type:      ws.EventTypePlayTile,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	leader.hub.DeliverInboundForTest(&ws.InboundMessage{
		SessionID: matchID,
		PlayerID:  "p1",
		Payload:   envelope,
	})

	waitForSessionDelta(t, send, matchID, consensus.OpApplyTurn, 3*time.Second)

	updated, ok := leader.manager.GetSession(context.Background(), matchID)
	if !ok || len(updated.GameBoard) == 0 {
		t.Fatal("expected replicated session to contain a played tile")
	}
}

func TestSessionReplication_FollowerWSReturnsRedirectEnvelope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes, cleanup := startSessionReplicationCluster(t, ctx, true)
	defer cleanup()

	leader := waitForSessionLeader(t, nodes)
	follower := followerSessionNodes(nodes, leader)[0]

	const matchID = "session-repl-redirect"
	session := proposeStartMatch(t, leader, matchID, []string{"p1", "p2"})
	waitForMatchActiveOnAllNodes(t, nodes, matchID)
	firstTile := session.Hands[0].Tiles[0]

	send := make(chan []byte, 4)
	follower.hub.RegisterTestClient(matchID, "p1", send)
	waitUntil(t, time.Second, func() bool { return follower.hub.ClientCount() == 1 })

	payload, _ := json.Marshal(ws.PlayTilePayload{
		SessionID:  matchID,
		PlayerID:   "p1",
		Tile:       firstTile,
		PlayAtLeft: true,
	})
	envelope, _ := json.Marshal(ws.EventEnvelope{
		Type:      ws.EventTypePlayTile,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
	})

	follower.hub.DeliverInboundForTest(&ws.InboundMessage{
		SessionID: matchID,
		PlayerID:  "p1",
		Payload:   envelope,
	})

	deadline := time.After(2 * time.Second)
	for {
		select {
		case raw := <-send:
			var errEnvelope ws.ErrorEnvelope
			if err := json.Unmarshal(raw, &errEnvelope); err != nil {
				t.Fatalf("unmarshal error envelope: %v", err)
			}
			if errEnvelope.Type != ws.EventTypeError {
				continue
			}
			if errEnvelope.Error != ws.ErrCodeLeaderRedirect {
				t.Fatalf("error code = %q, want %q", errEnvelope.Error, ws.ErrCodeLeaderRedirect)
			}
			if errEnvelope.Redirect == nil {
				t.Fatal("expected redirect metadata in error envelope")
			}
			if errEnvelope.Redirect.LeaderID != leader.id {
				t.Fatalf("redirect leader_id = %q, want %q", errEnvelope.Redirect.LeaderID, leader.id)
			}
			return
		case <-deadline:
			t.Fatal("expected leader redirect envelope from follower gateway")
		}
	}
}

func waitForSessionDelta(t *testing.T, send <-chan []byte, matchID, op string, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case raw := <-send:
			var envelope ws.EventEnvelope
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if envelope.Type == ws.EventTypeStateSnapshot {
				continue
			}
			if envelope.Type != ws.EventTypeSessionDelta {
				continue
			}
			var delta ws.SessionDeltaPayload
			if err := json.Unmarshal(envelope.Payload, &delta); err != nil {
				t.Fatalf("unmarshal session delta: %v", err)
			}
			if delta.MatchID != matchID || delta.Op != op {
				continue
			}
			return
		case <-deadline:
			t.Fatalf("timed out waiting for session delta match=%s op=%s", matchID, op)
		}
	}
}

// stubGameRepo mirrors the consensus FSM integration stub for deterministic replication tests.
type stubGameRepo struct {
	savedSessions int
	careerUpdates int
}

func (s *stubGameRepo) SaveSession(_ context.Context, _ *models.GameSession) error {
	s.savedSessions++
	return nil
}

func (s *stubGameRepo) GetSession(_ context.Context, _ string) (*models.GameSession, error) {
	return nil, nil
}

func (s *stubGameRepo) ListActiveSessionIDs(_ context.Context) ([]string, error) {
	return nil, nil
}

func (s *stubGameRepo) SaveMatchRecord(_ context.Context, _ models.MatchRecord) error {
	return nil
}

func (s *stubGameRepo) GetPlayersByIDs(_ context.Context, _ []string) ([]models.Player, error) {
	return nil, nil
}

func (s *stubGameRepo) UpdatePlayerCareers(_ context.Context, _ []models.Player) error {
	s.careerUpdates++
	return nil
}

func (s *stubGameRepo) ListLeaderboard(_ context.Context, _ int) ([]models.LeaderboardEntry, error) {
	return nil, nil
}

func (s *stubGameRepo) GetPlayerCareer(_ context.Context, _ string, _ int) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (s *stubGameRepo) GetMatchRecord(_ context.Context, _ string) (*models.MatchRecord, error) {
	return nil, nil
}

func (s *stubGameRepo) ApplyMatchRatings(_ context.Context, _ string, _ models.ELODeltas) error {
	return nil
}

func (s *stubGameRepo) GetPlayerProfile(_ context.Context, _ string) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (s *stubGameRepo) ListPlayerMatchHistory(_ context.Context, _ string, _ int, _ string) (*models.MatchHistoryPage, error) {
	return nil, nil
}

func (s *stubGameRepo) GetLedgerState(_ context.Context, _ string) (*models.LedgerState, error) {
	return nil, nil
}

func (s *stubGameRepo) GetMatchWithPlayers(_ context.Context, _ string) (*models.MatchRecord, []models.Player, error) {
	return nil, nil, nil
}
