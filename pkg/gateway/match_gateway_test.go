package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"domino_jc_project/pkg/consensus"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
)

type matchGatewayStubRepo struct{}

func (matchGatewayStubRepo) SaveSession(context.Context, *models.GameSession) error { return nil }
func (matchGatewayStubRepo) GetSession(context.Context, string) (*models.GameSession, error) {
	return nil, nil
}
func (matchGatewayStubRepo) ListActiveSessionIDs(context.Context) ([]string, error) { return nil, nil }
func (matchGatewayStubRepo) SaveMatchRecord(context.Context, models.MatchRecord) error { return nil }
func (matchGatewayStubRepo) GetPlayersByIDs(context.Context, []string) ([]models.Player, error) {
	return nil, nil
}
func (matchGatewayStubRepo) UpdatePlayerCareers(context.Context, []models.Player) error { return nil }
func (matchGatewayStubRepo) ListLeaderboard(context.Context, int) ([]models.LeaderboardEntry, error) {
	return nil, nil
}
func (matchGatewayStubRepo) GetPlayerCareer(context.Context, string, int) (*models.PlayerCareerStats, error) {
	return nil, nil
}
func (matchGatewayStubRepo) GetMatchRecord(context.Context, string) (*models.MatchRecord, error) {
	return nil, nil
}
func (matchGatewayStubRepo) ApplyMatchRatings(context.Context, string, models.ELODeltas) error {
	return nil
}
func (matchGatewayStubRepo) GetPlayerProfile(context.Context, string) (*models.PlayerCareerStats, error) {
	return nil, nil
}
func (matchGatewayStubRepo) ListPlayerMatchHistory(context.Context, string, int, string) (*models.MatchHistoryPage, error) {
	return nil, nil
}
func (matchGatewayStubRepo) GetLedgerState(context.Context, string) (*models.LedgerState, error) {
	return nil, nil
}
func (matchGatewayStubRepo) GetMatchWithPlayers(context.Context, string) (*models.MatchRecord, []models.Player, error) {
	return nil, nil, nil
}

func startMatchGatewayCluster(t *testing.T) ([]*consensus.RaftNode, []*engine.GameManager, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	base := 9400 + time.Now().Nanosecond()%1000
	peers := map[string]string{
		"node-1": fmt.Sprintf("127.0.0.1:%d", base+1),
		"node-2": fmt.Sprintf("127.0.0.1:%d", base+2),
		"node-3": fmt.Sprintf("127.0.0.1:%d", base+3),
	}

	rafts := make([]*consensus.RaftNode, 0, 3)
	managers := make([]*engine.GameManager, 0, 3)
	transports := make([]*consensus.NetworkTransport, 0, 3)

	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("node-%d", i)
		manager := engine.NewGameManager(matchGatewayStubRepo{})
		fsm := consensus.NewManagedGameFSM(ctx, manager)
		raft := consensus.NewRaftNode(id, peers, fsm)

		transport := consensus.NewNetworkTransport(ctx, raft)
		if err := transport.StartServer(peers[id]); err != nil {
			t.Fatalf("start transport %s: %v", id, err)
		}
		raft.Start(ctx)

		rafts = append(rafts, raft)
		managers = append(managers, manager)
		transports = append(transports, transport)
	}

	cleanup := func() {
		cancel()
		for _, transport := range transports {
			transport.Shutdown()
			transport.Wait()
		}
	}
	return rafts, managers, cleanup
}

func waitForGatewayLeader(t *testing.T, rafts []*consensus.RaftNode) *consensus.RaftNode {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		var leader *consensus.RaftNode
		leaderCount := 0
		for _, raft := range rafts {
			if raft.IsLeader() {
				leaderCount++
				leader = raft
			}
		}
		if leaderCount == 1 && leader != nil {
			return leader
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for cluster leader")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func proposeGatewayStartMatch(t *testing.T, leader *consensus.RaftNode, matchID string, players []string) {
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
	if _, err := leader.ProposeAndWait(entry); err != nil {
		t.Fatalf("propose start match: %v", err)
	}
}

func mutateBody(clientID string, seq uint64, matchID string, action MatchActionData) []byte {
	actionRaw, err := json.Marshal(action)
	if err != nil {
		panic(err)
	}
	body, err := json.Marshal(MatchMutateRequest{
		ClientID:       clientID,
		SequenceNumber: seq,
		MatchID:        matchID,
		ActionData:     actionRaw,
	})
	if err != nil {
		panic(err)
	}
	return body
}

func TestMatchGateway_ReadReturnsSession(t *testing.T) {
	rafts, managers, cleanup := startMatchGatewayCluster(t)
	defer cleanup()

	leader := waitForGatewayLeader(t, rafts)
	const matchID = "gateway-read-match"
	proposeGatewayStartMatch(t, leader, matchID, []string{"p1", "p2"})

	leaderIdx := 0
	for i, raft := range rafts {
		if raft.NodeID == leader.NodeID {
			leaderIdx = i
			break
		}
	}

	gateway := NewMatchGateway(rafts[leaderIdx], managers[leaderIdx])
	mux := http.NewServeMux()
	gateway.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/match/read?match_id="+matchID, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("read status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp MatchReadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal read response: %v", err)
	}
	if !resp.Found || resp.Session == nil {
		t.Fatalf("expected session in read response: %+v", resp)
	}
	if resp.Session.Status != models.SessionStatusActive {
		t.Fatalf("session status = %q, want active", resp.Session.Status)
	}
}

func TestMatchGateway_IdempotentMutations(t *testing.T) {
	rafts, managers, cleanup := startMatchGatewayCluster(t)
	defer cleanup()

	leader := waitForGatewayLeader(t, rafts)
	const matchID = "gateway-idempotent"
	proposeGatewayStartMatch(t, leader, matchID, []string{"p1", "p2"})

	leaderIdx := 0
	for i, raft := range rafts {
		if raft.NodeID == leader.NodeID {
			leaderIdx = i
			break
		}
	}

	session, ok := managers[leaderIdx].GetSession(context.Background(), matchID)
	if !ok {
		t.Fatal("expected started session")
	}
	firstTile := session.Hands[0].Tiles[0]

	cache := NewSessionCache(100)
	gateway := NewMatchGateway(rafts[leaderIdx], managers[leaderIdx], WithSessionCache(cache))
	mux := http.NewServeMux()
	gateway.Register(mux)

	body := mutateBody("client-1", 1, matchID, MatchActionData{
		Kind:       string(models.TurnKindPlayTile),
		PlayerID:   "p1",
		Tile:       firstTile,
		PlayAtLeft: true,
	})

	req1 := httptest.NewRequest(http.MethodPost, "/match/mutate", bytes.NewReader(body))
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first mutate status = %d, body=%s", rec1.Code, rec1.Body.String())
	}

	boardLenAfterFirst := len(session.GameBoard)
	updated, ok := managers[leaderIdx].GetSession(context.Background(), matchID)
	if ok {
		boardLenAfterFirst = len(updated.GameBoard)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/match/mutate", bytes.NewReader(body))
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("replay mutate status = %d, body=%s", rec2.Code, rec2.Body.String())
	}

	var replay MatchMutateResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &replay); err != nil {
		t.Fatalf("unmarshal replay response: %v", err)
	}
	if !replay.IdempotentReplay {
		t.Fatal("expected idempotent_replay=true on duplicate mutation")
	}

	finalSession, ok := managers[leaderIdx].GetSession(context.Background(), matchID)
	if !ok {
		t.Fatal("expected session after mutations")
	}
	if len(finalSession.GameBoard) != boardLenAfterFirst {
		t.Fatalf("board grew on replay: before=%d after=%d", boardLenAfterFirst, len(finalSession.GameBoard))
	}
}

func TestMatchGateway_FollowerForwardsToLeader(t *testing.T) {
	rafts, managers, cleanup := startMatchGatewayCluster(t)
	defer cleanup()

	leader := waitForGatewayLeader(t, rafts)
	const matchID = "gateway-forward"
	proposeGatewayStartMatch(t, leader, matchID, []string{"p1", "p2"})

	var leaderIdx, followerIdx int
	for i, raft := range rafts {
		if raft.NodeID == leader.NodeID {
			leaderIdx = i
		} else if followerIdx == 0 && !raft.IsLeader() {
			followerIdx = i
		}
	}

	leaderGateway := NewMatchGateway(rafts[leaderIdx], managers[leaderIdx])
	leaderMux := http.NewServeMux()
	leaderGateway.Register(leaderMux)
	leaderServer := httptest.NewServer(leaderMux)
	defer leaderServer.Close()

	followerGateway := NewMatchGateway(
		rafts[followerIdx],
		managers[followerIdx],
		WithHTTPPeerAddresses(map[string]string{
			leader.NodeID: leaderServer.URL,
		}),
	)
	followerMux := http.NewServeMux()
	followerGateway.Register(followerMux)

	session, ok := managers[leaderIdx].GetSession(context.Background(), matchID)
	if !ok {
		t.Fatal("expected started session")
	}
	firstTile := session.Hands[0].Tiles[0]

	body := mutateBody("client-forward", 7, matchID, MatchActionData{
		Kind:       string(models.TurnKindPlayTile),
		PlayerID:   "p1",
		Tile:       firstTile,
		PlayAtLeft: true,
	})

	req := httptest.NewRequest(http.MethodPost, "/match/mutate", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	followerMux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded mutate status = %d, body=%s", rec.Code, rec.Body.String())
	}

	var resp MatchMutateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal mutate response: %v", err)
	}
	if !resp.OK || resp.Result == nil {
		t.Fatalf("unexpected mutate response: %+v", resp)
	}

	updated, ok := managers[leaderIdx].GetSession(context.Background(), matchID)
	if !ok || len(updated.GameBoard) == 0 {
		t.Fatal("expected leader state to reflect forwarded mutation")
	}
}

func TestMatchGateway_NoLeaderReturnsRetryAfter(t *testing.T) {
	manager := engine.NewGameManager(matchGatewayStubRepo{})
	fsm := consensus.NewManagedGameFSM(context.Background(), manager)
	peers := map[string]string{"solo": "127.0.0.1:1"}
	raft := consensus.NewRaftNode("solo", peers, fsm)

	gateway := NewMatchGateway(raft, manager)
	mux := http.NewServeMux()
	gateway.Register(mux)

	body := mutateBody("client-solo", 1, "match-solo", MatchActionData{
		Kind:     string(models.TurnKindPass),
		PlayerID: "p1",
	})
	req := httptest.NewRequest(http.MethodPost, "/match/mutate", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header when leader is unknown")
	}
}
