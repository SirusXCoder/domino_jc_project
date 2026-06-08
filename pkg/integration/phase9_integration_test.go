package integration_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/resilience"
	"domino_jc_project/pkg/ws"
)

// phase9StatsRepo is an in-memory stats/ledger repository for E2E tests.
type phase9StatsRepo struct {
	mu      sync.Mutex
	matches map[string]*models.MatchRecord
	players map[string]models.Player
}

func newPhase9StatsRepo() *phase9StatsRepo {
	return &phase9StatsRepo{
		matches: make(map[string]*models.MatchRecord),
		players: map[string]models.Player{
			"p1": {PlayerID: "p1", ELO: 1500, PeakELO: 1500, MatchesPlayed: 0},
			"p2": {PlayerID: "p2", ELO: 1500, PeakELO: 1500, MatchesPlayed: 0},
		},
	}
}

func (r *phase9StatsRepo) SaveMatchRecord(_ context.Context, record models.MatchRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := record
	if copy.UID == "" {
		copy.UID = "0x" + copy.MatchID
	}
	r.matches[copy.MatchID] = &copy
	return nil
}

func (r *phase9StatsRepo) GetPlayersByIDs(_ context.Context, ids []string) ([]models.Player, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]models.Player, 0, len(ids))
	for _, id := range ids {
		if p, ok := r.players[id]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *phase9StatsRepo) UpdatePlayerCareers(_ context.Context, players []models.Player) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range players {
		r.players[p.PlayerID] = p
	}
	return nil
}

func (r *phase9StatsRepo) ListLeaderboard(_ context.Context, _ int) ([]models.LeaderboardEntry, error) {
	return nil, nil
}

func (r *phase9StatsRepo) GetPlayerCareer(_ context.Context, _ string, _ int) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (r *phase9StatsRepo) GetMatchRecord(_ context.Context, matchID string) (*models.MatchRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.matches[matchID]
	if !ok {
		return nil, context.Canceled
	}
	return m, nil
}

func (r *phase9StatsRepo) ApplyMatchRatings(_ context.Context, matchUID string, deltas models.ELODeltas) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.matches {
		if m.UID == matchUID {
			raw, _ := json.Marshal(deltas)
			m.ELodDeltas = string(raw)
			m.RatingsApplied = true
			return nil
		}
	}
	return nil
}

func (r *phase9StatsRepo) GetPlayerProfile(_ context.Context, _ string) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (r *phase9StatsRepo) ListPlayerMatchHistory(_ context.Context, _ string, _ int, _ string) (*models.MatchHistoryPage, error) {
	return nil, nil
}

func (r *phase9StatsRepo) GetLedgerState(_ context.Context, _ string) (*models.LedgerState, error) {
	return nil, nil
}

func (r *phase9StatsRepo) GetMatchWithPlayers(_ context.Context, matchID string) (*models.MatchRecord, []models.Player, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.matches[matchID]
	if !ok {
		return nil, nil, context.Canceled
	}
	players := make([]models.Player, 0, len(m.Players))
	for _, stub := range m.Players {
		if p, ok := r.players[stub.PlayerID]; ok {
			players = append(players, p)
		} else {
			players = append(players, stub)
		}
	}
	return m, players, nil
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
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

func TestPhase9_FullMatchLifecycleWithStatsBroadcast(t *testing.T) {
	repo := newPhase9StatsRepo()
	ledgerBreaker := resilience.NewBreaker(resilience.DefaultBreakerConfig("ledger-test"))
	ratingBreaker := resilience.NewBreaker(resilience.DefaultBreakerConfig("rating-test"))

	hub := ws.NewHub(nil)
	ratingWorker := engine.NewRatingWorker(
		repo,
		engine.WithStatsBroadcaster(hub),
		engine.WithRatingBreaker(ratingBreaker),
	)
	ledgerWorker := engine.NewLedgerWorker(
		repo,
		16,
		engine.WithRatingProcessor(ratingWorker),
		engine.WithLedgerBreaker(ledgerBreaker),
	)
	hub.SetMatchLedger(ledgerWorker)

	go hub.Run()
	go ledgerWorker.Run()

	send := make(chan []byte, 8)
	hub.RegisterTestClient("s1", "p1", send)
	waitUntil(t, time.Second, func() bool { return hub.ClientCount() == 1 })

	session := models.NewGameSession("s1", []string{"p1", "p2"})
	session.Status = models.SessionStatusCompleted
	session.MutationsLocked = true
	outcome := &models.MatchOutcome{
		MatchID:  "s1",
		WinnerID: "p1",
		Scores:   models.Scores{"p1": 0, "p2": 12},
		Reason:   models.MatchEndEmptyHand,
	}

	hub.TerminateMatch(context.Background(), "s1", outcome, session)

	waitUntil(t, 3*time.Second, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		m, ok := repo.matches["s1"]
		return ok && m.RatingsApplied
	})

	if ledgerWorker.BreakerState() != resilience.StateClosed {
		t.Fatalf("ledger breaker = %v, want closed", ledgerWorker.BreakerState())
	}
	if ratingWorker.BreakerState() != resilience.StateClosed {
		t.Fatalf("rating breaker = %v, want closed", ratingWorker.BreakerState())
	}

	var gotMatchEnd, gotStats bool
	deadline := time.After(3 * time.Second)
	for !(gotMatchEnd && gotStats) {
		select {
		case payload := <-send:
			var envelope ws.EventEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			switch envelope.Type {
			case ws.EventTypeMatchEnd:
				gotMatchEnd = true
			case ws.EventTypePlayerStatsUpdated:
				gotStats = true
				var stats ws.PlayerStatsUpdatedPayload
				if err := json.Unmarshal(envelope.Payload, &stats); err != nil {
					t.Fatalf("unmarshal stats payload: %v", err)
				}
				if stats.PlayerID != "p1" {
					t.Fatalf("stats player_id = %q, want p1", stats.PlayerID)
				}
				if stats.ELODelta <= 0 {
					t.Fatalf("winner elo_delta = %f, want positive", stats.ELODelta)
				}
			}
		case <-deadline:
			t.Fatalf("expected match_end=%v stats_updated=%v", gotMatchEnd, gotStats)
		}
	}
}

func TestPhase9_PendingStatsDeliveredOnReconnect(t *testing.T) {
	repo := newPhase9StatsRepo()
	hub := ws.NewHub(nil)
	ratingWorker := engine.NewRatingWorker(repo, engine.WithStatsBroadcaster(hub))
	ledgerWorker := engine.NewLedgerWorker(repo, 16, engine.WithRatingProcessor(ratingWorker))
	hub.SetMatchLedger(ledgerWorker)

	go hub.Run()
	go ledgerWorker.Run()

	// Pre-seed match so rating can run without TerminateMatch fan-out.
	record, err := models.NewMatchRecord("s2", "p1", models.Scores{"p1": 0, "p2": 10}, models.MatchEndEmptyHand, []models.Player{
		{PlayerID: "p1"}, {PlayerID: "p2"},
	})
	if err != nil {
		t.Fatalf("NewMatchRecord: %v", err)
	}
	if err := repo.SaveMatchRecord(context.Background(), record); err != nil {
		t.Fatalf("SaveMatchRecord: %v", err)
	}

	if err := ratingWorker.ProcessMatch(context.Background(), record); err != nil {
		t.Fatalf("ProcessMatch: %v", err)
	}

	waitUntil(t, time.Second, func() bool {
		return hub.PendingStatsCount("s2", "p1") > 0
	})

	send := make(chan []byte, 4)
	hub.RegisterTestClient("s2", "p1", send)
	waitUntil(t, time.Second, func() bool { return hub.ClientCount() == 1 })

	select {
	case payload := <-send:
		var envelope ws.EventEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if envelope.Type != ws.EventTypePlayerStatsUpdated {
			t.Fatalf("type = %q, want %q", envelope.Type, ws.EventTypePlayerStatsUpdated)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected flushed stats on reconnect")
	}

	if hub.PendingStatsCount("s2", "p1") != 0 {
		t.Fatalf("pending after reconnect = %d, want 0", hub.PendingStatsCount("s2", "p1"))
	}
}

func TestPhase9_CircuitBreakerRemainsClosedDuringNormalOps(t *testing.T) {
	repo := newPhase9StatsRepo()
	breaker := resilience.NewBreaker(resilience.DefaultBreakerConfig("ledger-normal"))
	worker := engine.NewLedgerWorker(repo, 8, engine.WithLedgerBreaker(breaker))

	go worker.Run()

	record, err := models.NewMatchRecord("m-normal", "p1", models.Scores{"p1": 0}, models.MatchEndEmptyHand, []models.Player{{PlayerID: "p1"}})
	if err != nil {
		t.Fatalf("NewMatchRecord: %v", err)
	}
	worker.Enqueue(record)
	waitUntil(t, 2*time.Second, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		_, ok := repo.matches["m-normal"]
		return ok
	})

	if worker.BreakerState() != resilience.StateClosed {
		t.Fatalf("breaker state = %v, want closed", worker.BreakerState())
	}
}
