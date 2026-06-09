package engine

import (
	"context"
	"testing"
	"time"

	"domino_jc_project/pkg/broker"
	"domino_jc_project/pkg/game"
	"domino_jc_project/pkg/models"
)

func TestProcessGameTurn_PersistsActiveMutation(t *testing.T) {
	repo := &trackingRepo{}
	manager := NewGameManager(repo)
	manager.SetGameEngine(game.NewGameEngine(broker.NewInMemoryBroker()))

	session := models.NewGameSession("s1", []string{"p1", "p2"})
	session.GenerateStandardDeck()
	session.ShuffleBoneyard()
	if err := session.DealHands(7); err != nil {
		t.Fatalf("deal hands: %v", err)
	}
	session.CurrentTurn = "p1"
	firstTile := session.Hands[0].Tiles[0]

	if err := manager.AddSession(session); err != nil {
		t.Fatalf("add session: %v", err)
	}

	result, err := manager.processGameTurn(context.Background(), "s1", models.TurnAction{
		Kind:       models.TurnKindPlayTile,
		PlayerID:   "p1",
		Tile:       firstTile,
		PlayAtLeft: true,
	})
	if err != nil {
		t.Fatalf("processGameTurn: %v", err)
	}
	if !result.Applied || !result.NeedsPersist {
		t.Fatalf("expected applied+persist result, got %+v", result)
	}
	if repo.saveCount != 1 {
		t.Fatalf("saveCount = %d, want 1", repo.saveCount)
	}
}

func TestProcessGameTurn_RejectsLockedSession(t *testing.T) {
	manager := NewGameManager(&trackingRepo{})
	session := models.NewGameSession("s1", []string{"p1"})
	session.MutationsLocked = true
	session.Status = models.SessionStatusCompleted
	if err := manager.AddSession(session); err != nil {
		t.Fatalf("add session: %v", err)
	}

	_, err := manager.processGameTurn(context.Background(), "s1", models.TurnAction{
		Kind:     models.TurnKindPass,
		PlayerID: "p1",
	})
	if err != models.ErrMutationsLocked {
		t.Fatalf("err = %v, want ErrMutationsLocked", err)
	}
}

func TestProcessGameTurn_EmptyHandPublishesMatchEnded(t *testing.T) {
	repo := &trackingRepo{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.NewInMemoryBroker()
	matchEndedCh, err := b.Subscribe(ctx, game.TopicMatchEnded)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	manager := NewGameManager(repo)
	manager.SetGameEngine(game.NewGameEngine(b))

	session := models.NewGameSession("s1", []string{"p1", "p2"})
	session.Status = models.SessionStatusActive
	session.CurrentTurn = "p1"
	session.Hands[0].Tiles = []models.DominoTile{models.NewTile(1, 2)}
	session.Hands[1].Tiles = []models.DominoTile{models.NewTile(3, 4)}
	if err := manager.AddSession(session); err != nil {
		t.Fatalf("add session: %v", err)
	}

	result, err := manager.processGameTurn(context.Background(), "s1", models.TurnAction{
		Kind:       models.TurnKindPlayTile,
		PlayerID:   "p1",
		Tile:       session.Hands[0].Tiles[0],
		PlayAtLeft: true,
	})
	if err != nil {
		t.Fatalf("processGameTurn: %v", err)
	}
	if !result.MatchEnded || result.Outcome == nil {
		t.Fatal("expected match to end with empty hand")
	}
	if result.Outcome.WinnerID != "p1" {
		t.Fatalf("winner = %q, want p1", result.Outcome.WinnerID)
	}

	select {
	case evt := <-matchEndedCh:
		payload, ok := evt.Payload.(game.MatchEndedPayload)
		if !ok {
			t.Fatalf("payload type = %T, want MatchEndedPayload", evt.Payload)
		}
		if payload.SessionID != "s1" {
			t.Fatalf("session_id = %q, want s1", payload.SessionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for match.ended event")
	}

	loaded, ok := manager.GetSession(context.Background(), "s1")
	if !ok {
		t.Fatal("session missing after termination")
	}
	if !loaded.MutationsLocked || loaded.Status != models.SessionStatusCompleted {
		t.Fatalf("session not locked/completed: locked=%v status=%q", loaded.MutationsLocked, loaded.Status)
	}
}

type trackingRepo struct {
	saveCount int
}

func (t *trackingRepo) SaveSession(_ context.Context, _ *models.GameSession) error {
	t.saveCount++
	return nil
}

func (t *trackingRepo) GetSession(_ context.Context, _ string) (*models.GameSession, error) {
	return nil, nil
}

func (t *trackingRepo) ListActiveSessionIDs(_ context.Context) ([]string, error) {
	return nil, nil
}

func (t *trackingRepo) SaveMatchRecord(_ context.Context, _ models.MatchRecord) error {
	return nil
}

func (t *trackingRepo) GetPlayersByIDs(_ context.Context, _ []string) ([]models.Player, error) {
	return nil, nil
}

func (t *trackingRepo) UpdatePlayerCareers(_ context.Context, _ []models.Player) error {
	return nil
}

func (t *trackingRepo) ListLeaderboard(_ context.Context, _ int) ([]models.LeaderboardEntry, error) {
	return nil, nil
}

func (t *trackingRepo) GetPlayerCareer(_ context.Context, _ string, _ int) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (t *trackingRepo) GetMatchRecord(_ context.Context, _ string) (*models.MatchRecord, error) {
	return nil, nil
}

func (t *trackingRepo) ApplyMatchRatings(_ context.Context, _ string, _ models.ELODeltas) error {
	return nil
}

func (t *trackingRepo) GetPlayerProfile(_ context.Context, _ string) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (t *trackingRepo) ListPlayerMatchHistory(_ context.Context, _ string, _ int, _ string) (*models.MatchHistoryPage, error) {
	return nil, nil
}

func (t *trackingRepo) GetLedgerState(_ context.Context, _ string) (*models.LedgerState, error) {
	return nil, nil
}

func (t *trackingRepo) GetMatchWithPlayers(_ context.Context, _ string) (*models.MatchRecord, []models.Player, error) {
	return nil, nil, nil
}
