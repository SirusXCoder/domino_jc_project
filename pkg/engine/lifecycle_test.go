package engine

import (
	"context"
	"testing"

	"domino_jc_project/pkg/models"
)

type memoryLedgerRepo struct {
	records []models.MatchRecord
}

func (m *memoryLedgerRepo) SaveMatchRecord(_ context.Context, record models.MatchRecord) error {
	m.records = append(m.records, record)
	return nil
}

type stubTerminator struct {
	calls []terminateCall
}

type terminateCall struct {
	sessionID string
	outcome   *models.MatchOutcome
}

func (s *stubTerminator) TerminateMatch(_ context.Context, sessionID string, outcome *models.MatchOutcome, _ *models.GameSession) {
	s.calls = append(s.calls, terminateCall{sessionID: sessionID, outcome: outcome})
}

func TestProcessGameTurn_PersistsActiveMutation(t *testing.T) {
	repo := &trackingRepo{}
	manager := NewGameManager(repo)
	manager.SetMatchTerminator(&stubTerminator{})

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

func TestProcessGameTurn_EmptyHandTerminatesMatch(t *testing.T) {
	repo := &trackingRepo{}
	terminator := &stubTerminator{}

	manager := NewGameManager(repo)
	manager.SetMatchTerminator(terminator)

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
	if len(terminator.calls) != 1 {
		t.Fatalf("terminator calls = %d, want 1", len(terminator.calls))
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
