package consensus_test

import (
	"context"
	"testing"

	"domino_jc_project/pkg/consensus"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
)

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

func TestManagedGameFSM_StartMatchTurnAndLedger(t *testing.T) {
	repo := &stubGameRepo{}
	manager := engine.NewGameManager(repo)
	fsm := consensus.NewManagedGameFSM(context.Background(), manager)

	startEntry, err := consensus.EncodeCommandWithPayload(
		consensus.OpStartMatch,
		"match-1",
		consensus.StartMatchPayload{
			PlayerUIDs: []string{"p1", "p2"},
			SetupGame:  true,
		},
	)
	if err != nil {
		t.Fatalf("encode start match: %v", err)
	}

	startResult := fsm.Apply(startEntry)
	if consensus.IsApplyError(startResult) {
		t.Fatalf("apply start match: %v", startResult)
	}
	applyResult, ok := consensus.AsApplyResult(startResult)
	if !ok || !applyResult.OK || applyResult.Session == nil {
		t.Fatalf("unexpected start result: %+v", startResult)
	}
	if applyResult.Session.Status != models.SessionStatusActive {
		t.Fatalf("session status = %q, want ACTIVE", applyResult.Session.Status)
	}

	firstTile := applyResult.Session.Hands[0].Tiles[0]
	turnEntry, err := consensus.EncodeCommandWithPayload(
		consensus.OpApplyTurn,
		"match-1",
		consensus.ApplyTurnPayload{
			Kind:       string(models.TurnKindPlayTile),
			PlayerID:   "p1",
			Tile:       firstTile,
			PlayAtLeft: true,
		},
	)
	if err != nil {
		t.Fatalf("encode turn: %v", err)
	}

	turnResult := fsm.Apply(turnEntry)
	if consensus.IsApplyError(turnResult) {
		t.Fatalf("apply turn: %v", turnResult)
	}
	applyResult, ok = consensus.AsApplyResult(turnResult)
	if !ok || !applyResult.OK || !applyResult.Applied || applyResult.Turn == nil {
		t.Fatalf("unexpected turn result: %+v", turnResult)
	}

	ledgerEntry, err := consensus.EncodeCommandWithPayload(
		consensus.OpLedgerBalance,
		"match-1",
		consensus.LedgerBalancePayload{
			PlayerID:      "p1",
			ELO:           1510,
			PeakELO:       1510,
			ELODelta:      10,
			MatchesPlayed: 1,
			Wins:          1,
		},
	)
	if err != nil {
		t.Fatalf("encode ledger: %v", err)
	}

	ledgerResult := fsm.Apply(ledgerEntry)
	if consensus.IsApplyError(ledgerResult) {
		t.Fatalf("apply ledger: %v", ledgerResult)
	}
	applyResult, ok = consensus.AsApplyResult(ledgerResult)
	if !ok || !applyResult.OK || applyResult.Ledger == nil || applyResult.Ledger.ELO != 1510 {
		t.Fatalf("unexpected ledger result: %+v", ledgerResult)
	}
	if repo.careerUpdates != 1 {
		t.Fatalf("careerUpdates = %d, want 1", repo.careerUpdates)
	}

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	restoredManager := engine.NewGameManager(repo)
	restoredFSM := consensus.NewManagedGameFSM(context.Background(), restoredManager)
	if err := restoredFSM.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}

	session, ok := restoredManager.GetSession(context.Background(), "match-1")
	if !ok || len(session.GameBoard) == 0 {
		t.Fatal("restored session missing played tile state")
	}
	profiles := restoredManager.LedgerProfiles()
	if profiles["p1"].ELO != 1510 {
		t.Fatalf("restored ledger elo = %v, want 1510", profiles["p1"].ELO)
	}
}

func TestManagedGameFSM_ApplyTurnRejectsInvalidPayload(t *testing.T) {
	manager := engine.NewGameManager(&stubGameRepo{})
	fsm := consensus.NewManagedGameFSM(context.Background(), manager)

	entry, err := consensus.EncodeCommand(consensus.Command{
		Op:      consensus.OpApplyTurn,
		MatchID: "missing",
	})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	result := fsm.Apply(entry)
	if !consensus.IsApplyError(result) {
		t.Fatalf("expected apply error, got %+v", result)
	}
}
