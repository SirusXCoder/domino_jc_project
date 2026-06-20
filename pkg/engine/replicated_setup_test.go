package engine_test

import (
	"context"
	"testing"

	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
)

type setupTestRepo struct {
	saved int
}

func (r *setupTestRepo) SaveSession(_ context.Context, _ *models.GameSession) error {
	r.saved++
	return nil
}

func (r *setupTestRepo) GetSession(_ context.Context, _ string) (*models.GameSession, error) {
	return nil, nil
}

func (r *setupTestRepo) ListActiveSessionIDs(_ context.Context) ([]string, error) {
	return nil, nil
}

func (r *setupTestRepo) SaveMatchRecord(_ context.Context, _ models.MatchRecord) error {
	return nil
}

func (r *setupTestRepo) GetPlayersByIDs(_ context.Context, _ []string) ([]models.Player, error) {
	return nil, nil
}

func (r *setupTestRepo) UpdatePlayerCareers(_ context.Context, _ []models.Player) error {
	return nil
}

func (r *setupTestRepo) ListLeaderboard(_ context.Context, _ int) ([]models.LeaderboardEntry, error) {
	return nil, nil
}

func (r *setupTestRepo) GetPlayerCareer(_ context.Context, _ string, _ int) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (r *setupTestRepo) GetMatchRecord(_ context.Context, _ string) (*models.MatchRecord, error) {
	return nil, nil
}

func (r *setupTestRepo) ApplyMatchRatings(_ context.Context, _ string, _ models.ELODeltas) error {
	return nil
}

func (r *setupTestRepo) GetPlayerProfile(_ context.Context, _ string) (*models.PlayerCareerStats, error) {
	return nil, nil
}

func (r *setupTestRepo) ListPlayerMatchHistory(_ context.Context, _ string, _ int, _ string) (*models.MatchHistoryPage, error) {
	return nil, nil
}

func (r *setupTestRepo) GetLedgerState(_ context.Context, _ string) (*models.LedgerState, error) {
	return nil, nil
}

func (r *setupTestRepo) GetMatchWithPlayers(_ context.Context, _ string) (*models.MatchRecord, []models.Player, error) {
	return nil, nil, nil
}

func TestApplyReplicatedStartMatch_DealsExactlySevenTiles(t *testing.T) {
	t.Parallel()

	const matchID = "match-7bones"
	manager := engine.NewGameManager(&setupTestRepo{})
	ctx := context.Background()

	session, err := manager.ApplyReplicatedStartMatch(ctx, matchID, engine.ReplicatedMatchSetup{
		PlayerUIDs:   []string{"p1", "p2"},
		SetupGame:    true,
		TilesPerHand: 7,
	})
	if err != nil {
		t.Fatalf("start match: %v", err)
	}

	assertHandSize(t, session, "p1", 7)
	assertHandSize(t, session, "p2", 7)
	if len(session.Boneyard) != 14 {
		t.Fatalf("boneyard = %d tiles, want 14", len(session.Boneyard))
	}

	// Re-applying setup for the same match must not accumulate extra tiles.
	session, err = manager.ApplyReplicatedStartMatch(ctx, matchID, engine.ReplicatedMatchSetup{
		PlayerUIDs:   []string{"p1", "p2"},
		SetupGame:    true,
		TilesPerHand: 7,
	})
	if err != nil {
		t.Fatalf("restart match: %v", err)
	}
	assertHandSize(t, session, "p1", 7)
	assertHandSize(t, session, "p2", 7)
	if len(session.Boneyard) != 14 {
		t.Fatalf("after restart boneyard = %d tiles, want 14", len(session.Boneyard))
	}
}

func TestApplyReplicatedStartMatch_DeterministicAcrossManagers(t *testing.T) {
	t.Parallel()

	const matchID = "match-deterministic"
	setup := engine.ReplicatedMatchSetup{
		PlayerUIDs:   []string{"p1", "p2"},
		SetupGame:    true,
		TilesPerHand: 7,
	}

	managerA := engine.NewGameManager(&setupTestRepo{})
	sessionA, err := managerA.ApplyReplicatedStartMatch(context.Background(), matchID, setup)
	if err != nil {
		t.Fatalf("manager A start: %v", err)
	}

	managerB := engine.NewGameManager(&setupTestRepo{})
	sessionB, err := managerB.ApplyReplicatedStartMatch(context.Background(), matchID, setup)
	if err != nil {
		t.Fatalf("manager B start: %v", err)
	}

	if !equalStringSlices(tileIDs(sessionA.Hands[0].Tiles), tileIDs(sessionB.Hands[0].Tiles)) {
		t.Fatalf("p1 hands diverged: A=%v B=%v", tileIDs(sessionA.Hands[0].Tiles), tileIDs(sessionB.Hands[0].Tiles))
	}
	if !equalStringSlices(tileIDs(sessionA.Hands[1].Tiles), tileIDs(sessionB.Hands[1].Tiles)) {
		t.Fatalf("p2 hands diverged: A=%v B=%v", tileIDs(sessionA.Hands[1].Tiles), tileIDs(sessionB.Hands[1].Tiles))
	}
}

func TestApplyReplicatedTurn_TwoPlayerPlaysPreserveHandIntegrity(t *testing.T) {
	t.Parallel()

	const matchID = "match-two-turns"
	manager := engine.NewGameManager(&setupTestRepo{})
	ctx := context.Background()

	session, err := manager.ApplyReplicatedStartMatch(ctx, matchID, engine.ReplicatedMatchSetup{
		PlayerUIDs:   []string{"p1", "p2"},
		SetupGame:    true,
		TilesPerHand: 7,
	})
	if err != nil {
		t.Fatalf("start match: %v", err)
	}

	p1First := session.Hands[0].Tiles[0]
	p1HandBefore := append([]models.DominoTile(nil), session.Hands[0].Tiles...)

	turn1, err := manager.ApplyReplicatedTurn(ctx, matchID, models.TurnAction{
		Kind:       models.TurnKindPlayTile,
		PlayerID:   "p1",
		Tile:       p1First,
		PlayAtLeft: true,
	})
	if err != nil {
		t.Fatalf("p1 play: %v", err)
	}
	if turn1 == nil || !turn1.Applied {
		t.Fatal("expected p1 turn to apply")
	}

	session, ok := manager.GetSession(ctx, matchID)
	if !ok {
		t.Fatal("session missing after p1 play")
	}
	assertHandSize(t, session, "p1", 6)
	assertHandSize(t, session, "p2", 7)
	if len(session.GameBoard) != 1 {
		t.Fatalf("board = %d tiles, want 1", len(session.GameBoard))
	}
	if session.CurrentTurn != "p2" {
		t.Fatalf("current_turn = %q, want p2", session.CurrentTurn)
	}
	if !handStillContainsExpectedTiles(t, session.Hands[0].Tiles, p1HandBefore, p1First.ID) {
		t.Fatal("p1 hand changed unexpectedly after first play")
	}

	p2Tile, playAtLeft, ok := findPlayableTile(session, "p2")
	if !ok {
		t.Fatal("p2 has no playable tile for second turn")
	}
	p2HandBefore := append([]models.DominoTile(nil), session.Hands[1].Tiles...)

	turn2, err := manager.ApplyReplicatedTurn(ctx, matchID, models.TurnAction{
		Kind:       models.TurnKindPlayTile,
		PlayerID:   "p2",
		Tile:       p2Tile,
		PlayAtLeft: playAtLeft,
	})
	if err != nil {
		t.Fatalf("p2 play: %v", err)
	}
	if turn2 == nil || !turn2.Applied {
		t.Fatal("expected p2 turn to apply")
	}

	session, ok = manager.GetSession(ctx, matchID)
	if !ok {
		t.Fatal("session missing after p2 play")
	}
	assertHandSize(t, session, "p1", 6)
	assertHandSize(t, session, "p2", 6)
	if len(session.GameBoard) != 2 {
		t.Fatalf("board = %d tiles, want 2", len(session.GameBoard))
	}
	if session.CurrentTurn != "p1" {
		t.Fatalf("current_turn = %q, want p1", session.CurrentTurn)
	}
	if !handStillContainsExpectedTiles(t, session.Hands[1].Tiles, p2HandBefore, p2Tile.ID) {
		t.Fatal("p2 hand changed unexpectedly after second play")
	}
}

func assertHandSize(t *testing.T, session *models.GameSession, playerID string, want int) {
	t.Helper()
	for _, hand := range session.Hands {
		if hand.PlayerID != playerID {
			continue
		}
		if len(hand.Tiles) != want {
			t.Fatalf("player %s hand = %d tiles, want %d (tiles=%v)", playerID, len(hand.Tiles), want, tileIDs(hand.Tiles))
		}
		return
	}
	t.Fatalf("player %s not found in session hands", playerID)
}

func tileIDs(tiles []models.DominoTile) []string {
	out := make([]string, len(tiles))
	for i, tile := range tiles {
		out[i] = tile.ID
	}
	return out
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func handStillContainsExpectedTiles(
	t *testing.T,
	current []models.DominoTile,
	before []models.DominoTile,
	playedID string,
) bool {
	t.Helper()

	expected := make([]string, 0, len(before)-1)
	for _, tile := range before {
		if tile.ID == playedID {
			continue
		}
		expected = append(expected, tile.ID)
	}
	got := tileIDs(current)
	if len(got) != len(expected) {
		t.Fatalf("hand size changed from %d to %d: got=%v expected=%v", len(expected), len(got), got, expected)
	}

	gotSet := make(map[string]int, len(got))
	for _, id := range got {
		gotSet[id]++
	}
	for _, id := range expected {
		gotSet[id]--
	}
	for id, count := range gotSet {
		if count != 0 {
			t.Fatalf("unexpected tile delta for %q: count=%d", id, count)
		}
	}
	return true
}

func findPlayableTile(session *models.GameSession, playerID string) (models.DominoTile, bool, bool) {
	for _, hand := range session.Hands {
		if hand.PlayerID != playerID {
			continue
		}
		for _, tile := range hand.Tiles {
			if session.CanPlay(tile, true) {
				return tile, true, true
			}
			if session.CanPlay(tile, false) {
				return tile, false, true
			}
		}
	}
	return models.DominoTile{}, false, false
}
