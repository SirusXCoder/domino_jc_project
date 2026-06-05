package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"domino_jc_project/pkg/database"
	"domino_jc_project/pkg/models"
)

func mockGameSession() *models.GameSession {
	return &models.GameSession{
		SessionID: "smoke-test-save-session",
		Status:    models.SessionStatusActive,
		Players:   []string{"player-a", "player-b"},
		Hands: []models.PlayerHand{
			{
				PlayerID: "player-a",
				Tiles: []models.DominoTile{
					models.NewTile(1, 2),
					models.NewTile(3, 4),
				},
				IsReady: true,
			},
			{
				PlayerID: "player-b",
				Tiles: []models.DominoTile{
					models.NewTile(0, 5),
					models.NewTile(2, 2),
				},
				IsReady: true,
			},
		},
		Boneyard: []models.DominoTile{
			models.NewTile(0, 0),
			models.NewTile(0, 1),
			models.NewTile(4, 4),
			models.NewTile(5, 6),
		},
		GameBoard: []models.DominoTile{
			models.NewTile(5, 6),
			models.NewTile(6, 2),
			models.NewTile(2, 3),
		},
		LeftOpenValue:  5,
		RightOpenValue: 3,
		CurrentTurn:    "player-a",
	}
}

// TestFlushPlayState_Stringify verifies complex slices marshal into raw JSON blobs.
func TestFlushPlayState_Stringify(t *testing.T) {
	session := mockGameSession()
	session.FlushPlayState()

	if session.BoneyardRaw == "" {
		t.Fatal("expected BoneyardRaw to be populated")
	}
	if session.GameBoardRaw == "" {
		t.Fatal("expected GameBoardRaw to be populated")
	}

	var boneyard []models.DominoTile
	if err := json.Unmarshal([]byte(session.BoneyardRaw), &boneyard); err != nil {
		t.Fatalf("BoneyardRaw is not valid JSON: %v", err)
	}
	if len(boneyard) != len(session.Boneyard) {
		t.Fatalf("BoneyardRaw tile count mismatch: got %d, want %d", len(boneyard), len(session.Boneyard))
	}

	var board []models.DominoTile
	if err := json.Unmarshal([]byte(session.GameBoardRaw), &board); err != nil {
		t.Fatalf("GameBoardRaw is not valid JSON: %v", err)
	}
	if len(board) != len(session.GameBoard) {
		t.Fatalf("GameBoardRaw tile count mismatch: got %d, want %d", len(board), len(session.GameBoard))
	}

	t.Logf("BoneyardRaw: %s", session.BoneyardRaw)
	t.Logf("GameBoardRaw: %s", session.GameBoardRaw)
}

// TestSaveSession_Integration persists a mock session to a live Dgraph instance.
// Skips when Dgraph is unreachable (set DGRAPH_ALPHA_GRPC to override the default).
func TestSaveSession_Integration(t *testing.T) {
	addr := os.Getenv("DGRAPH_ALPHA_GRPC")
	if addr == "" {
		addr = "localhost:9080"
	}

	dgClient, conn, err := database.InitDgraphClient(database.Config{Address: addr})
	if err != nil {
		t.Skipf("Dgraph not available at %s: %v", addr, err)
	}
	defer conn.Close()

	repo := NewDgraphGameRepository(dgClient)
	session := mockGameSession()

	ctx := context.Background()
	if err := repo.SaveSession(ctx, session); err != nil {
		t.Fatalf("SaveSession failed: %v", err)
	}

	if session.UID == "" {
		t.Fatal("expected Dgraph to assign a UID after insert")
	}

	if session.BoneyardRaw == "" || session.GameBoardRaw == "" {
		t.Fatal("expected FlushPlayState to populate raw blobs before save")
	}

	t.Logf("session saved with UID=%s", session.UID)
	t.Logf("BoneyardRaw: %s", session.BoneyardRaw)
	t.Logf("GameBoardRaw: %s", session.GameBoardRaw)
}

// TestGetSession_Integration round-trips a session through SaveSession and GetSession,
// verifying raw blobs are hydrated back into typed slices.
func TestGetSession_Integration(t *testing.T) {
	addr := os.Getenv("DGRAPH_ALPHA_GRPC")
	if addr == "" {
		addr = "localhost:9080"
	}

	dgClient, conn, err := database.InitDgraphClient(database.Config{Address: addr})
	if err != nil {
		t.Skipf("Dgraph not available at %s: %v", addr, err)
	}
	defer conn.Close()

	repo := NewDgraphGameRepository(dgClient)
	session := mockGameSession()
	session.SessionID = "hydration-roundtrip-test"

	ctx := context.Background()
	if err := repo.SaveSession(ctx, session); err != nil {
		t.Fatalf("SaveSession failed: %v", err)
	}

	loaded, err := repo.GetSession(ctx, session.SessionID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}

	if loaded.SessionID != session.SessionID {
		t.Fatalf("SessionID mismatch: got %q, want %q", loaded.SessionID, session.SessionID)
	}
	if len(loaded.Boneyard) != len(session.Boneyard) {
		t.Fatalf("Boneyard length mismatch: got %d, want %d", len(loaded.Boneyard), len(session.Boneyard))
	}
	if len(loaded.GameBoard) != len(session.GameBoard) {
		t.Fatalf("GameBoard length mismatch: got %d, want %d", len(loaded.GameBoard), len(session.GameBoard))
	}
	if loaded.Boneyard[0].ID != session.Boneyard[0].ID {
		t.Fatalf("hydrated boneyard tile mismatch: got %q, want %q", loaded.Boneyard[0].ID, session.Boneyard[0].ID)
	}
	if loaded.GameBoard[0].ID != session.GameBoard[0].ID {
		t.Fatalf("hydrated board tile mismatch: got %q, want %q", loaded.GameBoard[0].ID, session.GameBoard[0].ID)
	}
	if loaded.LeftOpenValue != session.LeftOpenValue || loaded.RightOpenValue != session.RightOpenValue {
		t.Fatalf("open values mismatch: got (%d,%d), want (%d,%d)",
			loaded.LeftOpenValue, loaded.RightOpenValue, session.LeftOpenValue, session.RightOpenValue)
	}

	t.Logf("hydrated session UID=%s with %d boneyard tiles and %d board tiles",
		loaded.UID, len(loaded.Boneyard), len(loaded.GameBoard))
}

// TestGetSession_NotFound verifies a clear error when the session does not exist.
func TestGetSession_NotFound(t *testing.T) {
	addr := os.Getenv("DGRAPH_ALPHA_GRPC")
	if addr == "" {
		addr = "localhost:9080"
	}

	dgClient, conn, err := database.InitDgraphClient(database.Config{Address: addr})
	if err != nil {
		t.Skipf("Dgraph not available at %s: %v", addr, err)
	}
	defer conn.Close()

	repo := NewDgraphGameRepository(dgClient)
	_, err = repo.GetSession(context.Background(), "nonexistent-session-id-xyz")
	if err == nil {
		t.Fatal("expected error for missing session")
	}
}
