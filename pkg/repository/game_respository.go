package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"domino_jc_project/pkg/models"

	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
)

// GameRepository defines the persistence operations for a Domino game session.
type GameRepository interface {
	SaveSession(ctx context.Context, session *models.GameSession) error
	GetSession(ctx context.Context, sessionID string) (*models.GameSession, error)
	ListActiveSessionIDs(ctx context.Context) ([]string, error)
	MatchLedgerRepository
	StatsRepository
}

// dgraphGameRepository is the concrete implementation backed by Dgraph.
type dgraphGameRepository struct {
	dg *dgo.Dgraph
}

// NewDgraphGameRepository constructs a new Dgraph-backed repository instance.
func NewDgraphGameRepository(dg *dgo.Dgraph) GameRepository {
	return &dgraphGameRepository{
		dg: dg,
	}
}

// SaveSession executes the full storage lifecycle to persist a game session.
func (r *dgraphGameRepository) SaveSession(ctx context.Context, session *models.GameSession) error {
	if session == nil {
		return fmt.Errorf("cannot save a nil game session")
	}

	// 1. Flush State: Marshal internal types into raw string blobs in-place
	session.FlushPlayState()

	// 2. JSON Mutation preparation: Marshal the struct into Dgraph's native JSON format
	mu := &api.Mutation{
		CommitNow: true,
	}

	sessionJSON, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("failed to marshal game session to JSON: %w", err)
	}
	mu.SetJson = sessionJSON

	// 3. Execute Mutation: Run a new transaction block to upsert/mutate the document
	txn := r.dg.NewTxn()
	defer txn.Discard(ctx)

	assigned, err := txn.Mutate(ctx, mu)
	if err != nil {
		return fmt.Errorf("failed to execute Dgraph mutation: %w", err)
	}

	// If this was a creation operation, capture and assign the newly generated UID back to the model
	if session.UID == "" {
		if newUID, exists := assigned.Uids["blank-0"]; exists {
			session.UID = newUID
		} else if len(assigned.Uids) > 0 {
			for _, uid := range assigned.Uids {
				session.UID = uid
				break
			}
		}
	}

	return nil
}

// GetSession loads a game session from Dgraph by its application session ID and hydrates play state.
func (r *dgraphGameRepository) GetSession(ctx context.Context, sessionID string) (*models.GameSession, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("session ID cannot be empty")
	}

	const query = `
	query querySession($sessionID: string) {
		session(func: has(session_id), first: 1) @filter(eq(session_id, $sessionID)) {
			uid
			session_id
			status
			players
			hands
			boneyard_raw
			game_board_raw
			left_open_value
			right_open_value
			current_turn
		}
	}`

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	vars := map[string]string{"$sessionID": sessionID}
	resp, err := txn.QueryWithVars(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("failed to query Dgraph for session %q: %w", sessionID, err)
	}

	var result struct {
		Session []models.GameSession `json:"session"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("failed to parse Dgraph response for session %q: %w", sessionID, err)
	}

	if len(result.Session) == 0 {
		return nil, fmt.Errorf("game session not found: session_id=%q", sessionID)
	}

	session := &result.Session[0]
	if err := session.HydratePlayState(); err != nil {
		return nil, fmt.Errorf("failed to hydrate play state for session %q: %w", sessionID, err)
	}

	return session, nil
}

// ListActiveSessionIDs returns session_id values for all persisted games marked ACTIVE.
func (r *dgraphGameRepository) ListActiveSessionIDs(ctx context.Context) ([]string, error) {
	const query = `
	query {
		sessions(func: eq(game_session.status, "ACTIVE")) {
			game_session.session_id
		}
	}`

	txn := r.dg.NewReadOnlyTxn()
	defer txn.Discard(ctx)

	resp, err := txn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active session IDs: %w", err)
	}

	var result struct {
		Sessions []struct {
			SessionID string `json:"game_session.session_id"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, fmt.Errorf("failed to parse active session ID response: %w", err)
	}

	ids := make([]string, 0, len(result.Sessions))
	for _, s := range result.Sessions {
		if s.SessionID != "" {
			ids = append(ids, s.SessionID)
		}
	}
	return ids, nil
}

// SaveMatchRecord upserts an immutable match outcome node into Dgraph.
func (r *dgraphGameRepository) SaveMatchRecord(ctx context.Context, record models.MatchRecord) error {
	mu := &api.Mutation{
		CommitNow: true,
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal match record: %w", err)
	}
	mu.SetJson = recordJSON

	txn := r.dg.NewTxn()
	defer txn.Discard(ctx)

	if _, err := txn.Mutate(ctx, mu); err != nil {
		return fmt.Errorf("failed to persist match record: %w", err)
	}

	return nil
}