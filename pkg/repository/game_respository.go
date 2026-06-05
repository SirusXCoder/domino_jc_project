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