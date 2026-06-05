package engine

import (
	"context"
	"fmt"
	"sync"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/repository"
)

// GameManager orchestrates active in-memory game sessions and coordinates
// persistence through the injected GameRepository.
type GameManager struct {
	mu       sync.RWMutex
	sessions map[string]*models.GameSession
	repo     repository.GameRepository
}

// NewGameManager constructs a GameManager backed by the given repository.
func NewGameManager(repo repository.GameRepository) *GameManager {
	return &GameManager{
		sessions: make(map[string]*models.GameSession),
		repo:     repo,
	}
}

// RegisterSession tracks an active game session in the manager's in-memory store.
func (m *GameManager) RegisterSession(session *models.GameSession) error {
	if session == nil {
		return fmt.Errorf("cannot register a nil game session")
	}
	if session.SessionID == "" {
		return fmt.Errorf("cannot register a session with an empty session ID")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessions[session.SessionID] = session
	return nil
}

// GetSession returns the in-memory session for the given session ID.
func (m *GameManager) GetSession(sessionID string) (*models.GameSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, ok := m.sessions[sessionID]
	return session, ok
}

func (m *GameManager) persistState(ctx context.Context, session *models.GameSession) error {
	return m.repo.SaveSession(ctx, session)
}
