package engine

import (
	"context"
	"fmt"
	"log"
	"sync"

	"domino_jc_project/pkg/game"
	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/repository"
)

// sessionEntry wraps an in-memory session with a dedicated mutex so mutations
// and persistence on one game never block operations on another.
type sessionEntry struct {
	session *models.GameSession
	mu      sync.Mutex
}

// GameManager orchestrates active in-memory game sessions and coordinates
// persistence through the injected GameRepository.
type GameManager struct {
	// Map mutex: only held during map reads/writes, never during long DB operations.
	mu             sync.RWMutex
	sessions       map[string]*sessionEntry
	recoveredIDs   map[string]bool
	hydrating      map[string]*sync.WaitGroup
	ledgerProfiles map[string]models.PlayerStatsUpdate
	repo           repository.GameRepository

	gameEngine *game.GameEngine
}

// SetGameEngine wires the broker-backed integration layer used by the hot game loop.
func (m *GameManager) SetGameEngine(engine *game.GameEngine) {
	m.gameEngine = engine
}

// NewGameManager constructs a GameManager backed by the given repository.
func NewGameManager(repo repository.GameRepository) *GameManager {
	return &GameManager{
		sessions:       make(map[string]*sessionEntry),
		recoveredIDs:   make(map[string]bool),
		hydrating:      make(map[string]*sync.WaitGroup),
		ledgerProfiles: make(map[string]models.PlayerStatsUpdate),
		repo:           repo,
	}
}

// BootstrapActiveSessions loads IDs of ACTIVE sessions from Dgraph into recoveredIDs
// so they can be lazily hydrated when a player reconnects after a server restart.
func (m *GameManager) BootstrapActiveSessions(ctx context.Context, repo repository.GameRepository) error {
	ids, err := repo.ListActiveSessionIDs(ctx)
	if err != nil {
		return fmt.Errorf("list active session IDs: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, id := range ids {
		if _, loaded := m.sessions[id]; !loaded {
			m.recoveredIDs[id] = true
		}
	}

	log.Printf("Registered %d active session ID(s) for lazy recovery", len(m.recoveredIDs))
	return nil
}

// CreateSession initializes a new game session and registers it in memory.
func (m *GameManager) CreateSession(sessionID string, playerUIDs []string) (*models.GameSession, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("cannot create a session with an empty session ID")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[sessionID]; exists {
		return nil, fmt.Errorf("session %q already exists", sessionID)
	}

	session := models.NewGameSession(sessionID, playerUIDs)
	m.sessions[sessionID] = &sessionEntry{session: session}
	delete(m.recoveredIDs, sessionID)
	return session, nil
}

// GetSession retrieves an in-memory session or lazily hydrates one from Dgraph when
// the ID was registered during startup recovery.
func (m *GameManager) GetSession(ctx context.Context, sessionID string) (*models.GameSession, bool) {
	m.mu.RLock()
	entry, exists := m.sessions[sessionID]
	if exists {
		m.mu.RUnlock()
		return entry.session, true
	}
	inRecovered := m.recoveredIDs[sessionID]
	m.mu.RUnlock()

	if !inRecovered {
		return nil, false
	}

	session, err := m.hydrateRecoveredSession(ctx, sessionID)
	if err != nil {
		log.Printf("failed to hydrate recovered session %q: %v", sessionID, err)
		return nil, false
	}
	return session, true
}

// AddSession inserts a newly created or hydrated session into the registry.
func (m *GameManager) AddSession(session *models.GameSession) error {
	if session == nil || session.SessionID == "" {
		return fmt.Errorf("cannot add an invalid or empty session")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessions[session.SessionID] = &sessionEntry{session: session}
	delete(m.recoveredIDs, session.SessionID)
	return nil
}

// EvictSession unloads a session from active memory once it is saved to Dgraph.
func (m *GameManager) EvictSession(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.sessions, sessionID)
}

func (m *GameManager) persistState(ctx context.Context, session *models.GameSession) error {
	return m.repo.SaveSession(ctx, session)
}

// hydrateRecoveredSession loads a single session from Dgraph, ensuring concurrent
// requests for the same ID share one database read.
func (m *GameManager) hydrateRecoveredSession(ctx context.Context, sessionID string) (*models.GameSession, error) {
	m.mu.Lock()

	if entry, ok := m.sessions[sessionID]; ok {
		m.mu.Unlock()
		return entry.session, nil
	}

	if wg, ok := m.hydrating[sessionID]; ok {
		m.mu.Unlock()
		wg.Wait()

		m.mu.RLock()
		entry, ok := m.sessions[sessionID]
		m.mu.RUnlock()
		if ok {
			return entry.session, nil
		}
		return nil, fmt.Errorf("session %q not found after concurrent hydration", sessionID)
	}

	if !m.recoveredIDs[sessionID] {
		m.mu.Unlock()
		return nil, fmt.Errorf("session %q is not eligible for recovery", sessionID)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	m.hydrating[sessionID] = wg
	m.mu.Unlock()

	session, err := m.repo.GetSession(ctx, sessionID)

	m.mu.Lock()
	delete(m.hydrating, sessionID)
	if err == nil {
		if err := m.addSessionLocked(session); err != nil {
			wg.Done()
			m.mu.Unlock()
			return nil, err
		}
	}
	wg.Done()
	m.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return session, nil
}

func (m *GameManager) addSessionLocked(session *models.GameSession) error {
	if session == nil || session.SessionID == "" {
		return fmt.Errorf("cannot add an invalid or empty session")
	}
	m.sessions[session.SessionID] = &sessionEntry{session: session}
	delete(m.recoveredIDs, session.SessionID)
	return nil
}

// lookupSession resolves an entry under a brief map read lock without holding
// the session mutex. Callers must lock entry.mu themselves.
func (m *GameManager) lookupSession(ctx context.Context, sessionID string) (*sessionEntry, error) {
	m.mu.RLock()
	entry, ok := m.sessions[sessionID]
	if ok {
		m.mu.RUnlock()
		return entry, nil
	}
	inRecovered := m.recoveredIDs[sessionID]
	m.mu.RUnlock()

	if inRecovered {
		if _, err := m.hydrateRecoveredSession(ctx, sessionID); err != nil {
			return nil, err
		}
		m.mu.RLock()
		entry, ok = m.sessions[sessionID]
		m.mu.RUnlock()
		if ok {
			return entry, nil
		}
	}

	return nil, fmt.Errorf("session %q not found", sessionID)
}

// withSessionWrite resolves a session under a brief map read lock, then runs fn
// while holding only that session's mutex (including any persistence afterward).
func (m *GameManager) withSessionWrite(
	ctx context.Context,
	sessionID string,
	fn func(*models.GameSession) error,
) error {
	entry, err := m.lookupSession(ctx, sessionID)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if err := fn(entry.session); err != nil {
		return err
	}

	return m.persistState(ctx, entry.session)
}
