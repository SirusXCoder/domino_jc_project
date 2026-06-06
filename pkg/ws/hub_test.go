package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"domino_jc_project/pkg/models"
)

type reconnectStubActions struct {
	session    *models.GameSession
	abandoned  atomic.Int32
	abandonErr error
}

func (s *reconnectStubActions) ApplyPlayTile(ctx context.Context, sessionID, playerID string, tile models.DominoTile, playAtLeft bool) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (s *reconnectStubActions) ApplyDrawFromBoneyard(ctx context.Context, sessionID, playerID string) (*models.DominoTile, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *reconnectStubActions) ApplyPassTurn(ctx context.Context, sessionID, playerID string) error {
	return fmt.Errorf("not implemented")
}

func (s *reconnectStubActions) HandlePlayerAbandoned(ctx context.Context, sessionID, playerID string) error {
	s.abandoned.Add(1)
	return s.abandonErr
}

func (s *reconnectStubActions) GetSession(ctx context.Context, sessionID string) (*models.GameSession, bool) {
	if s.session != nil && s.session.SessionID == sessionID {
		return s.session, true
	}
	return nil, false
}

func TestHub_DisconnectMarksDisconnectedAndAbandonsAfterGrace(t *testing.T) {
	actions := &reconnectStubActions{
		session: models.NewGameSession("s1", []string{"p1"}),
	}

	hub := NewHub(actions, WithReconnectGracePeriod(50*time.Millisecond))
	go hub.Run()

	client := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 4),
	}

	hub.register <- client
	waitFor(t, 2*time.Second, func() bool {
		return hub.PlayerConnectionStatus("s1", "p1") == ConnectionStatusConnected
	})

	hub.unregister <- client
	waitFor(t, 2*time.Second, func() bool {
		return hub.PlayerConnectionStatus("s1", "p1") == ConnectionStatusDisconnected
	})

	waitFor(t, 2*time.Second, func() bool {
		return hub.PlayerConnectionStatus("s1", "p1") == ConnectionStatusAbandoned
	})

	if actions.abandoned.Load() != 1 {
		t.Fatalf("abandoned invocations = %d, want 1", actions.abandoned.Load())
	}
}

func TestHub_ReconnectCancelsGraceAndSendsSnapshot(t *testing.T) {
	actions := &reconnectStubActions{
		session: models.NewGameSession("s1", []string{"p1"}),
	}

	hub := NewHub(actions, WithReconnectGracePeriod(500*time.Millisecond))
	go hub.Run()

	first := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 4),
	}
	hub.register <- first
	waitFor(t, 2*time.Second, func() bool {
		return hub.ClientCount() == 1
	})

	hub.unregister <- first
	waitFor(t, 2*time.Second, func() bool {
		return hub.PlayerConnectionStatus("s1", "p1") == ConnectionStatusDisconnected
	})

	second := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 4),
	}
	hub.register <- second

	waitFor(t, 2*time.Second, func() bool {
		return hub.PlayerConnectionStatus("s1", "p1") == ConnectionStatusConnected
	})

	select {
	case payload := <-second.send:
		var envelope EventEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("unmarshal snapshot envelope: %v", err)
		}
		if envelope.Type != EventTypeStateSnapshot {
			t.Fatalf("envelope type = %q, want %q", envelope.Type, EventTypeStateSnapshot)
		}
	case <-time.After(time.Second):
		t.Fatal("expected state snapshot on reconnect")
	}

	time.Sleep(600 * time.Millisecond)
	if hub.PlayerConnectionStatus("s1", "p1") != ConnectionStatusConnected {
		t.Fatalf("status after grace window = %q, want %q", hub.PlayerConnectionStatus("s1", "p1"), ConnectionStatusConnected)
	}
	if actions.abandoned.Load() != 0 {
		t.Fatalf("abandoned invocations = %d, want 0", actions.abandoned.Load())
	}
}

func TestHub_StaleUnregisterIgnoredAfterReplacement(t *testing.T) {
	hub := NewHub(nil, WithReconnectGracePeriod(50*time.Millisecond))
	go hub.Run()

	first := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 1),
	}
	second := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 1),
	}

	hub.register <- first
	waitFor(t, time.Second, func() bool { return hub.ClientCount() == 1 })

	hub.register <- second
	waitFor(t, time.Second, func() bool { return hub.ClientCount() == 1 })

	hub.unregister <- first
	time.Sleep(100 * time.Millisecond)

	if hub.PlayerConnectionStatus("s1", "p1") != ConnectionStatusConnected {
		t.Fatalf("status = %q, want %q after stale unregister", hub.PlayerConnectionStatus("s1", "p1"), ConnectionStatusConnected)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
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
