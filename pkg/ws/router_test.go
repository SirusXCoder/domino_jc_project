package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"domino_jc_project/pkg/models"
)

type stubGameActions struct {
	playTileFn func(ctx context.Context, sessionID, playerID string, tile models.DominoTile, playAtLeft bool) (bool, error)
	drawFn     func(ctx context.Context, sessionID, playerID string) (*models.DominoTile, error)
	passFn     func(ctx context.Context, sessionID, playerID string) error
	session    *models.GameSession
}

func (s *stubGameActions) ApplyPlayTile(ctx context.Context, sessionID, playerID string, tile models.DominoTile, playAtLeft bool) (bool, error) {
	if s.playTileFn != nil {
		return s.playTileFn(ctx, sessionID, playerID, tile, playAtLeft)
	}
	if s.session == nil {
		return false, fmt.Errorf("session %q not found", sessionID)
	}
	return true, nil
}

func (s *stubGameActions) ApplyDrawFromBoneyard(ctx context.Context, sessionID, playerID string) (*models.DominoTile, error) {
	if s.drawFn != nil {
		return s.drawFn(ctx, sessionID, playerID)
	}
	return nil, nil
}

func (s *stubGameActions) ApplyPassTurn(ctx context.Context, sessionID, playerID string) error {
	if s.passFn != nil {
		return s.passFn(ctx, sessionID, playerID)
	}
	return nil
}

func (s *stubGameActions) HandlePlayerAbandoned(ctx context.Context, sessionID, playerID string) error {
	return nil
}

func (s *stubGameActions) GetSession(ctx context.Context, sessionID string) (*models.GameSession, bool) {
	if s.session != nil && s.session.SessionID == sessionID {
		return s.session, true
	}
	return nil, false
}

func TestEventRouter_UnknownEventType(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	actions := &stubGameActions{}
	hub.router = NewEventRouter(hub, actions)

	client := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 1),
	}
	hub.mu.Lock()
	hub.clients = map[string]map[string]*Client{"s1": {"p1": client}}
	hub.mu.Unlock()

	payload, _ := json.Marshal(EventEnvelope{
		Type:      "NOT_A_REAL_EVENT",
		Timestamp: time.Now().UnixMilli(),
		Payload:   json.RawMessage(`{}`),
	})

	hub.handleInbound(&InboundMessage{
		SessionID: "s1",
		PlayerID:  "p1",
		Payload:   payload,
	})

	select {
	case out := <-client.send:
		var errEnv ErrorEnvelope
		if err := json.Unmarshal(out, &errEnv); err != nil {
			t.Fatalf("unmarshal error envelope: %v", err)
		}
		if errEnv.Error != ErrCodeUnknownEventType {
			t.Fatalf("error code = %q, want %q", errEnv.Error, ErrCodeUnknownEventType)
		}
	case <-time.After(time.Second):
		t.Fatal("expected error envelope on client send channel")
	}
}

func TestEventRouter_PlayTileSessionNotFound(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	hub.router = NewEventRouter(hub, &stubGameActions{})

	client := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 1),
	}
	hub.mu.Lock()
	hub.clients = map[string]map[string]*Client{"s1": {"p1": client}}
	hub.mu.Unlock()

	playPayload, _ := json.Marshal(PlayTilePayload{
		SessionID:  "s1",
		PlayerID:   "p1",
		Tile:       models.NewTile(1, 2),
		PlayAtLeft: true,
	})
	envelope, _ := json.Marshal(EventEnvelope{
		Type:      EventTypePlayTile,
		Timestamp: time.Now().UnixMilli(),
		Payload:   playPayload,
	})

	hub.handleInbound(&InboundMessage{
		SessionID: "s1",
		PlayerID:  "p1",
		Payload:   envelope,
	})

	select {
	case out := <-client.send:
		var errEnv ErrorEnvelope
		if err := json.Unmarshal(out, &errEnv); err != nil {
			t.Fatalf("unmarshal error envelope: %v", err)
		}
		if errEnv.Error != ErrCodeSessionNotFound {
			t.Fatalf("error code = %q, want %q", errEnv.Error, ErrCodeSessionNotFound)
		}
	case <-time.After(time.Second):
		t.Fatal("expected error envelope on client send channel")
	}
}

func TestEventRouter_PlayerMoveInvokesApplyPlayTile(t *testing.T) {
	hub := NewHub(nil)

	tile := models.NewTile(3, 4)
	session := models.NewGameSession("s1", []string{"p1"})
	session.Hands[0].Tiles = []models.DominoTile{tile}

	called := false
	actions := &stubGameActions{
		session: session,
		playTileFn: func(ctx context.Context, sessionID, playerID string, got models.DominoTile, playAtLeft bool) (bool, error) {
			called = true
			if sessionID != "s1" || playerID != "p1" || got.ID != tile.ID || !playAtLeft {
				t.Fatalf("unexpected ApplyPlayTile args: session=%s player=%s tile=%s playAtLeft=%v", sessionID, playerID, got.ID, playAtLeft)
			}
			return true, nil
		},
	}

	router := NewEventRouter(hub, actions)

	movePayload, _ := json.Marshal(MovePayload{
		SessionID:  "s1",
		PlayerID:   "p1",
		TileID:     tile.ID,
		PlayAtLeft: true,
	})
	envelope, _ := json.Marshal(EventEnvelope{
		Type:      EventTypePlayerMove,
		Timestamp: time.Now().UnixMilli(),
		Payload:   movePayload,
	})

	router.Route(context.Background(), &InboundMessage{
		SessionID: "s1",
		PlayerID:  "p1",
		Payload:   envelope,
	})

	if !called {
		t.Fatal("expected ApplyPlayTile to be called")
	}
}
