package ws

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"domino_jc_project/pkg/models"
)

type maskingStubActions struct {
	session *models.GameSession
}

func (s *maskingStubActions) ApplyPlayTile(context.Context, string, string, models.DominoTile, bool) (bool, error) {
	return false, nil
}

func (s *maskingStubActions) ApplyDrawFromBoneyard(context.Context, string, string) (*models.DominoTile, error) {
	return nil, nil
}

func (s *maskingStubActions) ApplyPassTurn(context.Context, string, string) error {
	return nil
}

func (s *maskingStubActions) HandlePlayerAbandoned(context.Context, string, string) error {
	return nil
}

func (s *maskingStubActions) GetSession(_ context.Context, sessionID string) (*models.GameSession, bool) {
	if s.session != nil && s.session.SessionID == sessionID {
		return s.session, true
	}
	return nil, false
}

func TestHub_SessionDeltaBroadcastIsMaskedPerPlayer(t *testing.T) {
	session := models.NewGameSession("match-mask", []string{"p1", "p2"})
	session.Status = models.SessionStatusActive
	session.Hands[0].Tiles = []models.DominoTile{models.NewTile(1, 2)}
	session.Hands[1].Tiles = []models.DominoTile{models.NewTile(3, 4), models.NewTile(5, 6)}

	sessionRaw, err := json.Marshal(session)
	if err != nil {
		t.Fatalf("marshal session: %v", err)
	}

	actions := &maskingStubActions{session: session}
	hub := NewHub(actions)
	go hub.Run()

	p1Send := make(chan []byte, 4)
	p2Send := make(chan []byte, 4)
	hub.RegisterTestClient("match-mask", "p1", p1Send)
	hub.RegisterTestClient("match-mask", "p2", p2Send)
	waitFor(t, 2*time.Second, func() bool { return hub.ClientCount() == 2 })

	drainUntilEmpty(t, p1Send)
	drainUntilEmpty(t, p2Send)

	hub.NotifySessionDelta(SessionDelta{
		MatchID: "match-mask",
		Op:      "APPLY_TURN",
		Applied: true,
		Session: sessionRaw,
	})

	p1Payload := readNextEnvelope(t, p1Send, EventTypeSessionDelta)
	p2Payload := readNextEnvelope(t, p2Send, EventTypeSessionDelta)

	var p1Delta SessionDeltaPayload
	if err := json.Unmarshal(p1Payload, &p1Delta); err != nil {
		t.Fatalf("unmarshal p1 delta: %v", err)
	}
	var p1Session models.GameSession
	if err := json.Unmarshal(p1Delta.Session, &p1Session); err != nil {
		t.Fatalf("unmarshal p1 session: %v", err)
	}
	if len(p1Session.Hands[0].Tiles) != 1 || len(p1Session.Hands[1].Tiles) != 0 {
		t.Fatalf("p1 view unexpected: own=%d opp=%d", len(p1Session.Hands[0].Tiles), len(p1Session.Hands[1].Tiles))
	}

	var p2Delta SessionDeltaPayload
	if err := json.Unmarshal(p2Payload, &p2Delta); err != nil {
		t.Fatalf("unmarshal p2 delta: %v", err)
	}
	var p2Session models.GameSession
	if err := json.Unmarshal(p2Delta.Session, &p2Session); err != nil {
		t.Fatalf("unmarshal p2 session: %v", err)
	}
	if len(p2Session.Hands[1].Tiles) != 2 || len(p2Session.Hands[0].Tiles) != 0 {
		t.Fatalf("p2 view unexpected: own=%d opp=%d", len(p2Session.Hands[1].Tiles), len(p2Session.Hands[0].Tiles))
	}
}

func drainUntilEmpty(t *testing.T, ch <-chan []byte) {
	t.Helper()
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func readNextEnvelope(t *testing.T, ch <-chan []byte, wantType string) json.RawMessage {
	t.Helper()
	select {
	case payload := <-ch:
		var envelope EventEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("unmarshal envelope: %v", err)
		}
		if envelope.Type != wantType {
			t.Fatalf("envelope type = %q, want %q", envelope.Type, wantType)
		}
		return envelope.Payload
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s envelope", wantType)
		return nil
	}
}
