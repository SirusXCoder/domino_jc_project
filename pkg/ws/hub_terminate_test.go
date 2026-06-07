package ws

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"domino_jc_project/pkg/models"
)

type stubLedger struct {
	records []models.MatchRecord
}

func (s *stubLedger) Enqueue(record models.MatchRecord) {
	s.records = append(s.records, record)
}

func TestHub_TerminateMatchBroadcastsAndEnqueuesLedger(t *testing.T) {
	ledger := &stubLedger{}
	hub := NewHub(nil, WithMatchLedger(ledger))
	go hub.Run()

	client := &Client{
		hub:       hub,
		sessionID: "s1",
		playerID:  "p1",
		send:      make(chan []byte, 4),
	}
	hub.register <- client
	waitFor(t, time.Second, func() bool { return hub.ClientCount() == 1 })

	session := models.NewGameSession("s1", []string{"p1", "p2"})
	session.Status = models.SessionStatusCompleted
	session.MutationsLocked = true
	outcome := &models.MatchOutcome{
		MatchID:  "s1",
		WinnerID: "p1",
		Scores:   models.Scores{"p1": 0, "p2": 12},
		Reason:   models.MatchEndEmptyHand,
	}

	hub.TerminateMatch(context.Background(), "s1", outcome, session)

	if len(ledger.records) != 1 {
		t.Fatalf("ledger records = %d, want 1", len(ledger.records))
	}
	if ledger.records[0].MatchID != "s1" {
		t.Fatalf("ledger match_id = %q, want s1", ledger.records[0].MatchID)
	}
	if ledger.records[0].EndReason != models.MatchEndEmptyHand {
		t.Fatalf("ledger end_reason = %q, want %q", ledger.records[0].EndReason, models.MatchEndEmptyHand)
	}

	var gotMatchEnd bool
	var gotSnapshot bool
	deadline := time.After(2 * time.Second)
	for !(gotMatchEnd && gotSnapshot) {
		select {
		case payload := <-client.send:
			var envelope EventEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			switch envelope.Type {
			case EventTypeMatchEnd:
				gotMatchEnd = true
			case EventTypeStateSnapshot:
				gotSnapshot = true
			}
		case <-deadline:
			t.Fatalf("expected match end and snapshot frames, got match_end=%v snapshot=%v", gotMatchEnd, gotSnapshot)
		}
	}
}
