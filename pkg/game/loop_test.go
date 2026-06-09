package game

import (
	"context"
	"testing"
	"time"

	"domino_jc_project/pkg/broker"
	"domino_jc_project/pkg/models"
)

func TestGameEngine_EndMatchDoesNotBlockOnSlowWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.NewInMemoryBroker()
	slowDelay := 2 * time.Second
	if err := StartBackgroundWorkersWithConfig(ctx, b, WorkerConfig{
		MatchEndedWorkers:         2,
		MatchmakingUpdateWorkers:  2,
		PlayerNotificationWorkers: 2,
		MatchEndedIODelay:         slowDelay,
		MatchmakingUpdateIODelay:  slowDelay,
		PlayerNotificationIODelay: slowDelay,
	}); err != nil {
		t.Fatalf("StartBackgroundWorkersWithConfig: %v", err)
	}

	engine := NewGameEngine(b)
	session := &models.GameSession{
		SessionID: "s1",
		Status:    models.SessionStatusCompleted,
		Players:   []string{"p1", "p2"},
	}
	outcome := &models.MatchOutcome{
		MatchID:  "s1",
		WinnerID: "p1",
		Scores:   models.Scores{"p1": 0, "p2": 10},
		Reason:   models.MatchEndEmptyHand,
	}

	start := time.Now()
	if err := engine.EndMatch(ctx, "s1", outcome, session); err != nil {
		t.Fatalf("EndMatch: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Fatalf("EndMatch blocked for %v; want < 50ms so the hot path stays non-blocking", elapsed)
	}

	// Workers sleep for seconds in the background; the publish path must already have returned.
	time.Sleep(100 * time.Millisecond)
	if elapsed >= slowDelay {
		t.Fatalf("game loop waited on consumer latency")
	}
}

func TestGameEngine_TickPublishesMatchmakingAndNotifications(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.NewInMemoryBroker()
	matchmakingCh, err := b.Subscribe(ctx, TopicMatchmakingUpdate)
	if err != nil {
		t.Fatalf("Subscribe matchmaking: %v", err)
	}
	notificationCh, err := b.Subscribe(ctx, TopicPlayerNotification)
	if err != nil {
		t.Fatalf("Subscribe notification: %v", err)
	}
	if err := StartBackgroundWorkersWithConfig(ctx, b, WorkerConfig{
		MatchEndedWorkers:         1,
		MatchmakingUpdateWorkers:  1,
		PlayerNotificationWorkers: 1,
		MatchEndedIODelay:         5 * time.Millisecond,
		MatchmakingUpdateIODelay:  5 * time.Millisecond,
		PlayerNotificationIODelay: 5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("StartBackgroundWorkersWithConfig: %v", err)
	}

	engine := NewGameEngine(b)
	session := &models.GameSession{
		SessionID:   "s2",
		Status:      models.SessionStatusActive,
		Players:     []string{"p1", "p2"},
		CurrentTurn: "p2",
	}

	start := time.Now()
	if err := engine.Tick(ctx, TickRequest{
		Session: session,
		Result:  &models.TurnResult{Applied: true, NeedsPersist: true},
	}); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("Tick blocked longer than expected")
	}

	waitForEvent(t, matchmakingCh, "matchmaking.update")
	waitForEvent(t, notificationCh, "player.notification")
}

func TestGameEngine_TickEndsMatchViaBroker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.NewInMemoryBroker()
	engine := NewGameEngine(b)

	matchEndedCh, err := b.Subscribe(ctx, TopicMatchEnded)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	session := &models.GameSession{
		SessionID: "s3",
		Status:    models.SessionStatusActive,
		Players:   []string{"p1", "p2"},
		CurrentTurn: "p1",
	}
	outcome := &models.MatchOutcome{
		MatchID:  "s3",
		WinnerID: "p1",
		Scores:   models.Scores{"p1": 0, "p2": 12},
		Reason:   models.MatchEndBlocked,
	}

	if err := engine.Tick(ctx, TickRequest{
		Session: session,
		Result: &models.TurnResult{
			MatchEnded: true,
			Outcome:    outcome,
		},
	}); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	evt := waitForEvent(t, matchEndedCh, TopicMatchEnded)
	payload, ok := evt.Payload.(MatchEndedPayload)
	if !ok {
		t.Fatalf("payload type = %T, want MatchEndedPayload", evt.Payload)
	}
	if payload.SessionID != "s3" {
		t.Fatalf("session_id = %q, want s3", payload.SessionID)
	}
	if payload.Outcome.WinnerID != "p1" {
		t.Fatalf("winner = %q, want p1", payload.Outcome.WinnerID)
	}
}

func waitForEvent(t *testing.T, ch <-chan broker.Event, label string) broker.Event {
	t.Helper()
	select {
	case evt := <-ch:
		return evt
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s event", label)
		return broker.Event{}
	}
}
