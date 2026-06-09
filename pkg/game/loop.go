package game

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"domino_jc_project/pkg/broker"
	"domino_jc_project/pkg/models"
)

// Broker topic names consumed by background workers.
const (
	TopicMatchEnded         = "match.ended"
	TopicMatchmakingUpdate  = "matchmaking.update"
	TopicPlayerNotification = "player.notification"
)

const (
	defaultWorkersPerTopic = 2
	defaultJobQueueSize    = 64

	// Mock I/O latencies used by background workers to prove the hot path stays non-blocking.
	mockMatchEndedIODelay         = 500 * time.Millisecond
	mockMatchmakingUpdateIODelay  = 200 * time.Millisecond
	mockPlayerNotificationIODelay = 100 * time.Millisecond
)

// MatchEndedPayload is the immutable snapshot handed off when a match completes.
type MatchEndedPayload struct {
	SessionID string               `json:"session_id"`
	Outcome   *models.MatchOutcome `json:"outcome"`
	Players   []string             `json:"players"`
	Session   *models.GameSession  `json:"session,omitempty"`
}

// MatchmakingUpdatePayload reports session queue and roster changes to matchmaking services.
type MatchmakingUpdatePayload struct {
	SessionID   string   `json:"session_id"`
	Status      string   `json:"status"`
	Players     []string `json:"players"`
	CurrentTurn string   `json:"current_turn,omitempty"`
}

// PlayerNotificationPayload targets a single player for outbound alerts or webhooks.
type PlayerNotificationPayload struct {
	SessionID string `json:"session_id"`
	PlayerID  string `json:"player_id"`
	Kind      string `json:"kind"`
	Message   string `json:"message"`
}

// TickRequest carries the in-memory session state produced by a completed turn.
type TickRequest struct {
	Session *models.GameSession
	Result  *models.TurnResult
}

// GameEngine is the broker-backed integration layer for the hot game loop.
// It publishes side-effect events instead of performing slow I/O synchronously.
type GameEngine struct {
	broker broker.Broker
}

// NewGameEngine constructs a GameEngine backed by the given event broker.
func NewGameEngine(b broker.Broker) *GameEngine {
	return &GameEngine{broker: b}
}

// Tick publishes matchmaking and player-notification events after a turn is applied.
// When the turn ends the match, EndMatch is invoked without blocking on consumer latency.
func (e *GameEngine) Tick(ctx context.Context, req TickRequest) error {
	if e == nil || e.broker == nil {
		return fmt.Errorf("game engine is not configured with a broker")
	}
	if req.Session == nil {
		return fmt.Errorf("tick requires a game session")
	}

	session := req.Session
	if err := e.publish(ctx, TopicMatchmakingUpdate, MatchmakingUpdatePayload{
		SessionID:   session.SessionID,
		Status:      session.Status,
		Players:     append([]string(nil), session.Players...),
		CurrentTurn: session.CurrentTurn,
	}); err != nil {
		return err
	}

	if session.CurrentTurn != "" {
		if err := e.publish(ctx, TopicPlayerNotification, PlayerNotificationPayload{
			SessionID: session.SessionID,
			PlayerID:  session.CurrentTurn,
			Kind:      "turn_changed",
			Message:   fmt.Sprintf("it is now %s's turn", session.CurrentTurn),
		}); err != nil {
			return err
		}
	}

	if req.Result != nil && req.Result.MatchEnded {
		return e.EndMatch(ctx, session.SessionID, req.Result.Outcome, session)
	}
	return nil
}

// EndMatch publishes a match.ended event and winner notifications without touching
// databases or external webhooks on the calling goroutine.
func (e *GameEngine) EndMatch(
	ctx context.Context,
	sessionID string,
	outcome *models.MatchOutcome,
	session *models.GameSession,
) error {
	if e == nil || e.broker == nil {
		return fmt.Errorf("game engine is not configured with a broker")
	}
	if outcome == nil {
		return fmt.Errorf("cannot end match %q without an outcome", sessionID)
	}

	players := []string(nil)
	if session != nil {
		players = append(players, session.Players...)
	}

	if err := e.publish(ctx, TopicMatchEnded, MatchEndedPayload{
		SessionID: sessionID,
		Outcome:   outcome,
		Players:   players,
		Session:   session,
	}); err != nil {
		return err
	}

	if outcome.WinnerID != "" {
		if err := e.publish(ctx, TopicPlayerNotification, PlayerNotificationPayload{
			SessionID: sessionID,
			PlayerID:  outcome.WinnerID,
			Kind:      "match_won",
			Message:   fmt.Sprintf("player %s won match %s", outcome.WinnerID, sessionID),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *GameEngine) publish(ctx context.Context, topic string, payload interface{}) error {
	return e.broker.Publish(ctx, broker.Event{
		Topic:   topic,
		Payload: payload,
	})
}

// WorkerConfig tunes per-topic worker pool sizes and downstream consumers.
// When a consumer is nil, workers fall back to mock I/O delays (primarily for tests).
type WorkerConfig struct {
	MatchEndedWorkers         int
	MatchmakingUpdateWorkers  int
	PlayerNotificationWorkers int

	MatchEndedIODelay         time.Duration
	MatchmakingUpdateIODelay  time.Duration
	PlayerNotificationIODelay time.Duration

	OnMatchEnded         func(context.Context, MatchEndedPayload)
	OnMatchmakingUpdate  func(context.Context, MatchmakingUpdatePayload)
	OnPlayerNotification func(context.Context, PlayerNotificationPayload)
}

func defaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		MatchEndedWorkers:         defaultWorkersPerTopic,
		MatchmakingUpdateWorkers:    defaultWorkersPerTopic,
		PlayerNotificationWorkers:   defaultWorkersPerTopic,
		MatchEndedIODelay:         mockMatchEndedIODelay,
		MatchmakingUpdateIODelay:  mockMatchmakingUpdateIODelay,
		PlayerNotificationIODelay: mockPlayerNotificationIODelay,
	}
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	d := defaultWorkerConfig()
	if c.MatchEndedWorkers == 0 {
		c.MatchEndedWorkers = d.MatchEndedWorkers
	}
	if c.MatchmakingUpdateWorkers == 0 {
		c.MatchmakingUpdateWorkers = d.MatchmakingUpdateWorkers
	}
	if c.PlayerNotificationWorkers == 0 {
		c.PlayerNotificationWorkers = d.PlayerNotificationWorkers
	}
	if c.MatchEndedIODelay <= 0 {
		c.MatchEndedIODelay = d.MatchEndedIODelay
	}
	if c.MatchmakingUpdateIODelay <= 0 {
		c.MatchmakingUpdateIODelay = d.MatchmakingUpdateIODelay
	}
	if c.PlayerNotificationIODelay <= 0 {
		c.PlayerNotificationIODelay = d.PlayerNotificationIODelay
	}
	return c
}

// StartBackgroundWorkers subscribes to integration topics and starts distinct worker
// pools that mock slow downstream I/O so the main game loop never waits on consumers.
func StartBackgroundWorkers(ctx context.Context, b broker.Broker) error {
	return StartBackgroundWorkersWithConfig(ctx, b, WorkerConfig{})
}

// StartBackgroundWorkersWithConfig is like StartBackgroundWorkers but accepts tuning options.
func StartBackgroundWorkersWithConfig(ctx context.Context, b broker.Broker, cfg WorkerConfig) error {
	if b == nil {
		return fmt.Errorf("broker is required")
	}
	cfg = cfg.withDefaults()

	if err := startTopicWorkerPool(ctx, b, TopicMatchEnded, cfg.MatchEndedWorkers, func(evt broker.Event) {
		processMatchEnded(ctx, evt, cfg)
	}); err != nil {
		return fmt.Errorf("start %s workers: %w", TopicMatchEnded, err)
	}
	if err := startTopicWorkerPool(ctx, b, TopicMatchmakingUpdate, cfg.MatchmakingUpdateWorkers, func(evt broker.Event) {
		processMatchmakingUpdate(ctx, evt, cfg)
	}); err != nil {
		return fmt.Errorf("start %s workers: %w", TopicMatchmakingUpdate, err)
	}
	if err := startTopicWorkerPool(ctx, b, TopicPlayerNotification, cfg.PlayerNotificationWorkers, func(evt broker.Event) {
		processPlayerNotification(ctx, evt, cfg)
	}); err != nil {
		return fmt.Errorf("start %s workers: %w", TopicPlayerNotification, err)
	}
	return nil
}

func startTopicWorkerPool(
	ctx context.Context,
	b broker.Broker,
	topic string,
	workers int,
	handler func(broker.Event),
) error {
	events, err := b.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	jobs := make(chan broker.Event, defaultJobQueueSize)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case evt, ok := <-jobs:
					if !ok {
						return
					}
					handler(evt)
					log.Printf("worker[%s#%d] finished event topic=%s ts=%d", topic, workerID, evt.Topic, evt.Timestamp)
				}
			}
		}()
	}

	go func() {
		<-ctx.Done()
		close(jobs)
		wg.Wait()
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-events:
				if !ok {
					return
				}
				select {
				case jobs <- evt:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return nil
}

func processMatchEnded(ctx context.Context, evt broker.Event, cfg WorkerConfig) {
	payload, ok := evt.Payload.(MatchEndedPayload)
	if !ok {
		log.Printf("worker[%s] unexpected payload type %T", TopicMatchEnded, evt.Payload)
		return
	}
	if cfg.OnMatchEnded != nil {
		cfg.OnMatchEnded(ctx, payload)
		return
	}
	log.Printf("worker[%s] persisting ledger + ratings for session=%s (mock I/O)", TopicMatchEnded, payload.SessionID)
	time.Sleep(cfg.MatchEndedIODelay)
}

func processMatchmakingUpdate(ctx context.Context, evt broker.Event, cfg WorkerConfig) {
	payload, ok := evt.Payload.(MatchmakingUpdatePayload)
	if !ok {
		log.Printf("worker[%s] unexpected payload type %T", TopicMatchmakingUpdate, evt.Payload)
		return
	}
	if cfg.OnMatchmakingUpdate != nil {
		cfg.OnMatchmakingUpdate(ctx, payload)
		return
	}
	log.Printf("worker[%s] syncing matchmaking index session=%s status=%s (mock I/O)", TopicMatchmakingUpdate, payload.SessionID, payload.Status)
	time.Sleep(cfg.MatchmakingUpdateIODelay)
}

func processPlayerNotification(ctx context.Context, evt broker.Event, cfg WorkerConfig) {
	payload, ok := evt.Payload.(PlayerNotificationPayload)
	if !ok {
		log.Printf("worker[%s] unexpected payload type %T", TopicPlayerNotification, evt.Payload)
		return
	}
	if cfg.OnPlayerNotification != nil {
		cfg.OnPlayerNotification(ctx, payload)
		return
	}
	log.Printf("worker[%s] dispatching notification player=%s kind=%s (mock I/O)", TopicPlayerNotification, payload.PlayerID, payload.Kind)
	time.Sleep(cfg.PlayerNotificationIODelay)
}
