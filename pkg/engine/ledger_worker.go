package engine

import (
	"context"
	"log"
	"time"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/repository"
	"domino_jc_project/pkg/resilience"
)

const defaultLedgerQueueSize = 256

// MatchRatingProcessor runs post-ledger career analytics for a persisted match.
type MatchRatingProcessor interface {
	ProcessMatch(ctx context.Context, record models.MatchRecord) error
}

// LedgerWorker asynchronously persists immutable MatchRecord snapshots to Dgraph.
type LedgerWorker struct {
	repo    repository.MatchLedgerRepository
	ratings MatchRatingProcessor
	jobs    chan models.MatchRecord
	spool   *resilience.DeadLetterSpool
	breaker *resilience.Breaker
	retry   resilience.RetryConfig
}

// NewLedgerWorker constructs a worker backed by the given ledger repository.
func NewLedgerWorker(repo repository.MatchLedgerRepository, queueSize int, opts ...LedgerWorkerOption) *LedgerWorker {
	if queueSize <= 0 {
		queueSize = defaultLedgerQueueSize
	}
	w := &LedgerWorker{
		repo:  repo,
		jobs:  make(chan models.MatchRecord, queueSize),
		spool: resilience.NewDeadLetterSpool(1024),
		retry: resilience.DefaultRetryConfig(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// LedgerWorkerOption configures optional post-ledger processors.
type LedgerWorkerOption func(*LedgerWorker)

// WithRatingProcessor attaches the ELO career analytics worker.
func WithRatingProcessor(processor MatchRatingProcessor) LedgerWorkerOption {
	return func(w *LedgerWorker) {
		w.ratings = processor
	}
}

// WithLedgerBreaker attaches a circuit breaker around ledger persistence.
func WithLedgerBreaker(b *resilience.Breaker) LedgerWorkerOption {
	return func(w *LedgerWorker) {
		w.breaker = b
	}
}

// WithLedgerRetry configures retry behavior for transient ledger errors.
func WithLedgerRetry(cfg resilience.RetryConfig) LedgerWorkerOption {
	return func(w *LedgerWorker) {
		w.retry = cfg
	}
}

// WithDeadLetterSpool overrides the default in-memory spool.
func WithDeadLetterSpool(spool *resilience.DeadLetterSpool) LedgerWorkerOption {
	return func(w *LedgerWorker) {
		w.spool = spool
	}
}

// BreakerState exposes the current circuit breaker state for observability/tests.
func (w *LedgerWorker) BreakerState() resilience.State {
	if w.breaker == nil {
		return resilience.StateClosed
	}
	return w.breaker.State()
}

// SpoolLen returns the number of dead-lettered ledger events.
func (w *LedgerWorker) SpoolLen() int {
	if w.spool == nil {
		return 0
	}
	return w.spool.Len()
}

// Run processes ledger jobs until the jobs channel is closed.
func (w *LedgerWorker) Run() {
	for record := range w.jobs {
		w.processRecord(record)
	}
}

func (w *LedgerWorker) processRecord(record models.MatchRecord) {
	ctx := context.Background()

	if err := w.persistWithResilience(ctx, record); err != nil {
		log.Printf("ledger: failed to persist match_id=%s: %v (spooling)", record.MatchID, err)
		if w.spool != nil {
			w.spool.Spool(record)
		}
		return
	}
	log.Printf("ledger: persisted immutable match record match_id=%s", record.MatchID)

	if w.ratings != nil {
		if err := w.ratings.ProcessMatch(ctx, record); err != nil {
			log.Printf("rating: failed to process match_id=%s: %v", record.MatchID, err)
		}
	}

	w.replaySpool(ctx)
}

func (w *LedgerWorker) persistWithResilience(ctx context.Context, record models.MatchRecord) error {
	persist := func() error {
		return w.repo.SaveMatchRecord(ctx, record)
	}

	if w.breaker == nil {
		return resilience.Retry(ctx, w.retry, persist)
	}

	var err error
	_, execErr := w.breaker.Execute(func() (interface{}, error) {
		err = resilience.Retry(ctx, w.retry, persist)
		return nil, err
	})
	if execErr != nil {
		return execErr
	}
	return err
}

func (w *LedgerWorker) replaySpool(ctx context.Context) {
	if w.spool == nil || w.breaker != nil && w.breaker.State() == resilience.StateOpen {
		return
	}

	for _, record := range w.spool.Drain() {
		if err := w.persistWithResilience(ctx, record); err != nil {
			log.Printf("ledger: spool replay failed match_id=%s: %v", record.MatchID, err)
			w.spool.Spool(record)
			return
		}
		log.Printf("ledger: replayed spooled match_id=%s", record.MatchID)
		if w.ratings != nil {
			if err := w.ratings.ProcessMatch(ctx, record); err != nil {
				log.Printf("rating: failed to process spooled match_id=%s: %v", record.MatchID, err)
			}
		}
	}
}

// Enqueue submits a match record for async persistence. Non-blocking; drops on full queue.
func (w *LedgerWorker) Enqueue(record models.MatchRecord) {
	select {
	case w.jobs <- record:
	default:
		log.Printf("ledger: queue full, dropping match_id=%s", record.MatchID)
		if w.spool != nil {
			w.spool.Spool(record)
		}
	}
}

// Stop closes the job channel for graceful shutdown.
func (w *LedgerWorker) Stop() {
	close(w.jobs)
}

// WaitUntilIdle polls until the job queue drains (tests only).
func (w *LedgerWorker) WaitUntilIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(w.jobs) == 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return len(w.jobs) == 0
}
