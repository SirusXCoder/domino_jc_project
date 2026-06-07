package engine

import (
	"context"
	"log"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/repository"
)

const defaultLedgerQueueSize = 256

// MatchRatingProcessor runs post-ledger career analytics for a persisted match.
type MatchRatingProcessor interface {
	ProcessMatch(ctx context.Context, record models.MatchRecord) error
}

// LedgerWorker asynchronously persists immutable MatchRecord snapshots to Dgraph.
type LedgerWorker struct {
	repo     repository.MatchLedgerRepository
	ratings  MatchRatingProcessor
	jobs     chan models.MatchRecord
}

// NewLedgerWorker constructs a worker backed by the given ledger repository.
func NewLedgerWorker(repo repository.MatchLedgerRepository, queueSize int, opts ...LedgerWorkerOption) *LedgerWorker {
	if queueSize <= 0 {
		queueSize = defaultLedgerQueueSize
	}
	w := &LedgerWorker{
		repo: repo,
		jobs: make(chan models.MatchRecord, queueSize),
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

// Run processes ledger jobs until the jobs channel is closed.
func (w *LedgerWorker) Run() {
	for record := range w.jobs {
		ctx := context.Background()
		if err := w.repo.SaveMatchRecord(ctx, record); err != nil {
			log.Printf("ledger: failed to persist match_id=%s: %v", record.MatchID, err)
			continue
		}
		log.Printf("ledger: persisted immutable match record match_id=%s", record.MatchID)

		if w.ratings != nil {
			if err := w.ratings.ProcessMatch(ctx, record); err != nil {
				log.Printf("rating: failed to process match_id=%s: %v", record.MatchID, err)
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
	}
}
