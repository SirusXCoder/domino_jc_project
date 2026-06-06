package engine

import (
	"context"
	"log"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/repository"
)

const defaultLedgerQueueSize = 256

// LedgerWorker asynchronously persists immutable MatchRecord snapshots to Dgraph.
type LedgerWorker struct {
	repo repository.MatchLedgerRepository
	jobs chan models.MatchRecord
}

// NewLedgerWorker constructs a worker backed by the given ledger repository.
func NewLedgerWorker(repo repository.MatchLedgerRepository, queueSize int) *LedgerWorker {
	if queueSize <= 0 {
		queueSize = defaultLedgerQueueSize
	}
	return &LedgerWorker{
		repo: repo,
		jobs: make(chan models.MatchRecord, queueSize),
	}
}

// Run processes ledger jobs until the jobs channel is closed.
func (w *LedgerWorker) Run() {
	for record := range w.jobs {
		if err := w.repo.SaveMatchRecord(context.Background(), record); err != nil {
			log.Printf("ledger: failed to persist match_id=%s: %v", record.MatchID, err)
		} else {
			log.Printf("ledger: persisted immutable match record match_id=%s", record.MatchID)
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
