package resilience

import (
	"sync"

	"domino_jc_project/pkg/models"
)

// DeadLetterSpool stores ledger events that could not be committed while the breaker is open.
type DeadLetterSpool struct {
	mu     sync.Mutex
	events []models.MatchRecord
	cap    int
}

// NewDeadLetterSpool creates an in-memory spool with a maximum capacity.
func NewDeadLetterSpool(capacity int) *DeadLetterSpool {
	if capacity <= 0 {
		capacity = 1024
	}
	return &DeadLetterSpool{cap: capacity}
}

// Spool appends a record when persistence fails. Drops oldest on overflow.
func (s *DeadLetterSpool) Spool(record models.MatchRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.events) >= s.cap {
		copy(s.events, s.events[1:])
		s.events[len(s.events)-1] = record
		return
	}
	s.events = append(s.events, record)
}

// Drain returns and clears all spooled records for replay.
func (s *DeadLetterSpool) Drain() []models.MatchRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.events) == 0 {
		return nil
	}
	out := make([]models.MatchRecord, len(s.events))
	copy(out, s.events)
	s.events = s.events[:0]
	return out
}

// Len returns the number of spooled events.
func (s *DeadLetterSpool) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}
