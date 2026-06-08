package resilience

import (
	"fmt"
	"time"

	"github.com/sony/gobreaker"
)

// State mirrors gobreaker states for observability and tests.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// BreakerConfig tunes circuit breaker thresholds.
type BreakerConfig struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts gobreaker.Counts) bool
	OnStateChange func(name string, from, to gobreaker.State)
}

// DefaultBreakerConfig returns production-friendly defaults for Dgraph calls.
func DefaultBreakerConfig(name string) BreakerConfig {
	return BreakerConfig{
		Name:        name,
		MaxRequests: 3,
		Interval:    30 * time.Second,
		Timeout:     15 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	}
}

// Breaker wraps sony/gobreaker with a typed State accessor.
type Breaker struct {
	cb *gobreaker.CircuitBreaker
}

// NewBreaker constructs a circuit breaker from cfg.
func NewBreaker(cfg BreakerConfig) *Breaker {
	settings := gobreaker.Settings{
		Name:          cfg.Name,
		MaxRequests:   cfg.MaxRequests,
		Interval:      cfg.Interval,
		Timeout:       cfg.Timeout,
		ReadyToTrip:   cfg.ReadyToTrip,
		OnStateChange: cfg.OnStateChange,
	}
	return &Breaker{cb: gobreaker.NewCircuitBreaker(settings)}
}

// Execute runs fn through the breaker.
func (b *Breaker) Execute(fn func() (interface{}, error)) (interface{}, error) {
	return b.cb.Execute(fn)
}

// Run executes a void operation through the breaker.
func (b *Breaker) Run(fn func() error) error {
	_, err := b.cb.Execute(func() (interface{}, error) {
		return nil, fn()
	})
	return err
}

// State returns the current breaker state.
func (b *Breaker) State() State {
	switch b.cb.State() {
	case gobreaker.StateOpen:
		return StateOpen
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	default:
		return StateClosed
	}
}

// Name returns the breaker identifier.
func (b *Breaker) Name() string {
	return b.cb.Name()
}

// ErrOpenCircuit is returned when the breaker is open and calls fail fast.
var ErrOpenCircuit = fmt.Errorf("circuit breaker open")
