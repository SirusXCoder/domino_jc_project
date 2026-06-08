package resilience_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/resilience"

	"github.com/sony/gobreaker"
)

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	err := resilience.Retry(context.Background(), resilience.RetryConfig{
		MaxAttempts: 4,
		BaseDelay:   1 * time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
	}, func() error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	breaker := resilience.NewBreaker(resilience.BreakerConfig{
		Name:        "test",
		MaxRequests: 1,
		Interval:    time.Second,
		Timeout:     50 * time.Millisecond,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 3
		},
	})

	for i := 0; i < 3; i++ {
		_ = breaker.Run(func() error { return errors.New("fail") })
	}
	if breaker.State() != resilience.StateOpen {
		t.Fatalf("state = %v, want open", breaker.State())
	}
}

func TestDeadLetterSpoolDrain(t *testing.T) {
	spool := resilience.NewDeadLetterSpool(4)
	spool.Spool(models.MatchRecord{MatchID: "m1"})
	spool.Spool(models.MatchRecord{MatchID: "m2"})

	if spool.Len() != 2 {
		t.Fatalf("len = %d, want 2", spool.Len())
	}

	drained := spool.Drain()
	if len(drained) != 2 {
		t.Fatalf("drained = %d, want 2", len(drained))
	}
	if spool.Len() != 0 {
		t.Fatalf("len after drain = %d, want 0", spool.Len())
	}
}
