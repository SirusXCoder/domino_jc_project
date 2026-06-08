package resilience

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// RetryConfig controls exponential backoff with jitter for transient failures.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetryConfig is suitable for Dgraph transient errors.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	}
}

// Retry executes fn with exponential backoff and jitter until success or attempts exhausted.
func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 100 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 5 * time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt == cfg.MaxAttempts {
			break
		}

		delay := backoffDelay(cfg, attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func backoffDelay(cfg RetryConfig, attempt int) time.Duration {
	exp := cfg.BaseDelay * time.Duration(1<<(attempt-1))
	if exp > cfg.MaxDelay {
		exp = cfg.MaxDelay
	}
	jitter := time.Duration(rand.Int63n(int64(exp / 2)))
	return exp/2 + jitter
}

// IsTransient reports whether an error is worth retrying (context errors are not).
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}
