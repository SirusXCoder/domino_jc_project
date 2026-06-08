package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTokenBucketLimiterAllowBurst(t *testing.T) {
	limiter := &TokenBucketLimiter{
		clients:    make(map[string]*clientBucket),
		refillRate: 1,
		maxTokens:  2,
		ttl:        time.Minute,
	}

	if !limiter.Allow("10.0.0.1") {
		t.Fatal("first request should be allowed")
	}
	if !limiter.Allow("10.0.0.1") {
		t.Fatal("second request should be allowed within burst")
	}
	if limiter.Allow("10.0.0.1") {
		t.Fatal("third immediate request should be denied")
	}
}

func TestTokenBucketLimiterRefill(t *testing.T) {
	limiter := &TokenBucketLimiter{
		clients:    make(map[string]*clientBucket),
		refillRate: 10,
		maxTokens:  1,
		ttl:        time.Minute,
	}

	if !limiter.Allow("10.0.0.2") {
		t.Fatal("first request should be allowed")
	}
	if limiter.Allow("10.0.0.2") {
		t.Fatal("second immediate request should be denied")
	}

	time.Sleep(150 * time.Millisecond)

	if !limiter.Allow("10.0.0.2") {
		t.Fatal("request should be allowed after refill")
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	limiter := &TokenBucketLimiter{
		clients:    make(map[string]*clientBucket),
		refillRate: 0,
		maxTokens:  1,
		ttl:        time.Minute,
	}

	var hits int
	handler := limiter.RateLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.7:12345"

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want %d", rec.Code, http.StatusOK)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if hits != 1 {
		t.Fatalf("handler hits = %d, want 1", hits)
	}
}
