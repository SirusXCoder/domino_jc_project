package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"domino_jc_project/pkg/telemetry"
)

func TestRateLimitMiddlewareTokenBucket(t *testing.T) {
	limiter := telemetry.NewTokenBucketLimiter(1.0, 3.0, 1*time.Hour)

	var hits int
	handler := limiter.RateLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.7:12345"

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		wantStatus := http.StatusOK
		if i >= 3 {
			wantStatus = http.StatusTooManyRequests
		}
		if rec.Code != wantStatus {
			t.Fatalf("request %d status = %d, want %d", i+1, rec.Code, wantStatus)
		}
	}

	if hits != 3 {
		t.Fatalf("handler hits = %d, want 3", hits)
	}
}
