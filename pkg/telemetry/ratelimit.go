package telemetry

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// clientBucket defines an individual token bucket per IP address
type clientBucket struct {
	tokens   float64
	lastSeen time.Time
}

// TokenBucketLimiter manages thread-safe rate limits across client IPs
type TokenBucketLimiter struct {
	mu         sync.RWMutex
	clients    map[string]*clientBucket
	refillRate float64       // Tokens added per second
	maxTokens  float64       // Maximum burst capacity
	ttl        time.Duration // Time-to-live before evicting a stale client map entry
}

// NewTokenBucketLimiter initializes a rate limiter with a background cleanup janitor
func NewTokenBucketLimiter(rate float64, burst float64, ttl time.Duration) *TokenBucketLimiter {
	limiter := &TokenBucketLimiter{
		clients:    make(map[string]*clientBucket),
		refillRate: rate,
		maxTokens:  burst,
		ttl:        ttl,
	}

	// Run background janitor to prevent memory leaks from historical IPs
	go limiter.startJanitor(time.Minute * 5)

	return limiter
}

// Allow checks if the request from a specific IP should be permitted
func (tbl *TokenBucketLimiter) Allow(ip string) bool {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	now := time.Now()
	client, exists := tbl.clients[ip]

	if !exists {
		// New client starts with full burst capacity
		tbl.clients[ip] = &clientBucket{
			tokens:   tbl.maxTokens - 1.0, // consume first token immediately
			lastSeen: now,
		}
		return true
	}

	// Calculate refilled tokens based on elapsed duration
	elapsed := now.Sub(client.lastSeen).Seconds()
	client.lastSeen = now

	client.tokens += elapsed * tbl.refillRate
	if client.tokens > tbl.maxTokens {
		client.tokens = tbl.maxTokens
	}

	// Evaluate if the client has enough tokens left
	if client.tokens >= 1.0 {
		client.tokens -= 1.0
		return true
	}

	return false
}

// RateLimitMiddleware wraps standard http.Handlers to enforce edge restrictions
func (tbl *TokenBucketLimiter) RateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			// Fallback to raw address if parsing fails
			ip = r.RemoteAddr
		}

		if !tbl.Allow(ip) {
			LoggerFromContext(r.Context()).Warn("Rate limit exceeded at edge boundary", "client_ip", ip)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "Too many requests. Edge rate limit exceeded."}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}

// startJanitor periodically sweeps stale map entries to keep memory tight
func (tbl *TokenBucketLimiter) startJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		tbl.mu.Lock()
		now := time.Now()
		for ip, bucket := range tbl.clients {
			if now.Sub(bucket.lastSeen) > tbl.ttl {
				delete(tbl.clients, ip)
			}
		}
		tbl.mu.Unlock()
	}
}
