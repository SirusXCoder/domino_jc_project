package gateway

import (
	"testing"
)

func TestSessionCache_PutGetAndEviction(t *testing.T) {
	cache := NewSessionCache(2)

	cache.Put("client-a", 1, MatchMutateResponse{OK: true})
	cache.Put("client-a", 2, MatchMutateResponse{OK: true})
	cache.Put("client-b", 1, MatchMutateResponse{OK: true})

	if cache.Len() != 2 {
		t.Fatalf("cache len = %d, want 2", cache.Len())
	}

	if _, ok := cache.Get("client-a", 1); ok {
		t.Fatal("expected oldest client-a:1 entry to be evicted")
	}
	if resp, ok := cache.Get("client-a", 2); !ok || !resp.OK {
		t.Fatalf("client-a:2 cache miss or not ok: %+v", resp)
	}
	if resp, ok := cache.Get("client-b", 1); !ok || !resp.OK {
		t.Fatalf("client-b:1 cache miss or not ok: %+v", resp)
	}
}

func TestSessionCacheKey(t *testing.T) {
	if got := SessionCacheKey("player-1", 42); got != "player-1:42" {
		t.Fatalf("SessionCacheKey() = %q, want %q", got, "player-1:42")
	}
}
