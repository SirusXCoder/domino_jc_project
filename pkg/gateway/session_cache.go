package gateway

import (
	"sync"
)

const defaultSessionCacheCapacity = 10_000

// SessionCache stores idempotent mutation responses keyed by client_id + sequence_number.
type SessionCache struct {
	mu       sync.RWMutex
	capacity int
	order    []string
	entries  map[string]MatchMutateResponse
}

// NewSessionCache constructs a bounded LRU idempotency cache.
func NewSessionCache(capacity int) *SessionCache {
	if capacity <= 0 {
		capacity = defaultSessionCacheCapacity
	}
	return &SessionCache{
		capacity: capacity,
		order:    make([]string, 0, capacity),
		entries:  make(map[string]MatchMutateResponse),
	}
}

// SessionCacheKey builds the composite idempotency key for a client mutation.
func SessionCacheKey(clientID string, sequenceNumber uint64) string {
	return clientID + ":" + itoa(sequenceNumber)
}

// Get returns a cached mutation response when the composite key exists.
func (c *SessionCache) Get(clientID string, sequenceNumber uint64) (MatchMutateResponse, bool) {
	if c == nil {
		return MatchMutateResponse{}, false
	}
	key := SessionCacheKey(clientID, sequenceNumber)

	c.mu.RLock()
	defer c.mu.RUnlock()

	resp, ok := c.entries[key]
	return resp, ok
}

// Put stores a mutation response and evicts the oldest entry when over capacity.
func (c *SessionCache) Put(clientID string, sequenceNumber uint64, resp MatchMutateResponse) {
	if c == nil {
		return
	}
	key := SessionCacheKey(clientID, sequenceNumber)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[key]; !exists {
		c.order = append(c.order, key)
		if len(c.order) > c.capacity {
			oldest := c.order[0]
			c.order = c.order[1:]
			delete(c.entries, oldest)
		}
	}
	c.entries[key] = resp
}

// Len reports the number of cached idempotency entries (for tests).
func (c *SessionCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
