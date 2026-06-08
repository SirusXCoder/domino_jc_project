package pagination

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// MatchHistoryCursor identifies the last row returned in a paginated match list.
// Pagination uses (end_time DESC, match_id DESC) for stable ordering.
type MatchHistoryCursor struct {
	EndTime time.Time `json:"end_time"`
	MatchID string    `json:"match_id"`
}

// Encode serializes a cursor to an opaque URL-safe token.
func Encode(c MatchHistoryCursor) (string, error) {
	if c.MatchID == "" || c.EndTime.IsZero() {
		return "", fmt.Errorf("cursor requires non-zero end_time and match_id")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("marshal cursor: %w", err)
	}
	return base64.URLEncoding.EncodeToString(raw), nil
}

// Decode parses an opaque cursor token produced by Encode.
func Decode(token string) (MatchHistoryCursor, error) {
	if token == "" {
		return MatchHistoryCursor{}, fmt.Errorf("cursor token cannot be empty")
	}
	raw, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return MatchHistoryCursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	var c MatchHistoryCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return MatchHistoryCursor{}, fmt.Errorf("parse cursor: %w", err)
	}
	if c.MatchID == "" || c.EndTime.IsZero() {
		return MatchHistoryCursor{}, fmt.Errorf("invalid cursor payload")
	}
	return c, nil
}

// PageMeta accompanies paginated result sets.
type PageMeta struct {
	NextCursor string `json:"next_cursor,omitempty"`
	HasMore    bool   `json:"has_more"`
}
