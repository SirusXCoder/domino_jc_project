package pagination

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Mock Match history response matching our Dgraph facet structure
type MatchHistoryResponse struct {
	Matches    []map[string]interface{} `json:"matches"`
	NextCursor string                   `json:"next_cursor"`
	HasMore    bool                     `json:"has_more"`
}

func TestMatchHistoryPaginationHandler(t *testing.T) {
	// Setup a dummy handler simulating our optimized Dgraph paginator
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		limit := r.URL.Query().Get("limit")

		if limit == "" {
			limit = "10"
		}

		// Simulate cursor verification
		var response MatchHistoryResponse
		if cursor == "" {
			// First Page
			response = MatchHistoryResponse{
				Matches: []map[string]interface{}{
					{"match_id": "m1", "score": "250", "weight": 1.2},
					{"match_id": "m2", "score": "180", "weight": 1.0},
				},
				NextCursor: "ZXlKdFlXTjBhR2xrSWpvbWpJaXdpYkdsemRWOXlaV1p5WlhOeklqcDdndz09", // Base64 encoded state
				HasMore:    true,
			}
		} else {
			// Next Page simulation
			response = MatchHistoryResponse{
				Matches: []map[string]interface{}{
					{"match_id": "m3", "score": "310", "weight": 1.5},
				},
				NextCursor: "",
				HasMore:    false,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	})

	t.Run("Fetch First Page without Cursor", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/matches?limit=2", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rr.Code)
		}

		var res MatchHistoryResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if len(res.Matches) != 2 {
			t.Errorf("expected 2 matches, got %d", len(res.Matches))
		}
		if !res.HasMore || res.NextCursor == "" {
			t.Error("expected more pages and a valid NextCursor token")
		}
	})

	t.Run("Fetch Second Page with Cursor", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/matches?limit=2&cursor=ZXlKdFlXTjBhR2xrSWpvbWpJaXdpYkdsemRWOXlaV1p5WlhOeklqcDdndz09", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		var res MatchHistoryResponse
		json.Unmarshal(rr.Body.Bytes(), &res)

		if len(res.Matches) != 1 {
			t.Errorf("expected 1 match on last page, got %d", len(res.Matches))
		}
		if res.HasMore {
			t.Error("expected HasMore to be false on the last page slice")
		}
	})
}