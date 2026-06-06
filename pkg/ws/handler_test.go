package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestServeConnect_RejectsMissingQueryParams(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	handler := NewHandler(hub)

	tests := []struct {
		name       string
		query      string
		wantStatus int
	}{
		{"missing both", "", http.StatusBadRequest},
		{"missing player_id", "session_id=s1", http.StatusBadRequest},
		{"missing session_id", "player_id=p1", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ws/connect?"+tt.query, nil)
			rec := httptest.NewRecorder()
			handler.ServeConnect(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

func TestServeConnect_UpgradesWithValidParams(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	server := httptest.NewServer(http.HandlerFunc(NewHandler(hub).ServeConnect))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/connect?session_id=s1&player_id=p1"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusSwitchingProtocols)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.ClientCount() == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected 1 registered client, got %d", hub.ClientCount())
}

func TestServeConnect_RejectsNonGet(t *testing.T) {
	hub := NewHub(nil)
	handler := NewHandler(hub)

	req := httptest.NewRequest(http.MethodPost, "/ws/connect?session_id=s1&player_id=p1", nil)
	rec := httptest.NewRecorder()
	handler.ServeConnect(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestIsValidJSON(t *testing.T) {
	if !isValidJSON([]byte(`{"type":"ping"}`)) {
		t.Fatal("expected valid JSON")
	}
	if isValidJSON([]byte(`not json`)) {
		t.Fatal("expected invalid JSON")
	}
}
