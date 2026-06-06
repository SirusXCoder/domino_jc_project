package ws

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// Handler serves the WebSocket upgrade endpoint.
type Handler struct {
	hub      *Hub
	upgrader websocket.Upgrader
}

// NewHandler builds a Handler wired to the given Hub.
func NewHandler(hub *Hub) *Handler {
	return &Handler{
		hub: hub,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  ReadBufferSize,
			WriteBufferSize: WriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				// TODO(production): restrict origins to trusted front-end domains.
				// Example: return r.Header.Get("Origin") == "https://game.example.com"
				return true
			},
		},
	}
}

// ServeConnect upgrades GET /ws/connect?session_id=XYZ&player_id=ABC to WebSocket.
func (h *Handler) ServeConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := r.URL.Query().Get("session_id")
	playerID := r.URL.Query().Get("player_id")
	if sessionID == "" || playerID == "" {
		http.Error(w, "session_id and player_id query parameters are required", http.StatusBadRequest)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws: upgrade failed session=%s player=%s: %v", sessionID, playerID, err)
		return
	}

	client := newClient(h.hub, conn, sessionID, playerID)
	h.hub.register <- client

	go client.writePump()
	go client.readPump()
}
