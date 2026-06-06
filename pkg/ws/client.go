package ws

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

// Client represents a single WebSocket peer. Outbound writes are serialized
// through the send channel and writePump to prevent concurrent write panics.
type Client struct {
	hub *Hub

	conn *websocket.Conn

	sessionID string
	playerID  string

	send chan []byte
}

func newClient(hub *Hub, conn *websocket.Conn, sessionID, playerID string) *Client {
	return &Client{
		hub:       hub,
		conn:      conn,
		sessionID: sessionID,
		playerID:  playerID,
		send:      make(chan []byte, sendBufferSize),
	}
}

// readPump pulls messages from the WebSocket connection and forwards valid
// payloads to the hub. It runs in its own goroutine and always unregisters
// the client when the connection ends.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("ws: read error session=%s player=%s: %v", c.sessionID, c.playerID, err)
			}
			return
		}

		if !isValidJSON(message) {
			c.sendError(ErrCodeInvalidJSON, "message is not valid JSON")
			continue
		}

		c.hub.inbound <- &InboundMessage{
			SessionID: c.sessionID,
			PlayerID:  c.playerID,
			Payload:   message,
		}
	}
}

// writePump drains the send channel and writes frames to the WebSocket.
// Periodic ping frames keep the connection alive through proxies and NAT.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel; tell the peer we are done.
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func isValidJSON(payload []byte) bool {
	return json.Valid(payload)
}

func (c *Client) sendError(code, message string) {
	payload, err := newErrorEnvelope(code, message)
	if err != nil {
		log.Printf("ws: failed to marshal error envelope session=%s player=%s: %v", c.sessionID, c.playerID, err)
		return
	}

	select {
	case c.send <- payload:
	default:
		log.Printf("ws: outbound buffer full session=%s player=%s", c.sessionID, c.playerID)
	}
}
