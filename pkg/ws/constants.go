package ws

import "time"

const (
	// ReadBufferSize and WriteBufferSize tune per-connection I/O buffers.
	ReadBufferSize  = 4096
	WriteBufferSize = 4096

	// sendBufferSize caps queued outbound messages per client before back-pressure.
	sendBufferSize = 256

	// maxMessageSize is the largest inbound frame we accept from a client.
	maxMessageSize = 512 * 1024 // 512 KiB

	// writeWait is the deadline for completing a write to the peer.
	writeWait = 10 * time.Second

	// pongWait is how long we wait for a pong before closing the connection.
	pongWait = 60 * time.Second

	// pingPeriod must be less than pongWait so the peer has time to respond.
	pingPeriod = 9 * time.Second

	// reconnectGracePeriod is how long a disconnected player may reconnect
	// before being marked abandoned.
	reconnectGracePeriod = 45 * time.Second
)
