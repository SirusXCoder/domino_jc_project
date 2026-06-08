package ws

import "domino_jc_project/pkg/models"

// StatsBroadcaster pushes post-rating career updates to connected clients.
type StatsBroadcaster interface {
	BroadcastPlayerStatsUpdate(sessionID string, updates []models.PlayerStatsUpdate)
}
