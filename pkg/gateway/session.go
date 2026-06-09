package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"domino_jc_project/pkg/consensus"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/models"
	"domino_jc_project/pkg/ws"
)

// ReplicatedGameHandler routes in-game mutations through the local Raft leader
// instead of evaluating game state directly on gateway followers.
type ReplicatedGameHandler struct {
	raft    *consensus.RaftNode
	manager *engine.GameManager
}

// NewReplicatedGameHandler constructs a consensus-backed GameActionHandler.
func NewReplicatedGameHandler(raft *consensus.RaftNode, manager *engine.GameManager) *ReplicatedGameHandler {
	return &ReplicatedGameHandler{
		raft:    raft,
		manager: manager,
	}
}

// WireSessionReplication connects Raft apply notifications to hub session fan-out.
func WireSessionReplication(node *consensus.RaftNode, hub *ws.Hub) {
	if node == nil || hub == nil {
		return
	}
	node.SetApplyNotifier(func(result consensus.ApplyResult) {
		delta := ws.SessionDelta{
			MatchID: result.MatchID,
			Op:      result.Op,
			Applied: result.Applied,
		}
		if result.Session != nil {
			raw, err := json.Marshal(result.Session)
			if err == nil {
				delta.Session = raw
			}
		}
		if result.Turn != nil {
			raw, err := json.Marshal(result.Turn)
			if err == nil {
				delta.Turn = raw
			}
		}
		hub.NotifySessionDelta(delta)
	})
}

func (h *ReplicatedGameHandler) ApplyPlayTile(
	ctx context.Context,
	sessionID, playerID string,
	tile models.DominoTile,
	playAtLeft bool,
) (bool, error) {
	result, err := h.proposeTurn(sessionID, consensus.ApplyTurnPayload{
		Kind:       string(models.TurnKindPlayTile),
		PlayerID:   playerID,
		Tile:       tile,
		PlayAtLeft: playAtLeft,
	})
	if err != nil {
		return false, err
	}
	if result.Turn == nil {
		return result.Applied, nil
	}
	return result.Turn.Applied, nil
}

func (h *ReplicatedGameHandler) ApplyDrawFromBoneyard(
	ctx context.Context,
	sessionID, playerID string,
) (*models.DominoTile, error) {
	result, err := h.proposeTurn(sessionID, consensus.ApplyTurnPayload{
		Kind:     string(models.TurnKindDraw),
		PlayerID: playerID,
	})
	if err != nil {
		return nil, err
	}
	if result.Turn == nil {
		return nil, nil
	}
	return result.Turn.DrawnTile, nil
}

func (h *ReplicatedGameHandler) ApplyPassTurn(ctx context.Context, sessionID, playerID string) error {
	_, err := h.proposeTurn(sessionID, consensus.ApplyTurnPayload{
		Kind:     string(models.TurnKindPass),
		PlayerID: playerID,
	})
	return err
}

func (h *ReplicatedGameHandler) HandlePlayerAbandoned(ctx context.Context, sessionID, playerID string) error {
	return h.manager.HandlePlayerAbandoned(ctx, sessionID, playerID)
}

func (h *ReplicatedGameHandler) GetSession(ctx context.Context, sessionID string) (*models.GameSession, bool) {
	return h.manager.GetSession(ctx, sessionID)
}

func (h *ReplicatedGameHandler) proposeTurn(
	sessionID string,
	payload consensus.ApplyTurnPayload,
) (consensus.ApplyResult, error) {
	command, err := consensus.EncodeCommandWithPayload(consensus.OpApplyTurn, sessionID, payload)
	if err != nil {
		return consensus.ApplyResult{}, fmt.Errorf("encode replicated turn: %w", err)
	}

	raw, err := h.raft.ProposeAndWait(command)
	if err != nil {
		return consensus.ApplyResult{}, mapProposalError(err)
	}
	if consensus.IsApplyError(raw) {
		if asErr, ok := raw.(error); ok {
			return consensus.ApplyResult{}, asErr
		}
		return consensus.ApplyResult{}, fmt.Errorf("replicated turn rejected: %v", raw)
	}

	applyResult, ok := consensus.AsApplyResult(raw)
	if !ok || !applyResult.OK {
		return consensus.ApplyResult{}, fmt.Errorf("replicated turn returned no confirmation")
	}
	return applyResult, nil
}

func mapProposalError(err error) error {
	var redirect *consensus.LeaderRedirectError
	if errors.As(err, &redirect) && redirect != nil && redirect.LeaderID != "" && redirect.LeaderAddress != "" {
		return &ws.RedirectableError{
			Metadata: ws.RedirectMetadata{
				LeaderID:      redirect.LeaderID,
				LeaderAddress: redirect.LeaderAddress,
			},
			Message: redirect.Error(),
		}
	}
	return err
}
