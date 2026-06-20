import type { DominoTile, GameSession, PlayerHand, PlayOption } from '../types/game'

export function canPlayTile(
  session: GameSession,
  tile: DominoTile,
  playAtLeft: boolean,
): boolean {
  if (session.left_open_value === -1 && session.right_open_value === -1) {
    return true
  }
  if (playAtLeft) {
    return (
      tile.value_left === session.left_open_value ||
      tile.value_right === session.left_open_value
    )
  }
  return (
    tile.value_left === session.right_open_value ||
    tile.value_right === session.right_open_value
  )
}

export function getPlayOptions(
  session: GameSession,
  tile: DominoTile,
): PlayOption {
  if (session.left_open_value === -1 && session.right_open_value === -1) {
    return { left: true, right: true }
  }
  return {
    left:
      tile.value_left === session.left_open_value ||
      tile.value_right === session.left_open_value,
    right:
      tile.value_left === session.right_open_value ||
      tile.value_right === session.right_open_value,
  }
}

export function isTilePlayable(session: GameSession, tile: DominoTile): boolean {
  const options = getPlayOptions(session, tile)
  return options.left || options.right
}

export function findHand(
  session: GameSession,
  playerId: string,
): PlayerHand | undefined {
  return session.hands.find((hand) => hand.player_id === playerId)
}

export function opponentHand(
  session: GameSession,
  playerId: string,
): PlayerHand | undefined {
  return session.hands.find((hand) => hand.player_id !== playerId)
}
