import type { DominoTile, GameSession, MatchActionData } from '../types/game'

/** Matches Go NewTile: tile_id is always "{lower}-{higher}". */
export function canonicalTileId(valueLeft: number, valueRight: number): string {
  const lo = Math.min(valueLeft, valueRight)
  const hi = Math.max(valueLeft, valueRight)
  return `${lo}-${hi}`
}

/** Ensures tile_id is set; the backend matches hand tiles by tile_id only. */
export function normalizeTileForMutate(tile: DominoTile): DominoTile {
  return {
    tile_id: tile.tile_id || canonicalTileId(tile.value_left, tile.value_right),
    value_left: tile.value_left,
    value_right: tile.value_right,
  }
}

/** Resolves the authoritative tile object from the player's current hand. */
export function resolveHandTile(
  session: GameSession,
  playerId: string,
  tile: DominoTile,
): DominoTile | undefined {
  const hand = session.hands.find((entry) => entry.player_id === playerId)
  if (!hand?.tiles?.length) {
    return undefined
  }

  const normalized = normalizeTileForMutate(tile)
  return hand.tiles.find(
    (candidate) =>
      candidate.tile_id === normalized.tile_id ||
      (candidate.value_left === normalized.value_left &&
        candidate.value_right === normalized.value_right),
  )
}

export function buildPlayTileAction(
  playerId: string,
  tile: DominoTile,
  playAtLeft: boolean,
): MatchActionData {
  const normalized = normalizeTileForMutate(tile)
  return {
    kind: 'PLAY_TILE',
    player_id: playerId,
    tile: {
      tile_id: normalized.tile_id,
      value_left: normalized.value_left,
      value_right: normalized.value_right,
    },
    play_at_left: playAtLeft,
  }
}
