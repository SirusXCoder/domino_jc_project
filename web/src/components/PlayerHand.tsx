import { useState } from 'react'
import type { DominoTile, GameSession } from '../types/game'
import {
  getPlayOptions,
  isTilePlayable,
} from '../game/playability'
import { DominoTileView } from './DominoTile'
import './PlayerHand.css'

interface PlayerHandProps {
  session: GameSession
  playerId: string
  onPlayTile: (tile: DominoTile, playAtLeft: boolean) => Promise<void>
}

export function PlayerHandPanel({
  session,
  playerId,
  onPlayTile,
}: PlayerHandProps) {
  const [selectedTile, setSelectedTile] = useState<DominoTile | null>(null)
  const [pending, setPending] = useState(false)

  const hand = session.hands.find((entry) => entry.player_id === playerId)
  const tiles = hand?.tiles ?? []
  const isMyTurn = session.current_turn === playerId && !session.mutations_locked

  const handleTileClick = (tile: DominoTile) => {
    if (!isMyTurn || pending) return
    const options = getPlayOptions(session, tile)
    if (!options.left && !options.right) return

    if (options.left && !options.right) {
      void submitPlay(tile, true)
      return
    }
    if (options.right && !options.left) {
      void submitPlay(tile, false)
      return
    }
    if (
      session.left_open_value === -1 &&
      session.right_open_value === -1
    ) {
      void submitPlay(tile, true)
      return
    }
    setSelectedTile((current) =>
      current?.tile_id === tile.tile_id ? null : tile,
    )
  }

  const submitPlay = async (tile: DominoTile, playAtLeft: boolean) => {
    setPending(true)
    try {
      await onPlayTile(tile, playAtLeft)
      setSelectedTile(null)
    } finally {
      setPending(false)
    }
  }

  const selectedOptions = selectedTile
    ? getPlayOptions(session, selectedTile)
    : null

  return (
    <section className="player-hand-panel">
      <header className="hand-header">
        <h2>Your Hand</h2>
        <span className={`turn-badge ${isMyTurn ? 'turn-badge--active' : ''}`}>
          {isMyTurn ? 'Your turn' : `Waiting for ${session.current_turn}`}
        </span>
      </header>

      <div className="hand-tiles">
        {tiles.map((tile) => {
          const playable = isMyTurn && isTilePlayable(session, tile)
          return (
            <DominoTileView
              key={tile.tile_id}
              tileId={tile.tile_id}
              valueLeft={tile.value_left}
              valueRight={tile.value_right}
              playable={playable}
              selected={selectedTile?.tile_id === tile.tile_id}
              disabled={!playable || pending}
              onClick={playable ? () => handleTileClick(tile) : undefined}
            />
          )
        })}
      </div>

      {selectedTile && selectedOptions && (
        <div className="play-end-picker">
          <span>Play {selectedTile.tile_id} on:</span>
          {selectedOptions.left && (
            <button
              type="button"
              disabled={pending}
              onClick={() => void submitPlay(selectedTile, true)}
            >
              Left ({session.left_open_value})
            </button>
          )}
          {selectedOptions.right && (
            <button
              type="button"
              disabled={pending}
              onClick={() => void submitPlay(selectedTile, false)}
            >
              Right ({session.right_open_value})
            </button>
          )}
          <button
            type="button"
            className="play-end-cancel"
            disabled={pending}
            onClick={() => setSelectedTile(null)}
          >
            Cancel
          </button>
        </div>
      )}
    </section>
  )
}
