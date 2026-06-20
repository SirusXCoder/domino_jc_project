import type { GameSession } from '../types/game'
import { opponentHand } from '../game/playability'
import { DominoTileView } from './DominoTile'
import './OpponentHand.css'

interface OpponentHandProps {
  session: GameSession
  playerId: string
}

export function OpponentHandPanel({ session, playerId }: OpponentHandProps) {
  const opponent = opponentHand(session, playerId)
  const tileCount = opponent?.tile_count ?? opponent?.tiles?.length ?? 0

  return (
    <section className="opponent-hand-panel">
      <header className="opponent-header">
        <h2>Opponent</h2>
        <span className="opponent-id">{opponent?.player_id ?? '—'}</span>
        <span className="opponent-count">{tileCount} tiles</span>
      </header>
      <div className="opponent-tiles">
        {Array.from({ length: tileCount }, (_, index) => (
          <DominoTileView
            key={`back-${index}`}
            valueLeft={0}
            valueRight={0}
            faceDown
            horizontal
          />
        ))}
      </div>
    </section>
  )
}
