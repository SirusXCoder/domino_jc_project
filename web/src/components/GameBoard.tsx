import type { GameSession } from '../types/game'
import { DominoTileView } from './DominoTile'
import './GameBoard.css'

interface GameBoardProps {
  session: GameSession
}

export function GameBoard({ session }: GameBoardProps) {
  const board = session.game_board ?? []
  const isEmpty = board.length === 0

  return (
    <section className="game-board">
      <header className="board-header">
        <h2>Board</h2>
        <div className="board-meta">
          <span>Boneyard: {session.boneyard_count ?? 0}</span>
          <span>Status: {session.status}</span>
        </div>
      </header>

      <div className="board-surface">
        {!isEmpty && session.left_open_value >= 0 && (
          <div className="board-end board-end--left">
            <span className="board-end-label">L</span>
            <span className="board-end-value">{session.left_open_value}</span>
          </div>
        )}

        <div className="board-chain">
          {isEmpty ? (
            <p className="board-empty">No tiles played yet</p>
          ) : (
            board.map((tile, index) => (
              <DominoTileView
                key={`${tile.tile_id}-${index}`}
                tileId={tile.tile_id}
                valueLeft={tile.value_left}
                valueRight={tile.value_right}
                horizontal
              />
            ))
          )}
        </div>

        {!isEmpty && session.right_open_value >= 0 && (
          <div className="board-end board-end--right">
            <span className="board-end-label">R</span>
            <span className="board-end-value">{session.right_open_value}</span>
          </div>
        )}
      </div>
    </section>
  )
}
