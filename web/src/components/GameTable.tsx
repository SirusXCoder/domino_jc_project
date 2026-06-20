import type { DominoTile, GameSession } from '../types/game'
import { OpponentHandPanel } from './OpponentHand'
import { GameBoard } from './GameBoard'
import { PlayerHandPanel } from './PlayerHand'
import './GameTable.css'

interface GameTableProps {
  session: GameSession
  playerId: string
  matchId: string
  wsConnected: boolean
  onPlayTile: (tile: DominoTile, playAtLeft: boolean) => Promise<void>
  onDisconnect: () => void
}

export function GameTable({
  session,
  playerId,
  matchId,
  wsConnected,
  onPlayTile,
  onDisconnect,
}: GameTableProps) {
  return (
    <div className="game-table">
      <header className="game-table-header">
        <div>
          <h1>Dominoes</h1>
          <p className="game-table-meta">
            Match <code>{matchId}</code> · You are <code>{playerId}</code>
          </p>
        </div>
        <div className="game-table-status">
          <span className={`ws-indicator ${wsConnected ? 'ws-indicator--live' : ''}`}>
            {wsConnected ? 'Live' : 'Reconnecting…'}
          </span>
          <button type="button" className="disconnect-btn" onClick={onDisconnect}>
            Leave
          </button>
        </div>
      </header>

      <OpponentHandPanel session={session} playerId={playerId} />
      <GameBoard session={session} />
      <PlayerHandPanel
        session={session}
        playerId={playerId}
        onPlayTile={onPlayTile}
      />
    </div>
  )
}
