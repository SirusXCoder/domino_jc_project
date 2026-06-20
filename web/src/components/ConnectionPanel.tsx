import { useState } from 'react'
import { createMatch } from '../api/match'
import './ConnectionPanel.css'

interface ConnectionPanelProps {
  onConnect: (matchId: string, playerId: string) => void
}

export function ConnectionPanel({ onConnect }: ConnectionPanelProps) {
  const [matchId, setMatchId] = useState('')
  const [playerId, setPlayerId] = useState('p1')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleCreate = async () => {
    setLoading(true)
    setError(null)
    try {
      const id = await createMatch(['p1', 'p2'])
      setMatchId(id)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create match')
    } finally {
      setLoading(false)
    }
  }

  const handleConnect = () => {
    if (!matchId.trim() || !playerId.trim()) {
      setError('Match ID and Player ID are required')
      return
    }
    setError(null)
    onConnect(matchId.trim(), playerId.trim())
  }

  return (
    <section className="connection-panel">
      <h1>Dominoes</h1>
      <p className="connection-subtitle">
        Connect to a match on the gateway to play in real time.
      </p>

      <div className="connection-fields">
        <label>
          Match ID
          <input
            type="text"
            value={matchId}
            onChange={(event) => setMatchId(event.target.value)}
            placeholder="match-abc123"
          />
        </label>
        <label>
          Player ID
          <select
            value={playerId}
            onChange={(event) => setPlayerId(event.target.value)}
          >
            <option value="p1">p1</option>
            <option value="p2">p2</option>
          </select>
        </label>
      </div>

      <div className="connection-actions">
        <button type="button" disabled={loading} onClick={() => void handleCreate()}>
          {loading ? 'Creating…' : 'Create Match'}
        </button>
        <button
          type="button"
          className="connection-connect"
          disabled={loading}
          onClick={handleConnect}
        >
          Connect
        </button>
      </div>

      {error && <p className="connection-error">{error}</p>}
    </section>
  )
}
