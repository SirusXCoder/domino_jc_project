import { useState } from 'react'
import { ConnectionPanel } from './components/ConnectionPanel'
import { GameTable } from './components/GameTable'
import { useMatchSession } from './hooks/useMatchSession'
import './App.css'

function App() {
  const [credentials, setCredentials] = useState<{
    matchId: string
    playerId: string
  } | null>(null)

  const { session, connectionStatus, wsConnected, error, playTile } =
    useMatchSession({
      matchId: credentials?.matchId ?? '',
      playerId: credentials?.playerId ?? '',
      enabled: credentials !== null,
    })

  if (!credentials) {
    return (
      <div className="app">
        <ConnectionPanel
          onConnect={(matchId, playerId) =>
            setCredentials({ matchId, playerId })
          }
        />
      </div>
    )
  }

  if (connectionStatus === 'connecting' || !session) {
    return (
      <div className="app app--centered">
        <p className="loading-message">Connecting to match…</p>
        {error && <p className="error-message">{error}</p>}
      </div>
    )
  }

  if (connectionStatus === 'error' && !session) {
    return (
      <div className="app app--centered">
        <p className="error-message">{error ?? 'Connection failed'}</p>
        <button type="button" onClick={() => setCredentials(null)}>
          Back
        </button>
      </div>
    )
  }

  return (
    <div className="app">
      <GameTable
        session={session}
        playerId={credentials.playerId}
        matchId={credentials.matchId}
        wsConnected={wsConnected}
        onPlayTile={playTile}
        onDisconnect={() => setCredentials(null)}
      />
      {error && <p className="toast-error">{error}</p>}
    </div>
  )
}

export default App
