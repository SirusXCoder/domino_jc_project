import { useCallback, useEffect, useRef, useState } from 'react'
import {
  buildWebSocketUrl,
  fetchMatchState,
  resetSequence,
  submitMutate,
} from '../api/match'
import { buildPlayTileAction, resolveHandTile } from '../game/tiles'
import type {
  DominoTile,
  EventEnvelope,
  GameSession,
  SessionDeltaPayload,
} from '../types/game'

export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'error'

interface UseMatchSessionOptions {
  matchId: string
  playerId: string
  enabled: boolean
}

interface UseMatchSessionResult {
  session: GameSession | null
  connectionStatus: ConnectionStatus
  wsConnected: boolean
  error: string | null
  playTile: (tile: DominoTile, playAtLeft: boolean) => Promise<void>
}

export function useMatchSession({
  matchId,
  playerId,
  enabled,
}: UseMatchSessionOptions): UseMatchSessionResult {
  const [session, setSession] = useState<GameSession | null>(null)
  const [connectionStatus, setConnectionStatus] =
    useState<ConnectionStatus>('idle')
  const [wsConnected, setWsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const sessionRef = useRef<GameSession | null>(null)
  const lastEventTimestampRef = useRef(0)

  const applySession = useCallback(
    (next: GameSession, timestamp?: number, force = false) => {
      if (
        !force &&
        timestamp !== undefined &&
        timestamp < lastEventTimestampRef.current
      ) {
        return
      }
      if (timestamp !== undefined) {
        lastEventTimestampRef.current = timestamp
      }
      sessionRef.current = next
      setSession(next)
    },
    [],
  )

  useEffect(() => {
    if (!enabled || !matchId || !playerId) {
      return
    }

    let cancelled = false
    resetSequence()
    setConnectionStatus('connecting')
    setError(null)
    setSession(null)
    sessionRef.current = null
    lastEventTimestampRef.current = 0
    setWsConnected(false)

    const connect = async () => {
      try {
        const state = await fetchMatchState(matchId, playerId)
        if (cancelled) return

        if (!state.found || !state.session) {
          throw new Error(state.error ?? 'Match not found')
        }
        applySession(state.session, Date.now(), true)

        const ws = new WebSocket(buildWebSocketUrl(matchId, playerId))
        wsRef.current = ws

        ws.onopen = () => {
          if (!cancelled) setWsConnected(true)
        }

        ws.onmessage = (event) => {
          try {
            const envelope = JSON.parse(event.data) as EventEnvelope
            if (envelope.type === 'SESSION_DELTA') {
              const payload = envelope.payload as SessionDeltaPayload
              if (payload.session) {
                applySession(payload.session, envelope.timestamp)
              }
            } else if (envelope.type === 'STATE_SNAPSHOT') {
              applySession(envelope.payload as GameSession, envelope.timestamp)
            }
          } catch {
            // Ignore malformed frames
          }
        }

        ws.onerror = () => {
          if (!cancelled) {
            setError('WebSocket connection error')
            setConnectionStatus('error')
          }
        }

        ws.onclose = () => {
          if (!cancelled) setWsConnected(false)
        }

        if (!cancelled) setConnectionStatus('connected')
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Connection failed')
          setConnectionStatus('error')
        }
      }
    }

    void connect()

    return () => {
      cancelled = true
      wsRef.current?.close()
      wsRef.current = null
    }
  }, [applySession, enabled, matchId, playerId])

  const playTile = useCallback(
    async (tile: DominoTile, playAtLeft: boolean) => {
      const current = sessionRef.current
      if (!current) {
        throw new Error('Match state is not loaded')
      }

      const handTile = resolveHandTile(current, playerId, tile)
      if (!handTile) {
        throw new Error(
          `Tile ${tile.tile_id ?? `${tile.value_left}-${tile.value_right}`} is not in your hand`,
        )
      }

      const response = await submitMutate(
        matchId,
        playerId,
        buildPlayTileAction(playerId, handTile, playAtLeft),
      )

      if (response.result?.session) {
        applySession(response.result.session, Date.now(), true)
      }
    },
    [applySession, matchId, playerId],
  )

  return {
    session,
    connectionStatus,
    wsConnected,
    error,
    playTile,
  }
}
