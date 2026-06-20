import type {
  MatchActionData,
  MatchMutateRequest,
  MatchMutateResponse,
  MatchStateResponse,
} from '../types/game'

const CLIENT_ID_KEY = 'domino_client_id'

function getClientId(): string {
  let id = sessionStorage.getItem(CLIENT_ID_KEY)
  if (!id) {
    id = crypto.randomUUID()
    sessionStorage.setItem(CLIENT_ID_KEY, id)
  }
  return id
}

let sequenceNumber = 0

export function resetSequence(): void {
  sequenceNumber = 0
}

export async function fetchMatchState(
  matchId: string,
  playerId: string,
): Promise<MatchStateResponse> {
  const params = new URLSearchParams({ match_id: matchId, player_id: playerId })
  const response = await fetch(`/match/state?${params}`)
  if (!response.ok) {
    throw new Error(`Failed to fetch match state (${response.status})`)
  }
  return response.json()
}

export async function createMatch(
  playerIds: string[] = ['p1', 'p2'],
): Promise<string> {
  const response = await fetch('/match/create', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      player_uids: playerIds,
      setup_game: true,
      tiles_per_hand: 7,
    }),
  })
  const data = await response.json()
  if (!response.ok || !data.ok) {
    throw new Error(data.error ?? `Failed to create match (${response.status})`)
  }
  return data.match_id as string
}

export async function submitMutate(
  matchId: string,
  playerId: string,
  actionData: MatchActionData,
): Promise<MatchMutateResponse> {
  sequenceNumber += 1
  const body: MatchMutateRequest = {
    client_id: getClientId(),
    sequence_number: sequenceNumber,
    match_id: matchId,
    player_id: playerId,
    action_data: actionData,
  }

  const response = await fetch('/match/mutate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })

  const data: MatchMutateResponse = await response.json()
  if (!response.ok || !data.ok) {
    throw new Error(data.error ?? `Mutation failed (${response.status})`)
  }
  return data
}

export function buildWebSocketUrl(matchId: string, playerId: string): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const params = new URLSearchParams({
    session_id: matchId,
    player_id: playerId,
  })
  return `${protocol}//${window.location.host}/ws/connect?${params}`
}
