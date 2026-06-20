export interface DominoTile {
  tile_id: string
  value_left: number
  value_right: number
  uid?: string
}

export interface PlayerHand {
  player_id: string
  tiles?: DominoTile[]
  tile_count?: number
  has_passed: boolean
  is_ready: boolean
  is_abandoned: boolean
}

export interface GameSession {
  session_id?: string
  status: string
  players: string[]
  hands: PlayerHand[]
  boneyard_count?: number
  game_board: DominoTile[]
  left_open_value: number
  right_open_value: number
  current_turn: string
  mutations_locked?: boolean
}

export interface MatchStateResponse {
  match_id: string
  player_id: string
  found: boolean
  session?: GameSession
  node_id?: string
  state?: string
  error?: string
}

export interface MatchActionData {
  kind: 'PLAY_TILE' | 'DRAW' | 'PASS'
  player_id: string
  tile?: DominoTile
  play_at_left?: boolean
}

export interface MatchMutateRequest {
  client_id: string
  sequence_number: number
  match_id: string
  player_id: string
  action_data: MatchActionData
}

export interface MatchMutateResponse {
  ok: boolean
  idempotent_replay?: boolean
  result?: {
    ok: boolean
    session?: GameSession
    applied?: boolean
  }
  error?: string
}

export interface EventEnvelope<T = unknown> {
  type: string
  timestamp: number
  payload: T
}

export interface SessionDeltaPayload {
  session_id: string
  match_id: string
  op: string
  applied?: boolean
  session?: GameSession
}

export interface PlayOption {
  left: boolean
  right: boolean
}
