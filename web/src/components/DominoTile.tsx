import './DominoTile.css'

const PIP_POSITIONS: Record<number, string[]> = {
  0: [],
  1: ['center'],
  2: ['top-left', 'bottom-right'],
  3: ['top-left', 'center', 'bottom-right'],
  4: ['top-left', 'top-right', 'bottom-left', 'bottom-right'],
  5: ['top-left', 'top-right', 'center', 'bottom-left', 'bottom-right'],
  6: [
    'top-left',
    'top-right',
    'middle-left',
    'middle-right',
    'bottom-left',
    'bottom-right',
  ],
}

interface DominoFaceProps {
  value: number
}

function DominoFace({ value }: DominoFaceProps) {
  const positions = PIP_POSITIONS[value] ?? []
  return (
    <div className="domino-face" aria-label={`${value}`}>
      {positions.map((pos) => (
        <span key={pos} className={`pip pip-${pos}`} />
      ))}
    </div>
  )
}

export interface DominoTileProps {
  tileId?: string
  valueLeft: number
  valueRight: number
  faceDown?: boolean
  horizontal?: boolean
  playable?: boolean
  selected?: boolean
  disabled?: boolean
  onClick?: () => void
}

export function DominoTileView({
  tileId,
  valueLeft,
  valueRight,
  faceDown = false,
  horizontal = true,
  playable = false,
  selected = false,
  disabled = false,
  onClick,
}: DominoTileProps) {
  const className = [
    'domino-tile',
    horizontal ? 'domino-tile--horizontal' : 'domino-tile--vertical',
    faceDown ? 'domino-tile--face-down' : '',
    playable ? 'domino-tile--playable' : '',
    selected ? 'domino-tile--selected' : '',
    disabled ? 'domino-tile--disabled' : '',
    onClick ? 'domino-tile--clickable' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <button
      type="button"
      className={className}
      onClick={onClick}
      disabled={disabled || !onClick}
      aria-label={
        faceDown
          ? 'Hidden domino'
          : `Domino ${valueLeft} ${valueRight}${tileId ? ` (${tileId})` : ''}`
      }
    >
      {faceDown ? (
        <span className="domino-back" />
      ) : (
        <>
          <DominoFace value={valueLeft} />
          <span className="domino-divider" />
          <DominoFace value={valueRight} />
        </>
      )}
    </button>
  )
}
