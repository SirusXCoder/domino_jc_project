package repository

import (
	"context"

	"domino_jc_project/pkg/models"
)

// MatchLedgerRepository persists immutable match outcome records.
type MatchLedgerRepository interface {
	SaveMatchRecord(ctx context.Context, record models.MatchRecord) error
}
