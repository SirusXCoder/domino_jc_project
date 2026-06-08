package pagination_test

import (
	"testing"
	"time"

	"domino_jc_project/pkg/pagination"
)

func TestCursorRoundTrip(t *testing.T) {
	original := pagination.MatchHistoryCursor{
		EndTime: time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC),
		MatchID: "match-abc",
	}

	token, err := pagination.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := pagination.Decode(token)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !decoded.EndTime.Equal(original.EndTime) {
		t.Fatalf("end_time = %v, want %v", decoded.EndTime, original.EndTime)
	}
	if decoded.MatchID != original.MatchID {
		t.Fatalf("match_id = %q, want %q", decoded.MatchID, original.MatchID)
	}
}

func TestDecodeRejectsEmptyToken(t *testing.T) {
	if _, err := pagination.Decode(""); err == nil {
		t.Fatal("expected error for empty cursor")
	}
}
