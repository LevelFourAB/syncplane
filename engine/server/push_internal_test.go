package server

import (
	"strings"
	"testing"

	"github.com/levelfourab/syncplane/engine/storage"
)

func rejection(id uint64, messageBytes int) storage.ClientRejection {
	return storage.ClientRejection{
		MutationID: id,
		Code:       "conflict",
		Message:    strings.Repeat("x", messageBytes-len("conflict")),
	}
}

func ids(recs []storage.ClientRejection) []uint64 {
	out := make([]uint64, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.MutationID)
	}
	return out
}

func TestTrimRejectionsKeepsWhatFits(t *testing.T) {
	// A list well inside the bound is untouched, oldest first.
	small := []storage.ClientRejection{rejection(1, 100), rejection(2, 100), rejection(3, 100)}
	if got := ids(trimRejections(small, 1)); len(got) != 3 || got[0] != 1 {
		t.Fatalf("trimRejections dropped from a list that fits: %v", got)
	}

	// Over the bound, the newest survive and the oldest go.
	quarter := maxRejectionRecordBytes / 4
	over := []storage.ClientRejection{
		rejection(1, quarter), rejection(2, quarter), rejection(3, quarter),
		rejection(4, quarter), rejection(5, quarter),
	}
	got := ids(trimRejections(over, 1))
	if len(got) == 0 || got[len(got)-1] != 5 {
		t.Fatalf("trimRejections dropped the newest verdict: %v", got)
	}
	if len(got) >= len(over) {
		t.Fatalf("trimRejections kept an over-budget list whole: %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("trimRejections did not keep ascending order: %v", got)
		}
	}
}

func TestTrimRejectionsKeepsTheNewestOverBudget(t *testing.T) {
	// A single verdict larger than the whole budget is still recorded: dropping
	// it would leave the resend it was written for with nothing to learn.
	huge := []storage.ClientRejection{rejection(1, 100), rejection(2, maxRejectionRecordBytes*2)}
	got := ids(trimRejections(huge, 1))
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("want only the newest verdict kept, got %v", got)
	}
}

func TestTrimRejectionsKeepsTheLatestBatchOverBudget(t *testing.T) {
	// The latest batch's verdicts outgrow the budget on their own: the older
	// verdicts go, and every one of the batch stays.
	quarter := maxRejectionRecordBytes / 4
	recs := []storage.ClientRejection{rejection(1, 100), rejection(2, 100)}
	for id := uint64(3); id <= 8; id++ {
		recs = append(recs, rejection(id, quarter))
	}
	got := ids(trimRejections(recs, 6))
	if len(got) != 6 || got[0] != 3 || got[5] != 8 {
		t.Fatalf("want verdicts 3 to 8, got %v", got)
	}
}

func TestTrimRejectionsEmpty(t *testing.T) {
	if got := trimRejections(nil, 0); len(got) != 0 {
		t.Fatalf("want nothing, got %v", ids(got))
	}
}
