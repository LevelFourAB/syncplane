package server

// What a captured write pays to note where its row's open history record is.
//
// The note table is one map behind one lock, shared by every workspace on the
// host, and this runs on the write path — so a cost that scaled with the number
// of rows under edit would be paid by every writer there, not only by the one
// editing. Run with: go test ./engine/server/ -bench NoteHistorySession

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkNoteHistorySession(b *testing.B) {
	// More rows under simultaneous edit than the table holds, every note taken
	// at one instant so that none of them ever expires. Eviction is then the
	// only thing bounding the table, which is the case worth measuring: the
	// number here is the walk it pays for, amortized over the writes that refill
	// the slack.
	const rows = 4 * historySessionMax
	keys := make([]string, rows)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := &Server{historySession: 5 * time.Minute}
	for _, k := range keys[:historySessionKeep] {
		s.noteHistorySession("ws", "historied", k, 1, now)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.noteHistorySession("ws", "historied", keys[i%rows], uint64(i)+1, now)
	}
}
