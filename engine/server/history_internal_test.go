package server

import (
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// A history row key is derived, never supplied, and two paths have to agree on
// it: the capture writes one record under it, and purgeRowHistory bounds a
// row's whole log by the prefix it shares. These pin the derivation itself —
// that it fits whatever the source key is, that it is settled by the source key
// alone, and that it separates rows the black-box specs cannot tell apart
// because they differ only past the truncation point.

var _ = Describe("historyRowKey", func() {
	// The name the specs derive against, and the widest one a schema can
	// declare — the budget has to hold for the latter.
	const entity = "historied"
	longEntity := strings.Repeat("e", naming.MaxEntityNameLength)

	// The escaped length at which "historied" + key + a 20-digit sequence stops
	// fitting. A key of exactly this length is the last one carried verbatim.
	budget := naming.MaxRowKeyBytes - len(entity) - len("18446744073709551615") - 2

	It("carries a source key that fits, verbatim", func() {
		key := strings.Repeat("k", budget)
		Expect(historyRowKey(entity, key, 7)).To(Equal(naming.RowKey([]string{entity, key, "7"})))
	})

	DescribeTable("stays a valid row key whatever the source key is",
		func(entity, key string) {
			derived := historyRowKey(entity, key, math.MaxUint64)
			Expect(naming.ValidateRowKey(derived)).To(Succeed())
			Expect(len(derived)).To(BeNumerically("<=", naming.MaxRowKeyBytes))
		},
		Entry("a key at the row-key limit", entity, strings.Repeat("k", naming.MaxRowKeyBytes)),
		// Escaping is what makes the source key's own length the wrong measure:
		// every `/` costs two bytes inside the joined key.
		Entry("a key of separators", entity, strings.Repeat("/", naming.MaxRowKeyBytes/2)),
		Entry("multi-byte runes", entity, strings.Repeat("é", naming.MaxRowKeyBytes/2)),
		Entry("the widest entity name", longEntity, strings.Repeat("k", naming.MaxRowKeyBytes)),
	)

	It("keeps a readable head of the source key in a stand-in", func() {
		// A budget that ran out mid-rune would leave a key that is not UTF-8,
		// which ValidateRowKey refuses — so the head stops short of one.
		key := strings.Repeat("é", naming.MaxRowKeyBytes/2)
		head, digest, found := strings.Cut(historyKeyPart(entity, key), historyKeyMarker)
		Expect(found).To(BeTrue())
		Expect(digest).To(HaveLen(historyKeyDigestLen))
		Expect(key).To(HavePrefix(head))
		Expect(utf8.ValidString(head)).To(BeTrue())
	})

	It("separates two long keys that differ only past the truncation", func() {
		head := strings.Repeat("k", naming.MaxRowKeyBytes-8)
		first := historyRowKey(entity, head+"aaaaaaa1", 1)
		second := historyRowKey(entity, head+"aaaaaaa2", 1)
		Expect(first).NotTo(Equal(second))
	})

	It("is settled by the source key alone, never by the sequence", func() {
		// This is what lets purgeRowHistory bound a row's log with ONE prefix.
		// A fit test that measured the sequence it was handed would carry a key
		// of this length verbatim while the workspace was young and stand in for
		// it later, splitting the row's records across two ranges.
		key := strings.Repeat("k", naming.MaxRowKeyBytes-len(entity)-3)
		prefix := naming.RowKey([]string{entity, historyKeyPart(entity, key)}) + "/"
		Expect(historyRowKey(entity, key, 9)).To(HavePrefix(prefix))
		Expect(historyRowKey(entity, key, math.MaxUint64)).To(HavePrefix(prefix))
	})
})

// The note table sits on the write path behind one lock, so what it costs a
// write is a property of the table rather than of the feature — and the black
// box cannot see it at all, since every note it drops only shortens a session.
// These pin the bounds that keep the cost constant: that the table cannot grow
// past its cap however many rows are edited at once, that an eviction leaves
// slack rather than parking it at the cap where every write would walk it, and
// that walking it is paced by the clock notes go stale on.

var _ = Describe("historySessions", func() {
	const window = 5 * time.Minute
	// A fixed instant, so the specs drive the table's clock rather than the
	// wall clock: every note carries the time it was taken.
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// note takes one note per call under distinct keys, and answers how many the
	// table holds afterwards.
	note := func(s *Server, at time.Time, keys ...string) int {
		for _, k := range keys {
			s.noteHistorySession("ws", "historied", k, 1, at)
		}
		return len(s.historyOpen.open)
	}
	held := func(s *Server, key string, at time.Time) bool {
		_, ok := s.openHistorySession("ws", "historied", key, at)
		return ok
	}

	It("drops a note the window has passed, and keeps one it has not", func() {
		s := &Server{historySession: window}
		note(s, base, "expired")
		note(s, base.Add(window/2), "live")
		// Past the pace the first note set, so this one walks the table.
		note(s, base.Add(window).Add(time.Nanosecond), "trigger")
		Expect(held(s, "expired", base.Add(window))).To(BeFalse())
		Expect(held(s, "live", base.Add(window))).To(BeTrue())
	})

	It("holds notes taken faster than the walk is paced", func() {
		s := &Server{historySession: window}
		for i := range 100 {
			note(s, base.Add(time.Duration(i)*time.Second), strconv.Itoa(i))
		}
		Expect(len(s.historyOpen.open)).To(Equal(100))
	})

	It("never grows past the cap, whatever is open", func() {
		s := &Server{historySession: window}
		// Every note is taken at one instant, so none of them ever expires and
		// the cap is the only thing that can bound the table — the shape a
		// workspace with more rows under simultaneous edit than the table holds
		// produces, and the one an expiry-only sweep cannot bound at all.
		for i := range 4 * historySessionMax {
			Expect(note(s, base, strconv.Itoa(i))).To(BeNumerically("<=", historySessionMax))
		}
	})

	It("leaves slack under the cap when it evicts", func() {
		s := &Server{historySession: window}
		keys := make([]string, historySessionMax)
		for i := range keys {
			keys[i] = strconv.Itoa(i)
		}
		Expect(note(s, base, keys...)).To(Equal(historySessionMax))
		// The slack is what amortizes the walk: without it the table would sit
		// at the cap and every write after this one would walk the whole of it.
		Expect(note(s, base, "over")).To(Equal(historySessionKeep + 1))
	})

	It("takes no note at all when folding is off", func() {
		s := &Server{historySession: HistorySessionOff}
		Expect(note(s, base, "a", "b")).To(BeZero())
		Expect(held(s, "a", base)).To(BeFalse())
	})
})
