package server

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

// How much of a cursor a seek bound carries is decided by the index the plan
// chose, and the two answers are not interchangeable: run the row key into an
// index that carries components past the ordering and the bound is a prefix of
// no entry at all, so the range comes back empty and rows go missing. These pin
// both bounds against entries of the very index they are composed with.
//
// The black-box half — that the rows are the same either way, and that an index
// the ordering spans stops re-reading the tie group — is in query_cursor_test.go.
var _ = Describe("seekRange", func() {
	desc := (&testdatav1.Issue{}).ProtoReflect().Descriptor()

	index := func(name string, paths ...string) query.Index {
		GinkgoHelper()
		comps := make([]query.IndexComponent, 0, len(paths))
		for _, p := range paths {
			descending := false
			if p[0] == '-' {
				descending, p = true, p[1:]
			}
			c, err := query.CompileIndexComponent(desc, []string{p}, descending)
			Expect(err).NotTo(HaveOccurred())
			comps = append(comps, c)
		}
		enc, err := query.NewIndexEncoder(comps)
		Expect(err).NotTo(HaveOccurred())
		return query.Index{Name: name, Encoder: enc}
	}

	// The row a cursor is minted from, and its neighbour in the same tie group:
	// same rank, so only the row key separates them.
	boundary := &testdatav1.Issue{Id: "i50", State: "open", Rank: 7, Title: "t"}
	next := &testdatav1.Issue{Id: "i51", State: "open", Rank: 7, Title: "t"}

	// The one entry a row takes in an index of scalar components.
	entryIn := func(idx query.Index, msg *testdatav1.Issue) []byte {
		GinkgoHelper()
		entries := idx.Encoder.Encode(msg.ProtoReflect(), msg.GetId())
		Expect(entries).To(HaveLen(1))
		return entries[0]
	}

	// The ordering every plan here is asked for, and the cursor at `boundary`.
	ordering := func() *query.Order {
		GinkgoHelper()
		o, err := query.CompileOrder(desc, []*syncplanev1.OrderKey{query.Desc("rank")})
		Expect(err).NotTo(HaveOccurred())
		return o
	}

	// `state == open` pins the leading component of both indexes, so the plan is
	// Ordered and the seek applies; only how far it runs differs.
	planFor := func(idx query.Index, ord *query.Order) *query.Plan {
		GinkgoHelper()
		pred, err := query.Compile(desc, query.Eq("state", "open"))
		Expect(err).NotTo(HaveOccurred())
		p := query.PlanQuery([]query.Index{idx}, pred, ord)
		Expect(p).NotTo(BeNil())
		Expect(p.Ordered).To(BeTrue())
		Expect(p.Ranges).To(HaveLen(1))
		return p
	}

	seek := func(idx query.Index, after, before proto.Message) (lo, hi []byte, p *query.Plan) {
		GinkgoHelper()
		ord := ordering()
		p = planFor(idx, ord)
		cursorOf := func(msg proto.Message) []byte {
			if msg == nil {
				return nil
			}
			return ord.Cursor(msg.ProtoReflect(), msg.(*testdatav1.Issue).GetId())
		}
		lo, hi, err := seekRange(p, ord, pageFor(cursorOf(after), cursorOf(before), 0, 10), p.Ranges[0])
		Expect(err).NotTo(HaveOccurred())
		return lo, hi, p
	}

	Describe("an index the ordering spans", func() {
		// [state, rank desc] ends exactly where the ordering does, so a cursor's
		// whole entry is one of its entries.
		narrow := index("by_state_rank", "state", "-rank")

		It("seeks to the boundary row's own entry for an after cursor", func() {
			lo, _, p := seek(narrow, boundary, nil)
			Expect(p.OrderSpansIndex).To(BeTrue())
			// The walk starts on the one position the page still has to reject,
			// rather than at the head of the tie group.
			Expect(lo).To(Equal(entryIn(narrow, boundary)))
			Expect(bytes.Compare(entryIn(narrow, next), lo)).To(Equal(1),
				"the row after the cursor stays inside the range")
		})

		It("ends at the boundary row's own entry for a before cursor", func() {
			_, hi, _ := seek(narrow, nil, boundary)
			// Exclusive, so the boundary row is outside — and so is every row of
			// its tie group that sorts after it.
			Expect(hi).To(Equal(entryIn(narrow, boundary)))
			Expect(bytes.Compare(entryIn(narrow, next), hi)).To(Equal(1))
		})
	})

	Describe("an index carrying a component past the ordering", func() {
		wide := index("by_state_rank_title", "state", "-rank", "title")

		It("stops the after bound before the row key, so the entries still start with it", func() {
			lo, _, p := seek(wide, boundary, nil)
			Expect(p.OrderSpansIndex).To(BeFalse())
			entry := entryIn(wide, boundary)
			Expect(bytes.HasPrefix(entry, lo)).To(BeTrue(),
				"a bound no entry starts with would exclude the whole tie group")
			Expect(len(lo)).To(BeNumerically("<", len(entry)))
			Expect(bytes.Compare(entryIn(wide, next), lo)).To(Equal(1))
		})

		It("keeps the boundary row inside a before bound, for the comparison to reject", func() {
			_, hi, _ := seek(wide, nil, boundary)
			Expect(bytes.Compare(entryIn(wide, boundary), hi)).To(Equal(-1),
				"the tie group at the boundary has to stay inside an exclusive end")
		})
	})
})
