package rebase_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

var _ = Describe("FoldTextDeltas", func() {
	var model *rebase.Model

	BeforeEach(func() {
		model = mustModel(&testdatav1.KeyedTextEntity{})
	})

	long := strings.Repeat("the quick brown fox jumps over the lazy dog ", 6)

	change := func(e *testdatav1.KeyedTextEntity, deltas ...*syncplanev1.FieldDelta) *syncplanev1.RowChange {
		return &syncplanev1.RowChange{
			Entity: "keyed_text", Key: e.GetId(), Version: 2,
			Body:       &syncplanev1.RowChange_Value{Value: deterministic(e)},
			TextDeltas: deltas,
		}
	}
	decode := func(r rebase.Row) *testdatav1.KeyedTextEntity {
		GinkgoHelper()
		Expect(r.Present).To(BeTrue())
		var e testdatav1.KeyedTextEntity
		Expect(proto.Unmarshal(r.Value, &e)).To(Succeed())
		return &e
	}

	It("keeps the confirmed document for an empty fragment and applies a non-empty one", func() {
		body, section := doc("a", long), doc("a", "section")
		confirmed := rebase.Row{Present: true, Value: deterministic(&testdatav1.KeyedTextEntity{
			Id: "k1", Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: body}},
			Sections: map[string]*testdatav1.TextHolder{"s1": {Body: section}},
		})}
		next := edited(section, "b", "!")
		got, err := model.FoldTextDeltas(confirmed, change(&testdatav1.KeyedTextEntity{
			Id: "k1", Title: "moved", Notes: []*testdatav1.KeyedNote{{Id: "n1"}},
			Sections: map[string]*testdatav1.TextHolder{"s1": {}},
		},
			&syncplanev1.FieldDelta{Field: "notes.n1.body"},
			&syncplanev1.FieldDelta{Field: "sections.s1.body", Fragment: asFragment(next, section)},
		))
		Expect(err).NotTo(HaveOccurred())
		e := decode(got)
		Expect(e.GetTitle()).To(Equal("moved"))
		Expect(e.GetNotes()[0].GetBody()).To(Equal(body))
		Expect(e.GetSections()["s1"].GetBody()).To(Equal(next))
	})

	It("applies a self-contained fragment where confirmed holds no document", func() {
		full := doc("a", "fresh")
		got, err := model.FoldTextDeltas(rebase.Row{}, change(&testdatav1.KeyedTextEntity{
			Id: "k1", Notes: []*testdatav1.KeyedNote{{Id: "n1"}},
		}, &syncplanev1.FieldDelta{Field: "notes.n1.body", Fragment: asFragment(full, nil)}))
		Expect(err).NotTo(HaveOccurred())
		Expect(decode(got).GetNotes()[0].GetBody()).To(Equal(full))
	})

	It("passes a change without deltas through and turns a tombstone into an absent row", func() {
		value := []byte{0x0a, 0x02, 'k', '1'}
		got, err := model.FoldTextDeltas(rebase.Row{}, &syncplanev1.RowChange{Body: &syncplanev1.RowChange_Value{Value: value}})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(rebase.Row{Present: true, Value: value}))

		got, err = model.FoldTextDeltas(rebase.Row{Present: true, Value: value},
			&syncplanev1.RowChange{Body: &syncplanev1.RowChange_Tombstone{Tombstone: true}})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Present).To(BeFalse())
	})

	It("reports a delta on a field the schema does not declare as text as an UndeclaredTextError", func() {
		_, err := model.FoldTextDeltas(rebase.Row{}, change(&testdatav1.KeyedTextEntity{Id: "k1"},
			&syncplanev1.FieldDelta{Field: "summary", Fragment: asFragment(doc("a", "x"), nil)}))
		var undeclared *rebase.UndeclaredTextError
		Expect(errors.As(err, &undeclared)).To(BeTrue())
		Expect(*undeclared).To(Equal(rebase.UndeclaredTextError{Entity: "keyed_text", Key: "k1", Field: "summary"}))
		Expect(err).To(MatchError(rebase.ErrTextDeltaUnusable))

		_, err = model.FoldTextDeltas(rebase.Row{}, change(&testdatav1.KeyedTextEntity{Id: "k1"},
			&syncplanev1.FieldDelta{Field: "notes.n9.body", Fragment: asFragment(doc("a", "x"), nil)}))
		Expect(errors.As(err, &undeclared)).To(BeFalse(), "a declared field at a position the value lacks")
	})

	DescribeTable("reports delta state it cannot fold",
		func(confirmed func() rebase.Row, delta func() *syncplanev1.FieldDelta) {
			_, err := model.FoldTextDeltas(confirmed(), change(&testdatav1.KeyedTextEntity{
				Id: "k1", Notes: []*testdatav1.KeyedNote{{Id: "n1"}},
			}, delta()))
			Expect(err).To(MatchError(rebase.ErrTextDeltaUnusable))
		},
		Entry("an empty fragment with no confirmed document",
			func() rebase.Row { return rebase.Row{} },
			func() *syncplanev1.FieldDelta { return &syncplanev1.FieldDelta{Field: "notes.n1.body"} }),
		Entry("a fragment built on text confirmed lacks",
			func() rebase.Row {
				return rebase.Row{Present: true, Value: deterministic(&testdatav1.KeyedTextEntity{
					Id: "k1", Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: doc("a", "x")}},
				})}
			},
			func() *syncplanev1.FieldDelta {
				other := doc("z", "unrelated")
				return &syncplanev1.FieldDelta{Field: "notes.n1.body", Fragment: asFragment(edited(other, "y", "!"), other)}
			}),
		Entry("a position the row value does not hold",
			func() rebase.Row { return rebase.Row{} },
			func() *syncplanev1.FieldDelta {
				return &syncplanev1.FieldDelta{Field: "notes.n9.body", Fragment: asFragment(doc("a", "x"), nil)}
			}),
		Entry("damaged fragment bytes",
			func() rebase.Row { return rebase.Row{} },
			func() *syncplanev1.FieldDelta {
				return &syncplanev1.FieldDelta{Field: "notes.n1.body", Fragment: []byte{0x02, 0xff, 0xff}}
			}),
		Entry("a confirmed row that does not decode",
			func() rebase.Row { return rebase.Row{Present: true, Value: []byte{0xff}} },
			func() *syncplanev1.FieldDelta {
				return &syncplanev1.FieldDelta{Field: "notes.n1.body", Fragment: asFragment(doc("a", "x"), nil)}
			}),
	)

	// The engine's own pulls are the ground truth: a reader folding every
	// page into what it holds ends up with the rows the engine stores. This
	// also pins that the engine and the Model name documents by the same
	// instance paths.
	It("reproduces the rows an engine ships as delta state", func() {
		origin := newNode()
		rng := rand.New(rand.NewPCG(uint64(GinkgoRandomSeed()), 7))
		held := map[string]rebase.Row{}
		var cursor []byte
		var emptyFragments, fragments int
		keys := []string{"k1", "k2"}

		catchUp := func() {
			GinkgoHelper()
			for {
				resp := origin.pull(cursor)
				for _, rc := range resp.GetPatch().GetChanges() {
					Expect(rc.GetEntity()).To(Equal("keyed_text"))
					for _, d := range rc.GetTextDeltas() {
						if len(d.GetFragment()) == 0 {
							emptyFragments++
						} else {
							fragments++
						}
					}
					got, err := model.FoldTextDeltas(held[rc.GetKey()], rc)
					Expect(err).NotTo(HaveOccurred())
					held[rc.GetKey()] = got
				}
				cursor = resp.GetNextCursor()
				if !resp.GetHasMore() {
					break
				}
			}
			for _, key := range keys {
				want := origin.row(&testdatav1.KeyedTextEntity{}, key)
				Expect(model.Equal(held[key], want)).To(BeTrue(), "row %s\nheld:\n%s\nengine:\n%s",
					key, describe(&testdatav1.KeyedTextEntity{}, held[key]), describe(&testdatav1.KeyedTextEntity{}, want))
			}
		}

		seed := func(key string) *testdatav1.KeyedTextEntity {
			return &testdatav1.KeyedTextEntity{
				Id: key,
				Notes: []*testdatav1.KeyedNote{
					{Id: "n1", Body: doc("s", long), Deep: &testdatav1.TextHolder{Body: doc("s", long)}},
					{Id: "n.2", Body: doc("s", long)},
				},
				Sections: map[string]*testdatav1.TextHolder{"s1": {Body: doc("s", long)}, "s/2": {Body: doc("s", long)}},
			}
		}
		for _, key := range keys {
			Expect(origin.push("w", crud(syncplanev1.CrudOp_CRUD_OP_PUT, "keyed_text", key, seed(key)))).To(BeTrue())
		}
		catchUp()

		// A write that moves no document ships every document as an empty
		// fragment, so the paths the engine names are all of them.
		Expect(origin.push("w", crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "keyed_text", "k1",
			&testdatav1.KeyedTextEntity{Title: "renamed"}, "title"))).To(BeTrue())
		resp := origin.pull(cursor)
		Expect(resp.GetPatch().GetChanges()).To(HaveLen(1))
		var named []string
		for _, d := range resp.GetPatch().GetChanges()[0].GetTextDeltas() {
			named = append(named, d.GetField())
		}
		docs, err := model.TextDocuments(origin.row(&testdatav1.KeyedTextEntity{}, "k1").Value)
		Expect(err).NotTo(HaveOccurred())
		var paths []string
		for p := range docs {
			paths = append(paths, p)
		}
		slices.Sort(named)
		slices.Sort(paths)
		Expect(named).To(Equal(paths))
		Expect(paths).To(ContainElement(`notes.n\.2.body`))
		catchUp()

		for round := 0; round < 25; round++ {
			key := keys[rng.IntN(len(keys))]
			cur := origin.row(&testdatav1.KeyedTextEntity{}, key)
			var e testdatav1.KeyedTextEntity
			Expect(proto.Unmarshal(cur.Value, &e)).To(Succeed())
			var mask []string
			patchValue := &testdatav1.KeyedTextEntity{Title: fmt.Sprint("t", round)}
			switch rng.IntN(4) {
			case 0:
				mask = []string{"title"}
			case 1:
				mask = []string{"notes.n1.body"}
				patchValue.Notes = []*testdatav1.KeyedNote{{Id: "n1", Body: edited(e.GetNotes()[0].GetBody(), "w", "!")}}
			case 2:
				mask = []string{`sections.s/2.body`, "title"}
				patchValue.Sections = map[string]*testdatav1.TextHolder{"s/2": {Body: edited(e.GetSections()["s/2"].GetBody(), "w", "?")}}
			case 3:
				mask = []string{"notes.n1.deep.body"}
				patchValue.Notes = []*testdatav1.KeyedNote{{Id: "n1", Deep: &testdatav1.TextHolder{
					Body: edited(e.GetNotes()[0].GetDeep().GetBody(), "w", "."),
				}}}
			}
			Expect(origin.push("w", crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "keyed_text", key, patchValue, mask...))).To(BeTrue())
			if rng.IntN(2) == 0 {
				catchUp()
			}
		}
		catchUp()
		Expect(emptyFragments).To(BeNumerically(">", 0))
		Expect(fragments).To(BeNumerically(">", 0))
	})
})
