package lww_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// Per-item merge: a mask naming a sub-path under a map/list field merges that one
// item instead of replacing the whole collection. Mirrors merge.test.ts.
var _ = Describe("Per-item merge", func() {
	Describe("maps", func() {
		It("adds a map entry without disturbing others", func() {
			dst := &testdatav1.Widget{Attrs: map[string]string{"a": "1", "b": "2"}}
			src := &testdatav1.Widget{Attrs: map[string]string{"c": "3"}}
			Expect(lww.Merge(dst, src, []string{"attrs.c"})).To(Succeed())
			Expect(dst.GetAttrs()).To(Equal(map[string]string{"a": "1", "b": "2", "c": "3"}))
		})

		It("replaces a single map entry's value", func() {
			dst := &testdatav1.Widget{Attrs: map[string]string{"a": "1", "b": "2"}}
			src := &testdatav1.Widget{Attrs: map[string]string{"a": "9"}}
			Expect(lww.Merge(dst, src, []string{"attrs.a"})).To(Succeed())
			Expect(dst.GetAttrs()).To(Equal(map[string]string{"a": "9", "b": "2"}))
		})

		It("clears a map entry the source lacks (clear-on-absent)", func() {
			dst := &testdatav1.Widget{Attrs: map[string]string{"a": "1", "b": "2"}}
			src := &testdatav1.Widget{}
			Expect(lww.Merge(dst, src, []string{"attrs.a"})).To(Succeed())
			Expect(dst.GetAttrs()).To(Equal(map[string]string{"b": "2"}))
		})

		It("handles non-string (int64) map keys", func() {
			dst := &testdatav1.Widget{ById: map[int64]string{1: "one"}}
			src := &testdatav1.Widget{ById: map[int64]string{2: "two"}}
			Expect(lww.Merge(dst, src, []string{"by_id.2"})).To(Succeed())
			Expect(dst.GetById()).To(Equal(map[int64]string{1: "one", 2: "two"}))
		})

		It("round-trips a map key containing dots and backslashes", func() {
			dst := &testdatav1.Widget{Attrs: map[string]string{"x": "0"}}
			src := &testdatav1.Widget{Attrs: map[string]string{`a.b\c`: "1"}}
			// "." and "\" in the key are escaped in the mask segment.
			Expect(lww.Merge(dst, src, []string{`attrs.a\.b\\c`})).To(Succeed())
			Expect(dst.GetAttrs()).To(Equal(map[string]string{"x": "0", `a.b\c`: "1"}))
		})
	})

	Describe("keyed message lists", func() {
		It("adds a new keyed element", func() {
			dst := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "u1", Name: "A"}}}
			src := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "u2", Name: "B"}}}
			Expect(lww.Merge(dst, src, []string{"members.u2"})).To(Succeed())
			Expect(dst.GetMembers()).To(HaveLen(2))
			Expect(dst.GetMembers()[1].GetUid()).To(Equal("u2"))
		})

		It("overwrites an existing element in place (preserving its index)", func() {
			dst := &testdatav1.Widget{Members: []*testdatav1.Member{
				{Uid: "u1", Name: "A"}, {Uid: "u2", Name: "B"},
			}}
			src := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "u1", Name: "A2", Role: 5}}}
			Expect(lww.Merge(dst, src, []string{"members.u1"})).To(Succeed())
			Expect(dst.GetMembers()).To(HaveLen(2))
			Expect(dst.GetMembers()[0].GetName()).To(Equal("A2"))
			Expect(dst.GetMembers()[0].GetRole()).To(BeEquivalentTo(5))
			Expect(dst.GetMembers()[1].GetUid()).To(Equal("u2"))
		})

		It("removes an element the source dropped", func() {
			dst := &testdatav1.Widget{Members: []*testdatav1.Member{
				{Uid: "u1", Name: "A"}, {Uid: "u2", Name: "B"},
			}}
			src := &testdatav1.Widget{}
			Expect(lww.Merge(dst, src, []string{"members.u1"})).To(Succeed())
			Expect(dst.GetMembers()).To(HaveLen(1))
			Expect(dst.GetMembers()[0].GetUid()).To(Equal("u2"))
		})

		It("merges a sub-field of one element without touching siblings", func() {
			dst := &testdatav1.Widget{Members: []*testdatav1.Member{
				{Uid: "u1", Name: "A", Role: 1},
			}}
			src := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "u1", Name: "renamed"}}}
			Expect(lww.Merge(dst, src, []string{"members.u1.name"})).To(Succeed())
			Expect(dst.GetMembers()[0].GetName()).To(Equal("renamed"))
			Expect(dst.GetMembers()[0].GetRole()).To(BeEquivalentTo(1), "unmasked sub-field survives")
		})

		It("creates + seeds the key field when descending into an absent element", func() {
			dst := &testdatav1.Widget{}
			src := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "u9", Name: "new"}}}
			Expect(lww.Merge(dst, src, []string{"members.u9.name"})).To(Succeed())
			Expect(dst.GetMembers()).To(HaveLen(1))
			Expect(dst.GetMembers()[0].GetUid()).To(Equal("u9"), "key field seeded")
			Expect(dst.GetMembers()[0].GetName()).To(Equal("new"))
		})

		It("infers identity from the element entity's key_fields", func() {
			dst := &testdatav1.Widget{Refs: []*testdatav1.Pair{{Ns: "n", Id: "1", Label: "L1"}}}
			src := &testdatav1.Widget{Refs: []*testdatav1.Pair{{Ns: "n", Id: "2", Label: "L2"}}}
			// Composite key "n/2".
			Expect(lww.Merge(dst, src, []string{"refs.n/2"})).To(Succeed())
			Expect(dst.GetRefs()).To(HaveLen(2))
			Expect(dst.GetRefs()[1].GetId()).To(Equal("2"))
		})

		Describe("DuplicateItemKey", func() {
			It("finds a key held by two elements, by its instance path", func() {
				w := &testdatav1.Widget{Members: []*testdatav1.Member{
					{Uid: "u1", Name: "first"}, {Uid: "u2"}, {Uid: "u1", Name: "second"},
				}}
				path, found := lww.DuplicateItemKey(w.ProtoReflect())
				Expect(found).To(BeTrue())
				Expect(path).To(Equal("members.u1"))
			})

			It("finds a key held twice by elements that differ in other fields", func() {
				w := &testdatav1.Widget{Items: []*testdatav1.Item{{Id: "a", Pos: "1"}, {Id: "a", Pos: "2"}}}
				path, found := lww.DuplicateItemKey(w.ProtoReflect())
				Expect(found).To(BeTrue())
				Expect(path).To(Equal("items.a"))
			})

			It("ignores a list of references that names one row twice", func() {
				w := &testdatav1.Widget{Refs: []*testdatav1.Pair{{Ns: "n", Id: "1"}, {Ns: "n", Id: "1"}}}
				_, found := lww.DuplicateItemKey(w.ProtoReflect())
				Expect(found).To(BeFalse())
			})

			It("accepts keyed lists that hold each key once", func() {
				w := &testdatav1.Widget{
					Members: []*testdatav1.Member{{Uid: "u1"}, {Uid: "u2"}, {Uid: ""}},
					Items:   []*testdatav1.Item{{Id: "a"}, {Id: "b"}},
				}
				_, found := lww.DuplicateItemKey(w.ProtoReflect())
				Expect(found).To(BeFalse())
			})

			It("ignores repeated values in lists a mask cannot address per element", func() {
				w := &testdatav1.Widget{Tags: []string{"a", "a"}, AtomicTags: []string{"b", "b"}}
				_, found := lww.DuplicateItemKey(w.ProtoReflect())
				Expect(found).To(BeFalse())
			})

			It("reports that the widget can hold a keyed list", func() {
				Expect(lww.HoldsKeyedList((&testdatav1.Widget{}).ProtoReflect().Descriptor())).To(BeTrue())
				Expect(lww.HoldsKeyedList((&testdatav1.Member{}).ProtoReflect().Descriptor())).To(BeFalse())
			})
		})
	})

	Describe("scalar set lists", func() {
		It("adds a value that is not already present", func() {
			dst := &testdatav1.Widget{Tags: []string{"a", "b"}}
			src := &testdatav1.Widget{Tags: []string{"c"}}
			Expect(lww.Merge(dst, src, []string{"tags.c"})).To(Succeed())
			Expect(dst.GetTags()).To(Equal([]string{"a", "b", "c"}))
		})

		It("is idempotent when the value already exists (no dup)", func() {
			dst := &testdatav1.Widget{Tags: []string{"a", "b"}}
			src := &testdatav1.Widget{Tags: []string{"a"}}
			Expect(lww.Merge(dst, src, []string{"tags.a"})).To(Succeed())
			Expect(dst.GetTags()).To(Equal([]string{"a", "b"}))
		})

		It("removes every occurrence of a value the source lacks", func() {
			dst := &testdatav1.Widget{Tags: []string{"a", "b", "a"}}
			src := &testdatav1.Widget{}
			Expect(lww.Merge(dst, src, []string{"tags.a"})).To(Succeed())
			Expect(dst.GetTags()).To(Equal([]string{"b"}))
		})
	})

	// The empty key encodes as the empty segment, so its item path ends in the
	// separator ("attrs."). That is not the bare field ("attrs").
	Describe("the empty item key", func() {
		It("sets the map entry with the empty key without disturbing others", func() {
			dst := &testdatav1.Widget{Attrs: map[string]string{"k": "new"}}
			src := &testdatav1.Widget{Attrs: map[string]string{"k": "old", "": "x"}}
			Expect(lww.Merge(dst, src, []string{"attrs."})).To(Succeed())
			Expect(dst.GetAttrs()).To(Equal(map[string]string{"k": "new", "": "x"}))
		})

		It("clears the map entry with the empty key when the source lacks it", func() {
			dst := &testdatav1.Widget{Attrs: map[string]string{"k": "v", "": "x"}}
			src := &testdatav1.Widget{Attrs: map[string]string{"k": "stale"}}
			Expect(lww.Merge(dst, src, []string{"attrs."})).To(Succeed())
			Expect(dst.GetAttrs()).To(Equal(map[string]string{"k": "v"}))
		})

		It("refuses the empty key on a map whose keys are integers", func() {
			dst := &testdatav1.Widget{ById: map[int64]string{1: "one"}}
			src := &testdatav1.Widget{ById: map[int64]string{2: "two"}}
			Expect(lww.Merge(dst, src, []string{"by_id."})).To(MatchError(ContainSubstring("invalid int64 map key")))
		})

		It("adds the empty-string element to a scalar set without disturbing others", func() {
			dst := &testdatav1.Widget{Tags: []string{"a", "b"}}
			src := &testdatav1.Widget{Tags: []string{"a", ""}}
			Expect(lww.Merge(dst, src, []string{"tags."})).To(Succeed())
			Expect(dst.GetTags()).To(Equal([]string{"a", "b", ""}))
		})

		It("removes the empty-string element from a scalar set when the source lacks it", func() {
			dst := &testdatav1.Widget{Tags: []string{"a", "", "b"}}
			src := &testdatav1.Widget{Tags: []string{"a"}}
			Expect(lww.Merge(dst, src, []string{"tags."})).To(Succeed())
			Expect(dst.GetTags()).To(Equal([]string{"a", "b"}))
		})

		It("replaces the keyed-list element with the empty key without disturbing others", func() {
			dst := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "u1", Role: 2}, {Uid: "", Role: 1}}}
			src := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "u1", Role: 1}, {Uid: "", Role: 3}}}
			Expect(lww.Merge(dst, src, []string{"members."})).To(Succeed())
			Expect(dst.GetMembers()).To(HaveLen(2))
			Expect(dst.GetMembers()[0].GetRole()).To(BeEquivalentTo(2))
			Expect(dst.GetMembers()[1].GetRole()).To(BeEquivalentTo(3))
		})

		It("descends into the keyed-list element with the empty key", func() {
			dst := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "", Name: "A", Role: 1}}}
			src := &testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "", Name: "B", Role: 9}}}
			Expect(lww.Merge(dst, src, []string{"members..name"})).To(Succeed())
			Expect(dst.GetMembers()).To(HaveLen(1))
			Expect(dst.GetMembers()[0].GetName()).To(Equal("B"))
			Expect(dst.GetMembers()[0].GetRole()).To(BeEquivalentTo(1))
		})
	})

	Describe("opaque + bare collection paths", func() {
		It("replaces an opaque list whole at a bare path", func() {
			dst := &testdatav1.Widget{AtomicTags: []string{"a", "b"}}
			src := &testdatav1.Widget{AtomicTags: []string{"x"}}
			Expect(lww.Merge(dst, src, []string{"atomic_tags"})).To(Succeed())
			Expect(dst.GetAtomicTags()).To(Equal([]string{"x"}))
		})

		It("replaces an opaque list whole even under a sub-path", func() {
			dst := &testdatav1.Widget{AtomicTags: []string{"a", "b"}}
			src := &testdatav1.Widget{AtomicTags: []string{"x"}}
			Expect(lww.Merge(dst, src, []string{"atomic_tags.x"})).To(Succeed())
			Expect(dst.GetAtomicTags()).To(Equal([]string{"x"}))
		})

		It("replaces a non-opaque list whole at a bare path", func() {
			dst := &testdatav1.Widget{Tags: []string{"a", "b"}}
			src := &testdatav1.Widget{Tags: []string{"x"}}
			Expect(lww.Merge(dst, src, []string{"tags"})).To(Succeed())
			Expect(dst.GetTags()).To(Equal([]string{"x"}))
		})

		It("replaces a map whole at a bare path", func() {
			dst := &testdatav1.Widget{Attrs: map[string]string{"a": "1", "b": "2"}}
			src := &testdatav1.Widget{Attrs: map[string]string{"c": "3"}}
			Expect(lww.Merge(dst, src, []string{"attrs"})).To(Succeed())
			Expect(dst.GetAttrs()).To(Equal(map[string]string{"c": "3"}))
		})
	})

	Describe("movable list pos field (LWW)", func() {
		It("treats a keyed list's order_field as an ordinary LWW sub-field", func() {
			dst := &testdatav1.Widget{Items: []*testdatav1.Item{{Id: "i1", Pos: "a0", Text: "hi"}}}
			src := &testdatav1.Widget{Items: []*testdatav1.Item{{Id: "i1", Pos: "a5"}}}
			Expect(lww.Merge(dst, src, []string{"items.i1.pos"})).To(Succeed())
			Expect(dst.GetItems()[0].GetPos()).To(Equal("a5"))
			Expect(dst.GetItems()[0].GetText()).To(Equal("hi"), "moving an item keeps its other fields")
		})
	})
})
