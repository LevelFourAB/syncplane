package query_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/query"
)

// Shared with the TS encoder (client/test/unit/indexkey-vectors.test.ts) —
// both must produce byte-identical entries, and both must agree with their own
// row comparator about the order those entries imply.
const indexVectorsPath = "../../testdata/indexkey/vectors.json"

type indexSpecVector struct {
	Name string `json:"name"`
	Key  []struct {
		Path []string `json:"path"`
		Desc bool     `json:"desc"`
	} `json:"key"`
}

type encodeCase struct {
	Name   string          `json:"name"`
	Type   string          `json:"type"`
	Row    json.RawMessage `json:"row"`
	RowKey string          `json:"rowKey"`
	Spec   indexSpecVector `json:"spec"`
	Want   []string        `json:"want"`
}

type indexOrderCase struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Rows []struct {
		Key string          `json:"key"`
		Row json.RawMessage `json:"row"`
	} `json:"rows"`
	Spec indexSpecVector `json:"spec"`
	Want []string        `json:"want"`
}

// cursorCase pins a query cursor: the bytes, the shape, and the two properties
// after/before rest on. Unlike an index entry a cursor crosses the wire, so the
// TS loader (client/test/unit/indexkey-vectors.test.ts) asserts the same
// literals against the same rows.
type cursorCase struct {
	Name    string    `json:"name"`
	Type    string    `json:"type"`
	Entity  string    `json:"entity"`
	OrderBy []struct {
		Path []string `json:"path"`
		Desc bool     `json:"desc"`
	} `json:"orderBy"`
	Rows []struct {
		Key string          `json:"key"`
		Row json.RawMessage `json:"row"`
	} `json:"rows"`
	Shape   string            `json:"shape"`
	Cursors map[string]string `json:"cursors"`
	Want    []string          `json:"want"`
}

type indexVectorFile struct {
	EncodeCases []encodeCase     `json:"encodeCases"`
	OrderCases  []indexOrderCase `json:"orderCases"`
	CursorCases []cursorCase     `json:"cursorCases"`
}

func loadIndexVectors() indexVectorFile {
	raw, err := os.ReadFile(indexVectorsPath)
	Expect(err).NotTo(HaveOccurred())
	var v indexVectorFile
	Expect(json.Unmarshal(raw, &v)).To(Succeed())
	return v
}

// encoderFor compiles a vector's spec against the row type it applies to.
func encoderFor(spec indexSpecVector, typ string) *query.IndexEncoder {
	desc := msgForType(typ).ProtoReflect().Descriptor()
	comps := make([]query.IndexComponent, 0, len(spec.Key))
	for _, k := range spec.Key {
		c, err := query.CompileIndexComponent(desc, k.Path, k.Desc)
		Expect(err).NotTo(HaveOccurred())
		comps = append(comps, c)
	}
	enc, err := query.NewIndexEncoder(comps)
	Expect(err).NotTo(HaveOccurred())
	return enc
}

func hexEntries(entries [][]byte) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, hex.EncodeToString(e))
	}
	return out
}

// orderFor is the SAME spec expressed as an ordering, which is what the
// encoding has to agree with.
func orderFor(spec indexSpecVector, typ string) *query.Order {
	keys := make([]*syncplanev1.OrderKey, 0, len(spec.Key))
	for _, k := range spec.Key {
		dir := syncplanev1.Direction_DIRECTION_ASC
		if k.Desc {
			dir = syncplanev1.Direction_DIRECTION_DESC
		}
		keys = append(keys, &syncplanev1.OrderKey{Path: k.Path, Dir: dir})
	}
	o, err := query.CompileOrder(msgForType(typ).ProtoReflect().Descriptor(), keys)
	Expect(err).NotTo(HaveOccurred())
	return o
}

var _ = Describe("Index key conformance vectors", func() {
	vectors := loadIndexVectors()

	It("has cases", func() {
		Expect(vectors.EncodeCases).NotTo(BeEmpty())
		Expect(vectors.OrderCases).NotTo(BeEmpty())
		Expect(vectors.CursorCases).NotTo(BeEmpty())
	})

	Describe("Encode", func() {
		for _, tc := range vectors.EncodeCases {
			It(tc.Name, func() {
				msg := msgForType(tc.Type)
				Expect(protojson.Unmarshal(tc.Row, msg)).To(Succeed())
				got := encoderFor(tc.Spec, tc.Type).Encode(msg.ProtoReflect(), tc.RowKey)
				Expect(hexEntries(got)).To(Equal(tc.Want))
			})
		}
	})

	Describe("Order", func() {
		for _, tc := range vectors.OrderCases {
			It(tc.Name, func() {
				enc := encoderFor(tc.Spec, tc.Type)
				ord := orderFor(tc.Spec, tc.Type)

				type row struct {
					key   string
					msg   proto.Message
					entry []byte
				}
				rows := make([]row, 0, len(tc.Rows))
				for _, r := range tc.Rows {
					msg := msgForType(tc.Type)
					Expect(protojson.Unmarshal(r.Row, msg)).To(Succeed())
					entries := enc.Encode(msg.ProtoReflect(), r.Key)
					// An ordering has no meaning over a fan-out component, so
					// every order vector's spec is single-entry.
					Expect(entries).To(HaveLen(1))
					rows = append(rows, row{key: r.Key, msg: msg, entry: entries[0]})
				}

				byEntry := append([]row(nil), rows...)
				sort.SliceStable(byEntry, func(i, j int) bool {
					return string(byEntry[i].entry) < string(byEntry[j].entry)
				})
				Expect(keysOf(byEntry, func(r row) string { return r.key })).To(Equal(tc.Want))

				// The encoding is only useful if it agrees with the comparator
				// it replaces, so the two are asserted against each other and
				// not merely against the same literal.
				byCompare := append([]row(nil), rows...)
				sort.SliceStable(byCompare, func(i, j int) bool {
					return ord.Less(
						byCompare[i].msg.ProtoReflect(), byCompare[j].msg.ProtoReflect(),
						byCompare[i].key, byCompare[j].key)
				})
				Expect(keysOf(byCompare, func(r row) string { return r.key })).
					To(Equal(keysOf(byEntry, func(r row) string { return r.key })))
			})
		}
	})

	Describe("Cursor", func() {
		for _, tc := range vectors.CursorCases {
			It(tc.Name, func() {
				keys := make([]*syncplanev1.OrderKey, 0, len(tc.OrderBy))
				for _, k := range tc.OrderBy {
					dir := syncplanev1.Direction_DIRECTION_ASC
					if k.Desc {
						dir = syncplanev1.Direction_DIRECTION_DESC
					}
					keys = append(keys, &syncplanev1.OrderKey{Path: k.Path, Dir: dir})
				}
				ord, err := query.CompileOrder(msgForType(tc.Type).ProtoReflect().Descriptor(), keys)
				Expect(err).NotTo(HaveOccurred())
				Expect(ord.Shape(tc.Entity)).To(Equal(tc.Shape))

				type row struct {
					key   string
					msg   proto.Message
					entry []byte
				}
				rows := make([]row, 0, len(tc.Rows))
				for _, r := range tc.Rows {
					msg := msgForType(tc.Type)
					Expect(protojson.Unmarshal(r.Row, msg)).To(Succeed())
					Expect(hex.EncodeToString(ord.EncodeCursor(tc.Entity, msg.ProtoReflect(), r.Key))).
						To(Equal(tc.Cursors[r.Key]), "cursor for %s", r.Key)
					rows = append(rows, row{key: r.Key, msg: msg, entry: ord.Cursor(msg.ProtoReflect(), r.Key)})
				}

				byCursor := append([]row(nil), rows...)
				sort.SliceStable(byCursor, func(i, j int) bool {
					return bytes.Compare(byCursor[i].entry, byCursor[j].entry) < 0
				})
				Expect(keysOf(byCursor, func(r row) string { return r.key })).To(Equal(tc.Want))

				// after/before are a byte comparison against one row's cursor, so
				// each has to select exactly what the comparator ranks that way.
				for _, pivot := range byCursor {
					var after, before []string
					// Built from the sorted order so the comparison is over the
					// SET as well as its order, which is what after/before select.
					for _, r := range byCursor {
						switch c := ord.Compare(
							r.msg.ProtoReflect(), pivot.msg.ProtoReflect(), r.key, pivot.key,
						); {
						case c > 0:
							after = append(after, r.key)
						case c < 0:
							before = append(before, r.key)
						}
					}
					Expect(selectBy(byCursor, func(r row) bool {
						return bytes.Compare(r.entry, pivot.entry) > 0
					}, func(r row) string { return r.key })).To(Equal(after), "after %s", pivot.key)
					Expect(selectBy(byCursor, func(r row) bool {
						return bytes.Compare(r.entry, pivot.entry) < 0
					}, func(r row) string { return r.key })).To(Equal(before), "before %s", pivot.key)
				}
			})
		}
	})
})

// selectBy is keysOf over the rows keep admits, preserving their order.
func selectBy[T any](rows []T, keep func(T) bool, key func(T) string) []string {
	var out []string
	for _, r := range rows {
		if keep(r) {
			out = append(out, key(r))
		}
	}
	return out
}

func keysOf[T any](rows []T, key func(T) string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, key(r))
	}
	return out
}

var _ = Describe("CompileIndexComponent", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()

	It("rejects a collaborative text field", func() {
		_, err := query.CompileIndexComponent(widget, []string{"body"}, false)
		Expect(err).To(MatchError(ContainSubstring("collaborative text field cannot be indexed")))
	})

	It("points a bare map at the fan-out syntax", func() {
		// An index component names a SHAPE, so a map means "every entry" and has
		// to say so; a literal key would declare an index over one entry, which
		// is a different feature.
		_, err := query.CompileIndexComponent(widget, []string{"attrs"}, false)
		Expect(err).To(MatchError(ContainSubstring(`must be followed by "*"`)))
		Expect(err).To(MatchError(ContainSubstring("attrs.*.key or attrs.*.value.<field>")))
	})

	It("rejects a literal map key in an index path", func() {
		_, err := query.CompileIndexComponent(widget, []string{"attrs", "color"}, false)
		Expect(err).To(MatchError(ContainSubstring(`must be followed by "*"`)))
	})

	It("rejects a bare trailing `*`, which does not say which side of the entry it means", func() {
		// map<string, string>: both sides are strings, so a default would be a
		// coin flip with no error to notice — which is why the selector is
		// required rather than inferred.
		_, err := query.CompileIndexComponent(widget, []string{"attrs", "*"}, false)
		Expect(err).To(MatchError(ContainSubstring(`must be followed by "key" or "value"`)))
	})

	It("rejects an unknown selector after `*`", func() {
		_, err := query.CompileIndexComponent(widget, []string{"attrs", "*", "entry"}, false)
		Expect(err).To(MatchError(ContainSubstring(`"entry" after "*" must be "key" or "value"`)))
	})

	It("indexes the entry key of a map", func() {
		c, err := query.CompileIndexComponent(widget, []string{"attrs", "*", "key"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Multi()).To(BeTrue())
	})

	It("indexes the entry value of a scalar-valued map", func() {
		c, err := query.CompileIndexComponent(widget, []string{"attrs", "*", "value"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Multi()).To(BeTrue())
	})

	It("rejects a path continuing past the entry key, which is a scalar", func() {
		_, err := query.CompileIndexComponent(widget, []string{"attrs", "*", "key", "nope"}, false)
		Expect(err).To(MatchError(ContainSubstring(`"key" is the entry key`)))
	})

	It("rejects descending into the value of a scalar-valued map", func() {
		_, err := query.CompileIndexComponent(widget, []string{"attrs", "*", "value", "nope"}, false)
		Expect(err).To(MatchError(ContainSubstring("cannot be descended into")))
	})

	It("points a component ending at a message-valued entry at a field inside it", func() {
		_, err := query.CompileIndexComponent(widget, []string{"custom", "*", "value"}, false)
		Expect(err).To(MatchError(ContainSubstring("custom.*.value.<field>")))
	})

	It("rejects `*` after a field that is not a map", func() {
		_, err := query.CompileIndexComponent(widget, []string{"tags", "*"}, false)
		Expect(err).To(MatchError(ContainSubstring(`"tags" is not a message field`)))
	})

	It("descends into a message-valued entry, and on through a list inside it", func() {
		c, err := query.CompileIndexComponent(
			widget, []string{"custom", "*", "value", "labels", "values"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Multi()).To(BeTrue())
	})

	It("rejects an unknown field", func() {
		_, err := query.CompileIndexComponent(widget, []string{"nope"}, false)
		Expect(err).To(MatchError(ContainSubstring(`has no field "nope"`)))
	})

	It("accepts a repeated scalar, which fans out over its elements", func() {
		c, err := query.CompileIndexComponent(widget, []string{"tags"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Multi()).To(BeTrue())
	})

	It("accepts a path through a repeated message field", func() {
		c, err := query.CompileIndexComponent(widget, []string{"members", "uid"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Multi()).To(BeTrue())
	})

	It("points a component ending at a message list at a field of the elements", func() {
		_, err := query.CompileIndexComponent(widget, []string{"members"}, false)
		Expect(err).To(MatchError(ContainSubstring("index a scalar field of the elements")))
	})

	It("rejects descending through a to-many reference into a non-key field", func() {
		issue := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
		_, err := query.CompileIndexComponent(issue, []string{"blocked_by", "title"}, false)
		Expect(err).To(MatchError(ContainSubstring("only its key fields are addressable")))
	})

	It("rejects two repeated fields on one component", func() {
		_, err := query.CompileIndexComponent(widget, []string{"members", "tags"}, false)
		Expect(err).To(MatchError(ContainSubstring("second repeated field")))
	})
})

var _ = Describe("IndexComponent.MaskTouches", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()
	issue := (&testdatav1.Issue{}).ProtoReflect().Descriptor()

	// touches compiles a component and asks whether a mask path — split the way
	// lww splits one — could move a row in it.
	touches := func(desc protoreflect.MessageDescriptor, path []string, mask string) bool {
		GinkgoHelper()
		c, err := query.CompileIndexComponent(desc, path, false)
		Expect(err).NotTo(HaveOccurred())
		return c.MaskTouches(lww.SplitPath(mask))
	}

	It("says no for a mask that diverges at the first segment", func() {
		Expect(touches(widget, []string{"label"}, "count")).To(BeFalse())
	})

	It("says no for a mask that diverges below a shared prefix", func() {
		Expect(touches(widget, []string{"settings", "color"}, "settings.size")).To(BeFalse())
	})

	It("says yes for the component's own path", func() {
		Expect(touches(widget, []string{"label"}, "label")).To(BeTrue())
	})

	It("says yes for a mask that stops above the component", func() {
		// "settings" replaces the whole submessage, colour included.
		Expect(touches(widget, []string{"settings", "color"}, "settings")).To(BeTrue())
	})

	It("says yes for a mask that descends past the component's leaf", func() {
		// Nothing well-formed produces this — a leaf a component can read has no
		// sub-path — so it is the defensive half of the prefix rule: a mask this
		// code cannot account for rewrites rather than skips.
		Expect(touches(issue, []string{"project"}, "project.id.deeper")).To(BeTrue())
	})

	It("says yes for a mask naming the oneof group a member belongs to", func() {
		// A oneof is merged whole, by GROUP name, so a mask never spells the
		// member the component reads — the one case a bare name comparison gets
		// wrong, and the reason this walks descriptors rather than strings.
		Expect(touches(widget, []string{"choice_number"}, "choice")).To(BeTrue())
		Expect(touches(widget, []string{"choice_number"}, "label")).To(BeFalse())
	})

	It("says yes for a mask addressing one item of a collection", func() {
		// "tags.red" writes one element of the set; the segment after a repeated
		// field is an item key, not a field name.
		Expect(touches(widget, []string{"tags"}, "tags.red")).To(BeTrue())
		Expect(touches(widget, []string{"members", "uid"}, "members.u1")).To(BeTrue())
		Expect(touches(widget, []string{"members", "uid"}, "members.u1.name")).To(BeTrue())
	})

	It("is not fooled by an escaped dot inside an item key", func() {
		// One segment, "a.b", not two — the split has to match the merge's or
		// the comparison is against a path the merge never walks.
		Expect(touches(widget, []string{"label"}, `attrs.a\.b`)).To(BeFalse())
	})

	It("follows the key field a reference component is extended with", func() {
		// The component compiled from "project" resolves to project.id, so a
		// mask naming the reference itself still reaches it.
		Expect(touches(issue, []string{"project"}, "project")).To(BeTrue())
		Expect(touches(issue, []string{"project"}, "project.id")).To(BeTrue())
		Expect(touches(issue, []string{"project"}, "title")).To(BeFalse())
	})
})

var _ = Describe("NewIndexEncoder", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()

	comp := func(path ...string) query.IndexComponent {
		GinkgoHelper()
		c, err := query.CompileIndexComponent(widget, path, false)
		Expect(err).NotTo(HaveOccurred())
		return c
	}

	It("lets several components fan out over the same list", func() {
		_, err := query.NewIndexEncoder([]query.IndexComponent{
			comp("members", "uid"), comp("members", "role"),
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("refuses to fan out over two different lists", func() {
		_, err := query.NewIndexEncoder([]query.IndexComponent{
			comp("members", "uid"), comp("tags"),
		})
		Expect(err).To(MatchError(ContainSubstring("one fan-out chain per index")))
	})
})

var _ = Describe("Encoding a row that fans out", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()

	encoder := func(paths ...[]string) *query.IndexEncoder {
		GinkgoHelper()
		comps := make([]query.IndexComponent, 0, len(paths))
		for _, p := range paths {
			c, err := query.CompileIndexComponent(widget, p, false)
			Expect(err).NotTo(HaveOccurred())
			comps = append(comps, c)
		}
		enc, err := query.NewIndexEncoder(comps)
		Expect(err).NotTo(HaveOccurred())
		return enc
	}

	It("writes one entry per element", func() {
		enc := encoder([]string{"tags"})
		entries := enc.Encode((&testdatav1.Widget{Tags: []string{"a", "b", "c"}}).ProtoReflect(), "w1")
		Expect(entries).To(HaveLen(3))
	})

	It("collapses duplicate elements, since an entry is a position and not a count", func() {
		enc := encoder([]string{"tags"})
		entries := enc.Encode((&testdatav1.Widget{Tags: []string{"a", "a", "b"}}).ProtoReflect(), "w1")
		Expect(entries).To(HaveLen(2))
	})

	It("still writes one entry for an empty list, so every row is reachable", func() {
		enc := encoder([]string{"tags"})
		entries := enc.Encode((&testdatav1.Widget{}).ProtoReflect(), "w1")
		Expect(entries).To(HaveLen(1))
		// Missing rather than present-and-empty: an empty list has no element to
		// match, so a probe for any value must not find this row.
		Expect(entries[0][0]).To(Equal(byte(0xff)))
	})

	It("keeps two components on one list belonging to the same element", func() {
		enc := encoder([]string{"members", "uid"}, []string{"members", "role"})
		msg := &testdatav1.Widget{Members: []*testdatav1.Member{
			{Uid: "u1", Role: 1},
			{Uid: "u2", Role: 2},
		}}
		entries := enc.Encode(msg.ProtoReflect(), "w1")
		Expect(entries).To(HaveLen(2))

		// A cross product would also produce (u1, 2) — the pairing is the whole
		// point, so it is asserted against the entries a single-element row of
		// each pair would write.
		want := func(uid string, role int32) []byte {
			one := enc.Encode((&testdatav1.Widget{
				Members: []*testdatav1.Member{{Uid: uid, Role: role}},
			}).ProtoReflect(), "w1")
			Expect(one).To(HaveLen(1))
			return one[0]
		}
		Expect(entries).To(ConsistOf(want("u1", 1), want("u2", 2)))
	})

	It("mixes a fan-out component with a plain one", func() {
		enc := encoder([]string{"label"}, []string{"tags"})
		entries := enc.Encode((&testdatav1.Widget{
			Label: "L", Tags: []string{"a", "b"},
		}).ProtoReflect(), "w1")
		Expect(entries).To(HaveLen(2))
	})
})

var _ = Describe("SpecHash", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()

	comps := func(paths [][]string, descs []bool) []query.IndexComponent {
		out := make([]query.IndexComponent, 0, len(paths))
		for i, p := range paths {
			c, err := query.CompileIndexComponent(widget, p, descs[i])
			Expect(err).NotTo(HaveOccurred())
			out = append(out, c)
		}
		return out
	}

	It("is stable for the same spec", func() {
		a := query.SpecHash(comps([][]string{{"label"}}, []bool{false}))
		Expect(a).To(Equal(query.SpecHash(comps([][]string{{"label"}}, []bool{false}))))
		Expect(a).To(HaveLen(16))
	})

	It("changes with the direction", func() {
		Expect(query.SpecHash(comps([][]string{{"label"}}, []bool{false}))).
			NotTo(Equal(query.SpecHash(comps([][]string{{"label"}}, []bool{true}))))
	})

	It("changes with the component order", func() {
		Expect(query.SpecHash(comps([][]string{{"label"}, {"count"}}, []bool{false, false}))).
			NotTo(Equal(query.SpecHash(comps([][]string{{"count"}, {"label"}}, []bool{false, false}))))
	})

	It("changes when signedness changes, since the payload does", func() {
		Expect(query.SpecHash(comps([][]string{{"big"}}, []bool{false}))).
			NotTo(Equal(query.SpecHash(comps([][]string{{"ubig"}}, []bool{false}))))
	})

	It("tells the two sides of one entry apart", func() {
		// On a map<string, string> both sides have the same path and the same
		// payload shape, so nothing but the selector distinguishes them — and an
		// index whose entries hold keys must not be read as one holding values.
		key := [][]string{{"attrs", "*", "key"}}
		value := [][]string{{"attrs", "*", "value"}}
		Expect(query.SpecHash(comps(key, []bool{false}))).
			NotTo(Equal(query.SpecHash(comps(value, []bool{false}))))
	})

	It("does not change with the order the two sides are declared in beyond position", func() {
		// Sanity check on the layout being part of the fingerprint: swapping the
		// components is a different index, not the same one.
		both := [][]string{{"attrs", "*", "key"}, {"attrs", "*", "value"}}
		swapped := [][]string{{"attrs", "*", "value"}, {"attrs", "*", "key"}}
		Expect(query.SpecHash(comps(both, []bool{false, false}))).
			NotTo(Equal(query.SpecHash(comps(swapped, []bool{false, false}))))
	})
})

var _ = Describe("UpperBound", func() {
	It("increments the last byte below 0xff", func() {
		Expect(query.UpperBound([]byte{0x01, 0x02})).To(Equal([]byte{0x01, 0x03}))
	})

	It("carries past trailing 0xff bytes", func() {
		Expect(query.UpperBound([]byte{0x01, 0xff, 0xff})).To(Equal([]byte{0x02}))
	})

	It("is unbounded above an all-0xff prefix", func() {
		Expect(query.UpperBound([]byte{0xff, 0xff})).To(BeNil())
	})

	It("is unbounded above an empty prefix, which every entry starts with", func() {
		Expect(query.UpperBound(nil)).To(BeNil())
	})
})

var _ = Describe("EncodePrefix", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()

	It("brackets exactly the rows sharing the probed component", func() {
		c, err := query.CompileIndexComponent(widget, []string{"label"}, false)
		Expect(err).NotTo(HaveOccurred())
		enc, err := query.NewIndexEncoder([]query.IndexComponent{c})
		Expect(err).NotTo(HaveOccurred())

		lo, err := enc.EncodePrefix([]protoreflect.Value{protoreflect.ValueOfString("hi")})
		Expect(err).NotTo(HaveOccurred())
		hi := query.UpperBound(lo)

		one := func(w *testdatav1.Widget) string {
			entries := enc.Encode(w.ProtoReflect(), "w1")
			Expect(entries).To(HaveLen(1))
			return string(entries[0])
		}
		inside := one(&testdatav1.Widget{Label: "hi"})
		before := one(&testdatav1.Widget{Label: "hh"})
		after := one(&testdatav1.Widget{Label: "hia"})

		Expect(inside >= string(lo)).To(BeTrue())
		Expect(inside < string(hi)).To(BeTrue())
		Expect(before < string(lo)).To(BeTrue())
		// "hia" extends the probed value, so it is a DIFFERENT value and must
		// fall outside — the terminator is what separates them.
		Expect(after >= string(hi)).To(BeTrue())
	})

	It("rejects more values than the index has components", func() {
		enc, err := query.NewIndexEncoder(nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = enc.EncodePrefix([]protoreflect.Value{protoreflect.ValueOfString("x")})
		Expect(err).To(MatchError(ContainSubstring("has 0 components")))
	})
})

// probeCase pins one planner decision. The ranges themselves are not compared
// byte-for-byte — encodeCases already pin the encoding, and the bounds are
// derived from it — so what is shared is the SHAPE of the decision, which is
// the part that could silently diverge between the two planners.
type probeCase struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Indexes []indexSpecVector `json:"indexes"`
	Cond    json.RawMessage   `json:"cond"`
	OrderBy []json.RawMessage `json:"orderBy"`
	Want    json.RawMessage   `json:"want"`
}

// probeWant is the non-"scan" form of a probeCase's expectation.
type probeWant struct {
	Index    string `json:"index"`
	Ranges   int    `json:"ranges"`
	Ordered  bool   `json:"ordered"`
	Residual bool   `json:"residual"`
}

var _ = Describe("Planner conformance vectors", func() {
	raw, err := os.ReadFile(indexVectorsPath)
	Expect(err).NotTo(HaveOccurred())
	var file struct {
		ProbeCases []probeCase `json:"probeCases"`
	}
	Expect(json.Unmarshal(raw, &file)).To(Succeed())

	It("has cases", func() {
		Expect(file.ProbeCases).NotTo(BeEmpty())
	})

	for _, tc := range file.ProbeCases {
		It(tc.Name, func() {
			desc := msgForType(tc.Type).ProtoReflect().Descriptor()

			candidates := make([]query.Index, 0, len(tc.Indexes))
			for _, spec := range tc.Indexes {
				candidates = append(candidates, query.Index{
					Name:    spec.Name,
					Encoder: encoderFor(spec, tc.Type),
				})
			}

			var cond *syncplanev1.Condition
			if len(tc.Cond) > 0 {
				cond = &syncplanev1.Condition{}
				Expect(protojson.Unmarshal(tc.Cond, cond)).To(Succeed())
			}
			pred, err := query.Compile(desc, cond)
			Expect(err).NotTo(HaveOccurred())

			keys := make([]*syncplanev1.OrderKey, 0, len(tc.OrderBy))
			for _, k := range tc.OrderBy {
				ok := &syncplanev1.OrderKey{}
				Expect(protojson.Unmarshal(k, ok)).To(Succeed())
				keys = append(keys, ok)
			}
			var order *query.Order
			if len(keys) > 0 {
				order, err = query.CompileOrder(desc, keys)
				Expect(err).NotTo(HaveOccurred())
			}

			plan := query.PlanQuery(candidates, pred, order)

			var scan string
			if json.Unmarshal(tc.Want, &scan) == nil {
				Expect(scan).To(Equal("scan"))
				Expect(plan).To(BeNil())
				return
			}
			var want probeWant
			Expect(json.Unmarshal(tc.Want, &want)).To(Succeed())
			Expect(plan).NotTo(BeNil())
			Expect(plan.Index).To(Equal(want.Index))
			Expect(plan.Ranges).To(HaveLen(want.Ranges))
			Expect(plan.Ordered).To(Equal(want.Ordered))
			Expect(plan.Residual != nil).To(Equal(want.Residual))
		})
	}
})
