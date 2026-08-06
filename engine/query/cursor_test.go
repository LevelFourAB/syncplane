package query_test

import (
	"bytes"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/text"
)

// orderOf compiles an ordering over Widget from paths written the way a caller
// writes them.
func orderOf(keys ...*syncplanev1.OrderKey) *query.Order {
	GinkgoHelper()
	o, err := query.CompileOrder((&testdatav1.Widget{}).ProtoReflect().Descriptor(), keys)
	Expect(err).NotTo(HaveOccurred())
	return o
}

func cursorOf(o *query.Order, msg proto.Message, key string) []byte {
	return o.Cursor(msg.ProtoReflect(), key)
}

var _ = Describe("Query cursors", func() {
	Describe("agreement with the comparator", func() {
		// The index-entry vectors double as cursor vectors: their specs are
		// orderings, and a cursor is the entry that ordering implies. Sorting
		// by cursor bytes has to reproduce Order.Compare exactly, since that
		// equivalence is the only thing after/before rest on.
		for _, tc := range loadIndexVectors().OrderCases {
			It(tc.Name, func() {
				ord := orderFor(tc.Spec, tc.Type)

				type row struct {
					key    string
					msg    proto.Message
					cursor []byte
				}
				rows := make([]row, 0, len(tc.Rows))
				for _, r := range tc.Rows {
					msg := msgForType(tc.Type)
					Expect(protojson.Unmarshal(r.Row, msg)).To(Succeed())
					rows = append(rows, row{key: r.Key, msg: msg, cursor: ord.Cursor(msg.ProtoReflect(), r.Key)})
				}

				byCursor := append([]row(nil), rows...)
				sort.SliceStable(byCursor, func(i, j int) bool {
					return bytes.Compare(byCursor[i].cursor, byCursor[j].cursor) < 0
				})
				Expect(keysOf(byCursor, func(r row) string { return r.key })).To(Equal(tc.Want))

				// after/before are byte comparisons against a cursor, so every
				// pair has to answer the same question the comparator does.
				for _, a := range rows {
					for _, b := range rows {
						Expect(bytes.Compare(a.cursor, b.cursor) > 0).
							To(Equal(ord.Compare(a.msg.ProtoReflect(), b.msg.ProtoReflect(), a.key, b.key) > 0),
								"a=%s b=%s", a.key, b.key)
					}
				}
			})
		}
	})

	Describe("collaborative text order keys", func() {
		// An index over a text field is refused because a keystroke would
		// rewrite the stored entry. A cursor is computed and discarded, so it
		// orders by the field's plain text like the comparator does.
		body := func(s string) []byte {
			d := text.New("p")
			Expect(d.Insert(0, s)).To(Succeed())
			return d.Encode()
		}

		It("orders by the visible text", func() {
			ord := orderOf(query.Asc("body"))
			a := &testdatav1.Widget{Id: "a", Body: body("apple")}
			b := &testdatav1.Widget{Id: "b", Body: body("banana")}
			Expect(bytes.Compare(cursorOf(ord, a, "a"), cursorOf(ord, b, "b"))).To(Equal(-1))
			Expect(ord.Compare(a.ProtoReflect(), b.ProtoReflect(), "a", "b")).To(Equal(-1))
		})

		It("encodes the same bytes as the equivalent plain string key", func() {
			// The comparator projects text to plain text, and so must the
			// encoder, or a page boundary would land somewhere the comparator
			// does not agree with.
			textOrd := orderOf(query.Asc("body"))
			stringOrd := orderOf(query.Asc("label"))
			withText := &testdatav1.Widget{Id: "w", Body: body("hello")}
			withLabel := &testdatav1.Widget{Id: "w", Label: "hello"}
			Expect(cursorOf(textOrd, withText, "w")).To(Equal(cursorOf(stringOrd, withLabel, "w")))
		})
	})

	Describe("framing", func() {
		ord := orderOf(query.Asc("count"), query.Desc("label"))
		row := &testdatav1.Widget{Id: "w1", Count: 3, Label: "x"}

		It("round-trips through the wire form", func() {
			cur := ord.EncodeCursor("widget", row.ProtoReflect(), "w1")
			entry, err := ord.DecodeCursor("widget", cur)
			Expect(err).NotTo(HaveOccurred())
			Expect(entry).To(Equal(ord.Cursor(row.ProtoReflect(), "w1")))
		})

		It("rejects a cursor minted under a different ordering", func() {
			cur := ord.EncodeCursor("widget", row.ProtoReflect(), "w1")
			other := orderOf(query.Asc("count"))
			_, err := other.DecodeCursor("widget", cur)
			Expect(err).To(MatchError(query.ErrCursorShape))
		})

		It("rejects a cursor minted against a different entity", func() {
			cur := ord.EncodeCursor("widget", row.ProtoReflect(), "w1")
			_, err := ord.DecodeCursor("gadget", cur)
			Expect(err).To(MatchError(query.ErrCursorShape))
		})

		It("tells two orderings over different entries of one map apart", func() {
			// Both resolve through the same map field, so a shape that only
			// rendered field names would call them the same ordering and accept
			// a cursor that positions the page by another column's value.
			a := orderOf(query.Asc("custom.a.number"))
			b := orderOf(query.Asc("custom.b.number"))
			Expect(a.Shape("widget")).NotTo(Equal(b.Shape("widget")))
		})

		It("rejects bytes that are not a cursor", func() {
			shape := ord.Shape("widget")
			_, err := ord.DecodeCursor("widget", []byte("short"))
			Expect(err).To(MatchError(ContainSubstring("too short")))

			bad := append([]byte{0x02}, shape...)
			_, err = ord.DecodeCursor("widget", bad)
			Expect(err).To(MatchError(ContainSubstring("cursor version")))

			// Right shape, garbage entry: the first component's tag is not one
			// of the three the encoding defines.
			bad = append(append([]byte{0x01}, shape...), 0x42)
			_, err = ord.DecodeCursor("widget", bad)
			Expect(err).To(MatchError(ContainSubstring("tag 0x42")))

			// A well-formed entry that runs out mid-string.
			bad = append(append([]byte{0x01}, shape...), 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 'a')
			_, err = ord.DecodeCursor("widget", bad)
			Expect(err).To(MatchError(ContainSubstring("unterminated string")))
		})
	})

	Describe("CursorPrefix", func() {
		ord := orderOf(query.Asc("count"))

		It("drops the row-key tiebreak, so rows tying on the order keys share it", func() {
			a := &testdatav1.Widget{Id: "a", Count: 7}
			b := &testdatav1.Widget{Id: "b", Count: 7}
			entryA := cursorOf(ord, a, "a")
			entryB := cursorOf(ord, b, "b")
			Expect(entryA).NotTo(Equal(entryB))

			prefixA, err := ord.CursorPrefix(entryA)
			Expect(err).NotTo(HaveOccurred())
			prefixB, err := ord.CursorPrefix(entryB)
			Expect(err).NotTo(HaveOccurred())
			Expect(prefixA).To(Equal(prefixB))
			Expect(bytes.HasPrefix(entryA, prefixA)).To(BeTrue())
		})

		It("is empty for an ordering with no keys", func() {
			// Row-key order: the entry is the row key alone, so there is no
			// component half to seek with.
			bare := orderOf()
			entry := cursorOf(bare, &testdatav1.Widget{Id: "a"}, "a")
			prefix, err := bare.CursorPrefix(entry)
			Expect(err).NotTo(HaveOccurred())
			Expect(prefix).To(BeEmpty())
		})
	})
})
