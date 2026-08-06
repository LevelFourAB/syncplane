package query_test

import (
	"encoding/json"
	"os"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

// The conformance vectors are shared with the TS client
// (client/test/unit/query-vectors.test.ts) — both evaluators must agree on
// every case.
const vectorsPath = "../../testdata/queryeval/vectors.json"

type evalCase struct {
	Name string          `json:"name"`
	Type string          `json:"type"` // row message type; "" => widget
	Row  json.RawMessage `json:"row"`
	Cond json.RawMessage `json:"cond"`
	Want bool            `json:"want"`
}

// msgForType returns a fresh message of the vector's row type (defaulting to
// Widget). Ref vectors use Issue, whose fields include first-class references;
// planner vectors also use FieldHistory, the engine's own history entity, whose
// by_source index is the one shape the feature cannot afford the planner to
// decline.
func msgForType(typ string) proto.Message {
	switch typ {
	case "issue":
		return &testdatav1.Issue{}
	case "field_history":
		return &syncplanev1.FieldHistory{}
	default:
		return &testdatav1.Widget{}
	}
}

type compareCase struct {
	Name    string            `json:"name"`
	A       json.RawMessage   `json:"a"`
	B       json.RawMessage   `json:"b"`
	KeyA    string            `json:"keyA"`
	KeyB    string            `json:"keyB"`
	OrderBy []json.RawMessage `json:"orderBy"`
	Want    int               `json:"want"`
}

type orderCase struct {
	Name string `json:"name"`
	Rows []struct {
		Key string          `json:"key"`
		Row json.RawMessage `json:"row"`
	} `json:"rows"`
	OrderBy []json.RawMessage `json:"orderBy"`
	Want    []string          `json:"want"`
}

// compileErrorCase is a query both compilers must REJECT. Only the rejection
// is shared; each language asserts its own error wording locally.
type compileErrorCase struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Cond    json.RawMessage   `json:"cond"`
	OrderBy []json.RawMessage `json:"orderBy"`
}

type vectorFile struct {
	EvalCases         []evalCase         `json:"evalCases"`
	CompareCases      []compareCase      `json:"compareCases"`
	OrderCases        []orderCase        `json:"orderCases"`
	CompileErrorCases []compileErrorCase `json:"compileErrorCases"`
}

func loadVectors() vectorFile {
	raw, err := os.ReadFile(vectorsPath)
	Expect(err).NotTo(HaveOccurred())
	var v vectorFile
	Expect(json.Unmarshal(raw, &v)).To(Succeed())
	Expect(v.EvalCases).NotTo(BeEmpty())
	Expect(v.CompareCases).NotTo(BeEmpty())
	Expect(v.OrderCases).NotTo(BeEmpty())
	Expect(v.CompileErrorCases).NotTo(BeEmpty())
	return v
}

func widgetOf(raw json.RawMessage) *testdatav1.Widget {
	w := &testdatav1.Widget{}
	Expect(protojson.Unmarshal(raw, w)).To(Succeed())
	return w
}

var widgetDesc = (&testdatav1.Widget{}).ProtoReflect().Descriptor()

var _ = Describe("Conformance vectors", func() {
	vectors := loadVectors()

	Describe("Eval", func() {
		for _, tc := range vectors.EvalCases {
			It(tc.Name, func() {
				cond := &syncplanev1.Condition{}
				Expect(protojson.Unmarshal(tc.Cond, cond)).To(Succeed())
				row := msgForType(tc.Type)
				compiled, err := query.Compile(row.ProtoReflect().Descriptor(), cond)
				Expect(err).NotTo(HaveOccurred())
				Expect(protojson.Unmarshal(tc.Row, row)).To(Succeed())
				Expect(compiled.Eval(row.ProtoReflect())).To(Equal(tc.Want))
			})
		}
	})

	Describe("Order", func() {
		for _, tc := range vectors.OrderCases {
			It(tc.Name, func() {
				keys := make([]*syncplanev1.OrderKey, len(tc.OrderBy))
				for i, raw := range tc.OrderBy {
					keys[i] = &syncplanev1.OrderKey{}
					Expect(protojson.Unmarshal(raw, keys[i])).To(Succeed())
				}
				order, err := query.CompileOrder(widgetDesc, keys)
				Expect(err).NotTo(HaveOccurred())
				type qrow struct {
					key string
					msg *testdatav1.Widget
				}
				rows := make([]qrow, len(tc.Rows))
				for i, r := range tc.Rows {
					rows[i] = qrow{key: r.Key, msg: widgetOf(r.Row)}
				}
				sort.SliceStable(rows, func(i, j int) bool {
					return order.Less(rows[i].msg.ProtoReflect(), rows[j].msg.ProtoReflect(), rows[i].key, rows[j].key)
				})
				got := make([]string, len(rows))
				for i, r := range rows {
					got[i] = r.key
				}
				Expect(got).To(Equal(tc.Want))
			})
		}
	})

	Describe("Compare", func() {
		for _, tc := range vectors.CompareCases {
			It(tc.Name, func() {
				keys := make([]*syncplanev1.OrderKey, len(tc.OrderBy))
				for i, raw := range tc.OrderBy {
					keys[i] = &syncplanev1.OrderKey{}
					Expect(protojson.Unmarshal(raw, keys[i])).To(Succeed())
				}
				order, err := query.CompileOrder(widgetDesc, keys)
				Expect(err).NotTo(HaveOccurred())
				a, b := widgetOf(tc.A), widgetOf(tc.B)
				got := order.Compare(a.ProtoReflect(), b.ProtoReflect(), tc.KeyA, tc.KeyB)
				Expect(sign(got)).To(Equal(tc.Want))
				// Antisymmetry: swapping the arguments flips the sign.
				Expect(sign(order.Compare(b.ProtoReflect(), a.ProtoReflect(), tc.KeyB, tc.KeyA))).To(Equal(-tc.Want))
			})
		}
	})

	Describe("Compile errors", func() {
		for _, tc := range vectors.CompileErrorCases {
			It(tc.Name, func() {
				desc := msgForType(tc.Type).ProtoReflect().Descriptor()
				if tc.Cond != nil {
					cond := &syncplanev1.Condition{}
					Expect(protojson.Unmarshal(tc.Cond, cond)).To(Succeed())
					_, err := query.Compile(desc, cond)
					Expect(err).To(HaveOccurred())
					return
				}
				Expect(tc.OrderBy).NotTo(BeEmpty(), "a compile-error case needs a cond or an orderBy")
				keys := make([]*syncplanev1.OrderKey, len(tc.OrderBy))
				for i, raw := range tc.OrderBy {
					keys[i] = &syncplanev1.OrderKey{}
					Expect(protojson.Unmarshal(raw, keys[i])).To(Succeed())
				}
				_, err := query.CompileOrder(desc, keys)
				Expect(err).To(HaveOccurred())
			})
		}
	})
})

var _ = Describe("Compile", func() {
	It("accepts a nil condition as match-all", func() {
		compiled, err := query.Compile(widgetDesc, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(compiled.Eval((&testdatav1.Widget{}).ProtoReflect())).To(BeTrue())
	})

	It("rejects unknown fields", func() {
		_, err := query.Compile(widgetDesc, cmpCond([]string{"nope"}))
		Expect(err).To(MatchError(ContainSubstring(`no field "nope"`)))
	})

	It("rejects descending through a non-message field", func() {
		_, err := query.Compile(widgetDesc, cmpCond([]string{"label", "x"}))
		Expect(err).To(MatchError(ContainSubstring("cannot descend")))
	})

	It("rejects descending through a repeated message field", func() {
		// tags is repeated string; also exercise the repeated branch guard.
		_, err := query.Compile(widgetDesc, cmpCond([]string{"tags", "x"}))
		Expect(err).To(MatchError(ContainSubstring("cannot descend")))
	})

	It("rejects an empty path", func() {
		_, err := query.Compile(widgetDesc, cmpCond(nil))
		Expect(err).To(MatchError(ContainSubstring("path is empty")))
	})

	It("rejects descending through a reference into a non-key field", func() {
		issueDesc := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
		_, err := query.Compile(issueDesc, cmpCond([]string{"project", "name"}))
		Expect(err).To(MatchError(ContainSubstring("only its key fields are addressable")))
	})

	It("allows descending through a reference into its key field", func() {
		issueDesc := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
		_, err := query.Compile(issueDesc, cmpCond([]string{"project", "id"}))
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects a condition without a kind", func() {
		_, err := query.Compile(widgetDesc, &syncplanev1.Condition{})
		Expect(err).To(MatchError(ContainSubstring("no kind")))
	})

	It("rejects a query value without a kind", func() {
		cond := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
			Path:  []string{"label"},
			Op:    syncplanev1.CmpOp_CMP_OP_EQ,
			Value: &syncplanev1.QueryValue{},
		}}}
		_, err := query.Compile(widgetDesc, cond)
		Expect(err).To(MatchError(ContainSubstring("query value has no kind")))
	})

	It("names the operator that works when a leaf is not comparable", func() {
		_, err := query.Compile(widgetDesc, cmpCond([]string{"raw"}))
		Expect(err).To(MatchError(ContainSubstring("bytes field raw is not comparable")))
		Expect(err).To(MatchError(ContainSubstring("use isSet/isNotSet")))
	})

	It("points a comparison on a reference at its key field", func() {
		issueDesc := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
		_, err := query.Compile(issueDesc, cmpCond([]string{"project"}))
		Expect(err).To(MatchError(ContainSubstring("compare its key field project.id")))
	})

	It("says a composite-key reference has no comparable value", func() {
		issueDesc := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
		_, err := query.Compile(issueDesc, cmpCond([]string{"pair"}))
		Expect(err).To(MatchError(ContainSubstring("composite-key reference")))
	})

	It("points a comparison on a repeated field at the rep operators", func() {
		_, err := query.Compile(widgetDesc, cmpCond([]string{"tags"}))
		Expect(err).To(MatchError(ContainSubstring("use has/hasAny/hasAll/isEmpty")))
	})

	It("points a comparison on a bare map at one of its entries", func() {
		// A whole map compares against nothing, but one ENTRY is an ordinary
		// value — so the hint names the key form rather than declaring maps
		// unqueryable. isSet stays off the list: on a map it is
		// constitutionally true (implicit presence), so it would be a false lead.
		_, err := query.Compile(widgetDesc, cmpCond([]string{"attrs"}))
		Expect(err).To(MatchError(ContainSubstring("address one entry by key instead, e.g. attrs.<key>")))
		Expect(err).NotTo(MatchError(ContainSubstring("isSet")))
	})

	It("reports a map key that does not parse as its key type", func() {
		_, err := query.Compile(widgetDesc, cmpCond([]string{"by_id", "not-a-number"}))
		Expect(err).To(MatchError(ContainSubstring("invalid int64 map key")))
	})

	It("names the entry, not the map, when a map value is incomparable", func() {
		_, err := query.Compile(widgetDesc, cmpCond([]string{"custom", "priority"}))
		Expect(err).To(MatchError(ContainSubstring(
			"the syncplane.testdata.v1.FieldValue value of map field custom is not comparable")))
	})

	It("allows a comparison against a collaborative text field", func() {
		// Text compares as its visible plain string, so it is a string leaf.
		_, err := query.Compile(widgetDesc, cmpCond([]string{"body"}))
		Expect(err).NotTo(HaveOccurred())
	})

	It("keeps a literal/field type mismatch a runtime no-match, not an error", func() {
		// count is comparable; only the literal is the wrong kind. That is a
		// legitimate query that matches nothing, unlike an incomparable leaf.
		compiled, err := query.Compile(widgetDesc, cmpCond([]string{"count"}))
		Expect(err).NotTo(HaveOccurred())
		Expect(compiled.Eval((&testdatav1.Widget{Count: 33}).ProtoReflect())).To(BeFalse())
	})

	It("allows isEmpty on a repeated message field but not membership", func() {
		empty := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Rep{Rep: &syncplanev1.RepCond{
			Path: []string{"members"},
			Op:   syncplanev1.RepOp_REP_OP_IS_EMPTY,
		}}}
		_, err := query.Compile(widgetDesc, empty)
		Expect(err).NotTo(HaveOccurred())

		has := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Rep{Rep: &syncplanev1.RepCond{
			Path:   []string{"members"},
			Op:     syncplanev1.RepOp_REP_OP_HAS,
			Values: []*syncplanev1.QueryValue{{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "u1"}}},
		}}}
		_, err = query.Compile(widgetDesc, has)
		Expect(err).To(MatchError(ContainSubstring("comparable elements")))
		Expect(err).To(MatchError(ContainSubstring("use any()")))
	})

	It("rejects any() over a repeated scalar, which has no element fields", func() {
		_, err := query.Compile(widgetDesc, query.Any("tags", query.Eq("uid", "u1")))
		Expect(err).To(MatchError(ContainSubstring("repeated message field")))
		Expect(err).To(MatchError(ContainSubstring("has/hasAny/hasAll")))
	})

	Describe("any() over a map", func() {
		// The element is the ENTRY, so a sub-condition names `key` or `value` —
		// which is what makes "which custom field holds X" expressible at all.
		// Addressing one entry by key answers the other question, "does THIS
		// field hold X".
		row := &testdatav1.Widget{
			Attrs: map[string]string{"color": "red", "size": "L"},
			Custom: map[string]*testdatav1.FieldValue{
				"priority": {Kind: &testdatav1.FieldValue_Number{Number: 5}},
				"team":     {Kind: &testdatav1.FieldValue_Text{Text: "platform"}},
				"labels":   {Kind: &testdatav1.FieldValue_Labels{Labels: &testdatav1.StringList{Values: []string{"ui", "api"}}}},
			},
		}
		matches := func(cond *syncplanev1.Condition) bool {
			GinkgoHelper()
			compiled, err := query.Compile(widgetDesc, cond)
			Expect(err).NotTo(HaveOccurred())
			return compiled.Eval(row.ProtoReflect())
		}

		It("finds an entry by its value, across keys", func() {
			Expect(matches(query.Any("custom", query.Eq("value.number", 5)))).To(BeTrue())
			Expect(matches(query.Any("custom", query.Eq("value.number", 9)))).To(BeFalse())
		})

		It("finds an entry by its key", func() {
			Expect(matches(query.Any("custom", query.Eq("key", "team")))).To(BeTrue())
			Expect(matches(query.Any("custom", query.In("key", "nope", "priority")))).To(BeTrue())
			Expect(matches(query.Any("custom", query.StartsWith("key", "pri")))).To(BeTrue())
			Expect(matches(query.Any("custom", query.StartsWith("key", "zz")))).To(BeFalse())
		})

		It("describes ONE entry when key and value are conjoined", func() {
			// The whole reason the entry scope is explicit: this is true only
			// because one entry satisfies both halves.
			Expect(matches(query.Any("custom", query.And(
				query.Eq("key", "priority"), query.Eq("value.number", 5))))).To(BeTrue())
			Expect(matches(query.Any("custom", query.And(
				query.Eq("key", "team"), query.Eq("value.number", 5))))).To(BeFalse())
		})

		It("reads a wrong-variant entry as not found, exactly as an addressed path does", func() {
			Expect(matches(query.Any("custom", query.Eq("value.text", "platform")))).To(BeTrue())
			Expect(matches(query.Any("custom", query.And(
				query.Eq("key", "priority"), query.IsSet("value.text"))))).To(BeFalse())
		})

		It("reaches a list nested inside one entry", func() {
			Expect(matches(query.Any("custom", query.HasAny("value.labels.values", "ui")))).To(BeTrue())
			Expect(matches(query.Any("custom", query.HasAny("value.labels.values", "zzz")))).To(BeFalse())
		})

		It("works over a scalar-valued map, where both sides are plain values", func() {
			Expect(matches(query.Any("attrs", query.Eq("value", "red")))).To(BeTrue())
			Expect(matches(query.Any("attrs", query.And(
				query.Eq("key", "size"), query.Eq("value", "L"))))).To(BeTrue())
			Expect(matches(query.Any("attrs", query.And(
				query.Eq("key", "size"), query.Eq("value", "red"))))).To(BeFalse())
		})

		It("matches nothing on an absent or empty map, so not(any(…)) reads as none", func() {
			bare := &testdatav1.Widget{}
			compiled, err := query.Compile(widgetDesc, query.Any("custom", query.Eq("value.number", 5)))
			Expect(err).NotTo(HaveOccurred())
			Expect(compiled.Eval(bare.ProtoReflect())).To(BeFalse())
			Expect(matches(query.Not(query.Any("custom", query.Eq("value.number", 9))))).To(BeTrue())
		})

		It("rejects a sub-path that is neither key nor value", func() {
			// Compiling against the synthetic entry type would reject it too, but
			// the error would name a message the schema author never wrote.
			_, err := query.Compile(widgetDesc, query.Any("custom", query.Eq("priority.number", 5)))
			Expect(err).To(MatchError(ContainSubstring("scopes one ENTRY")))
			Expect(err).To(MatchError(ContainSubstring(`"key" or "value"`)))
		})

		It("rejects a path continuing past the entry key", func() {
			_, err := query.Compile(widgetDesc, query.Any("custom", query.Eq("key.nope", "x")))
			Expect(err).To(MatchError(ContainSubstring("cannot descend into")))
		})

		It("rejects a comparison against the whole value of a message-valued map", func() {
			_, err := query.Compile(widgetDesc, query.Any("custom", query.Eq("value", "x")))
			Expect(err).To(MatchError(ContainSubstring("not comparable")))
		})

		It("points a comparison against a whole map at both entry forms", func() {
			_, err := query.Compile(widgetDesc, query.Eq("attrs", "x"))
			Expect(err).To(MatchError(ContainSubstring("address one entry by key")))
			Expect(err).To(MatchError(ContainSubstring("any(attrs,")))
		})
	})

	It("rejects any() over a singular message field", func() {
		_, err := query.Compile(widgetDesc, query.Any("settings", query.Eq("color", "red")))
		Expect(err).To(MatchError(ContainSubstring("repeated message field")))
	})

	It("rejects any() with no condition", func() {
		cond := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Exists{Exists: &syncplanev1.ExistsCond{
			Path: []string{"members"},
		}}}
		_, err := query.Compile(widgetDesc, cond)
		Expect(err).To(MatchError(ContainSubstring("needs a condition")))
	})

	It("resolves an any() sub-condition against the ELEMENT type", func() {
		_, err := query.Compile(widgetDesc, query.Any("members", query.Eq("uid", "u1")))
		Expect(err).NotTo(HaveOccurred())
		// `label` is a field of Widget, not of Member — proof the sub-condition
		// is not resolving against the row.
		_, err = query.Compile(widgetDesc, query.Any("members", query.Eq("label", "x")))
		Expect(err).To(MatchError(ContainSubstring(`has no field "label"`)))
	})

	It("keeps an any() over a reference list to the target's key fields", func() {
		issueDesc := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
		_, err := query.Compile(issueDesc, query.Any("blocked_by", query.Eq("id", "i2")))
		Expect(err).NotTo(HaveOccurred())
		_, err = query.Compile(issueDesc, query.Any("blocked_by", query.Contains("title", "x")))
		Expect(err).To(MatchError(ContainSubstring("holds only its key fields")))
	})

	It("rewrites membership over a to-many single-key reference", func() {
		issueDesc := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
		compiled, err := query.Compile(issueDesc, query.Has("blocked_by", "i3"))
		Expect(err).NotTo(HaveOccurred())
		row := &testdatav1.Issue{Id: "i1", BlockedBy: []*testdatav1.Issue{{Id: "i2"}, {Id: "i3"}}}
		Expect(compiled.Eval(row.ProtoReflect())).To(BeTrue())
	})

	It("refuses membership over a composite-key reference list", func() {
		_, err := query.Compile(widgetDesc, query.Has("refs", "x"))
		Expect(err).To(MatchError(ContainSubstring("composite-key reference")))
	})

	It("still allows isSet on any leaf", func() {
		for _, path := range [][]string{{"raw"}, {"attrs"}, {"doc"}, {"tags"}} {
			cond := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Present{Present: &syncplanev1.PresentCond{
				Path: path,
				Op:   syncplanev1.PresentOp_PRESENT_OP_IS_SET,
			}}}
			_, err := query.Compile(widgetDesc, cond)
			Expect(err).NotTo(HaveOccurred(), "path %v", path)
		}
	})

	It("rejects rep has with a value count other than one", func() {
		cond := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Rep{Rep: &syncplanev1.RepCond{
			Path: []string{"tags"},
			Op:   syncplanev1.RepOp_REP_OP_HAS,
		}}}
		_, err := query.Compile(widgetDesc, cond)
		Expect(err).To(MatchError(ContainSubstring("exactly one value")))
	})
})

var _ = Describe("CompareStringsUTF16", func() {
	It("matches byte order for ASCII", func() {
		Expect(query.CompareStringsUTF16("abc", "abd")).To(Equal(-1))
		Expect(query.CompareStringsUTF16("b", "a")).To(Equal(1))
		Expect(query.CompareStringsUTF16("same", "same")).To(Equal(0))
	})

	It("treats a prefix as smaller", func() {
		Expect(query.CompareStringsUTF16("ab", "abc")).To(Equal(-1))
	})

	It("orders astral characters below later BMP characters", func() {
		// U+1D11E (surrogate pair, first unit 0xD834) vs U+FFFD: UTF-16 says
		// the astral char is smaller; UTF-8 byte order says the opposite.
		Expect(query.CompareStringsUTF16("\U0001D11E", "�")).To(Equal(-1))
	})
})

func cmpCond(path []string) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
		Path:  path,
		Op:    syncplanev1.CmpOp_CMP_OP_EQ,
		Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "x"}},
	}}}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}
