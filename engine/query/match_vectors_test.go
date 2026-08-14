package query_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

// The match and rank sections of the shared search vectors; the analyzer
// sections are consumed by engine/search. The TS harness is
// client/test/unit/search-vectors.test.ts.
const searchVectorsPath = "../../testdata/search/vectors.json"

type matchVectorFile struct {
	MatchCases      []matchCase        `json:"matchCases"`
	RankCases       []rankCase         `json:"rankCases"`
	MatchErrorCases []matchErrorCase   `json:"matchErrorCases"`
	OrderErrorCases []orderErrorCase   `json:"orderErrorCases"`
	CursorCases     []searchCursorCase `json:"cursorCases"`
}

type matchCase struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	Row   json.RawMessage `json:"row"`
	Query string          `json:"query"`
	Want  bool            `json:"want"`
}

type rankCase struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Query string `json:"query"`
	Rows  []struct {
		Key string          `json:"key"`
		Row json.RawMessage `json:"row"`
	} `json:"rows"`
	Want []struct {
		Key  string `json:"key"`
		Rank uint64 `json:"rank"`
	} `json:"want"`
}

type matchErrorCase struct {
	Name string          `json:"name"`
	Type string          `json:"type"`
	Cond json.RawMessage `json:"cond"`
}

type orderErrorCase struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Cond    json.RawMessage   `json:"cond"`
	OrderBy []json.RawMessage `json:"orderBy"`
}

type searchCursorCase struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Entity    string            `json:"entity"`
	Query     string            `json:"query"`
	OrderBy   []json.RawMessage `json:"orderBy"`
	Row       json.RawMessage   `json:"row"`
	Key       string            `json:"key"`
	Shape     string            `json:"shape"`
	CursorHex string            `json:"cursorHex"`
}

func orderKeysOf(raws []json.RawMessage) []*syncplanev1.OrderKey {
	keys := make([]*syncplanev1.OrderKey, len(raws))
	for i, raw := range raws {
		keys[i] = &syncplanev1.OrderKey{}
		Expect(protojson.Unmarshal(raw, keys[i])).To(Succeed())
	}
	return keys
}

var _ = Describe("shared match vectors", func() {
	raw, err := os.ReadFile(searchVectorsPath)
	if err != nil {
		panic(err)
	}
	var file matchVectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		panic(err)
	}

	rowOf := func(typ string, raw json.RawMessage) protoreflect.Message {
		msg := msgForType(typ)
		Expect(protojson.Unmarshal(raw, msg)).To(Succeed())
		return msg.ProtoReflect()
	}

	for _, tc := range file.MatchCases {
		tc := tc
		It("matches: "+tc.Name, func() {
			desc := msgForType(tc.Type).ProtoReflect().Descriptor()
			pred, err := query.Compile(desc, query.Match(tc.Query))
			Expect(err).NotTo(HaveOccurred())
			Expect(pred.Eval(rowOf(tc.Type, tc.Row))).To(Equal(tc.Want))
		})
	}

	for _, tc := range file.RankCases {
		tc := tc
		It("ranks: "+tc.Name, func() {
			desc := msgForType(tc.Type).ProtoReflect().Descriptor()
			pred, err := query.Compile(desc, query.Match(tc.Query))
			Expect(err).NotTo(HaveOccurred())
			matchers := pred.Matchers()
			Expect(matchers).To(HaveLen(1))
			m := matchers[0]

			type ranked struct {
				key  string
				rank uint64
			}
			got := make([]ranked, 0, len(tc.Rows))
			for _, r := range tc.Rows {
				got = append(got, ranked{key: r.Key, rank: m.Rank(rowOf(tc.Type, r.Row))})
			}
			sort.SliceStable(got, func(i, j int) bool {
				if got[i].rank != got[j].rank {
					return got[i].rank < got[j].rank
				}
				return query.CompareStringsUTF16(got[i].key, got[j].key) < 0
			})
			want := make([]ranked, 0, len(tc.Want))
			for _, w := range tc.Want {
				want = append(want, ranked{key: w.Key, rank: w.Rank})
			}
			Expect(got).To(Equal(want))
		})
	}

	for _, tc := range file.MatchErrorCases {
		tc := tc
		It("rejects: "+tc.Name, func() {
			desc := msgForType(tc.Type).ProtoReflect().Descriptor()
			cond := &syncplanev1.Condition{}
			Expect(protojson.Unmarshal(tc.Cond, cond)).To(Succeed())
			_, err := query.Compile(desc, cond)
			Expect(err).To(HaveOccurred())
		})
	}

	for _, tc := range file.OrderErrorCases {
		tc := tc
		It("rejects order: "+tc.Name, func() {
			desc := msgForType(tc.Type).ProtoReflect().Descriptor()
			var cond *syncplanev1.Condition
			if len(tc.Cond) > 0 {
				cond = &syncplanev1.Condition{}
				Expect(protojson.Unmarshal(tc.Cond, cond)).To(Succeed())
			}
			pred, err := query.Compile(desc, cond)
			Expect(err).NotTo(HaveOccurred())
			_, err = query.CompileOrderWith(desc, orderKeysOf(tc.OrderBy), pred)
			Expect(err).To(HaveOccurred())
		})
	}

	for _, tc := range file.CursorCases {
		tc := tc
		It("encodes cursor: "+tc.Name, func() {
			desc := msgForType(tc.Type).ProtoReflect().Descriptor()
			pred, err := query.Compile(desc, query.Match(tc.Query))
			Expect(err).NotTo(HaveOccurred())
			order, err := query.CompileOrderWith(desc, orderKeysOf(tc.OrderBy), pred)
			Expect(err).NotTo(HaveOccurred())
			Expect(order.Shape(tc.Entity)).To(Equal(tc.Shape))
			cursor := order.EncodeCursor(tc.Entity, rowOf(tc.Type, tc.Row), tc.Key)
			Expect(hex.EncodeToString(cursor)).To(Equal(tc.CursorHex))

			// A cursor must decode against the ordering that minted it.
			entry, err := order.DecodeCursor(tc.Entity, cursor)
			Expect(err).NotTo(HaveOccurred())
			Expect(entry).NotTo(BeEmpty())
		})
	}
})
