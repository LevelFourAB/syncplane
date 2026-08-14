package search_test

import (
	"encoding/json"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/search"
)

// vectorsPath is the shared cross-language conformance file; the TS harness
// (client/test/unit/search-vectors.test.ts) runs the identical cases and the
// pinned terms are what make the two analyzers interchangeable.
const vectorsPath = "../../testdata/search/vectors.json"

type vectorFile struct {
	AnalyzeCases []analyzeCase `json:"analyzeCases"`
	QueryCases   []queryCase   `json:"queryCases"`
}

type vectorOptions struct {
	KeepDiacritics bool `json:"keepDiacritics"`
}

func (o vectorOptions) options() search.Options {
	return search.Options{KeepDiacritics: o.KeepDiacritics}
}

type analyzeCase struct {
	Name    string        `json:"name"`
	Text    string        `json:"text"`
	Options vectorOptions `json:"options"`
	Want    []string      `json:"want"`
}

type queryCase struct {
	Name    string        `json:"name"`
	Query   string        `json:"query"`
	Options vectorOptions `json:"options"`
	Want    []wantTerm    `json:"want"`
}

type wantTerm struct {
	Term   string `json:"term"`
	Prefix bool   `json:"prefix"`
}

var _ = Describe("shared analyzer vectors", func() {
	raw, err := os.ReadFile(vectorsPath)
	if err != nil {
		panic(err)
	}
	var file vectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		panic(err)
	}

	for _, tc := range file.AnalyzeCases {
		tc := tc
		It("analyzes: "+tc.Name, func() {
			got := search.Analyze(tc.Text, tc.Options.options())
			want := tc.Want
			if len(want) == 0 {
				Expect(got).To(BeEmpty())
				return
			}
			Expect(got).To(Equal(want))
		})
	}

	for _, tc := range file.QueryCases {
		tc := tc
		It("analyzes query: "+tc.Name, func() {
			got := search.AnalyzeQuery(tc.Query, tc.Options.options())
			want := make([]search.QueryTerm, 0, len(tc.Want))
			for _, w := range tc.Want {
				want = append(want, search.QueryTerm{Term: w.Term, Prefix: w.Prefix})
			}
			if len(want) == 0 {
				Expect(got).To(BeEmpty())
				return
			}
			Expect(got).To(Equal(want))
		})
	}
})
