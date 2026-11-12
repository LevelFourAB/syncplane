package server

import (
	"testing"

	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/search"
	"github.com/levelfourab/syncplane/engine/storage"
)

// countingTx answers PostingCounts with fixed counts, one per range, the
// overflow range last, and a fixed row count.
type countingTx struct {
	storage.ReadTx
	counts []int
	rows   int
	max    int
}

func (t *countingTx) PostingCounts(_, _ string, ranges []storage.IndexRange, max int) ([]int, int, error) {
	t.max = max
	return t.counts[:len(ranges)], t.rows, nil
}

func TestSearchPlanChoosesRangesFromCounts(t *testing.T) {
	info := &entityInfo{name: "doc", search: &searchIndex{name: searchIndexName}}
	terms := []search.QueryTerm{{Term: "rare"}, {Term: "mid"}, {Term: "common"}}
	termRange := func(i int) storage.IndexRange {
		lo, hi := query.SearchTermRange(terms[i])
		return storage.IndexRange{Lo: lo, Hi: hi}
	}
	olo, ohi := query.SearchOverflowRange()
	overflow := storage.IndexRange{Lo: olo, Hi: ohi}
	const budget = 1000
	// many is a row count far above every count, which no share declines.
	const many = 1 << 20
	// share is the most candidates 1000 rows serve from the index.
	const share = 1000 * searchScanSharePercent / 100

	for _, tc := range []struct {
		name     string
		counts   []int // rare, mid, common, overflow
		rows     int   // rows in the index
		ok       bool
		all, any []storage.IndexRange
	}{
		{
			name:   "a term far above the smallest is left out",
			counts: []int{10, 10 * searchNarrowRatio, 10*searchNarrowRatio + 1, 0},
			rows:   many,
			ok:     true,
			all:    []storage.IndexRange{termRange(0), termRange(1)},
		},
		{
			name:   "overflow rows join the candidates",
			counts: []int{10, 20, 30, 2},
			rows:   many,
			ok:     true,
			all:    []storage.IndexRange{termRange(0), termRange(1), termRange(2)},
			any:    []storage.IndexRange{overflow},
		},
		{
			name:   "a term no row holds leaves the overflow rows alone",
			counts: []int{0, 20, 30, 2},
			rows:   many,
			ok:     true,
			any:    []storage.IndexRange{overflow},
		},
		{
			name:   "a term no row holds and no overflow row matches nothing",
			counts: []int{0, 20, 30, 0},
			rows:   many,
			ok:     true,
		},
		{
			name:   "candidates above the budget decline",
			counts: []int{budget - 1, budget, budget, 2},
			rows:   many,
			ok:     false,
		},
		{
			name:   "candidates at the budget are served",
			counts: []int{budget - 2, budget, budget, 2},
			rows:   many,
			ok:     true,
			all:    []storage.IndexRange{termRange(0), termRange(1), termRange(2)},
			any:    []storage.IndexRange{overflow},
		},
		{
			name:   "candidates above the scan share of the rows decline",
			counts: []int{share + 1, 900, 950, 0},
			rows:   1000,
			ok:     false,
		},
		{
			name:   "overflow rows count toward the scan share",
			counts: []int{share - 10, 900, 950, 11},
			rows:   1000,
			ok:     false,
		},
		{
			name:   "candidates at the scan share of the rows are served",
			counts: []int{share - 10, 900, 950, 10},
			rows:   1000,
			ok:     true,
			all:    []storage.IndexRange{termRange(0), termRange(1), termRange(2)},
			any:    []storage.IndexRange{overflow},
		},
		{
			name:   "a term in every row declines",
			counts: []int{1000, 1000, 1000, 0},
			rows:   1000,
			ok:     false,
		},
		{
			name:   "a term no row holds in an empty index matches nothing",
			counts: []int{0, 0, 0, 0},
			rows:   0,
			ok:     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &countingTx{counts: tc.counts, rows: tc.rows}
			all, any, ok, err := searchPlan(tx, info, terms, budget)
			if err != nil {
				t.Fatal(err)
			}
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !equalRanges(all, tc.all) || !equalRanges(any, tc.any) {
				t.Fatalf("all = %v, any = %v; want %v, %v", all, any, tc.all, tc.any)
			}
			if tx.max != budget*searchNarrowRatio {
				t.Fatalf("max = %d, want %d", tx.max, budget*searchNarrowRatio)
			}
		})
	}
}

func equalRanges(a, b []storage.IndexRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if string(a[i].Lo) != string(b[i].Lo) || string(a[i].Hi) != string(b[i].Hi) {
			return false
		}
	}
	return true
}
