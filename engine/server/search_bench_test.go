package server_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// What the term-postings index buys a search, and where it stops buying it.
//
// A match condition is answered from the postings of its most selective term
// and re-checked against every candidate row (searchindex.go). The cost has
// two halves that move in opposite directions: the walk is O(postings read),
// the re-check is O(candidates x document), and a term that occurs everywhere
// makes both as expensive as the scan the index was meant to replace.
//
// So the sub-benchmarks are picked by term SELECTIVITY rather than by query
// syntax:
//
//   - rare — one term in searchBenchMatches rows. The case the index exists
//     for: a bounded posting list and a bounded number of re-checks.
//   - common — one term in every row. The postings ARE the entity, so the
//     index can only lose; past MaxIndexScanCandidates it declines and the
//     query scans.
//   - common+rare — the shape a real search box produces once a user types a
//     second word. A row must carry both, so the rare term alone already
//     bounds the answer, and the walk finds that out without reading the
//     common term's postings.
//   - prefix — the same rare term as a prefix, which reads a range of terms
//     rather than one. What it costs over `rare` is the width of that range.
//
// Both arms drive the SAME query over the SAME rows, differing only in
// whether the index is usable, so the ratio between them is the whole result.
// A FIXED number of rows match, so the work the index avoids grows with the
// entity while the work it does stays flat.
var searchBenchSizes = []int{1000, 10000, 50000}

// searchBenchMatches is how many rows carry the rare term. Ten, like the value
// index benchmarks, so a search number and an index number are read on the
// same selectivity.
const searchBenchMatches = 10

// The corpus vocabulary. Filler is what every document is built from, so any
// filler word is a term the whole entity carries; rareTerm is planted in
// searchBenchMatches rows and occurs nowhere else. commonTerm is prepended to
// every document rather than left to the PRNG, so "matches every row" is a
// property of the fixture instead of a probability.
const (
	rareTerm   = "zygote"
	rarePrefix = "zygo"
	commonTerm = "lorem"
)

var searchFiller = []string{
	"ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
	"sed", "eiusmod", "tempor", "incididunt", "labore", "magna", "aliqua",
	"enim", "minim", "veniam", "quis", "nostrud", "exercitation", "ullamco",
	"laboris", "nisi", "aliquip", "commodo", "consequat", "duis", "aute",
}

// searchBenchServer builds a server over a fresh on-disk store holding n
// articles, searchBenchMatches of which carry rareTerm. With built set the
// search index is claimed; without it every read scans.
func searchBenchServer(b *testing.B, n int, built bool, opts ...server.Option) *server.Server {
	b.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })

	srv, err := server.New(store, append([]server.Option{
		server.WithEntities(&testdatav1.Article{}),
	}, opts...)...)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = srv.Close() })

	rng := rand.New(rand.NewPCG(1, 2))
	if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
		for i := 0; i < n; i++ {
			a := &testdatav1.Article{
				Id:      fmt.Sprintf("a%07d", i),
				Title:   searchWords(rng, 5),
				Summary: commonTerm + " " + searchWords(rng, 20),
				Meta:    &testdatav1.ArticleMeta{Author: searchWords(rng, 2)},
				State:   "published",
			}
			// The matching rows are spread across the key space rather than
			// clustered, so neither the scan nor the batched candidate reads
			// can get lucky and finish early.
			if i%(n/searchBenchMatches) == 0 {
				a.Summary += " " + rareTerm
			}
			if err := tx.Put(a); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	if built {
		if err := srv.BuildIndexes(ctx, benchWS); err != nil {
			b.Fatal(err)
		}
	}
	return srv
}

func searchWords(rng *rand.Rand, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(searchFiller[rng.IntN(len(searchFiller))])
	}
	return b.String()
}

// searchBenchQueries are the shapes, with how many rows each must return. The
// count is asserted on every iteration: a benchmark that stopped matching what
// it claims to match is measuring a different query.
func searchBenchQueries(n int) []struct {
	name string
	spec query.Spec
	want int
} {
	return []struct {
		name string
		spec query.Spec
		want int
	}{
		{"rare", query.Spec{Where: query.Match(rareTerm + " ")}, searchBenchMatches},
		{"common", query.Spec{Where: query.Match(commonTerm + " ")}, n},
		// The common term comes FIRST, which is where a walk that seeded from
		// the leading term rather than the most selective one would pick it.
		{"common+rare", query.Spec{Where: query.Match(commonTerm + " " + rareTerm + " ")}, searchBenchMatches},
		{"prefix", query.Spec{Where: query.Match(rarePrefix)}, searchBenchMatches},
	}
}

// BenchmarkSearchQuery measures one search, index against scan.
//
// The shape to read off it: `rare` flat in the corpus size and the scan
// linear in it; `common+rare` next to `rare` rather than next to `common`,
// which is what says the walk bounded itself by its most selective term; and
// `common` no better than the scan, because a term every row carries leaves
// nothing to narrow. A regression here looks like an index column starting to
// slope with the corpus.
func BenchmarkSearchQuery(b *testing.B) {
	ctx := context.Background()
	for _, n := range searchBenchSizes {
		for _, mode := range []struct {
			name  string
			built bool
		}{{"scan", false}, {"index", true}} {
			b.Run(fmt.Sprintf("%s/rows=%d", mode.name, n), func(b *testing.B) {
				var opts []server.Option
				if !mode.built {
					opts = append(opts, server.WithLimits(server.Limits{DisableIndexes: true}))
				}
				srv := searchBenchServer(b, n, mode.built, opts...)
				for _, q := range searchBenchQueries(n) {
					b.Run(q.name, func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							searchRun(b, srv, ctx, q.spec, q.want)
						}
					})
				}
			})
		}
	}
}

// BenchmarkSearchRelevance prices the ordering rather than the walk.
//
// Rank is a pure function of the row (match.go), so the evaluator has already
// computed one for every candidate by the time the ordering asks for it — the
// memo is what keeps `relevance` next to `unordered`. A gap that widens with
// the corpus means ranks are being recomputed rather than reused.
//
// `page` is the shape a search box actually issues: a term most rows carry,
// ranked, cut to twenty. Nothing about a limit bounds the work — every row
// still has to be ranked to know which twenty win — so this column is the
// honest cost of relevance ordering, and the one to watch.
func BenchmarkSearchRelevance(b *testing.B) {
	ctx := context.Background()
	for _, n := range searchBenchSizes {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			srv := searchBenchServer(b, n, true)
			shapes := []struct {
				name string
				spec query.Spec
				want int
			}{
				{"unordered", query.Spec{Where: query.Match(rareTerm + " ")}, searchBenchMatches},
				{"relevance", query.Spec{
					Where:   query.Match(rareTerm + " "),
					OrderBy: []*syncplanev1.OrderKey{query.Relevance()},
				}, searchBenchMatches},
				{"page", query.Spec{
					Where:   query.Match(commonTerm + " "),
					OrderBy: []*syncplanev1.OrderKey{query.Relevance()},
					Limit:   20,
				}, 20},
			}
			for _, s := range shapes {
				b.Run(s.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						searchRun(b, srv, ctx, s.spec, s.want)
					}
				})
			}
		})
	}
}

// BenchmarkSearchWrite measures what search maintenance costs the write path —
// the other half of the trade, paid on every write to a searchable entity.
//
// A row occupies one index position per DISTINCT term its searchable fields
// analyze to, so the cost is set by a document's VOCABULARY and not by its
// length: `terms` is how many distinct words each document holds, and
// entries/row is reported beside the timings because that is the number the
// per-row caps are spent against.
//
// The largest size crosses SearchTermsPerRow, where a row stops storing
// postings and stores the overflow marker alone. Its entries/row collapses to
// 1 and its write gets CHEAPER than the size below it — while every search
// then has to consider it, which is the cost the read side pays instead.
//
//   - put — the whole entry set is replaced, because a PUT replaces the whole
//     row and the write path holds no pre-image to diff against.
//   - patch/searchable — the mask names a searchable field, so the set is
//     replaced for the same reason.
//   - patch/untouched — the mask reaches no searchable field and no index
//     component, so maintenance is skipped entirely
//     (entityInfo.maskTouchesIndexes) and the write costs what it would on an
//     entity with no search at all. The gap between it and patch/searchable is
//     what the guard is worth, and it widens with the document.
func BenchmarkSearchWrite(b *testing.B) {
	ctx := context.Background()

	newServer := func(b *testing.B) *server.Server {
		b.Helper()
		store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = store.Close() })
		srv, err := server.New(store, server.WithEntities(&testdatav1.Article{}))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = srv.Close() })
		return srv
	}

	for _, terms := range searchWriteTerms {
		rng := rand.New(rand.NewPCG(1, 2))
		article := func(i int) *testdatav1.Article {
			return &testdatav1.Article{
				Id:      fmt.Sprintf("a%07d", i),
				Title:   searchWords(rng, 5),
				Summary: distinctWords(terms),
				Meta:    &testdatav1.ArticleMeta{Author: searchWords(rng, 2)},
				State:   "published",
			}
		}

		b.Run(fmt.Sprintf("put/terms=%d", terms), func(b *testing.B) {
			srv := newServer(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row := article(i)
				if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
					return tx.Put(row)
				}); err != nil {
					b.Fatal(err)
				}
			}
			// Reported AFTER the loop: ResetTimer clears the extra metrics, so
			// a value stated before it never reaches the output.
			b.StopTimer()
			b.ReportMetric(searchEntriesPerRow(b, article(0)), "entries/row")
		})

		// Both PATCH cases edit ONE row, over and over, so what differs between
		// them is the mask and nothing else.
		patch := func(b *testing.B, field string, set func(*testdatav1.Article)) {
			b.Helper()
			srv := newServer(b)
			if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
				return tx.Put(article(0))
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row := &testdatav1.Article{Id: fmt.Sprintf("a%07d", 0)}
				set(row)
				if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
					return tx.Patch(row, []string{field}, nil)
				}); err != nil {
					b.Fatal(err)
				}
			}
		}

		b.Run(fmt.Sprintf("patch/searchable/terms=%d", terms), func(b *testing.B) {
			patch(b, "title", func(a *testdatav1.Article) { a.Title = "edited " + rareTerm })
		})

		b.Run(fmt.Sprintf("patch/untouched/terms=%d", terms), func(b *testing.B) {
			patch(b, "state", func(a *testdatav1.Article) { a.State = "draft" })
		})
	}
}

// searchWriteTerms is how many distinct words a benchmarked document holds.
// The first two bracket what a real row holds — a title-and-blurb row and a
// long one — and the third is past SearchTermsPerRow, where the row overflows.
var searchWriteTerms = []int{20, 200, 300}

// distinctWords builds a document of n words that share no term, so a
// document's length and its entry count move together.
func distinctWords(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "term%04d", i)
	}
	return b.String()
}

// searchEntriesPerRow is how many search index positions one row occupies.
func searchEntriesPerRow(b *testing.B, row *testdatav1.Article) float64 {
	b.Helper()
	decl, err := query.CompileSearchDecl(row.ProtoReflect().Descriptor())
	if err != nil {
		b.Fatal(err)
	}
	return float64(len(decl.SearchEntries(row.ProtoReflect(), "probe")))
}

// searchRun executes one query and checks it returned the rows it claims to.
func searchRun(b *testing.B, srv *server.Server, ctx context.Context, spec query.Spec, want int) {
	b.Helper()
	rows := 0
	if err := srv.View(server.Trusted(ctx), benchWS, func(tx *server.ViewTx) error {
		return tx.Query(&testdatav1.Article{}, spec, func(proto.Message, uint64) error {
			rows++
			return nil
		})
	}); err != nil {
		b.Fatal(err)
	}
	if rows != want {
		b.Fatalf("got %d rows, want %d", rows, want)
	}
}
