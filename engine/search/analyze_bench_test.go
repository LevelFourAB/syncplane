package search_test

import (
	"strings"
	"testing"

	"github.com/levelfourab/syncplane/engine/search"
)

// What analysis costs, per document.
//
// Every searchable field of every candidate row is analyzed on every ranked
// read — the rank is a pure function of the row, so nothing about it can be
// precomputed into the index (match.go) — and again on every write that
// touches a searchable field. So this is the inner loop of both halves of
// search, and the input alphabet is what decides which pipeline stages it
// pays for:
//
//   - ascii — the overwhelming majority of real documents, and the case where
//     NFKC, the case mapper and the diacritic fold are all provably identity.
//   - latin1 — accented text, which folds: the stage that needs NFD, a filter
//     and NFC.
//   - cjk — no word separators, so segmentation emits character bigrams and a
//     document of n characters yields n-1 terms rather than n/5.
//
// The sizes bracket a title and a body, because the two are analyzed by the
// same code on the same read and one of them is 50x the other.
func BenchmarkAnalyze(b *testing.B) {
	for _, doc := range analyzeDocs {
		b.Run(doc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(doc.text)))
			for i := 0; i < b.N; i++ {
				if terms := search.Analyze(doc.text, search.Options{}); len(terms) == 0 {
					b.Fatal("no terms")
				}
			}
		})
	}
}

// BenchmarkAnalyzeQuery is the other caller: once per query rather than once
// per row, so it matters for its latency floor rather than for its total.
func BenchmarkAnalyzeQuery(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if terms := search.AnalyzeQuery("quarterly revenue rep", search.Options{}); len(terms) != 3 {
			b.Fatalf("got %d terms", len(terms))
		}
	}
}

var analyzeDocs = func() []struct {
	name string
	text string
} {
	repeat := func(words []string, n int) string {
		var sb strings.Builder
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(words[i%len(words)])
		}
		return sb.String()
	}
	ascii := []string{"quarterly", "revenue", "report", "for", "the", "northern", "region"}
	latin1 := []string{"trimestriel", "chiffre", "d'affaires", "région", "señor", "år", "café"}
	// Han characters rather than a transliteration: the bigram path is only
	// taken for text in the ranges the analyzer treats as unsegmented.
	cjk := []string{"季度收入报告", "北部地区", "年度总结"}
	return []struct {
		name string
		text string
	}{
		{"ascii/title", repeat(ascii, 6)},
		{"ascii/body", repeat(ascii, 300)},
		{"latin1/title", repeat(latin1, 6)},
		{"latin1/body", repeat(latin1, 300)},
		{"cjk/title", repeat(cjk, 3)},
		{"cjk/body", repeat(cjk, 60)},
	}
}()
