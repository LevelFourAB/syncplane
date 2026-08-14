// Package search implements the text analyzer behind full-text search: the
// pipeline that turns field text and query strings into the terms the match
// evaluator and the search index agree on.
//
// The TypeScript client (client/src/search/) implements the identical
// pipeline; cross-language conformance vectors live in
// testdata/search/vectors.json. Term-for-term agreement between the two is
// the contract that makes a locally executed match and a server-executed
// match return the same rows, so every stage is defined without reference to
// locale, ICU, or any other environment-dependent tables:
//
//  1. Unicode NFKC normalization, so full-width forms, ligatures and
//     compatibility characters compare equal to their plain spellings.
//  2. Unicode full lowercase (Default Case Conversion, no locale), the
//     mapping JavaScript's String.prototype.toLowerCase applies.
//  3. Unless Options.KeepDiacritics is set: NFD, drop the combining marks of
//     the Combining Diacritical Marks block (U+0300–U+036F), NFC. The range
//     restriction folds Latin, Greek and Cyrillic accents while leaving
//     scripts whose marks carry meaning — Thai, Devanagari, Hebrew, Arabic —
//     untouched.
//  4. Segmentation. Runs of letters, digits and combining marks form one
//     term each, capped at 64 code points. Code points in the CJK ranges
//     below instead form runs emitted as character bigrams (a one-character
//     run is emitted as is), which makes text in scripts written without
//     word separators searchable without a dictionary.
//
// The CJK ranges are fixed by this package rather than taken from Unicode
// script tables: U+3005–U+3007, U+3040–U+30FF (Hiragana, Katakana),
// U+3400–U+4DBF, U+4E00–U+9FFF, U+F900–U+FAFF, U+20000–U+2FA1F (Han),
// U+AC00–U+D7A3 (Hangul syllables).
//
// A term's position is its index in the returned slice; scoring measures
// proximity as index distance.
package search

import (
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

// maxTermLength is the cap, in code points, past which a term is truncated.
const maxTermLength = 64

// Options configure analysis. The zero value is the default analyzer, and
// what a schema that sets no analyzer options gets.
type Options struct {
	// KeepDiacritics disables the diacritic fold, so "café" and "cafe"
	// analyze to distinct terms.
	KeepDiacritics bool
}

// QueryTerm is one term of an analyzed query string.
type QueryTerm struct {
	Term string

	// Prefix marks a term that matches any indexed term it prefixes rather
	// than only itself. Only the final term of a query can carry it, and only
	// when the query ends in the term itself — a trailing space or other
	// separator makes the final term exact.
	Prefix bool
}

// Analyze returns the terms of text in document order. It returns nil for
// text that yields no terms, including the empty string.
func Analyze(text string, opts Options) []string {
	tokens := tokenize(normalize(text, opts))
	if len(tokens) == 0 {
		return nil
	}
	terms := make([]string, len(tokens))
	for i, t := range tokens {
		terms[i] = t.term
	}
	return terms
}

// AnalyzeQuery returns the terms of a query string, in order, with the final
// term marked as a prefix when the query ends inside it. It returns nil for
// a query that yields no terms.
func AnalyzeQuery(query string, opts Options) []QueryTerm {
	tokens := tokenize(normalize(query, opts))
	if len(tokens) == 0 {
		return nil
	}
	terms := make([]QueryTerm, len(tokens))
	for i, t := range tokens {
		terms[i] = QueryTerm{Term: t.term}
	}
	if last := tokens[len(tokens)-1]; last.atEnd {
		terms[len(terms)-1].Prefix = true
	}
	return terms
}

func normalize(s string, opts Options) string {
	s = norm.NFKC.String(s)
	// A cases.Caser is stateful, so one is built per call rather than shared.
	s = cases.Lower(language.Und).String(s)
	if !opts.KeepDiacritics {
		s = foldDiacritics(s)
	}
	return s
}

func foldDiacritics(s string) string {
	d := norm.NFD.String(s)
	var b strings.Builder
	b.Grow(len(d))
	for _, r := range d {
		if r >= 0x0300 && r <= 0x036f {
			continue
		}
		b.WriteRune(r)
	}
	return norm.NFC.String(b.String())
}

type token struct {
	term string
	// atEnd marks a token whose final code point is the final code point of
	// the input, which is what lets AnalyzeQuery treat it as a prefix.
	atEnd bool
}

const (
	classSkip = iota
	classWord
	classCJK
)

func classify(r rune) int {
	if isCJK(r) {
		return classCJK
	}
	if unicode.In(r, unicode.L, unicode.M, unicode.N) {
		return classWord
	}
	return classSkip
}

// isCJK reports membership in the fixed ranges the package doc lists. The
// ranges are literal rather than unicode.Script tables so both languages
// agree independent of their Unicode table versions, and so U+30FC
// (KATAKANA-HIRAGANA PROLONGED SOUND MARK, script Common) stays inside a
// Katakana run instead of splitting it.
func isCJK(r rune) bool {
	switch {
	case r >= 0x3040 && r <= 0x30ff,
		r >= 0x3400 && r <= 0x4dbf,
		r >= 0x4e00 && r <= 0x9fff,
		r >= 0xf900 && r <= 0xfaff,
		r >= 0x20000 && r <= 0x2fa1f,
		r >= 0xac00 && r <= 0xd7a3,
		r >= 0x3005 && r <= 0x3007:
		return true
	}
	return false
}

func tokenize(s string) []token {
	var tokens []token
	var run []rune
	runClass := classSkip

	flush := func(atEnd bool) {
		switch runClass {
		case classWord:
			term := run
			if len(term) > maxTermLength {
				term = term[:maxTermLength]
			}
			tokens = append(tokens, token{term: string(term), atEnd: atEnd})
		case classCJK:
			if len(run) == 1 {
				tokens = append(tokens, token{term: string(run), atEnd: atEnd})
				break
			}
			for i := 0; i+2 <= len(run); i++ {
				tokens = append(tokens, token{
					term:  string(run[i : i+2]),
					atEnd: atEnd && i+2 == len(run),
				})
			}
		}
		run = run[:0]
	}

	for _, r := range s {
		c := classify(r)
		if c != runClass {
			flush(false)
			runClass = c
		}
		if c != classSkip {
			run = append(run, r)
		}
	}
	flush(true)
	return tokens
}
