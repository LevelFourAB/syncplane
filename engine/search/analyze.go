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
	"sync"
	"unicode"
	"unicode/utf8"

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
//
// Every returned term is a slice of one string — text itself when it needed
// no normalization — so retaining a single term retains the whole of it.
func Analyze(text string, opts Options) []string {
	s := normalize(text, opts)
	terms := make([]string, 0, estimateTerms(s))
	tokenize(s, func(term string, _ bool) {
		terms = append(terms, term)
	})
	if len(terms) == 0 {
		return nil
	}
	return terms
}

// AnalyzeQuery returns the terms of a query string, in order, with the final
// term marked as a prefix when the query ends inside it. It returns nil for
// a query that yields no terms.
func AnalyzeQuery(query string, opts Options) []QueryTerm {
	s := normalize(query, opts)
	terms := make([]QueryTerm, 0, estimateTerms(s))
	tokenize(s, func(term string, atEnd bool) {
		terms = append(terms, QueryTerm{Term: term, Prefix: atEnd})
	})
	if len(terms) == 0 {
		return nil
	}
	return terms
}

// estimateTerms sizes the term slice from the document's length. Five bytes
// per term is roughly a western word; CJK text runs about twice that density
// and pays one growth for it.
func estimateTerms(s string) int {
	return 1 + len(s)/5
}

func normalize(s string, opts Options) string {
	// All-ASCII text is its own NFKC and NFD form and carries no combining
	// mark, so every stage but the case mapping is provably identity on it —
	// and over ASCII the Unicode default case conversion is A-Z to a-z and
	// nothing else. Text that is already lowercase comes back unchanged, which
	// is what lets a term alias the field it was read from.
	if ascii, lower := scanASCII(s); ascii {
		if lower {
			return s
		}
		return lowerASCII(s)
	}
	s = norm.NFKC.String(s)
	s = lowerFull(s)
	if !opts.KeepDiacritics {
		s = foldDiacritics(s)
	}
	return s
}

// scanASCII reports whether s is all ASCII, and whether it holds no A-Z.
func scanASCII(s string) (ascii, lower bool) {
	lower = true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf {
			return false, false
		}
		if c >= 'A' && c <= 'Z' {
			lower = false
		}
	}
	return true, lower
}

func lowerASCII(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

// lowerCasers pools the Unicode case mappers. A cases.Caser carries mapping
// state across its input and so cannot be shared between goroutines, but it is
// also expensive enough to build that constructing one per document showed up
// in search profiles.
var lowerCasers = sync.Pool{New: func() any {
	c := cases.Lower(language.Und)
	return &c
}}

func lowerFull(s string) string {
	c := lowerCasers.Get().(*cases.Caser)
	out := c.String(s)
	lowerCasers.Put(c)
	return out
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

const (
	classSkip = iota
	classWord
	classCJK
)

func classify(r rune) int {
	if r < utf8.RuneSelf {
		// No ASCII code point is CJK or a combining mark, so the class is
		// decided by letter-or-digit alone — and answering that from the byte
		// avoids a range-table search per character of every document.
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return classWord
		}
		return classSkip
	}
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

// tokenize calls emit once per term of the normalized string s, in document
// order. atEnd marks a term whose final code point is the final code point of
// s, which is what lets AnalyzeQuery treat it as a prefix.
//
// Every term is a SUBSTRING of s rather than a copy of one: a run of the same
// class is contiguous by construction, and neither the length cap nor the
// bigram split does anything but cut one. That is what keeps analysis of a
// document allocation-free beyond the term slice itself.
func tokenize(s string, emit func(term string, atEnd bool)) {
	runStart, runRunes := -1, 0
	runClass := classSkip

	flush := func(end int) {
		run := s[runStart:end]
		atEnd := end == len(s)
		switch runClass {
		case classWord:
			if runRunes > maxTermLength {
				run = run[:runeOffset(run, maxTermLength)]
			}
			emit(run, atEnd)
		case classCJK:
			emitBigrams(run, runRunes, atEnd, emit)
		}
		runStart, runRunes = -1, 0
	}

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		c := classify(r)
		if c != runClass {
			if runStart >= 0 {
				flush(i)
			}
			runClass, runStart, runRunes = c, -1, 0
			if c != classSkip {
				runStart = i
			}
		}
		if c != classSkip {
			runRunes++
		}
		i += size
	}
	if runStart >= 0 {
		flush(len(s))
	}
}

// emitBigrams splits an unsegmented run into overlapping character pairs. A
// one-character run is emitted as itself, so a single CJK character is
// findable rather than lost.
func emitBigrams(run string, runes int, atEnd bool, emit func(string, bool)) {
	if runes == 1 {
		emit(run, atEnd)
		return
	}
	lo := 0
	_, n := utf8.DecodeRuneInString(run)
	for mid := n; mid < len(run); {
		_, n := utf8.DecodeRuneInString(run[mid:])
		hi := mid + n
		emit(run[lo:hi], atEnd && hi == len(run))
		lo, mid = mid, hi
	}
}

// runeOffset is the byte offset of the n-th code point of s, which must hold
// at least n of them.
func runeOffset(s string, n int) int {
	off := 0
	for i := 0; i < n; i++ {
		_, size := utf8.DecodeRuneInString(s[off:])
		off += size
	}
	return off
}
