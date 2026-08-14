package query

// Full-text match: the compiled form of a MatchCond and of the entity search
// declaration it evaluates against.
//
// Everything here is defined row-locally on purpose. A match and its rank are
// pure functions of (row, query, declaration) — no document frequencies, no
// corpus statistics — because the client evaluates over whatever subset it
// has synced and the server over everything, and the two must order the same
// rows identically. That rules BM25-style scoring out and is why the rank is
// a lexicographic tuple of per-row signals instead. The TypeScript mirror is
// client/src/query/match.ts; testdata/search/vectors.json pins both.

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf16"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/search"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// maxMatchTerms caps how many distinct terms of a query a match requires.
// Terms past the cap are ignored rather than rejected, so a pasted paragraph
// still searches by its head instead of failing.
const maxMatchTerms = 32

// SearchDecl is an entity's compiled search declaration: the fields a match
// condition reads, most important first, and the analyzer options their text
// and every query against them are analyzed with.
type SearchDecl struct {
	fields []searchFieldDecl
	opts   search.Options
	// spec is the declaration part of a Matcher's fingerprint: analyzer
	// options and field paths, everything a rank depends on besides the query.
	spec string
}

type searchFieldDecl struct {
	steps []pathStep
	path  string
}

// CompileSearchDecl resolves an entity's search declaration from its
// descriptor options: fields marked (syncplane.v1.field_behavior).searchable
// first, in field order, then the fields of EntityOptions.search, in
// declaration order. It returns nil for an entity that declares neither, and
// an error for a declaration that cannot search — a path that does not
// resolve, a leaf that is not a singular string or collaborative-text field, a
// duplicate path, or a search declaration naming no fields at all.
func CompileSearchDecl(desc protoreflect.MessageDescriptor) (*SearchDecl, error) {
	eo := protoopts.Entity(desc)
	var paths []string
	fds := desc.Fields()
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		if protoopts.FieldBehavior(fd).GetSearchable() {
			paths = append(paths, string(fd.Name()))
		}
	}
	spec := eo.GetSearch()
	for _, f := range spec.GetField() {
		paths = append(paths, f.GetPath())
	}
	if len(paths) == 0 {
		if spec != nil {
			return nil, fmt.Errorf("search on %s names no fields: declare SearchSpec.field entries or mark fields searchable", desc.FullName())
		}
		return nil, nil
	}
	d := &SearchDecl{opts: search.Options{KeepDiacritics: spec.GetKeepDiacritics()}}
	for _, p := range paths {
		steps, err := resolveFields(desc, strings.Split(p, "."))
		if err != nil {
			return nil, fmt.Errorf("search field %q: %w", p, err)
		}
		if leafKind(steps) != rvString {
			return nil, fmt.Errorf("search field %q: %s is not searchable; search reads singular string or collaborative-text fields",
				p, describeLeaf(steps[len(steps)-1]))
		}
		for _, prev := range d.fields {
			if samePath(prev.steps, steps) {
				return nil, fmt.Errorf("search field %q is declared twice", p)
			}
		}
		d.fields = append(d.fields, searchFieldDecl{steps: steps, path: p})
	}
	var b strings.Builder
	b.WriteString("search:")
	b.WriteString(strconv.FormatBool(d.opts.KeepDiacritics))
	for _, f := range d.fields {
		b.WriteByte('|')
		b.WriteString(f.path)
	}
	d.spec = b.String()
	return d, nil
}

// Matcher is one compiled match condition: the declaration it reads and the
// required terms of its query. It memoizes ranks per row message, so it is
// safe for concurrent use and cheap to consult from both an evaluation and an
// ordering of the same rows.
type Matcher struct {
	decl  *SearchDecl
	terms []search.QueryTerm

	mu    sync.Mutex
	cache map[protoreflect.Message]uint64
}

// matcherCacheLimit bounds the rank memo. A scan ranks rows once each, so the
// cache only has to survive the comparator revisiting rows the heap retains;
// past the limit it resets rather than growing with the entity.
const matcherCacheLimit = 4096

func newMatcher(decl *SearchDecl, query string) *Matcher {
	return &Matcher{decl: decl, terms: requiredTerms(search.AnalyzeQuery(query, decl.opts))}
}

// requiredTerms reduces an analyzed query to the terms a row must satisfy:
// duplicates collapse, a prefix term subsumed by an equal exact term drops
// (matching the exact term implies matching the prefix), and everything past
// maxMatchTerms is ignored.
func requiredTerms(qts []search.QueryTerm) []search.QueryTerm {
	exact := make(map[string]struct{}, len(qts))
	for _, t := range qts {
		if !t.Prefix {
			exact[t.Term] = struct{}{}
		}
	}
	out := make([]search.QueryTerm, 0, len(qts))
	seen := make(map[string]struct{}, len(qts))
	for _, t := range qts {
		if t.Prefix {
			if _, ok := exact[t.Term]; ok {
				continue
			}
		}
		if _, dup := seen[t.Term]; dup {
			continue
		}
		seen[t.Term] = struct{}{}
		out = append(out, t)
		if len(out) == maxMatchTerms {
			break
		}
	}
	return out
}

// Terms returns the required terms, in query order. The final one may carry
// Prefix.
func (m *Matcher) Terms() []search.QueryTerm { return m.terms }

// Matches reports whether every required term occurs in some declared field
// of msg. A matcher with no terms matches every row.
func (m *Matcher) Matches(msg protoreflect.Message) bool {
	return m.Rank(msg)>>40 == 0
}

// Rank scores msg against the query: a 48-bit integer where SMALLER IS
// BETTER, so ascending rank order is best-match-first and the value encodes
// into cursors as an ordinary number component. From most to least
// significant byte:
//
//	miss       required terms not found anywhere (0 for a matching row)
//	prefixOnly terms found only as a prefix of a longer term, never exactly
//	attribute  first declared field containing every term, 255 when none does
//	proximity  smallest token window covering every term in that field
//	first      earliest matched token position in that field
//	occurrence 255 minus the total number of matching tokens
//
// Every byte caps at 255, and a matcher with no terms ranks every row 0. The
// rank reads msg alone — never the store — which is what keeps a client
// ranking over its synced subset identical to the server ranking over
// everything.
func (m *Matcher) Rank(msg protoreflect.Message) uint64 {
	m.mu.Lock()
	if r, ok := m.cache[msg]; ok {
		m.mu.Unlock()
		return r
	}
	m.mu.Unlock()
	r := m.rank(msg)
	m.mu.Lock()
	if m.cache == nil || len(m.cache) >= matcherCacheLimit {
		m.cache = make(map[protoreflect.Message]uint64, 64)
	}
	m.cache[msg] = r
	m.mu.Unlock()
	return r
}

const rankByteMax = 255

// termEvent is one matching token: its position in the field and which
// required term it matched. Events are generated in (position, term) order.
type termEvent struct {
	pos, term int
}

func (m *Matcher) rank(msg protoreflect.Message) uint64 {
	n := len(m.terms)
	if n == 0 {
		return 0
	}
	matched := make([]bool, n)
	exact := make([]bool, n)
	occ := 0
	attribute := rankByteMax
	proximity := rankByteMax
	first := rankByteMax

	seen := make([]bool, n)
	for fi, f := range m.decl.fields {
		v, found := resolveValue(msg, f.steps)
		if !found || v.kind != rvString {
			continue
		}
		tokens := search.Analyze(v.str, m.decl.opts)
		if len(tokens) == 0 {
			continue
		}
		var events []termEvent
		inField := 0
		for i := range seen {
			seen[i] = false
		}
		for pos, tok := range tokens {
			for j, t := range m.terms {
				if t.Prefix {
					if !strings.HasPrefix(tok, t.Term) {
						continue
					}
				} else if tok != t.Term {
					continue
				}
				events = append(events, termEvent{pos: pos, term: j})
				occ++
				matched[j] = true
				if tok == t.Term {
					exact[j] = true
				}
				if !seen[j] {
					seen[j] = true
					inField++
				}
			}
		}
		if attribute == rankByteMax && inField == n {
			// 254, not 255: the sentinel byte must stay worse than any real
			// field, however many are declared.
			attribute = min(fi, rankByteMax-1)
			proximity = min(minWindow(events, n), rankByteMax)
			first = min(events[0].pos, rankByteMax)
		}
	}

	miss, prefixOnly := 0, 0
	for j := range matched {
		if !matched[j] {
			miss++
		} else if !exact[j] {
			prefixOnly++
		}
	}
	miss = min(miss, rankByteMax)
	prefixOnly = min(prefixOnly, rankByteMax)
	occComp := rankByteMax - min(occ, rankByteMax)
	return ((((uint64(miss)<<8|uint64(prefixOnly))<<8|uint64(attribute))<<8|
		uint64(proximity))<<8|uint64(first))<<8 | uint64(occComp)
}

// minWindow is the smallest position span covering at least one event of
// every term — the classic two-pointer minimal cover. events must be sorted
// by position (generation order guarantees it) and cover all n terms.
func minWindow(events []termEvent, n int) int {
	if n == 1 {
		return 0
	}
	counts := make([]int, n)
	have := 0
	best := int(^uint(0) >> 1)
	l := 0
	for r := 0; r < len(events); r++ {
		if counts[events[r].term] == 0 {
			have++
		}
		counts[events[r].term]++
		for have == n {
			if w := events[r].pos - events[l].pos; w < best {
				best = w
			}
			counts[events[l].term]--
			if counts[events[l].term] == 0 {
				have--
			}
			l++
		}
	}
	return best
}

// specString fingerprints everything this matcher's ranks depend on: the
// declaration and the required terms. Order.Shape folds it into relevance
// cursors, so a cursor minted for one search cannot bound a different one. A
// term never contains a space (the analyzer splits on them), so the join is
// unambiguous; the trailing * marks a prefix term and cannot appear in a term
// either. Non-ASCII code units render as \uXXXX escapes because the two
// fingerprint implementations walk different units — Go UTF-8 bytes, TS
// UTF-16 code units — and only agree on ASCII.
func (m *Matcher) specString() string {
	var b strings.Builder
	b.WriteString(m.decl.spec)
	b.WriteString("?")
	for i, t := range m.terms {
		if i > 0 {
			b.WriteByte(' ')
		}
		for _, u := range utf16.Encode([]rune(t.Term)) {
			if u < 0x80 {
				b.WriteByte(byte(u))
			} else {
				fmt.Fprintf(&b, "\\u%04x", u)
			}
		}
		if t.Prefix {
			b.WriteByte('*')
		}
	}
	return b.String()
}

type matchNode struct {
	m *Matcher
}

func (n *matchNode) eval(s scope) bool {
	if s.entry != nil {
		// Unreachable: compile rejects a match inside any(). A map entry has
		// no searchable fields, so answer false rather than panic.
		return false
	}
	return n.m.Matches(s.msg)
}
