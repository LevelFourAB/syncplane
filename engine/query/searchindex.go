package query

// The search index: term postings derived from an entity's search
// declaration. It rides the same storage as every other secondary index — a
// replace-set of entries per row, bytewise-ordered, derived and disposable —
// but its entries are TERMS rather than field values, so a row occupies one
// position per distinct term its searchable fields analyze to:
//
//	entry    := 0x01 term     one per distinct term
//	         |  0x02          the overflow marker, alone
//	term     := CESU-8, escaped and terminated as every index string is
//
// An entry does not end with the row key, as a value index entry does: every
// search read collects the SET of rows a range names and uses neither the
// order of the entries within a term nor their bytes past it. The rows that
// hold one term therefore all hold the same entry, which the storage contract
// permits (see storage.IndexEntry).
//
// A row whose distinct terms exceed SearchTermsPerRow stores the single
// overflow marker instead. Every term probe unions the overflow rows into its
// candidates, so an oversized document is always considered rather than
// silently unfindable — correctness never depends on a truncated posting set.
//
// The index generates CANDIDATES only: matched rows are re-evaluated (and
// ranked) from the row itself, so the index changes which rows are considered,
// never which match — the same contract every other index honors. The
// TypeScript client maintains its own copy of the index as local index
// positions (client/src/optimistic/index-maintain.ts). What the two sides must
// agree on is the analysis, the per-row term cap and the overflow rule, as
// they decide which rows a probe finds; the entry bytes are local to each
// side.

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/search"
)

// searchIndexEncodingVersion is folded into every search-index SpecHash.
// Bumping it — for an entry-encoding change, or an analyzer change that moves
// existing terms — invalidates every stored search index, which then rebuilds.
const searchIndexEncodingVersion = 2

// SearchTermsPerRow caps the distinct terms one row stores postings for; past
// it the row stores the overflow marker alone. It is a constant rather than a
// tunable because both sides maintain their own copy of the index and have to
// agree on which rows overflow.
const SearchTermsPerRow = 256

const (
	searchTermTag     byte = 0x01
	searchOverflowTag byte = 0x02
)

// SearchEntries returns the entries msg occupies in the entity's search
// index: one per distinct term of its searchable fields in first-occurrence
// order, or the overflow marker alone past SearchTermsPerRow. A row whose
// fields analyze to nothing occupies no entries — such a row can match no
// term probe, and the overflow union covers only rows that opted out by size.
func (d *SearchDecl) SearchEntries(msg protoreflect.Message) [][]byte {
	seen := make(map[string]struct{})
	var terms []string
	for _, f := range d.fields {
		v, found := resolveValue(msg, f.steps)
		if !found || v.kind != rvString {
			continue
		}
		for _, t := range search.Analyze(v.str, d.opts) {
			if _, dup := seen[t]; dup {
				continue
			}
			if len(seen) == SearchTermsPerRow {
				return [][]byte{{searchOverflowTag}}
			}
			seen[t] = struct{}{}
			terms = append(terms, t)
		}
	}
	out := make([][]byte, 0, len(terms))
	for _, t := range terms {
		out = append(out, appendIndexString([]byte{searchTermTag}, t))
	}
	return out
}

// SearchSpecHash fingerprints everything the stored search entries depend on:
// the declaration (fields and analyzer options), the per-row term cap, and
// the encoding version. A stored index whose hash no longer matches is stale
// and rebuilds, exactly like a value index.
func (d *SearchDecl) SearchSpecHash() string {
	return fingerprint(fmt.Sprintf("searchv%d|%s|cap:%d", searchIndexEncodingVersion, d.spec, SearchTermsPerRow))
}

// Heads returns the top-level field of each search field, in declaration
// order and without repeats.
func (d *SearchDecl) Heads() []protoreflect.FieldDescriptor {
	var out []protoreflect.FieldDescriptor
	for _, f := range d.fields {
		if len(f.steps) > 0 && !slices.Contains(out, f.steps[0].fd) {
			out = append(out, f.steps[0].fd)
		}
	}
	return out
}

// MaskTouches reports whether a field-mask path could change any searchable
// field, given the path split the way lww splits a mask. Conservative the
// same way IndexComponent.MaskTouches is: undecidable cases answer true.
func (d *SearchDecl) MaskTouches(segs []string) bool {
	for _, f := range d.fields {
		if maskTouchesSteps(f.steps, segs) {
			return true
		}
	}
	return false
}

// SearchTermRange brackets the postings a query term selects: the term's own
// entries, or — for a prefix term — the entries of every term it prefixes.
func SearchTermRange(t search.QueryTerm) (lo, hi []byte) {
	if t.Prefix {
		// The term's own encoding carries a terminator, so it would match only
		// the exact term; the prefix bound drops it, exactly as a startsWith
		// range does.
		raw := appendIndexString(nil, t.Term)
		pre := append([]byte{searchTermTag}, raw[:len(raw)-2]...)
		return pre, UpperBound(pre)
	}
	pre := appendIndexString([]byte{searchTermTag}, t.Term)
	return pre, UpperBound(pre)
}

// SearchOverflowRange brackets the overflow markers — the rows whose term
// sets outgrew their postings. Every term probe unions these rows into its
// candidates.
func SearchOverflowRange() (lo, hi []byte) {
	return []byte{searchOverflowTag}, []byte{searchOverflowTag + 1}
}

// RequiredMatcher returns a match condition every row satisfying the
// predicate must satisfy — one reachable from the root through conjunctions
// alone — or nil when there is none. A match under or/not constrains nothing
// by itself, so only a required one can narrow an index walk's candidates.
func (c *Compiled) RequiredMatcher() *Matcher {
	var find func(n node) *Matcher
	find = func(n node) *Matcher {
		switch t := n.(type) {
		case *matchNode:
			return t.m
		case *andNode:
			for _, sub := range t.conds {
				if m := find(sub); m != nil {
					return m
				}
			}
		}
		return nil
	}
	if c.root == nil {
		return nil
	}
	return find(c.root)
}
