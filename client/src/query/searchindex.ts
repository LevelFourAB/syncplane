// The search index: term postings derived from an entity's search
// declaration. Mirror of engine/query/searchindex.go — the entry bytes, the
// per-row term cap, the overflow rule and the probe ranges must agree, since
// each side maintains its own copy of the same index over the same rows.
//
// 	entry := 0x01 term rowKey     one per distinct term
// 	      |  0x02 rowKey          the overflow marker, alone
//
// A row whose distinct terms exceed SEARCH_TERMS_PER_ROW stores the single
// overflow marker instead, and every term probe unions the overflow rows into
// its candidates — an oversized document is always considered rather than
// silently unfindable. The index generates CANDIDATES only: matched rows are
// re-evaluated and ranked from the row itself.

import type { DescMessage } from '@bufbuild/protobuf';

import { analyze, type QueryTerm } from '../search/index.js';
import type { Cond } from './ast.js';
import { resolvePath } from './evaluate.js';
import { appendIndexString, bytesToKey, fingerprint, upperBound } from './indexkey.js';
import type { SearchDecl } from './match.js';

/**
 * Folded into every search-index spec hash. Bumping it — for an entry-encoding
 * change, or an analyzer change that moves existing terms — invalidates every
 * stored search index, which then rebuilds.
 */
const SEARCH_INDEX_ENCODING_VERSION = 1;

/**
 * Caps the distinct terms one row stores postings for; past it the row stores
 * the overflow marker alone. A constant rather than a tunable because both
 * sides maintain their own copy of the index and have to agree on which rows
 * overflow.
 */
export const SEARCH_TERMS_PER_ROW = 256;

/** The reserved name the search index is stored under, one per entity. */
export const SEARCH_INDEX_NAME = '__search';

const TERM_TAG = 0x01;
const OVERFLOW_TAG = 0x02;

/**
 * The entries a row occupies in its entity's search index: one per distinct
 * term of its searchable fields in first-occurrence order, or the overflow
 * marker alone past the cap. A row whose fields analyze to nothing occupies
 * no entries.
 */
export function searchEntries(
	decl: SearchDecl,
	value: unknown,
	key: string,
	schema: DescMessage,
): string[] {
	const seen = new Set<string>();
	const terms: string[] = [];
	for (const f of decl.fields) {
		const r = resolvePath(value, f.path, schema);
		if (!r.found || typeof r.value !== 'string') continue;
		for (const t of analyze(r.value, decl.options)) {
			if (seen.has(t)) continue;
			if (seen.size === SEARCH_TERMS_PER_ROW) {
				const out: number[] = [ OVERFLOW_TAG ];
				appendIndexString(out, key);
				return [ bytesToKey(out) ];
			}
			seen.add(t);
			terms.push(t);
		}
	}
	return terms.map((t) => {
		const out: number[] = [ TERM_TAG ];
		appendIndexString(out, t);
		appendIndexString(out, key);
		return bytesToKey(out);
	});
}

/**
 * Fingerprints everything the stored search entries depend on: the
 * declaration, the per-row term cap, and the encoding version. Identical to
 * the Go side's, so both maintain the same index under the same fingerprint.
 */
export function searchSpecHash(decl: SearchDecl): string {
	return fingerprint(
		`searchv${SEARCH_INDEX_ENCODING_VERSION}|${decl.spec}|cap:${SEARCH_TERMS_PER_ROW}`,
	);
}

/**
 * Brackets the postings a query term selects: the term's own entries, or —
 * for a prefix term — the entries of every term it prefixes. `hi` is
 * undefined when nothing sorts above the range.
 */
export function searchTermRange(t: QueryTerm): { lo: string; hi: string | undefined } {
	const out: number[] = [ TERM_TAG ];
	appendIndexString(out, t.term);
	let lo = bytesToKey(out);
	// The term's own encoding carries a terminator, so it would match only the
	// exact term; the prefix bound drops it, exactly as a startsWith range does.
	if (t.prefix) lo = lo.slice(0, -2);
	return { lo, hi: upperBound(lo) };
}

/** Brackets the overflow markers — the rows whose term sets outgrew their
 * postings. Every term probe unions these rows into its candidates. */
export function searchOverflowRange(): { lo: string; hi: string } {
	return { lo: String.fromCharCode(OVERFLOW_TAG), hi: String.fromCharCode(OVERFLOW_TAG + 1) };
}

/**
 * A match condition every row satisfying the predicate must satisfy — one
 * reachable from the root through conjunctions alone — or undefined when
 * there is none. A match under or/not constrains nothing by itself, so only a
 * required one can narrow an index walk's candidates.
 */
export function requiredMatchCond(cond: Cond): (Cond & { kind: 'match' }) | undefined {
	switch (cond.kind) {
		case 'match':
			return cond;
		case 'and':
			for (const sub of cond.conds) {
				const m = requiredMatchCond(sub);
				if (m) return m;
			}
			return undefined;
		default:
			return undefined;
	}
}
