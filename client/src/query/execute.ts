// Answering a local query from a secondary index.
//
// The scan path (`scanDecoded` + `runQuery`) is the DEFINITION of what a query
// returns: it reads the whole merged view, filters, sorts and slices. This file
// is an optimization over it and nothing more — it must return exactly the same
// rows in exactly the same order, and it declines whenever it cannot promise
// that. Every decline is correct; only a wrong answer is a bug.
//
// The shape:
//
//     confirmed = the index range(s), in order, minus rows the overlay shadows
//     overlay   = every o/ row of the entity, decoded and filtered
//     result    = sort(confirmed ∪ overlay) sliced to offset+limit
//
// Why the overlay is scanned whole rather than indexed: it is bounded by the
// pending queue — what this device has written and not yet had acked — so
// reading all of it is O(pending), not O(entity). Indexing it would mean
// maintaining entries for uncommitted state that a rebase throws away on every
// pull, for no asymptotic gain.
//
// Two subtleties the ordering rests on:
//
//   - Within ONE workspace the index order IS `compareRows` order, because the
//     entry encoding was built to reproduce it and every entry ends with the
//     row key. Across workspaces it is not: `compareRows` breaks a final tie on
//     the row key alone, while the scan path feeds it rows ordered by
//     `<workspace>/<key>`, so two rows in different workspaces that tie on
//     every order key AND share a row key would resolve differently. Rather
//     than reproduce that, a cross-workspace query declines.
//   - The result is re-sorted even when the plan is ordered, because the
//     overlay rows have to be merged in. The sort is over the RESULT, not the
//     entity, which is the whole point.

import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import { fromBinary } from '@bufbuild/protobuf';

import {
	confirmedKey,
	type ConfirmedRecord,
	confirmedWsPrefix,
	indexPositionPrefix,
	metaIndexBuilt,
	type OverlayRecord,
	overlayWsPrefix,
} from '../keys.js';
import type { KVRead } from '../kv/store.js';
import type { EntityDef, EntityRegistry } from '../mutations.js';
import type { CompiledIndex } from '../optimistic/index-maintain.js';
import { sortOrderedFields } from '../ordering.js';
import { foldTextLog, type Row } from '../read.js';
import type { CompiledQuery, Cond, PageWindow } from './ast.js';
import { compareRows, evalCond } from './evaluate.js';
import { upperBound } from './indexkey.js';
import type { IndexRange, QueryPlan } from './plan.js';
import { planQuery } from './plan.js';

/**
 * How many index positions one query may walk. An index narrows the candidates; a
 * residual predicate the range could not express still rejects some of them,
 * and in the worst case rejects nearly all — at which point the walk has become
 * a scan with extra reads. Past this the query falls back rather than failing,
 * because an optimization that gives up must not change the answer.
 */
const MAX_INDEX_CANDIDATES = 10000;

/**
 * A result row together with the merged-view bytes it was decoded from — the
 * same bytes a scan would have read, collaborative-text log folded in.
 *
 * The bytes are what a caller keeping rows ACROSS reads compares to decide
 * whether the row it holds is still current: identical bytes decode to an
 * identical value, while the row version does not settle it — an optimistic
 * edit moves the bytes and leaves the confirmed version alone.
 */
export interface IndexedRow<T> {
	row: Row<T>;
	bytes: Uint8Array;
}

/** An index-served page, with the matches it was cut from. */
export interface IndexedPage<T> {
	rows: IndexedRow<T>[];
	/** Matches before the page was cut; see RowSet.total. */
	total: number;
	/** Whether matches remain past the end `limit` counted from. */
	hasMore: boolean;
}

/**
 * Answers a query from an index, or returns undefined to mean "scan instead".
 *
 * `undefined` is never "no rows" — an empty page is an empty array. It means
 * no index serves this query, none is built yet, or the walk exceeded its
 * candidate budget.
 */
export async function queryByIndex<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	indexes: CompiledIndex[],
	compiled: CompiledQuery,
	workspaceId: string | undefined,
	window?: PageWindow,
): Promise<IndexedPage<MessageShape<D>> | undefined> {
	// A cross-workspace query would need a k-way merge whose tie-break differs
	// from the scan path's; see the header.
	if (workspaceId === undefined || indexes.length === 0) return undefined;

	const built: CompiledIndex[] = [];
	for (const idx of indexes) {
		if (await tx.has(metaIndexBuilt(workspaceId, def.name, idx.name, idx.specHash))) {
			built.push(idx);
		}
	}
	if (built.length === 0) return undefined;

	const plan = planQuery(
		built.map((idx) => ({ name: idx.name, encoder: idx.encoder })),
		compiled.where,
		compiled.orderBy,
	);
	if (plan === undefined) return undefined;
	const chosen = built.find((idx) => idx.name === plan.index)!;

	// The overlay first, so its size bounds how far the confirmed walk has to
	// go: only an overlay row can displace a confirmed one from the result.
	const oPrefix = overlayWsPrefix(def.name, workspaceId);
	const shadowed = new Set<string>();
	const overlayRows: IndexedRow<MessageShape<D>>[] = [];
	for await (const [ k, v ] of tx.scan(oPrefix)) {
		const key = k.slice(oPrefix.length);
		shadowed.add(key);
		const record = v as OverlayRecord;
		if (record.del) continue; // a local tombstone: shadowed and gone
		const decoded = await decodeRow(tx, registry, def, schema, workspaceId, key, record.v);
		if (!matches(decoded.value, compiled.where, schema)) continue;
		if (window && !window.within(decoded.value, key)) continue;
		// The confirmed base version survives an optimistic edit — it is what a
		// compare-and-swap writes back — so it is read rather than zeroed. A row
		// that exists only optimistically has no base, hence 0n. The scan path
		// resolves it the same way.
		const base = await tx.get(confirmedKey(def.name, workspaceId, key)) as
			| ConfirmedRecord
			| undefined;
		overlayRows.push({
			row: { key, value: decoded.value, workspaceId, version: base ? BigInt(base.ver) : 0n },
			bytes: decoded.bytes,
		});
	}

	const budget = candidateBudget(compiled, overlayRows.length, window);
	const prefix = indexPositionPrefix(def.name, chosen.name, chosen.specHash, workspaceId);
	// indexScan yields the KV keys of the records holding each position, which
	// for an index position is always the c/ row it was computed from.
	const rowPrefix = confirmedWsPrefix(def.name, workspaceId);
	const confirmed: IndexedRow<MessageShape<D>>[] = [];
	const seen = new Set<string>();
	let walked = 0;
	// Matches past the page, which `total` still has to account for.
	let beyond = 0;

	for (const range of plan.ranges) {
		const seek = seekRange(plan, range, window);
		for await (
			const cKey of tx.indexScan(
				seek.lo === undefined ? prefix : prefix + seek.lo,
				seek.hi === undefined ? successorOf(prefix) : prefix + seek.hi,
			)
		) {
			if (++walked > MAX_INDEX_CANDIDATES) return undefined;
			// A position of THIS index can only sit on a row of this entity and
			// workspace — that is what its prefix means, and the bounds above are
			// inside it. The guard is a fence against a record that acquired one
			// some other way, not a path the layout can reach.
			if (!cKey.startsWith(rowPrefix)) continue;
			const key = cKey.slice(rowPrefix.length);
			// One row can appear several times: across ranges (an `in` whose
			// values overlap nothing else is disjoint, but the planner does not
			// promise it), and within one range, because an index over a repeated
			// field puts one position per element on the row. The scan path visits
			// each row once and the two must agree.
			if (seen.has(key)) continue;
			seen.add(key);
			// The overlay is authoritative for a row it holds — edited or
			// tombstoned — so the confirmed copy drops out here and reappears,
			// if it still matches, from the overlay stream.
			if (shadowed.has(key)) continue;
			// An ordered plan yields matches in result order, so once enough of
			// them are in hand no later one can reach the page — bounded above by
			// how many the overlay could displace. What follows is still counted:
			// `total` is the whole match set, not the page.
			const settled = budget !== undefined && plan.ordered && confirmed.length >= budget;
			// With nothing left for the row to decide, a position IS a match, so
			// counting the rest of the range costs no row reads at all. A residual
			// predicate takes that away — the row is the only thing that can
			// settle it — which is the price of reporting a total here.
			if (settled && plan.residual === undefined) {
				beyond++;
				continue;
			}
			const entry = await readConfirmed(tx, registry, def, schema, workspaceId, key);
			// A position and its row are one record, so a position cannot outlive
			// its row — and a reader holds the shared lock, so no commit lands
			// between the index read and this one. The guard stays because
			// answering from a row that is not there would be silent and wrong.
			if (entry === undefined) continue;
			if (!matches(entry.row.value, plan.residual, schema)) continue;
			// The seek bound deliberately drops the cursor's row-key half, so
			// the boundary position is inside the range and this is what rejects
			// it — the seek narrows, the comparison decides.
			if (window && !window.within(entry.row.value, key)) continue;
			if (settled) beyond++;
			else confirmed.push(entry);
		}
	}

	const out = [ ...confirmed, ...overlayRows ];
	if (compiled.orderBy && compiled.orderBy.length > 0) {
		const orderBy = compiled.orderBy;
		out.sort((a, b) => compareRows(a.row, b.row, orderBy, schema));
	} else {
		// No ordering: the scan path returns rows in key order, so this must too.
		out.sort((a, b) => (a.row.key < b.row.key ? -1 : a.row.key > b.row.key ? 1 : 0));
	}
	const total = out.length + beyond;
	if (compiled.limit !== undefined && window?.backward) {
		// A backward page takes its limit from the END of the bounded range.
		// `beyond` is always 0 here — candidateBudget declines to settle early
		// for one — so `out` is the whole match set and the tail of it is the
		// page.
		return out.length > compiled.limit
			? { rows: out.slice(out.length - compiled.limit), total, hasMore: true }
			: { rows: out, total, hasMore: false };
	}
	const start = compiled.offset ?? 0;
	const end = compiled.limit !== undefined ? start + compiled.limit : undefined;
	return {
		rows: start !== 0 || end !== undefined ? out.slice(start, end) : out,
		total,
		hasMore: end !== undefined && total > end,
	};
}

/**
 * Narrows one range of an ordered plan to the part of it the page's cursors can
 * still contain. It returns the range unchanged for a plan that is not ordered,
 * for a plan with several ranges (which have no shared prefix to compose with),
 * and for a query with no cursors.
 *
 * The bound is the plan's equality prefix followed by the cursor's components,
 * whose row-key half is dropped on purpose: an index may carry components past
 * the order keys, and a bound running on into the row key would not be a prefix
 * of any entry in such an index. The boundary position it therefore admits is
 * rejected by the exact comparison in the walk.
 */
function seekRange(plan: QueryPlan, range: IndexRange, window?: PageWindow): IndexRange {
	if (
		!plan.ordered || plan.ranges.length !== 1 || plan.prefix === undefined || window === undefined
	) {
		return range;
	}
	let { lo, hi } = range;
	if (window.seek.after !== undefined) {
		// Inclusive, because rows tying with the cursor on every order key sit
		// at exactly this position and some of them sort after it.
		const at = plan.prefix + window.seek.after;
		if (lo === undefined || lo < at) lo = at;
	}
	if (window.seek.before !== undefined) {
		// upperBound rather than the position itself, for the mirror reason: the
		// tie group at the boundary has to stay inside an exclusive end.
		const at = upperBound(plan.prefix + window.seek.before);
		if (at !== undefined && (hi === undefined || hi > at)) hi = at;
	}
	return { lo, hi };
}

/**
 * How many confirmed matches suffice, or undefined for "all of them".
 *
 * Only a limit bounds the work. With one, a confirmed row past
 * `offset + limit + overlayCount` can only reach the page if more than
 * `overlayCount` rows ahead of it disappear — and only the overlay can remove
 * one, by shadowing it. So that many is always enough.
 */
function candidateBudget(
	compiled: CompiledQuery,
	overlayCount: number,
	window: PageWindow | undefined,
): number | undefined {
	if (compiled.limit === undefined) return undefined;
	// A backward page wants the LAST matches of the range, so the first ones the
	// ordered walk produces are exactly the wrong ones to settle on. The seek
	// still narrows the range; only the early stop is off.
	if (window?.backward) return undefined;
	return (compiled.offset ?? 0) + compiled.limit + overlayCount;
}

/** The least key sorting above every key with this prefix. */
function successorOf(prefix: string): string | undefined {
	for (let i = prefix.length - 1; i >= 0; i--) {
		const c = prefix.charCodeAt(i);
		if (c !== 0xffff) return prefix.slice(0, i) + String.fromCharCode(c + 1);
	}
	return undefined;
}

async function readConfirmed<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	workspaceId: string,
	key: string,
): Promise<IndexedRow<MessageShape<D>> | undefined> {
	const record = await tx.get(confirmedKey(def.name, workspaceId, key)) as
		| ConfirmedRecord
		| undefined;
	if (record === undefined) return undefined;
	const decoded = await decodeRow(tx, registry, def, schema, workspaceId, key, record.v);
	return {
		row: { key, value: decoded.value, workspaceId, version: BigInt(record.ver) },
		bytes: decoded.bytes,
	};
}

/**
 * Decodes one row the way the scan path does, collaborative-text fold
 * included.
 *
 * Text fields cannot be index COMPONENTS, but they are still part of the row a
 * query returns — and their local value is the stored document plus every
 * unacked edit in the `ot/` log. Skipping the fold here would return text with
 * the user's own keystrokes missing, which is the divergence this whole file
 * exists to avoid. The fold is per returned row rather than per entity row, so
 * it costs O(result).
 */
async function decodeRow<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	workspaceId: string,
	key: string,
	bytes: Uint8Array,
): Promise<{ value: MessageShape<D>; bytes: Uint8Array }> {
	const folded = await foldTextLog(tx, registry, def.name, workspaceId, key, bytes);
	const value = fromBinary(schema, folded);
	sortOrderedFields(schema, value);
	return { value, bytes: folded };
}

const matches = (value: unknown, cond: Cond | undefined, schema: DescMessage): boolean =>
	cond === undefined || evalCond(value, cond, schema);

/**
 * Throws when an index-served result differs from the scan's. The comparison is
 * exact — same rows, same order, same versions — because that equality IS the
 * contract; anything weaker would pass while the index quietly answered a
 * different question.
 *
 * Only reachable under the debug flag; see SyncClientOptions.debugVerifyIndexReads.
 */
export function assertSameResult(
	indexed: { rows: readonly Row<unknown>[]; total: number },
	scanned: { rows: readonly Row<unknown>[]; total: number },
	entity: string,
): void {
	const describe = (page: { rows: readonly Row<unknown>[]; total: number }): string =>
		`${page.rows.map((r) => `${r.workspaceId}/${r.key}@${r.version}`).join(', ')}] of ${page.total}`;
	const differs = indexed.total !== scanned.total
		|| indexed.rows.length !== scanned.rows.length
		|| indexed.rows.some((row, i) =>
			row.key !== scanned.rows[i]!.key
			|| row.workspaceId !== scanned.rows[i]!.workspaceId
			|| row.version !== scanned.rows[i]!.version
		);
	if (differs) {
		throw new Error(
			`index-served query on "${entity}" diverged from the scan:\n`
				+ `  index: [${describe(indexed)}\n`
				+ `  scan:  [${describe(scanned)}\n`
				+ 'An index is an optimization; the scan is the definition.',
		);
	}
}
