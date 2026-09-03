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
import type { QueryDeclineReason } from '../sync/events.js';
import type { CompiledQuery, Cond, PageWindow, Path } from './ast.js';
import { compareRows, evalCond, type Matcher, matcherOf } from './evaluate.js';
import type {
	CompiledGroupQuery,
	CompiledGroupRowsQuery,
	Group,
	GroupValue,
	RowGroup,
} from './group.js';
import { GroupBuckets, groupValuesOf } from './group.js';
import { IndexEncoder, upperBound } from './indexkey.js';
import type { GroupPlan, IndexRange, QueryPlan } from './plan.js';
import { planGroup, planGroupRows, planQuery } from './plan.js';
import { requiredMatchCond, searchOverflowRange, searchTermRange } from './searchindex.js';

/**
 * How many ROWS one query may read through an index. An index narrows the
 * candidates; a residual predicate the range could not express still rejects
 * some of them, and in the worst case rejects nearly all — at which point the
 * walk has become a scan with extra reads. Past this the query falls back
 * rather than failing, because an optimization that gives up must not change
 * the answer.
 *
 * Rows, not positions: an ordered plan that has settled its page keeps walking
 * only to COUNT what remains (see `beyond`), which reads keys in bulk and no
 * rows. Charging that walk here would turn the most selective indexed query on
 * a large entity into a permanent full scan the moment its range outgrew the
 * ceiling.
 */
const MAX_INDEX_CANDIDATES = 10000;

/**
 * Candidate rows one getMany reads at a time. Large enough that a typical page
 * costs one batch, small enough that the reads a settled walk turns out not to
 * have needed stay bounded by one batch.
 */
const READ_BATCH = 64;

/**
 * The candidate counts a search's seed pass offers a term, in order. They grow
 * geometrically so the total read stays within a constant factor of the
 * winning term's posting list, and they start small because the term a search
 * is worth serving from usually has a short one. The server's mirror is
 * searchSeedCaps.
 */
const SEARCH_SEED_CAPS = [ 64, 512, 4096 ];

/**
 * How many index positions a narrowing pass will read per candidate it could
 * remove — the point where the reads it costs stop being cheaper than the
 * candidate re-checks it saves.
 *
 * That is a property of the STORE, not of the query, which is why this number
 * is smaller than the server's searchNarrowRatio: stepping an IndexedDB cursor
 * and reading a row out of it are within a small factor of each other, where
 * on the server a point read costs many index steps. The two sides are free to
 * disagree — a candidate set is a superset that every side re-checks in full,
 * so a walk that narrowed less still answers identically.
 */
const SEARCH_NARROW_RATIO = 4;

/**
 * Cap on the shadowed rows whose confirmed records a query reads so the count
 * shortcut can subtract their positions (see countAssist below). Past it a
 * point read per pending row rivals the range walk the shortcut avoids, and
 * the walk needs no cap to stay correct.
 */
const SHADOW_COUNT_CAP = 2048;

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

/**
 * Where a read's rows came from, for a caller that reports it (see QueryEvent
 * in sync/events.ts).
 *
 * queryByIndex writes through the object it is handed rather than returning
 * one, so `undefined` keeps its single meaning of "scan instead". Exactly one
 * of `index` and `declined` is set when it returns; a caller that declines
 * before calling — a watch with no query, an unknown entity — sets `declined`
 * itself.
 */
export interface IndexReport {
	index?: string;
	declined?: QueryDeclineReason;
	/** Index positions walked; 0 unless an index was actually consulted. */
	candidates: number;
}

/**
 * One group of an index-served grouped row read: the public shape with each row
 * against the bytes it was decoded from, so a caller holding rows across reads
 * can tell whether one moved (see IndexedRow).
 */
export interface RowGroupPage<T> extends Omit<RowGroup<T>, 'rows'> {
	rows: IndexedRow<T>[];
}

/** An index-served page, with the matches it was cut from. */
export interface IndexedPage<T> {
	rows: IndexedRow<T>[];
	/** Matches before the page was cut; see RowSet.total. Undefined exactly
	 * when the query carried `total: false`. */
	total: number | undefined;
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
	report?: IndexReport,
): Promise<IndexedPage<MessageShape<D>> | undefined> {
	// A cross-workspace query would need a k-way merge whose tie-break differs
	// from the scan path's; see the header.
	if (workspaceId === undefined) return decline(report, 'cross-workspace');
	if (indexes.length === 0) return decline(report, 'no-index');

	const built = (await Promise.all(
		indexes.map(async (idx) =>
			(await tx.has(metaIndexBuilt(workspaceId, def.name, idx.name, idx.specHash)))
				? idx
				: undefined
		),
	)).filter((idx): idx is CompiledIndex => idx !== undefined);
	if (built.length === 0) return decline(report, 'not-built');

	// A required match narrows candidates harder than any value range, so the
	// term-postings walk goes first. A term-less match (which matches
	// every row) narrows nothing, and an unbuilt search index simply falls
	// through to the planner — which cannot serve a match and declines to the
	// scan.
	const matchCond = compiled.where === undefined ? undefined : requiredMatchCond(compiled.where);
	if (matchCond !== undefined) {
		const searchIdx = built.find((idx) => idx.search !== undefined);
		if (searchIdx !== undefined) {
			const matcher = matcherOf(matchCond, schema);
			if (matcher.terms.length > 0) {
				return querySearchIndex(
					tx,
					registry,
					def,
					schema,
					searchIdx,
					matcher,
					compiled,
					workspaceId,
					window,
					report,
				);
			}
		}
	}

	const plan = planQuery(
		built.filter((idx) => idx.encoder !== undefined)
			.map((idx) => ({ name: idx.name, encoder: idx.encoder! })),
		compiled.where,
		compiled.orderBy,
	);
	if (plan === undefined) return decline(report, 'no-plan');
	// A union plan reads one set of ranges per branch, each from its own
	// index; a single plan is one branch of itself. Every index a branch
	// names came from `built`, because those were the planner's candidates.
	const branches = plan.union ?? [ { index: plan.index, ranges: plan.ranges } ];

	// The overlay first, so its size bounds how far the confirmed walk has to
	// go: only an overlay row can displace a confirmed one from the result.
	const oPrefix = overlayWsPrefix(def.name, workspaceId);
	const shadowed = new Set<string>();
	const kept: { key: string; value: MessageShape<D>; bytes: Uint8Array }[] = [];
	for await (const [ k, v ] of tx.scan(oPrefix)) {
		const key = k.slice(oPrefix.length);
		shadowed.add(key);
		const record = v as OverlayRecord;
		if (record.del) continue; // a local tombstone: shadowed and gone
		const decoded = await decodeRow(tx, registry, def, schema, workspaceId, key, record.v);
		if (!matches(decoded.value, compiled.where, schema)) continue;
		if (window && !window.within(decoded.value, key)) continue;
		kept.push({ key, ...decoded });
	}
	// Whether the count shortcut below can survive the overlay: a shadowed
	// row's confirmed positions are still in the store's index — the overlay
	// does not touch the confirmed record — so the count can keep them out by
	// subtracting the positions the records themselves carry (ConfirmedRecord.ix,
	// the field the store's index is built from). That takes every shadowed
	// row's confirmed record, not only the matching ones, and is capped for it
	// (SHADOW_COUNT_CAP).
	const wantTotal = compiled.total !== false;
	const countAssist = wantTotal
		&& shadowed.size > 0
		&& shadowed.size <= SHADOW_COUNT_CAP
		&& compiled.limit !== undefined
		&& !window?.backward
		&& plan.ordered
		&& plan.residual === undefined
		&& (window === undefined || window.seek.before === undefined)
		&& plan.ranges.length === 1
		&& plan.distinctPerRange;
	// The confirmed base version survives an optimistic edit — it is what a
	// compare-and-swap writes back — so it is read rather than zeroed. A row
	// that exists only optimistically has no base, hence 0n. The scan path
	// resolves it the same way. One getMany rather than a get per row: the
	// bases are per overlay row, and a well-fed pending queue would otherwise
	// turn them into a queue-sized run of point reads on every query. Under
	// countAssist the read widens from the matching overlay rows to every
	// shadowed one, because the subtraction needs each row's positions.
	const baseKeys = countAssist ? [ ...shadowed ] : kept.map((c) => c.key);
	const baseRecords = await tx.getMany(
		baseKeys.map((k) => confirmedKey(def.name, workspaceId, k)),
	);
	const bases = new Map<string, ConfirmedRecord>();
	for (let i = 0; i < baseKeys.length; i++) {
		const record = baseRecords[i] as ConfirmedRecord | undefined;
		if (record !== undefined) bases.set(baseKeys[i]!, record);
	}
	const overlayRows: IndexedRow<MessageShape<D>>[] = kept.map((c) => {
		const base = bases.get(c.key);
		return {
			row: { key: c.key, value: c.value, workspaceId, version: base ? BigInt(base.ver) : 0n },
			bytes: c.bytes,
		};
	});

	const budget = candidateBudget(compiled, overlayRows.length, window);
	// indexScan yields the KV keys of the records holding each position, which
	// for an index position is always the c/ row it was computed from.
	const rowPrefix = confirmedWsPrefix(def.name, workspaceId);
	const confirmed: IndexedRow<MessageShape<D>>[] = [];
	const seen = new Set<string>();
	let walked = 0;
	// Row reads so far — what MAX_INDEX_CANDIDATES bounds; see its comment.
	let reads = 0;
	// Matches past the page, which `total` still has to account for.
	let beyond = 0;
	// Flushed rows the page window rejected — the positions at the `after`
	// boundary its seek could not leave out: the boundary row alone when the
	// bound carried the row key, its whole tie group otherwise (see seekRange).
	// The remainder count has to subtract them: they are positions of the
	// range without being matches of the query.
	let windowRejected = 0;
	// Whether every position past a settled walk is one distinct live match,
	// which is what lets the walk count the remainder in one indexCount request
	// instead of walking it. Each condition removes a way that could fail: a
	// residual rejects rows, an unpinned fan-out gives one row several
	// positions, and a second range can repeat a row the first already yielded.
	// A shadowed row is answered by the overlay instead — survivable only when
	// countAssist read the positions the count must take back out.
	//
	// A forward page window is compatible: its seek can admit positions at the
	// AFTER boundary that the page excludes, but those sit at the start of the
	// range, and a settled walk is provably past them — settling takes `budget`
	// flushed rows that passed `within`, and within one workspace index order
	// agrees with the comparator, so every position after an accepted one is
	// accepted. A `before` bound breaks that: whatever its seek keeps inside the
	// range sits at the END, in exactly the remainder the count would claim.
	const countable = plan.residual === undefined
		&& (window === undefined || window.seek.before === undefined)
		&& (shadowed.size === 0 || countAssist)
		&& plan.ranges.length === 1
		&& plan.distinctPerRange;
	// Candidate row keys not yet read. Rows are fetched in getMany batches — one
	// backend transaction per batch rather than per row — and classified when the
	// batch flushes, in position order, so the outcome per row is the same as
	// reading them one at a time.
	let batch: string[] = [];
	const flush = async (): Promise<void> => {
		if (batch.length === 0) return;
		const keys = batch;
		batch = [];
		for (const entry of await readConfirmedMany(tx, registry, def, schema, workspaceId, keys)) {
			// A position and its row are one record, so a position cannot outlive
			// its row — and a reader holds the shared lock, so no commit lands
			// between the index read and this one. The guard stays because
			// answering from a row that is not there would be silent and wrong.
			if (entry === undefined) continue;
			if (!matches(entry.row.value, plan.residual, schema)) continue;
			// A seek bound only ever narrows: it leaves the boundary row inside
			// the range, and on an index it could not carry the row key into,
			// that row's whole tie group with it. This is what rejects them —
			// the seek narrows, the comparison decides.
			if (window && !window.within(entry.row.value, entry.row.key)) {
				windowRejected++;
				continue;
			}
			if (budget !== undefined && plan.ordered && confirmed.length >= budget) beyond++;
			else confirmed.push(entry);
		}
	};

	ranges: for (const br of branches) {
		const branchIdx = built.find((idx) => idx.name === br.index)!;
		const prefix = indexPositionPrefix(def.name, branchIdx.name, branchIdx.specHash, workspaceId);
		for (const range of br.ranges) {
			const seek = seekRange(plan, range, window);
			const lo = seek.lo === undefined ? prefix : prefix + seek.lo;
			const hi = seek.hi === undefined ? successorOf(prefix) : prefix + seek.hi;
			for await (const cKey of tx.indexScan(lo, hi)) {
				walked++;
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
				// how many the overlay could displace. A batched key is in hand only
				// when nothing left can reject its row; with a residual or a window
				// the row still decides, so only flushed rows count.
				const inHand = confirmed.length
					+ (plan.residual === undefined && window === undefined ? batch.length : 0);
				const settled = budget !== undefined && plan.ordered && inHand >= budget;
				// With nothing left for the row to decide, a position IS a match, so
				// counting the rest of the range costs no row reads at all. A residual
				// predicate takes that away — the row is the only thing that can
				// settle it — which is the price of reporting a total here.
				if (settled && plan.residual === undefined) {
					// With the total opted out the walk only owes hasMore, and this
					// position settles it: not seen, not shadowed, no residual left to
					// reject its row — one distinct live match past the page. A `before`
					// bound is the exception (what its seek keeps at the end could
					// still fail `within`), so that shape keeps walking.
					if (!wantTotal && (window === undefined || window.seek.before === undefined)) {
						beyond = 1;
						break ranges;
					}
					if (countable) {
						// Shadowed rows' confirmed positions are in the count — whether
						// the walk already skipped them or never reached them — without
						// being matches. Each one inside the counted range comes back
						// out; under distinctPerRange that is at most one per row,
						// exactly what the walk would have skipped.
						let shadowedInRange = 0;
						for (const record of bases.values()) {
							for (const p of record.ix ?? []) {
								if (p >= lo && (hi === undefined || p < hi)) shadowedInRange++;
							}
						}
						// Assigned, not accumulated: the count covers every position past
						// the ones in hand, including any a too-large flush has already
						// classified as beyond — adding would count those twice. The
						// flush below still classifies the in-hand tail, and either way
						// the total comes out as the range's position count, less the
						// boundary positions a page window rejected and the shadowed
						// positions the overlay answers for.
						beyond = (await tx.indexCount(lo, hi))
							- confirmed.length - batch.length - windowRejected - shadowedInRange;
						break ranges;
					}
					beyond++;
					continue;
				}
				if (++reads > MAX_INDEX_CANDIDATES) return decline(report, 'budget', walked);
				batch.push(key);
				if (batch.length >= READ_BATCH) await flush();
			}
		}
	}
	await flush();

	if (report) {
		report.index = [ ...new Set(branches.map((b) => b.index)) ].join('+');
		report.candidates = walked;
	}

	const out = [ ...confirmed, ...overlayRows ];
	if (compiled.orderBy && compiled.orderBy.length > 0) {
		const orderBy = compiled.orderBy;
		out.sort((a, b) => compareRows(a.row, b.row, orderBy, schema));
	} else {
		// No ordering: the scan path returns rows in key order, so this must too.
		out.sort((a, b) => (a.row.key < b.row.key ? -1 : a.row.key > b.row.key ? 1 : 0));
	}
	// `matched` still counts what the walk classified even when the total is
	// opted out — hasMore rides on it, and the fast exit above left `beyond` at
	// 1 for exactly that comparison.
	const matched = out.length + beyond;
	const total = wantTotal ? matched : undefined;
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
		hasMore: end !== undefined && matched > end,
	};
}

/**
 * Answers a GROUPED read from an index, or returns undefined to mean "bucket a
 * scan instead" — the mirror of the server's groupRows index half.
 *
 * The walk seeks to the first position of the range, cuts it at the end of the
 * partition component (the prefix every position of that group shares), does
 * the group's work inside [prefix, prefix⁺), and seeks to prefix⁺ — the next
 * distinct value. Groups therefore come out in value order, so a limited read
 * stops after limit+1 of them rather than bucketing the whole entity.
 *
 * An entity with ANYTHING in the overlay declines outright. Every local read
 * owes the overlay merge, and here that would mean subtracting each shadowed
 * row's confirmed positions from whichever group they fall in and adding the
 * pending rows' own groups back — for a set the pending queue already bounds,
 * which is what makes the scan the honest answer while a write is in flight.
 */
export async function groupByIndex<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	indexes: CompiledIndex[],
	compiled: CompiledGroupQuery,
	multi: boolean,
	workspaceId: string | undefined,
	report?: IndexReport,
): Promise<{ groups: Group[]; hasMore: boolean } | undefined> {
	// Positions are keyed per workspace, so a cross-workspace read would have to
	// merge one walk per workspace; the scan does that for free.
	if (workspaceId === undefined) return decline(report, 'cross-workspace');
	if (indexes.length === 0) return decline(report, 'no-index');
	const built = (await Promise.all(
		indexes.map(async (idx) =>
			(await tx.has(metaIndexBuilt(workspaceId, def.name, idx.name, idx.specHash)))
				? idx
				: undefined
		),
	)).filter((idx): idx is CompiledIndex => idx !== undefined);
	if (built.length === 0) return decline(report, 'not-built');
	for await (const _ of tx.scan(overlayWsPrefix(def.name, workspaceId))) {
		return decline(report, 'no-plan');
	}
	const plan = planGroup(
		built.filter((idx) => idx.encoder !== undefined)
			.map((idx) => ({ name: idx.name, encoder: idx.encoder! })),
		compiled.where,
		compiled.by,
		multi,
	);
	if (plan === undefined) return decline(report, 'no-plan');
	const chosen = built.find((idx) => idx.name === plan.index)!;
	const encoder = chosen.encoder!;

	const prefix = indexPositionPrefix(def.name, chosen.name, chosen.specHash, workspaceId);
	const rowPrefix = confirmedWsPrefix(def.name, workspaceId);
	// A position may stand in for a row only when nothing left can reject it: the
	// range answers the predicate whole, and each position inside a group belongs
	// to a distinct row.
	const countable = plan.residual === undefined && plan.positionsAreRows;
	const buckets = new GroupBuckets(compiled.distinct);
	// One past the limit: the extra group is what reports that more exist,
	// exactly as the extra row does for a page.
	const want = compiled.limit === undefined ? undefined : compiled.limit + 1;
	let walked = 0;
	let lo = prefix + (plan.range.lo ?? '');
	let hi = plan.range.hi === undefined ? successorOf(prefix) : prefix + plan.range.hi;
	while (want === undefined || buckets.size < want) {
		const at = await tx.indexSeek(lo, hi, plan.desc ? 'desc' : 'asc');
		if (at === undefined) break;
		let group: string;
		try {
			group = encoder.prefixOf(at.position.slice(prefix.length), plan.partition);
		} catch {
			// Positions that are not entries of this index: the store disagrees
			// with the declaration, and only the scan can say what the groups are.
			return decline(report, 'no-plan', walked);
		}
		const above = upperBound(group);
		const from = maxKey(prefix + group, lo);
		const until = above === undefined ? hi : minKey(prefix + above, hi);
		const args = {
			tx,
			registry,
			def,
			schema,
			by: compiled.by,
			encoder,
			plan,
			workspaceId,
			rowPrefix,
		};
		const got = countable
			? await countGroup(args, group, from, until, at.key)
			: await walkGroup(args, group, from, until, compiled.distinct);
		if (got === undefined) return decline(report, 'budget', walked);
		walked += got.walked;
		if (walked > MAX_INDEX_CANDIDATES) return decline(report, 'budget', walked);
		if (got.value !== undefined) buckets.add(got.value, got.count);
		if (plan.desc) {
			hi = prefix + group;
		} else {
			if (above === undefined) break;
			lo = prefix + above;
		}
	}
	if (report) {
		report.index = chosen.name;
		report.candidates = walked;
	}
	return buckets.result(compiled.limit);
}

/**
 * One group whose positions ARE its matching rows: the number comes from the
 * store's count of the range, and the only row read is the one that names the
 * group.
 *
 * Undefined means the position no longer describes the row it points at, which
 * a rebuilt index makes possible and which the scan is the only thing that can
 * resolve.
 */
async function countGroup<D extends DescMessage>(
	at: GroupWalk<D>,
	group: string,
	lo: string,
	hi: string | undefined,
	nameKey: string,
): Promise<GroupContribution | undefined> {
	const count = await at.tx.indexCount(lo, hi);
	if (count === 0) return { count: 0, walked: 1 };
	const [ row ] = await readConfirmedMany(at.tx, at.registry, at.def, at.schema, at.workspaceId, [
		nameKey.slice(at.rowPrefix.length),
	]);
	if (row === undefined) return undefined;
	const value = groupValueAt(row.row.value, at, group);
	return value === undefined ? undefined : { value, count, walked: 2 };
}

/**
 * One group whose rows still have to be read — because a residual can reject
 * them, or because a component past the partition puts one row at several of
 * its positions.
 *
 * A distinct read stops at the first row that survives: it owes a value and no
 * number, so the rest of the group is work nothing would look at.
 */
async function walkGroup<D extends DescMessage>(
	at: GroupWalk<D>,
	group: string,
	lo: string,
	hi: string | undefined,
	distinct: boolean,
): Promise<GroupContribution | undefined> {
	const keys: string[] = [];
	const seen = new Set<string>();
	let walked = 0;
	for await (const cKey of at.tx.indexScan(lo, hi)) {
		walked++;
		if (walked > MAX_INDEX_CANDIDATES) return undefined;
		if (!cKey.startsWith(at.rowPrefix)) continue;
		const key = cKey.slice(at.rowPrefix.length);
		// One row can hold several positions of a group when a component past the
		// partition fans out; a scan visits each row once and the two must agree.
		if (seen.has(key)) continue;
		seen.add(key);
		keys.push(key);
	}
	let value: GroupValue | undefined;
	let count = 0;
	for (let i = 0; i < keys.length; i += READ_BATCH) {
		const batch = keys.slice(i, i + READ_BATCH);
		const rows = await readConfirmedMany(
			at.tx,
			at.registry,
			at.def,
			at.schema,
			at.workspaceId,
			batch,
		);
		for (const entry of rows) {
			if (entry === undefined) continue;
			if (!matches(entry.row.value, at.plan.residual, at.schema)) continue;
			value ??= groupValueAt(entry.row.value, at, group);
			count++;
		}
		if (value !== undefined && distinct) break;
	}
	if (count === 0) return { count: 0, walked };
	// A group with matching rows that none of them describes means the positions
	// no longer match the rows; only the scan can settle that.
	return value === undefined ? undefined : { value, count, walked };
}

/**
 * Answers a grouped ROW read from an index, or returns undefined to mean
 * "bucket a scan instead" — the mirror of the server's groupRows index half.
 *
 * The skip-scan is groupByIndex's, with each group's work changed: inside one
 * group's sub-range the positions are already in the requested order — that is
 * what planGroupRows declines an index for not supplying — so the walk reads
 * `perGroup + 1` rows and moves on. A group that reports its total pays for the
 * rest of its positions; one that does not stops at the row that answers
 * `hasMore`.
 *
 * An entity with ANYTHING in the overlay declines outright, for groupByIndex's
 * reason: merging pending writes into a per-group page means finding which
 * groups they moved between, over a set the pending queue already bounds.
 */
export async function groupRowsByIndex<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	indexes: CompiledIndex[],
	compiled: CompiledGroupRowsQuery,
	multi: boolean,
	workspaceId: string | undefined,
	report?: IndexReport,
): Promise<{ groups: RowGroupPage<MessageShape<D>>[]; hasMore: boolean } | undefined> {
	if (workspaceId === undefined) return decline(report, 'cross-workspace');
	if (indexes.length === 0) return decline(report, 'no-index');
	const built = (await Promise.all(
		indexes.map(async (idx) =>
			(await tx.has(metaIndexBuilt(workspaceId, def.name, idx.name, idx.specHash)))
				? idx
				: undefined
		),
	)).filter((idx): idx is CompiledIndex => idx !== undefined);
	if (built.length === 0) return decline(report, 'not-built');
	for await (const _ of tx.scan(overlayWsPrefix(def.name, workspaceId))) {
		return decline(report, 'no-plan');
	}
	const plan = planGroupRows(
		built.filter((idx) => idx.encoder !== undefined)
			.map((idx) => ({ name: idx.name, encoder: idx.encoder! })),
		compiled.where,
		compiled.by,
		multi,
		compiled.orderBy,
	);
	if (plan === undefined) return decline(report, 'no-plan');
	const chosen = built.find((idx) => idx.name === plan.index)!;
	const encoder = chosen.encoder!;

	const prefix = indexPositionPrefix(def.name, chosen.name, chosen.specHash, workspaceId);
	const rowPrefix = confirmedWsPrefix(def.name, workspaceId);
	const groups: RowGroupPage<MessageShape<D>>[] = [];
	// One past the limit: the extra group is what reports that more exist.
	const want = compiled.limit === undefined ? undefined : compiled.limit + 1;
	let walked = 0;
	let lo = prefix + (plan.range.lo ?? '');
	let hi = plan.range.hi === undefined ? successorOf(prefix) : prefix + plan.range.hi;
	while (want === undefined || groups.length < want) {
		const at = await tx.indexSeek(lo, hi, plan.desc ? 'desc' : 'asc');
		if (at === undefined) break;
		let group: string;
		try {
			group = encoder.prefixOf(at.position.slice(prefix.length), plan.partition);
		} catch {
			// Positions that are not entries of this index: the store disagrees
			// with the declaration, and only the scan can say what the groups are.
			return decline(report, 'no-plan', walked);
		}
		const above = upperBound(group);
		const from = maxKey(prefix + group, lo);
		const until = above === undefined ? hi : minKey(prefix + above, hi);
		const args = {
			tx,
			registry,
			def,
			schema,
			by: compiled.by,
			encoder,
			plan,
			workspaceId,
			rowPrefix,
		};
		const got = await walkGroupRows(args, compiled, group, from, until);
		if (got === undefined) return decline(report, 'budget', walked);
		walked += got.walked;
		if (walked > MAX_INDEX_CANDIDATES) return decline(report, 'budget', walked);
		if (got.group !== undefined) groups.push(got.group);
		if (plan.desc) {
			hi = prefix + group;
		} else {
			if (above === undefined) break;
			lo = prefix + above;
		}
	}
	if (report) {
		report.index = chosen.name;
		report.candidates = walked;
	}
	return compiled.limit !== undefined && groups.length > compiled.limit
		? { groups: groups.slice(0, compiled.limit), hasMore: true }
		: { groups, hasMore: false };
}

/**
 * One group's head: the rows its positions name, in position order, which the
 * plan promised is the requested order.
 *
 * Undefined means the walk outran its budget, or that a group holding rows is
 * described by none of them — both of which only the scan can resolve. A group
 * with no matching row comes back without one, which is how a residual that
 * rejects everything in a sub-range makes a group simply not exist.
 */
async function walkGroupRows<D extends DescMessage>(
	at: GroupWalk<D>,
	compiled: CompiledGroupRowsQuery,
	group: string,
	lo: string,
	hi: string | undefined,
): Promise<{ group?: RowGroupPage<MessageShape<D>>; walked: number } | undefined> {
	const wantTotal = compiled.total !== false;
	// How many matching rows settle the answer: the page plus the row that
	// reports there are more. A group that owes a total has no such bound.
	const need = wantTotal ? undefined : compiled.perGroup + 1;
	const rows: IndexedRow<MessageShape<D>>[] = [];
	let value: GroupValue | undefined;
	let total = 0;
	let walked = 0;
	let batch: string[] = [];
	const flush = async (): Promise<void> => {
		if (batch.length === 0) return;
		const keys = batch;
		batch = [];
		for (
			const entry of await readConfirmedMany(
				at.tx,
				at.registry,
				at.def,
				at.schema,
				at.workspaceId,
				keys,
			)
		) {
			if (entry === undefined) continue;
			if (!matches(entry.row.value, at.plan.residual, at.schema)) continue;
			value ??= groupValueAt(entry.row.value, at, group);
			total++;
			if (rows.length < compiled.perGroup) rows.push(entry);
		}
	};
	for await (const cKey of at.tx.indexScan(lo, hi)) {
		walked++;
		if (walked > MAX_INDEX_CANDIDATES) return undefined;
		if (!cKey.startsWith(at.rowPrefix)) continue;
		batch.push(cKey.slice(at.rowPrefix.length));
		// The walk reads ahead by a batch, and `total` only moves during a flush,
		// so sizing the batch to what the group still needs is what keeps it from
		// reading rows nothing will look at. Each position is a distinct row —
		// planGroupRows declines an index where a component past the partition
		// fans out — so nothing here has to dedupe.
		const room = need === undefined ? READ_BATCH : Math.min(READ_BATCH, need - total);
		if (batch.length >= room) {
			await flush();
			if (need !== undefined && total >= need) break;
		}
	}
	await flush();
	if (total === 0) return { walked };
	// A group with matching rows that none of them describes means the positions
	// no longer match the rows; only the scan can settle that.
	if (value === undefined) return undefined;
	return {
		group: {
			value: value.found ? value.value : undefined,
			rows,
			total: wantTotal ? total : undefined,
			hasMore: total > compiled.perGroup,
		},
		walked,
	};
}

/** What one group contributed, and how much of the walk's budget it cost. */
interface GroupContribution {
	value?: GroupValue;
	count: number;
	walked: number;
}

/** Everything one group's work needs, gathered once so the two shapes of it
 * take one argument rather than eleven. */
interface GroupWalk<D extends DescMessage> {
	tx: KVRead;
	registry: EntityRegistry;
	def: EntityDef;
	schema: D;
	/** The path the read partitions on; see groupValueAt. */
	by: Path;
	encoder: IndexEncoder;
	plan: GroupPlan;
	workspaceId: string;
	/** KV-key prefix every confirmed row of this entity and workspace carries,
	 * which is what a position's record key is stripped of. */
	rowPrefix: string;
}

/**
 * Which of a row's group values the walk is standing in: the one whose encoding
 * is what the position prefix ends with.
 *
 * A singular path has exactly one, so the comparison never runs. A repeated one
 * has several, and only the encoding says WHICH element put this row in this
 * group — the position carries the value and the row carries the list.
 */
function groupValueAt<D extends DescMessage>(
	row: unknown,
	at: GroupWalk<D>,
	group: string,
): GroupValue | undefined {
	const values = groupValuesOf(row, at.by, at.schema);
	if (values.length === 1) return values[0];
	const component = at.encoder.components[at.plan.partition - 1]!;
	for (const v of values) {
		if (!v.found) continue;
		try {
			if (group.endsWith(new IndexEncoder([ component ]).encodePrefix([ v.value! ]))) return v;
		} catch {
			// A value this column cannot encode was never written as a position.
		}
	}
	return undefined;
}

const maxKey = (a: string, b: string): string => (a < b ? b : a);
const minKey = (a: string, b: string | undefined): string | undefined =>
	b === undefined ? a : (a < b ? a : b);

/**
 * Answers a query whose predicate carries a required match from the entity's
 * term-postings index — the mirror of the server's queryBySearch, plus the
 * overlay merge every local read owes.
 *
 * Candidates come from the most selective term's postings (a prefix range for
 * a prefix term), narrowed by the other terms where that is cheaper than
 * re-reading rows, unioned with the overflow rows, and minus the rows the
 * overlay shadows; every candidate is re-read and re-checked against the FULL
 * predicate, and overlay rows are evaluated whole — so the result is exactly
 * the scan's. Because candidates are only ever narrowed and never completed,
 * dropping a narrowing pass leaves more rows to re-check rather than fewer
 * rows in the answer, which is what lets both passes bound their reads.
 *
 * No early settling: a relevance ordering is computed per row, so the walk
 * cannot know which candidates the page will keep, and the candidate set is
 * already bounded by the budget it declines over.
 */
async function querySearchIndex<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	idx: CompiledIndex,
	matcher: Matcher,
	compiled: CompiledQuery,
	workspaceId: string,
	window?: PageWindow,
	report?: IndexReport,
): Promise<IndexedPage<MessageShape<D>> | undefined> {
	// The overlay is authoritative for rows it holds — edited or tombstoned —
	// so its keys drop out of the candidate set and matching rows come back
	// from here.
	const oPrefix = overlayWsPrefix(def.name, workspaceId);
	const shadowed = new Set<string>();
	const kept: { key: string; value: MessageShape<D>; bytes: Uint8Array }[] = [];
	for await (const [ k, v ] of tx.scan(oPrefix)) {
		const key = k.slice(oPrefix.length);
		shadowed.add(key);
		const record = v as OverlayRecord;
		if (record.del) continue;
		const decoded = await decodeRow(tx, registry, def, schema, workspaceId, key, record.v);
		if (!matches(decoded.value, compiled.where, schema)) continue;
		if (window && !window.within(decoded.value, key)) continue;
		kept.push({ key, ...decoded });
	}
	const baseRecords = await tx.getMany(
		kept.map((c) => confirmedKey(def.name, workspaceId, c.key)),
	);
	const overlayRows: IndexedRow<MessageShape<D>>[] = kept.map((c, i) => {
		const base = baseRecords[i] as ConfirmedRecord | undefined;
		return {
			row: { key: c.key, value: c.value, workspaceId, version: base ? BigInt(base.ver) : 0n },
			bytes: c.bytes,
		};
	});

	const prefix = indexPositionPrefix(def.name, idx.name, idx.specHash, workspaceId);
	const rowPrefix = confirmedWsPrefix(def.name, workspaceId);
	let walked = 0;
	/**
	 * Reads the rows a posting range names, giving up once it has seen more
	 * than `max` of them — 'wide' — or once the whole walk has outrun its
	 * budget — 'budget'. Only 'ok' means the range was read completely, and
	 * only a completely read range may narrow anything.
	 */
	const scanKeys = async (
		lo: string,
		hi: string | undefined,
		max: number,
		keep: (key: string) => void,
	): Promise<'ok' | 'wide' | 'budget'> => {
		const scanLo = prefix + lo;
		const scanHi = hi === undefined ? successorOf(prefix) : prefix + hi;
		let seen = 0;
		for await (const cKey of tx.indexScan(scanLo, scanHi)) {
			if (++walked > MAX_INDEX_CANDIDATES) return 'budget';
			if (++seen > max) return 'wide';
			if (!cKey.startsWith(rowPrefix)) continue;
			keep(cKey.slice(rowPrefix.length));
		}
		return 'ok';
	};

	/**
	 * Picks the term whose postings bound the walk most tightly, offering each
	 * a growing cap rather than reading any of them whole: the first term to
	 * fit under a cap has the shortest list of them all, to within the ratio
	 * between two caps, and every other term is abandoned as soon as it is
	 * known not to fit. A query of common words therefore costs a bounded read
	 * per term before it declines, instead of reading every list to the budget
	 * to discover the same thing.
	 */
	const seed = async (): Promise<{ keys: Set<string>; at: number } | undefined> => {
		// One term is its own most selective: there is nothing to compare it
		// against, so the rounds would only read the same list twice.
		// The final round is at the budget — the cap that decides whether the
		// walk happens at all — so a cap that reaches it adds nothing.
		const caps = matcher.terms.length === 1
			? [ MAX_INDEX_CANDIDATES ]
			: [ ...SEARCH_SEED_CAPS.filter((c) => c < MAX_INDEX_CANDIDATES), MAX_INDEX_CANDIDATES ];
		for (const max of caps) {
			for (let at = 0; at < matcher.terms.length; at++) {
				const { lo, hi } = searchTermRange(matcher.terms[at]!);
				const keys = new Set<string>();
				const outcome = await scanKeys(lo, hi, max, (key) => keys.add(key));
				if (outcome === 'budget') return undefined;
				if (outcome === 'ok') return { keys, at };
			}
		}
		return undefined;
	};

	const seeded = await seed();
	if (seeded === undefined) return decline(report, 'budget', walked);

	// Narrow with the remaining terms. A row that matches carries every term,
	// so intersecting can only remove candidates — but reading a term's
	// postings to find that out is only worth it while the list is small
	// against the rows it could save re-reading, which is what the cap
	// expresses. A term that blows it is skipped rather than declined.
	let all = seeded.keys;
	for (let at = 0; at < matcher.terms.length; at++) {
		if (at === seeded.at || all.size === 0) continue;
		const { lo, hi } = searchTermRange(matcher.terms[at]!);
		const next = new Set<string>();
		const kept = all;
		const outcome = await scanKeys(
			lo,
			hi,
			Math.min(kept.size * SEARCH_NARROW_RATIO, MAX_INDEX_CANDIDATES),
			(key) => {
				if (kept.has(key)) next.add(key);
			},
		);
		if (outcome === 'budget') return decline(report, 'budget', walked);
		if (outcome === 'ok') all = next;
	}

	// The overflow rows carry no postings to narrow on, so they join whatever
	// the terms left — including an empty intersection.
	const overflow = searchOverflowRange();
	if (
		await scanKeys(overflow.lo, overflow.hi, MAX_INDEX_CANDIDATES, (key) => all.add(key)) !== 'ok'
	) {
		return decline(report, 'budget', walked);
	}

	const keys = [ ...all ].filter((k) => !shadowed.has(k)).sort();
	const confirmed: IndexedRow<MessageShape<D>>[] = [];
	for (let at = 0; at < keys.length; at += READ_BATCH) {
		for (
			const entry of await readConfirmedMany(
				tx,
				registry,
				def,
				schema,
				workspaceId,
				keys.slice(at, at + READ_BATCH),
			)
		) {
			// A position cannot outlive its row (they are one record); the guard
			// stays because answering from a row that is not there would be
			// silent and wrong.
			if (entry === undefined) continue;
			if (!matches(entry.row.value, compiled.where, schema)) continue;
			if (window && !window.within(entry.row.value, entry.row.key)) continue;
			confirmed.push(entry);
		}
	}

	if (report) {
		report.index = idx.name;
		report.candidates = walked;
	}

	const out = [ ...confirmed, ...overlayRows ];
	if (compiled.orderBy && compiled.orderBy.length > 0) {
		const orderBy = compiled.orderBy;
		out.sort((a, b) => compareRows(a.row, b.row, orderBy, schema));
	} else {
		out.sort((a, b) => (a.row.key < b.row.key ? -1 : a.row.key > b.row.key ? 1 : 0));
	}
	const total = compiled.total !== false ? out.length : undefined;
	if (compiled.limit !== undefined && window?.backward) {
		return out.length > compiled.limit
			? { rows: out.slice(out.length - compiled.limit), total, hasMore: true }
			: { rows: out, total, hasMore: false };
	}
	const start = compiled.offset ?? 0;
	const end = compiled.limit !== undefined ? start + compiled.limit : undefined;
	return {
		rows: start !== 0 || end !== undefined ? out.slice(start, end) : out,
		total,
		hasMore: end !== undefined && out.length > end,
	};
}

/** Records why the index was not used and returns the "scan instead" answer. */
function decline(
	report: IndexReport | undefined,
	reason: QueryDeclineReason,
	walked = 0,
): undefined {
	if (report) {
		report.declined = reason;
		report.candidates = walked;
	}
	return undefined;
}

/**
 * Narrows one range of an ordered plan to the part of it the page's cursors can
 * still contain. It returns the range unchanged for a plan that is not ordered,
 * for a plan with several ranges (which have no shared prefix to compose with),
 * and for a query with no cursors.
 *
 * The bound is the plan's equality prefix followed by as much of the cursor as
 * the chosen index can take:
 *
 * - `orderSpansIndex` — the whole entry, row-key tiebreak included, which is a
 *   prefix of the boundary row's entry and of nothing else. The `after` bound
 *   stays INCLUSIVE, so the only position it admits that the page excludes is
 *   the boundary row itself; the `before` bound is the entry exactly, which as
 *   an exclusive end leaves the boundary row outside. Both cost one comparison,
 *   not one per row of the tie group.
 * - otherwise, the components alone: the index carries components past the
 *   order keys, and a bound running on into the row key would not be a prefix
 *   of any entry in it. That admits the whole tie group at the boundary, which
 *   the exact comparison in the walk then rejects.
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
		// at or after this position and some of them sort after it.
		const at = plan.prefix
			+ (plan.orderSpansIndex ? window.seek.after.entry : window.seek.after.components);
		if (lo === undefined || lo < at) lo = at;
	}
	if (window.seek.before !== undefined) {
		// upperBound over the components, for the mirror reason: the tie group at
		// the boundary has to stay inside an exclusive end. The whole entry needs
		// no such widening — it names one row, and that row is excluded.
		const at = plan.orderSpansIndex
			? plan.prefix + window.seek.before.entry
			: upperBound(plan.prefix + window.seek.before.components);
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

/**
 * Reads and decodes a batch of confirmed rows in one getMany. The result is
 * parallel to `keys`; a missing row is undefined in its slot, so a caller
 * classifying entries sees them in position order.
 */
async function readConfirmedMany<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	workspaceId: string,
	keys: readonly string[],
): Promise<(IndexedRow<MessageShape<D>> | undefined)[]> {
	const records = await tx.getMany(keys.map((k) => confirmedKey(def.name, workspaceId, k)));
	const out: (IndexedRow<MessageShape<D>> | undefined)[] = new Array(keys.length);
	for (let i = 0; i < keys.length; i++) {
		const record = records[i] as ConfirmedRecord | undefined;
		if (record === undefined) continue;
		const key = keys[i]!;
		const decoded = await decodeRow(tx, registry, def, schema, workspaceId, key, record.v);
		out[i] = {
			row: { key, value: decoded.value, workspaceId, version: BigInt(record.ver) },
			bytes: decoded.bytes,
		};
	}
	return out;
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
	indexed: { rows: readonly Row<unknown>[]; total: number | undefined },
	scanned: { rows: readonly Row<unknown>[]; total: number | undefined },
	entity: string,
): void {
	const describe = (page: { rows: readonly Row<unknown>[]; total: number | undefined }): string =>
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
