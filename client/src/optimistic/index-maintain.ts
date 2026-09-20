// Secondary index maintenance on the client — the mirror of
// engine/server/index.go, over the KV store instead of a SQL adapter.
//
// The same three properties hold here as on the server, and they are what make
// the rest of the design simple:
//
// - An index is never on the wire. Nothing in a request or response names one,
//   so building, rebuilding or losing one is invisible to the server.
// - Losing one costs a rebuild, never correctness. Positions are recomputable
//   from the confirmed rows at any time.
// - Using one is a pure OPTIMIZATION. Every read that consults an index has a
//   legal scan fallback and the two must produce identical results, which is
//   what lets a read fall back the moment an index is not built.
//
// Only CONFIRMED rows are indexed. The optimistic overlay is bounded by the
// pending queue, so a query scans all of it and merges — cheap, and it keeps
// the answer identical to the merged view without the index tracking
// uncommitted state. It also sidesteps the `ot/` text fold changing an indexed
// value out from under a confirmed-only index, since text fields cannot be
// index components at all.
//
// WHERE THE POSITIONS LIVE, and what follows from it
//
// Positions are a field on a record (INDEX_FIELD, see kv/store.ts), and the KV
// store maintains one secondary index over that field for every backend it has.
// Maintenance is therefore not a set of writes at all — there is nothing to add
// or delete — it is a value computed from the row and handed to a put.
//
// A row splits its positions across two records:
//
// - VALUE-index positions ride on the confirmed row itself
//   (ConfirmedRecord.ix), so they cost no KV record: the put they ride on was
//   happening anyway.
// - SEARCH-index positions sit in the row's `x/` sibling (SearchIndexRecord),
//   written only when the term set actually changes.
//
// The second record is there because a store rewrites every position of a
// record it puts, changed or not, and a searchable row holds one position per
// distinct term — typically an order of magnitude more than its value
// positions. Sharing one record made a change to any field pay for the whole
// term set, and most updates leave the searchable text alone. The price is one
// read and one put on the pull path for a row whose text DID change, and one
// extra put per searchable row during a bootstrap or a build.
//
// That collapses most of what maintenance used to be:
//
// - A DELETE drops the row's value positions with the record. Its `x/` sibling
//   is a separate key and has to be deleted alongside — every path that removes
//   a confirmed row removes both, or an orphaned search record would keep
//   answering for a row that is gone.
// - A REWRITE needs no knowledge of where the row used to be. The position list
//   is replaced wholesale, so nothing has to reproduce the previous value's
//   entries in order to remove them — no per-row pointer, no decode of the bytes
//   being replaced, and no failure mode when those bytes no longer decode.
// - A SUPERSEDED declaration reclaims itself. Its positions sit under a prefix
//   carrying the old spec fingerprint, and the first rewrite of the row drops
//   them along with every other position; the build pass rewrites every row, so
//   a spec change reclaims its own keyspace as it rebuilds.
//
// It also puts one obligation on every writer, which is the price of the layout:
// a row's positions are only as good as the last write of that row. Rewriting a
// confirmed record without recomputing them UNINDEXES the row rather than
// leaving a stale entry behind — and leaving its `x/` sibling alone leaves the
// search postings describing the PREVIOUS text, which is why a writer that can
// change the searchable fields has to decide about both records. Two
// consequences worth stating outright:
//
// - A staged bootstrap swap promotes rows carrying no positions, since giving
//   them any means decoding the whole snapshot inside the transaction that
//   installs it. It claims no index-built marker and asks for a background
//   build (index-build.ts). A cold bootstrap, whose chunks each hold a
//   transaction of their own, does decode and does claim.
// - A build that does not DECLARE an index does not preserve it. Where the
//   `i/` keyspace let one tab leave another build's entries alone, a position
//   list is replaced by whoever writes the row, so two tabs declaring different
//   indexes over one entity invalidate each other's. The previous layout left
//   such an index STALE in the same situation rather than incomplete, which is
//   no more correct; neither is a state the built markers can survive, and the
//   fix for both is that a build only trusts markers it earned itself.

import type { DescMessage } from '@bufbuild/protobuf';

import {
	type ConfirmedRecord,
	indexPositionPrefix,
	searchIndexKey,
	type SearchIndexRecord,
} from '../keys.js';
import type { KVWrite } from '../kv/store.js';
import type { EntityRegistry } from '../mutations.js';
import {
	compileIndexComponent,
	type IndexComponent,
	IndexEncoder,
	specHash,
} from '../query/indexkey.js';
import { type SearchDecl, searchDeclOf } from '../query/match.js';
import {
	SEARCH_INDEX_NAME,
	searchEntries,
	searchSpecHash,
} from '../query/searchindex.js';
import { reportError } from '../report.js';

/**
 * One compiled index of one entity: the name it is stored under, the
 * fingerprint of the declaration its entries were built from, and what
 * produces a row's positions in it — a component encoder for a value index,
 * or the entity's search declaration for the term-postings index (exactly one
 * of the two is set).
 */
export interface CompiledIndex {
	name: string;
	specHash: string;
	encoder?: IndexEncoder;
	search?: SearchDecl;
}

/**
 * Compiles an entity's declared indexes. Both declaration surfaces have already
 * desugared to `EntityDef.indexes` by the time this runs, so there is one shape
 * to handle.
 *
 * A component the encoder rejects — a text field, or a leaf no comparison
 * reaches — drops the WHOLE index rather than the component: a partial index
 * would silently answer a different question than the query that chose it.
 * Registration on the server rejects the same declarations outright, so this
 * only fires for a schema the server never accepted.
 *
 * Dropping one costs performance and nothing else, which is why it is not
 * fatal — but it IS reported, because "every query over this field scans
 * forever" is indistinguishable from a working index without a signal.
 *
 * An index SUBSUMED by another is dropped too, silently — see subsumedBy. That
 * asymmetry in reporting is deliberate: a dropped-because-unusable index makes
 * some query scan forever, while a dropped-because-subsumed one changes no plan
 * at all, so reporting it would be noise.
 */
export function compileIndexes(schema: DescMessage, defs: {
	name: string;
	key: { path: string[]; desc: boolean }[];
}[]): CompiledIndex[] {
	const compiled: CompiledIndex[] = [];
	for (const def of defs) {
		try {
			// One declared key is one component: a `*` fan-out over a map names the
			// side of the entry it stores (`custom.*.key`, `custom.*.value.number`),
			// so the layout is declared rather than derived here.
			const comps = def.key.map((k) => compileIndexComponent(schema, k.path, k.desc));
			compiled.push({
				name: def.name,
				specHash: specHash(comps),
				encoder: new IndexEncoder(comps),
			});
		} catch (err) {
			// Unusable, not fatal: queries scan, which is always correct.
			reportError(
				`secondary index "${def.name}" cannot be maintained; queries over it will scan`,
				err,
			);
		}
	}
	const drop = subsumedBy(compiled.map((c) => c.encoder!.components));
	const kept = compiled.filter((_, i) => !drop[i]!);
	// The search index rides the same list: same markers, same build, same
	// position encoding — it differs only in the record its positions are
	// stored on. Compiled last so subsumption above stays about value indexes,
	// which are the only ones with components to compare.
	try {
		const decl = searchDeclOf(schema);
		if (decl) {
			kept.push({ name: SEARCH_INDEX_NAME, specHash: searchSpecHash(decl), search: decl });
		}
	} catch (err) {
		// Unusable, not fatal: match queries scan, which is always correct.
		reportError('the search index cannot be maintained; match queries will scan', err);
	}
	return kept;
}

/**
 * Which of the given component lists another list already covers, and which
 * therefore need not be maintained.
 *
 * The planner walks an index left to right, consuming equality terms into a
 * fixed prefix and taking at most one range on the component after it — so an
 * index serves every query that constrains a PREFIX of its components, not only
 * one that constrains all of them. An index whose components are a prefix of a
 * longer index's is then redundant: every query it serves, the longer one serves
 * from the same prefix with the same bounds.
 *
 * That pairing is the normal outcome of using both declaration surfaces on one
 * field — you mark the field you filter on, then declare a composite so you can
 * also order within it — which is why this is worth doing rather than a schema
 * smell to leave alone. On the benchmark corpus it removes 42% of the entries.
 *
 * Components match on resolved proto path, direction AND which side of a map
 * entry they store. Agreeing on the path but not on `desc` is not a prefix: the
 * encoding inverts the payload, so the entries interleave differently and the
 * longer index does not reproduce the shorter one's order. The entry side has to
 * be in the signature for a blunter reason — `attrs.*.key` and `attrs.*.value`
 * resolve to the same path, and reading one index's entries as the other's would
 * answer with keys where values were asked for.
 *
 * Two lists that are EQUAL subsume each other, so the tie-break keeps the first
 * — otherwise a duplicated declaration would drop both and leave the queries
 * over it scanning. The rule stays transitive under that tie-break: of a chain
 * of prefixes exactly the longest survives, and of a set of equals exactly the
 * earliest.
 */
function subsumedBy(lists: IndexComponent[][]): boolean[] {
	const sigs = lists.map((comps) =>
		comps.map((c) => `${c.protoPath.join('.')}:${c.desc}:${c.keyComponent}`)
	);
	return sigs.map((sig, i) =>
		sigs.some((other, j) =>
			j !== i
			&& (other.length > sig.length || (other.length === sig.length && j < i))
			&& sig.every((s, n) => other[n] === s)
		)
	);
}

/**
 * The VALUE-index positions one confirmed row occupies, given its decoded
 * value. The search index is not among them — see searchPositions.
 *
 * The order is by index and then by fan-out element, which is what the encoders
 * produce and not a sorted order. Nothing depends on it: the store's index sorts
 * the positions itself, and the list on the record is a set.
 *
 * A row is never absent from an index it is subject to. An index that fans out
 * over a repeated field still yields one position for an empty list, which is
 * what lets a range that does not constrain the fan-out component see every row
 * (see query/indexkey.ts).
 */
export function rowPositions(
	indexes: CompiledIndex[],
	entity: string,
	workspaceId: string,
	key: string,
	value: unknown,
	schema: DescMessage,
): string[] {
	const positions: string[] = [];
	for (const idx of indexes) {
		if (idx.search) continue;
		// An index over a repeated field yields one position per element, so the
		// count is bounded by the data rather than by the number of indexes.
		const entries = idx.encoder!.encode(value, key, schema);
		const prefix = indexPositionPrefix(entity, idx.name, idx.specHash, workspaceId);
		for (const encoded of entries) positions.push(prefix + encoded);
	}
	return positions;
}

/** The entity's search index among its compiled ones, or undefined when it
 * declares no searchable field. */
export function searchIndexOf(indexes: readonly CompiledIndex[]): CompiledIndex | undefined {
	return indexes.find((idx) => idx.search !== undefined);
}

/**
 * The SEARCH-index positions one row occupies, given its decoded value — one
 * per distinct term of its searchable fields, or the single overflow marker
 * past the per-row term cap. Empty for a row whose searchable fields analyze to
 * nothing, which is a row with no `x/` record.
 */
export function searchPositions(
	idx: CompiledIndex,
	entity: string,
	workspaceId: string,
	key: string,
	value: unknown,
	schema: DescMessage,
): string[] {
	const prefix = indexPositionPrefix(entity, idx.name, idx.specHash, workspaceId);
	return searchEntries(idx.search!, value, key, schema).map((entry) => prefix + entry);
}

/**
 * Whether two position lists hold the same positions, order aside.
 *
 * Both sides are duplicate-free (one entry per distinct term), so equal lengths
 * plus containment is set equality. Order is not equality here because the
 * entries come out in first-occurrence order: reordering a sentence changes the
 * list without changing the postings it stands for.
 */
export function samePositions(a: readonly string[], b: readonly string[]): boolean {
	if (a.length !== b.length) return false;
	const set = new Set(a);
	return b.every((p) => set.has(p));
}

/**
 * Writes a row's search-index record, or deletes it when `positions` is empty.
 *
 * Unconditional: a caller holding the stored positions should compare them
 * first (see samePositions), since the store rewrites every position of a
 * record it puts.
 */
export async function writeSearchRecord(
	tx: KVWrite,
	entity: string,
	workspaceId: string,
	key: string,
	positions: string[],
): Promise<void> {
	const k = searchIndexKey(entity, workspaceId, key);
	if (positions.length === 0) await tx.del(k);
	else await tx.put(k, { ix: positions } satisfies SearchIndexRecord);
}

/**
 * `record` with its VALUE-index positions recomputed from `value` — the one
 * place a confirmed row and those positions are tied together, so a writer
 * cannot update one and forget the other. The row's search positions live in a
 * record of their own and are not touched here (see writeSearchRecord).
 *
 * The list is REPLACED, never merged. That is what makes a rewrite self-cleaning
 * — the previous value's positions go with it, and so do any left behind by a
 * declaration that has since changed — and it is why every writer of a confirmed
 * row has to come through here. See the header for what one that does not costs.
 *
 * A row with no positions carries no field at all rather than an empty array:
 * the two are the same to the store, and omitting it keeps the record of an
 * unindexed entity exactly what it was before indexes existed.
 */
export function indexedRecord(
	record: ConfirmedRecord,
	indexes: CompiledIndex[],
	entity: string,
	workspaceId: string,
	key: string,
	value: unknown,
	schema: DescMessage,
): ConfirmedRecord {
	const positions = rowPositions(indexes, entity, workspaceId, key, value, schema);
	if (positions.length > 0) return { ...record, ix: positions };
	if (record.ix === undefined) return record;
	const { ix: _unindexed, ...rest } = record;
	return rest;
}

/** Deletes every key under each prefix — for the meta markers, which are
 * already per workspace and need no indirection. */
export async function deletePrefixes(tx: KVWrite, prefixes: string[]): Promise<void> {
	for (const prefix of prefixes) {
		const keys: string[] = [];
		for await (const [ k ] of tx.scan(prefix)) keys.push(k);
		for (const k of keys) await tx.del(k);
	}
}

/**
 * The indexes of one entity, compiled once and cached. Compiling walks the
 * descriptors, which is wasteful per row write, and the result only changes
 * when the registry does.
 */
export class IndexCache {
	private byEntity = new Map<string, CompiledIndex[]>();

	constructor(private readonly registry: EntityRegistry) {}

	/** The compiled indexes of one entity; empty for an entity with none, and
	 * for an entity the registry does not know. */
	for(entity: string): CompiledIndex[] {
		let compiled = this.byEntity.get(entity);
		if (compiled === undefined) {
			const def = this.registry.get(entity);
			compiled = def ? compileIndexes(def.schema, def.indexes) : [];
			this.byEntity.set(entity, compiled);
		}
		return compiled;
	}

	/** Whether any registered entity declares an index. Lets the write paths
	 * skip index work entirely for a schema that has none. */
	get empty(): boolean {
		for (const name of this.registry.names()) {
			if (this.for(name).length > 0) return false;
		}
		return true;
	}
}
