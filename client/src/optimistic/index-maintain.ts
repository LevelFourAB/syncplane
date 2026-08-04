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
// A row's positions are a field ON the row (ConfirmedRecord.ix), and the KV
// store maintains one secondary index over that field for every backend it has
// (see kv/store.ts INDEX_FIELD). Maintenance is therefore not a set of writes at
// all — there is nothing to add or delete — it is a value computed from the row
// and handed to the same put the row was going through anyway. Indexing a row
// costs zero KV records.
//
// That collapses most of what maintenance used to be:
//
// - A DELETE needs no index work. Dropping the record drops its positions with
//   it, which is also what makes wiping confirmed state, dropping an entity and
//   purging a workspace index-free operations.
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
// leaving a stale entry behind. Two consequences worth stating outright:
//
// - A bootstrap installs rows carrying no positions (it does not decode them),
//   which is why the index-built markers are not claimed at a swap and why the
//   background build exists (index-build.ts).
// - A build that does not DECLARE an index does not preserve it. Where the
//   `i/` keyspace let one tab leave another build's entries alone, a position
//   list is replaced by whoever writes the row, so two tabs declaring different
//   indexes over one entity invalidate each other's. The previous layout left
//   such an index STALE in the same situation rather than incomplete, which is
//   no more correct; neither is a state the built markers can survive, and the
//   fix for both is that a build only trusts markers it earned itself.

import type { DescMessage } from '@bufbuild/protobuf';

import { type ConfirmedRecord, indexPosition } from '../keys.js';
import type { KVWrite } from '../kv/store.js';
import type { EntityRegistry } from '../mutations.js';
import {
	compileIndexComponent,
	type IndexComponent,
	IndexEncoder,
	specHash,
} from '../query/indexkey.js';
import { reportError } from '../report.js';

/**
 * One compiled index of one entity: the name it is stored under, the
 * fingerprint of the declaration its entries were built from, and the encoder
 * producing a row's position in it.
 */
export interface CompiledIndex {
	name: string;
	specHash: string;
	encoder: IndexEncoder;
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
	const drop = subsumedBy(compiled.map((c) => c.encoder.components));
	return compiled.filter((_, i) => !drop[i]!);
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
 * The positions one confirmed row occupies, given its decoded value.
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
		// An index over a repeated field yields one position per element, so the
		// count is bounded by the data rather than by the number of indexes.
		for (const encoded of idx.encoder.encode(value, key, schema)) {
			positions.push(indexPosition(entity, idx.name, idx.specHash, workspaceId, encoded));
		}
	}
	return positions;
}

/**
 * `record` with its index positions recomputed from `value` — the one place a
 * confirmed row and its positions are tied together, so a writer cannot update
 * one and forget the other.
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
