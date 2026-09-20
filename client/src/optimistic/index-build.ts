// Building an index over confirmed rows that already exist.
//
// Maintenance (index-maintain.ts) keeps an index correct from the moment it is
// declared, but only for rows written AFTER that. Everything already in the
// store has no entries, so a fresh index — or one whose declaration changed —
// covers part of the entity and is useless until something walks the rest.
// Two more states arrive here the same way: a staged bootstrap swap promotes
// its snapshot without positions (a decode per row inside the one transaction
// that must commit before anything is readable is what it is avoiding), and a
// cold bootstrap that RESUMED an earlier attempt installs rows it cannot vouch
// for. Both leave their markers unclaimed and ask for a pass.
//
// The `meta/ws/<ws>/index/<entity>/<name>@<hash>~<layout>` marker is what makes
// that safe: a query uses an index only once its marker exists, and until then
// it scans. Nothing blocks on a build and nothing is wrong while one runs. A
// marker naming a different spec or a different position layout is not the
// current one, so both changes fall out as a re-walk (see keys.ts
// metaIndexBuilt).
//
// An interrupted walk RESUMES rather than restarting. The position commits with
// the batch it describes (metaIndexBuildCursor), so the work already done
// survives a closed tab — without which a client whose session is shorter than
// the walk would pay a full walk's writes every session and never claim an
// index. Two things scope that position: it is only a position and never a
// claim about coverage, and it carries the snapshot generation it was taken
// under, so a bootstrap landing in between discards it instead of resuming into
// rows that have since been replaced.
//
// COORDINATION: none beyond the store's write lock. The plan for this called
// for a leader-elected task, but a build is idempotent and writes only derived
// state, so two tabs racing at first start cost one duplicated walk and nothing
// else — whereas tying it to stream leadership would couple index freshness to
// which tab happens to be visible. Each batch takes the same kvMutex every
// other writer takes, so a build never interleaves with a rebase.

import { fromBinary } from '@bufbuild/protobuf';

import {
	type ConfirmedRecord,
	confirmedWsPrefix,
	type IndexBuildCursor,
	metaCursor,
	metaIndexBuildCursor,
	metaIndexBuilt,
	metaIndexEntityPrefix,
	metaSnapshotGen,
} from '../keys.js';
import type { KVStore, KVWrite } from '../kv/store.js';
import type { EntityRegistry } from '../mutations.js';
import { upperBound } from '../query/indexkey.js';
import {
	type CompiledIndex,
	type IndexCache,
	indexedRecord,
	searchIndexOf,
	searchPositions,
	writeSearchRecord,
} from './index-maintain.js';

/**
 * Rows one transaction indexes. A build holds the store's write lock for the
 * length of a batch, so an unbounded one would stall every writer for the
 * length of a full entity walk.
 */
export const BUILD_BATCH = 200;

/** Runs a write transaction under the store's cross-tab lock. */
export type RunLocked = <T>(fn: () => Promise<T>) => Promise<T>;

/** What one pass over a workspace (or one entity of it) did. */
export interface BuildResult {
	/** Confirmed rows rewritten with their positions. */
	rows: number;
	/**
	 * Whether every index ended the pass MARKED built. False when the pass had
	 * nothing it could claim: no snapshot installed yet, the client stopping
	 * mid-walk, or a bootstrap replacing the rows underneath it — all states
	 * whose next pass is what finishes the job.
	 */
	ready: boolean;
}

/**
 * Brings every index of every registered entity up to date for one workspace
 * and marks each built. `rows` is 0 when every index was already marked;
 * indexes already marked under their current spec are skipped, so a repeat
 * pass over an unchanged workspace costs one key lookup per index.
 *
 * Safe to call at any time and safe to abandon: it never claims a marker it has
 * not earned. `stopped` is polled between batches so a client closing mid-walk
 * does not keep writing into a store it is done with.
 */
export async function buildIndexes(
	kv: KVStore,
	runLocked: RunLocked,
	registry: EntityRegistry,
	indexes: IndexCache,
	workspaceId: string,
	stopped?: () => boolean,
): Promise<BuildResult> {
	let rows = 0;
	let ready = true;
	for (const entity of registry.names()) {
		const compiled = indexes.for(entity);
		if (compiled.length === 0) continue;
		if (stopped?.()) return { rows, ready: false };
		const built = await buildEntity(kv, runLocked, registry, entity, compiled, workspaceId, stopped);
		rows += built.rows;
		ready &&= built.ready;
	}
	return { rows, ready };
}

/**
 * Claims every current index of every registered entity as built for one
 * workspace, walking nothing.
 *
 * Only a caller that has just made that claim TRUE may use it. Two do:
 *
 * - Dropping an entity (dropConfirmedEntities). An index over no rows is
 *   complete, and the changes that refill it are indexed as they land; without
 *   this the markers it deleted would never come back.
 * - A cold bootstrap's install (Coordinator.doBootstrap), which wrote every c/
 *   row of the workspace itself and wrote them carrying their positions.
 *
 * A staged swap is NOT one — it promotes its snapshot without positions — and
 * neither is a cold bootstrap that resumed an earlier attempt, whose adopted
 * rows it did not write. Both ask for a walk instead.
 */
export async function markIndexesBuilt(
	tx: KVWrite,
	indexes: IndexCache,
	workspaceId: string,
	entities: Iterable<string>,
): Promise<void> {
	for (const entity of entities) {
		for (const idx of indexes.for(entity)) {
			await tx.put(metaIndexBuilt(workspaceId, entity, idx.name, idx.specHash), true);
		}
	}
}

/**
 * What the workspace's confirmed rows currently are: `installed` says they are a
 * whole snapshot, `gen` which one (undefined on a store that predates the
 * marker, or has never swapped one in). Read outside the batch loop, so it takes
 * its own transaction.
 *
 * A cursor is what makes the rows a snapshot rather than a fragment of one. A
 * workspace without one is either empty or mid-bootstrap — a cold bootstrap
 * writes its rows straight into c/ (Coordinator.doBootstrap) — and neither is
 * something an index can claim coverage of, since rows keep arriving behind a
 * walk that has already passed their keys.
 */
async function readInstalledSnapshot(
	kv: KVStore,
	workspaceId: string,
): Promise<{ installed: boolean; gen: number | undefined }> {
	const tx = await kv.read();
	try {
		return {
			installed: await tx.has(metaCursor(workspaceId)),
			gen: await tx.get(metaSnapshotGen(workspaceId)) as number | undefined,
		};
	} finally {
		tx.release();
	}
}

/**
 * Where a previous walk of this entity stopped, or undefined to start over.
 *
 * A cursor taken under a different snapshot generation is discarded rather than
 * resumed: the rows it counted as done have since been replaced wholesale, so
 * skipping past them would leave the new ones unindexed under a marker claiming
 * otherwise. That is the same hazard the generation check at the marker write
 * covers, caught one step earlier.
 */
async function readBuildCursor(
	kv: KVStore,
	workspaceId: string,
	entity: string,
	gen: number | undefined,
): Promise<string | undefined> {
	const tx = await kv.read();
	try {
		const cursor = await tx.get(metaIndexBuildCursor(workspaceId, entity)) as
			| IndexBuildCursor
			| undefined;
		return cursor !== undefined && cursor.g === gen ? cursor.k : undefined;
	} finally {
		tx.release();
	}
}

async function buildEntity(
	kv: KVStore,
	runLocked: RunLocked,
	registry: EntityRegistry,
	entity: string,
	compiled: CompiledIndex[],
	workspaceId: string,
	stopped?: () => boolean,
): Promise<BuildResult> {
	const markers = compiled.map((idx) => metaIndexBuilt(workspaceId, entity, idx.name, idx.specHash));

	const tx = await kv.read();
	let allBuilt = true;
	try {
		for (const marker of markers) {
			if (!(await tx.has(marker))) {
				allBuilt = false;
				break;
			}
		}
	} finally {
		tx.release();
	}
	if (allBuilt) return { rows: 0, ready: true };

	// Walk the entity in batches, resuming by row key. Maintenance is already
	// live, so a row written while this runs gets its entries from the write
	// path — the build only has to visit rows that existed before it started,
	// and revisiting one is harmless.
	const def = registry.get(entity);
	if (def === undefined) return { rows: 0, ready: false };
	const prefix = confirmedWsPrefix(entity, workspaceId);
	// The snapshot this walk describes. A bootstrap landing mid-walk replaces
	// every row underneath it, which would leave the batches already visited
	// describing rows that no longer exist — so the markers are only claimed if
	// this still matches at the end. See metaSnapshotGen.
	const { installed, gen: startedAt } = await readInstalledSnapshot(kv, workspaceId);
	// Nothing to describe yet: no snapshot is installed, so walking would either
	// index an empty workspace or race a bootstrap filling it. Not `ready`: no
	// marker exists, and the pass that follows the bootstrap is what earns one.
	if (!installed) return { rows: 0, ready: false };
	// Resume where the last attempt stopped. Without this a walk is worth
	// nothing until it finishes: a client whose session is shorter than the walk
	// would restart the entity every time and never claim an index, while paying
	// a full walk's writes each session.
	let after = await readBuildCursor(kv, workspaceId, entity, startedAt);
	let walked = 0;
	for (;;) {
		if (stopped?.()) return { rows: walked, ready: false };
		const done = await runLocked(async () => {
			const wtx = await kv.write();
			try {
				// Collect the batch before writing any of it: iterating a prefix
				// while writing (even into a disjoint keyspace) is needlessly
				// subtle across the KV backends.
				const batch: { key: string; record: ConfirmedRecord }[] = [];
				// Bounded BELOW at the resume position rather than scanned from the
				// front and skipped past: a scan maps to one getAll over its whole
				// bound (idb.ts), so skipping still materializes every row of the
				// entity, and a walk of N rows in batches of BUILD_BATCH would read
				// the entity once per batch.
				//
				// The bound is INCLUSIVE on `after` and the boundary row dropped by
				// the filter below. Row keys are user data, so an exclusive bound
				// built by appending a sentinel to `after` would skip a row whose key
				// legitimately begins with it.
				for await (
					const [ k, v ] of wtx.scanRange(
						after === undefined ? prefix : prefix + after,
						upperBound(prefix),
					)
				) {
					const key = k.slice(prefix.length);
					if (after !== undefined && key <= after) continue;
					batch.push({ key, record: v as ConfirmedRecord });
					if (batch.length >= BUILD_BATCH) break;
				}
				const search = searchIndexOf(compiled);
				for (const { key, record } of batch) {
					const value = fromBinary(def.schema, record.v);
					// The row is REWRITTEN carrying the positions its current value
					// encodes to, which is the whole of indexing it: positions are a
					// field on the record, so there is no keyspace to add entries to.
					// Replacing the list rather than merging into it is also what
					// reclaims positions left by a declaration that has SINCE CHANGED —
					// they sit under the old spec's prefix and simply do not come back.
					//
					// Idempotent: a walk that revisits a row it has already indexed
					// writes the identical record and costs a repeated put.
					await wtx.put(
						prefix + key,
						indexedRecord(record, compiled, entity, workspaceId, key, value, def.schema),
					);
					// Written rather than compared against what is stored: the walk is
					// what MAKES the search record current, and the case that reaches
					// here most often — an index declared but never built — has nothing
					// stored to compare against.
					if (search !== undefined) {
						await writeSearchRecord(
							wtx,
							entity,
							workspaceId,
							key,
							searchPositions(search, entity, workspaceId, key, value, def.schema),
						);
					}
				}
				const last = batch[batch.length - 1]?.key;
				// The cursor commits WITH the rows it describes, so it can never
				// name a position whose indexed rows did not make it to the store.
				if (last !== undefined) {
					const cursor: IndexBuildCursor = { g: startedAt, k: last };
					await wtx.put(metaIndexBuildCursor(workspaceId, entity), cursor);
				}
				await wtx.commit();
				after = last ?? after;
				walked += batch.length;
				return batch.length < BUILD_BATCH;
			} catch (err) {
				wtx.release();
				throw err;
			}
		});
		if (done) break;
	}

	const claimed = await runLocked(async () => {
		const wtx = await kv.write();
		try {
			// Under the same lock a bootstrap takes, so this reads either the
			// generation the walk started under or a later one — never a value
			// mid-change. A later one means the rows this walk indexed have since
			// been replaced wholesale; abandoning is correct and costs nothing,
			// since that bootstrap asked for a build of its own.
			//
			// The cursor is checked again for the case the generation cannot see: a
			// bootstrap that CLEARED the workspace during the walk and is still
			// streaming its replacement into c/ has not written a new generation
			// yet, and on a store that had none it would compare equal.
			if (
				(await wtx.get(metaSnapshotGen(workspaceId))) !== startedAt
				|| !(await wtx.has(metaCursor(workspaceId)))
			) {
				wtx.release();
				return false;
			}
			// A superseded declaration leaves a marker nothing looks up. Its
			// POSITIONS are already gone — the walk above rewrote every row with
			// the list its current specs encode to, and that list replaces rather
			// than extends — so only the markers are left to reclaim, here at the
			// point the current specs are claimed instead.
			const current = new Set(markers);
			const stale: string[] = [];
			for await (const [ k ] of wtx.scan(metaIndexEntityPrefix(workspaceId, entity))) {
				if (!current.has(k)) stale.push(k);
			}
			for (const k of stale) await wtx.del(k);
			for (const marker of markers) await wtx.put(marker, true);
			// The walk is over, so the position is spent. Leaving it would strand a
			// key per entity per workspace, and a later rebuild (a changed spec)
			// must start from the beginning rather than from where the PREVIOUS
			// build happened to finish.
			await wtx.del(metaIndexBuildCursor(workspaceId, entity));
			await wtx.commit();
			return true;
		} catch (err) {
			wtx.release();
			throw err;
		}
	});
	return { rows: walked, ready: claimed };
}
