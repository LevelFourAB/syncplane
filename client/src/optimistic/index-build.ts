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
	legacySearchIndexWsPrefix,
	metaCursor,
	metaIndexBuildCursor,
	metaIndexBuilt,
	metaIndexEntityPrefix,
	metaSnapshotGen,
	searchIndexWsPrefix,
	searchPostingsScope,
	searchSpecTag,
} from '../keys.js';
import type { KVStore, KVWrite } from '../kv/store.js';
import type { EntityRegistry } from '../mutations.js';
import { upperBound } from '../query/indexkey.js';
import { SearchPostingsWriter, specTagsUnder } from '../query/postings.js';
import {
	type CompiledIndex,
	deletePrefixes,
	type IndexCache,
	indexedRecord,
	recordSearchRow,
	searchIndexOf,
} from './index-maintain.js';

/**
 * Rows one transaction indexes. A build holds the store's write lock for the
 * length of a batch, so an unbounded one would stall every writer for the
 * length of a full entity walk.
 */
export const BUILD_BATCH = 200;

/**
 * Rows whose search postings a build records before it writes them, in the
 * transaction of the batch that reaches it. See SearchPostingsWriter for why
 * a write of more rows costs less per row.
 */
const BUILD_POSTINGS_ROWS = 500;

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
	const search = searchIndexOf(compiled);

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
	// The search postings of the rows visited since the last flush, and the
	// version each of those rows had when its batch read it. They are written
	// every BUILD_POSTINGS_ROWS rows rather than every batch, since a write of
	// postings costs by the distinct terms it touches; see query/postings.ts.
	const postings = new SearchPostingsWriter();
	const visited = new Map<string, string>();
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
					// The postings, by contrast, are a diff against the row's term list,
					// which is what makes a revisit free here too: a row already posted
					// under its current terms writes nothing. The case that reaches here
					// most often — an index declared but never built — has no term list,
					// and the rows come in key order, so every posting appends to its
					// term's tail.
					if (search !== undefined) {
						recordSearchRow(postings, entity, workspaceId, search, key, value, def.schema);
					}
				}
				const last = batch[batch.length - 1]?.key;
				const finished = batch.length < BUILD_BATCH;
				const flush = search === undefined || finished || postings.rows >= BUILD_POSTINGS_ROWS;
				if (flush && postings.rows > 0) {
					// A row an earlier batch visited may have been rewritten or deleted
					// since, by a writer that posted what it wrote — and that found no
					// term list to diff against, since this one was not written yet. Its
					// recorded terms describe a row that is no longer there.
					const earlier = [ ...visited.keys() ];
					const now = earlier.length === 0
						? []
						: await wtx.getMany(earlier.map((k) => prefix + k));
					for (let i = 0; i < earlier.length; i++) {
						const record = now[i] as ConfirmedRecord | undefined;
						if (record?.ver !== visited.get(earlier[i]!)) {
							postings.drop(entity, workspaceId, earlier[i]!);
						}
					}
					await postings.flush(wtx);
					visited.clear();
				} else {
					for (const { key, record } of batch) visited.set(key, record.ver);
				}
				// The cursor commits WITH the rows it describes and their postings,
				// so it can never name a position whose rows did not make it to the
				// store indexed. Between flushes it stays where the last one left
				// it, and a walk resumed from there revisits the rows after it.
				if (flush && last !== undefined) {
					const cursor: IndexBuildCursor = { g: startedAt, k: last };
					await wtx.put(metaIndexBuildCursor(workspaceId, entity), cursor);
				}
				await wtx.commit();
				after = last ?? after;
				walked += batch.length;
				return finished;
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
			// A superseded declaration leaves a marker nothing looks up. Its value
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
			// The search records of the previous layouts, and the postings of every
			// search declaration but the current one, go in the same transaction
			// as those declarations' markers. Before it, a tab of that build may
			// still trust its marker and read them; after it, no build trusts
			// them. The walk cannot reclaim them as it reclaims a superseded
			// spec's value positions, because they sit at other keys than the
			// records it rewrites.
			const scope = searchPostingsScope(entity, workspaceId);
			const tag = search === undefined ? undefined : searchSpecTag(search.specHash);
			const superseded = (await specTagsUnder(wtx, scope)).filter((t) => t !== tag);
			await deletePrefixes(wtx, [
				legacySearchIndexWsPrefix(entity, workspaceId),
				searchIndexWsPrefix(entity, workspaceId),
				...superseded.map((t) => scope + t),
			]);
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
