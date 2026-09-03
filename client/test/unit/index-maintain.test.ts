// Client-side index maintenance: a confirmed row's index POSITIONS must track
// its value exactly, through every path that writes or deletes one.
//
// These read the positions off the rows directly, which no production read
// does — it goes through the store's index (see kv/store.ts). They exist
// because a maintenance bug that strands a position is invisible to a
// differential read test: a stale position points at a row that is re-read and
// re-checked, so the ANSWER stays right while the store rots.

import { create, toBinary } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import {
	confirmedKey,
	type ConfirmedRecord,
	confirmedWsPrefix,
	type IndexBuildCursor,
	indexPositionPrefix,
	metaCursor,
	metaIndexBuildCursor,
	metaIndexBuilt,
	metaSnapshotGen,
	splitWsKey,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead, KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { BUILD_BATCH, buildIndexes } from '../../src/optimistic/index-build.js';
import { compileIndexes, IndexCache } from '../../src/optimistic/index-maintain.js';
import {
	applyServerChanges,
	dropConfirmedEntities,
	wipeConfirmedState,
} from '../../src/optimistic/rebase.js';
import { TouchSet } from '../../src/touch.js';

const ws = 'ws1';
const registry = new EntityRegistry([ WidgetSchema ]);

/**
 * Wraps a store so every entry a caller iterates out of `prefix` is counted.
 *
 * Rows READ, not rows written: a walk that re-scans an entity once per batch
 * writes exactly the entries one that resumes by range writes, so what
 * separates the two is only visible on the read side.
 *
 * It counts what a scan YIELDS, which under the memory backend is what the scan
 * was asked for — an iteration that stops early stops the generator. IDB
 * materializes a whole range up front (idb.ts), so the same walk costs it more
 * than this counts; the bound a scan is given is the part both backends share
 * and the part under test.
 */
function countingRowReads(
	inner: KVStore,
	prefix: string,
): { kv: KVStore; rows: () => number } {
	let rows = 0;
	async function* count(it: AsyncIterable<[string, unknown]>): AsyncIterable<[string, unknown]> {
		for await (const entry of it) {
			if (entry[0].startsWith(prefix)) rows++;
			yield entry;
		}
	}
	function wrap<T extends KVRead>(tx: T): T {
		const scan = tx.scan.bind(tx);
		const scanRange = tx.scanRange.bind(tx);
		tx.scan = (p) => count(scan(p));
		tx.scanRange = (lo, hi) => count(scanRange(lo, hi));
		return tx;
	}
	const kv: KVStore = {
		read: async () => wrap(await inner.read()),
		write: async () => wrap(await inner.write()),
		close: () => inner.close(),
	};
	return { kv, rows: () => rows };
}

/**
 * Every index position in the store, as [position, rowKey] pairs in position
 * order — read off the rows that carry them rather than through the store's
 * index, so an assertion about maintenance cannot be satisfied by a backend
 * that indexes something else.
 */
async function entries(kv: KVStore): Promise<[string, string][]> {
	const out: [string, string][] = [];
	const tx = await kv.read();
	for await (const [ k, v ] of tx.scan('c/')) {
		// `c/<entity>/<ws>/<key>`, and the row key may contain slashes.
		const afterEntity = k.slice(k.indexOf('/', 2) + 1);
		const { key } = splitWsKey(afterEntity);
		for (const position of (v as ConfirmedRecord).ix ?? []) out.push([ position, key ]);
	}
	tx.release();
	out.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
	return out;
}

const valueChange = (key: string, label: string, version: bigint) =>
	create(RowChangeSchema, {
		entity: 'widget',
		key,
		version,
		body: {
			case: 'value',
			value: toBinary(WidgetSchema, create(WidgetSchema, { id: key, label })),
		},
	});

const tombstone = (key: string, version: bigint) =>
	create(RowChangeSchema, {
		entity: 'widget',
		key,
		version,
		body: { case: 'tombstone', value: true },
	});

async function apply(kv: KVStore, indexes: IndexCache | undefined, changes: unknown[]) {
	const tx = await kv.write();
	// A build walks only a workspace whose rows are a whole installed snapshot,
	// and the cursor is what says so — without one these rows look like a
	// bootstrap still filling the store (see readInstalledSnapshot).
	await tx.put(metaCursor(ws), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		ws,
		changes as Parameters<typeof applyServerChanges>[3],
		new TouchSet(),
		indexes,
	);
	await tx.commit();
}

describe('client index maintenance', () => {
	let kv: KVStore;
	let indexes: IndexCache;

	beforeEach(() => {
		kv = new MemoryKVStore();
		indexes = new IndexCache(registry);
	});

	it('writes no entries for an entity that declares no index', async () => {
		// Widget declares none, so the whole mechanism must stay dormant: an
		// unused index is not merely wasteful here, it is write amplification
		// on every row of every entity.
		await apply(kv, indexes, [ valueChange('w1', 'a', 1n) ]);
		expect(await entries(kv)).toEqual([]);
	});

	it('writes nothing when no cache is passed at all', async () => {
		await apply(kv, undefined, [ valueChange('w1', 'a', 1n) ]);
		expect(await entries(kv)).toEqual([]);
	});

	it('compiles an index over a field whose proto and accessor names differ', async () => {
		// A dropped index is invisible — no entries, no marker, no error, just
		// every query over the field scanning forever — so this asserts on the
		// compile step directly. Todo.sort_key is the only field in reach whose
		// two names differ; every testdata field spells them the same, which is
		// why nothing else here can catch it.
		const compiled = compileIndexes(TodoSchema, [
			{ name: 'by_sort_key', key: [ { path: [ 'sort_key' ], desc: false } ] },
		]);
		expect(compiled).toHaveLength(1);
		expect(compiled[0]!.encoder!.components[0]!.path).toEqual([ 'sortKey' ]);
	});

	describe('subsumption', () => {
		// Marking a field and then declaring a composite that leads with it is
		// the normal outcome of using both declaration surfaces, and the shorter
		// index is then maintained for nothing: the planner pins an equality
		// prefix, so the longer one answers every query the shorter one does.
		const of = (
			...defs: { name: string; key: { path: string[]; desc: boolean }[] }[]
		): string[] => compileIndexes(WidgetSchema, defs).map((i) => i.name);

		const label = { path: [ 'label' ], desc: false };
		const count = { path: [ 'count' ], desc: false };
		const countDesc = { path: [ 'count' ], desc: true };

		it('drops an index whose components lead a longer one, in either order', () => {
			expect(of(
				{ name: 'short', key: [ label ] },
				{ name: 'long', key: [ label, countDesc ] },
			)).toEqual([ 'long' ]);
			expect(of(
				{ name: 'long', key: [ label, countDesc ] },
				{ name: 'short', key: [ label ] },
			)).toEqual([ 'long' ]);
		});

		it('keeps an index that only agrees on a component up to its direction', () => {
			// The encoding inverts a descending payload, so the entries interleave
			// differently: `long` does not reproduce `short`'s order and cannot
			// stand in for it.
			expect(
				of(
					{ name: 'short', key: [ countDesc ] },
					{ name: 'long', key: [ count, label ] },
				).sort(),
			).toEqual([ 'long', 'short' ]);
		});

		it('keeps an index that shares no leading component', () => {
			expect(
				of(
					{ name: 'byCount', key: [ count ] },
					{ name: 'byLabel', key: [ label, count ] },
				).sort(),
			).toEqual([ 'byCount', 'byLabel' ]);
		});

		it('keeps exactly one of a set of identical declarations', () => {
			// Two equal lists subsume each other, so a rule without a tie-break
			// would drop both and leave every query over them scanning.
			expect(of(
				{ name: 'first', key: [ label ] },
				{ name: 'second', key: [ label ] },
				{ name: 'third', key: [ label ] },
			)).toEqual([ 'first' ]);
		});

		it('keeps both sides of one map entry apart', () => {
			// `attrs.*.key` and `attrs.*.value` resolve to the SAME path and the
			// same direction, so a signature that read the path alone would call
			// them one component — and drop one index, answering with keys where
			// values were asked for.
			const entryKey = { path: [ 'attrs', '*', 'key' ], desc: false };
			const entryValue = { path: [ 'attrs', '*', 'value' ], desc: false };
			expect(
				of(
					{ name: 'byKey', key: [ entryKey ] },
					{ name: 'byValue', key: [ entryValue ] },
				).sort(),
			).toEqual([ 'byKey', 'byValue' ]);
			// The two LAYOUTS are different indexes for the same reason: neither
			// component list leads the other.
			expect(
				of(
					{ name: 'keyFirst', key: [ entryKey, entryValue ] },
					{ name: 'valueFirst', key: [ entryValue, entryKey ] },
				).sort(),
			).toEqual([ 'keyFirst', 'valueFirst' ]);
			// A value-only index IS led by the value-first layout, so it goes.
			expect(of(
				{ name: 'valueOnly', key: [ entryValue ] },
				{ name: 'valueFirst', key: [ entryValue, entryKey ] },
			)).toEqual([ 'valueFirst' ]);
		});

		it('keeps only the longest of a chain of prefixes', () => {
			expect(of(
				{ name: 'a', key: [ label ] },
				{ name: 'ab', key: [ label, count ] },
				{ name: 'abc', key: [ label, count, countDesc ] },
			)).toEqual([ 'abc' ]);
		});
	});

	describe('with an indexed entity', () => {
		// Widget has no `indexed` field in the shared test schema, so the index
		// is injected here rather than by changing a schema several other
		// suites pin. What is under test is the maintenance, not the
		// declaration — that is covered by the schema builder's own tests.
		let indexed: IndexCache;
		let indexedRegistry: () => EntityRegistry;

		beforeEach(() => {
			const def = registry.get('widget')!;
			const fake = {
				get: () => ({
					...def,
					indexes: [ { name: 'by_label', key: [ { path: [ 'label' ], desc: false } ] } ],
				}),
				names: () => [ 'widget' ],
			} as unknown as EntityRegistry;
			indexedRegistry = () => fake;
			indexed = new IndexCache(fake);
		});

		it('writes one entry per row, pointing back at the row key', async () => {
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n), valueChange('w2', 'b', 2n) ]);
			const got = await entries(kv);
			expect(got).toHaveLength(2);
			expect(got.map(([ , rowKey ]) => rowKey).sort()).toEqual([ 'w1', 'w2' ]);
		});

		it('orders entries by the indexed value, not by row key', async () => {
			// w2 sorts first by label and last by key, so a keyspace that
			// happened to be ordered by row key would pass a weaker assertion.
			await apply(kv, indexed, [ valueChange('w1', 'zebra', 1n), valueChange('w2', 'apple', 2n) ]);
			expect((await entries(kv)).map(([ , rowKey ]) => rowKey)).toEqual([ 'w2', 'w1' ]);
		});

		it('replaces the entry when the row moves, leaving exactly one', async () => {
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n) ]);
			const before = (await entries(kv))[0]![0];
			await apply(kv, indexed, [ valueChange('w1', 'z', 2n) ]);
			const after = await entries(kv);
			expect(after).toHaveLength(1);
			expect(after[0]![0]).not.toBe(before);
		});

		it('removes the entry when the row is tombstoned', async () => {
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n), valueChange('w2', 'b', 2n) ]);
			await apply(kv, indexed, [ tombstone('w1', 3n) ]);
			expect((await entries(kv)).map(([ , rowKey ]) => rowKey)).toEqual([ 'w2' ]);
		});

		it('re-indexes a row whose previous bytes no longer decode', async () => {
			// Where a row USED to sit is never recomputed — the position list is
			// replaced, not amended — so bytes that no longer decode cannot break a
			// rewrite. A layout that had to name the entries it was deleting could
			// not say this: it had to reset the entity's whole index and rebuild.
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n), valueChange('w2', 'b', 2n) ]);
			const marker = metaIndexBuilt(ws, 'widget', 'by_label', indexed.for('widget')[0]!.specHash);
			const seed = await kv.write();
			await seed.put(marker, true);
			// Wire type 7 does not exist, so nothing can decode this. Written
			// without positions, as any rewrite that skips maintenance is.
			const corrupt: ConfirmedRecord = { v: new Uint8Array([ 0x0f ]), ver: '1' };
			await seed.put(confirmedKey('widget', ws, 'w1'), corrupt);
			await seed.commit();

			await apply(kv, indexed, [ valueChange('w1', 'z', 3n) ]);

			// w1 is back at its new label, w2 never moved, and the marker still
			// holds — nothing had to be thrown away to get here.
			expect((await entries(kv)).map(([ , rowKey ]) => rowKey)).toEqual([ 'w2', 'w1' ]);
			const r = await kv.read();
			expect(await r.has(marker)).toBe(true);
			r.release();
		});

		it('keeps the positions on the row, in no keyspace of their own', async () => {
			// The layout in one assertion: an indexed row carries its positions,
			// and nothing else in the store describes them.
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n) ]);
			const r = await kv.read();
			const record = await r.get(confirmedKey('widget', ws, 'w1')) as ConfirmedRecord;
			expect(record.ix).toHaveLength(1);
			const keys: string[] = [];
			for await (const [ k ] of r.scan('')) keys.push(k);
			expect(keys.filter((k) => !k.startsWith('c/') && !k.startsWith('meta/'))).toEqual([]);
			r.release();
		});

		it('leaves the record of an unindexed entity untouched', async () => {
			// An entity with no compiled index must not grow a field for it: the
			// absent list and the empty one mean the same thing, and the record of
			// an unindexed entity is what it was before indexes existed.
			await apply(kv, indexes, [ valueChange('w1', 'a', 1n) ]);
			const r = await kv.read();
			const record = await r.get(confirmedKey('widget', ws, 'w1')) as ConfirmedRecord;
			expect(record).toEqual({ v: expect.any(Uint8Array), ver: '1' });
			r.release();
		});

		it('takes the index with the rows when confirmed state is wiped', async () => {
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n) ]);
			const tx = await kv.write();
			await wipeConfirmedState(tx, registry, ws, new TouchSet());
			await tx.commit();
			expect(await entries(kv)).toEqual([]);
		});

		it('re-claims the built marker when the entity is dropped', async () => {
			// A drop empties the entity, so its indexes cover it trivially and
			// the rows that refill it are indexed as they arrive. Markers naming
			// the CURRENT specs are therefore re-earned in the same transaction;
			// only ones naming a spec that has since changed stay deleted.
			const marker = metaIndexBuilt(ws, 'widget', 'by_label', indexed.for('widget')[0]!.specHash);
			const stale = metaIndexBuilt(ws, 'widget', 'by_label', 'gone');
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n) ]);
			const seed = await kv.write();
			await seed.put(marker, true);
			await seed.put(stale, true);
			await seed.commit();

			const tx = await kv.write();
			await dropConfirmedEntities(tx, ws, [ 'widget' ], new TouchSet(), indexed);
			await tx.commit();

			const r = await kv.read();
			expect(await r.has(marker)).toBe(true);
			expect(await r.has(stale)).toBe(false);
			r.release();
			expect(await entries(kv)).toEqual([]);
		});

		it('reclaims the entries of a declaration that has been superseded', async () => {
			// Changing an index's components moves its prefix: the fingerprint is
			// part of it, so the new spec starts empty and the old one's positions
			// are unreachable. Reclaiming them needs nothing of its own — the build
			// rewrites every row with the list the CURRENT specs encode to, and that
			// list replaces rather than extends.
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n), valueChange('w2', 'b', 2n) ]);
			const oldHash = indexed.for('widget')[0]!.specHash;
			expect(await entries(kv)).toHaveLength(2);

			// Same NAME, different components. A name the build does not know is
			// deliberately left alone (it may be another tab's), so re-declaring
			// under the same name is what the sweep is scoped to.
			const def = registry.get('widget')!;
			const changed = {
				get: () => ({
					...def,
					indexes: [
						{
							name: 'by_label',
							key: [ { path: [ 'label' ], desc: false }, {
								path: [ 'count' ],
								desc: true,
							} ],
						},
					],
				}),
				names: () => [ 'widget' ],
			} as unknown as EntityRegistry;
			const after = new IndexCache(changed);
			const newHash = after.for('widget')[0]!.specHash;
			expect(newHash).not.toBe(oldHash);

			const mutex = new ChainMutex();
			await buildIndexes(kv, (fn) => mutex.run(fn), changed, after, ws);

			// Two positions, both under the new spec: the old ones are gone rather
			// than merely unread.
			const got = await entries(kv);
			expect(got).toHaveLength(2);
			const newPrefix = indexPositionPrefix('widget', 'by_label', newHash, ws);
			expect(got.every(([ k ]) => k.startsWith(newPrefix))).toBe(true);
		});

		it('re-walks an entity whose built marker predates the current position layout', async () => {
			// A marker written by a client using an older position layout claims
			// coverage of positions the current layout cannot address, so trusting
			// it would leave every query over the index reading an empty range and
			// returning nothing. Spelled out rather than derived from
			// metaIndexBuilt: what is under test is that the key MOVED.
			const idx = indexed.for('widget')[0]!;
			const legacy = `meta/ws/${ws}/index/widget/by_label@${idx.specHash}`;
			expect(metaIndexBuilt(ws, 'widget', 'by_label', idx.specHash)).not.toBe(legacy);

			// Rows installed unindexed, as a bootstrap swap installs them.
			await apply(kv, undefined, [ valueChange('w1', 'a', 1n), valueChange('w2', 'b', 2n) ]);
			const seed = await kv.write();
			await seed.put(legacy, true);
			await seed.commit();

			const mutex = new ChainMutex();
			const built = await buildIndexes(kv, (fn) => mutex.run(fn), indexedRegistry(), indexed, ws);

			expect(built).toEqual({ rows: 2, ready: true });
			const prefix = indexPositionPrefix('widget', 'by_label', idx.specHash, ws);
			expect((await entries(kv)).map(([ k ]) => k.startsWith(prefix))).toEqual([ true, true ]);
			const r = await kv.read();
			expect(await r.has(metaIndexBuilt(ws, 'widget', 'by_label', idx.specHash))).toBe(true);
			// The stale marker is swept with every other one the current specs do
			// not name, so the layout bump costs one build and no residue.
			expect(await r.has(legacy)).toBe(false);
			r.release();
		});

		describe('a build spanning batches', () => {
			// BUILD_BATCH is 200, so a corpus above it takes more than one batch
			// and can be stopped between two.
			const ROWS = 250;
			/** Row keys, zero-padded so KV order and numeric order agree. */
			const keyAt = (i: number) => `w${String(i).padStart(4, '0')}`;

			/** Seeds ROWS rows labelled the same as their key, all indexed. */
			async function seed(): Promise<void> {
				await apply(
					kv,
					undefined,
					Array.from({ length: ROWS }, (_, i) => valueChange(keyAt(i), keyAt(i), BigInt(i + 1))),
				);
			}

			/**
			 * Runs a build that stops after `batches` batches have COMMITTED.
			 *
			 * Counted off the locked sections rather than off calls to `stopped`,
			 * which is also polled once before the walk starts — counting those
			 * would stop the build before it had written anything.
			 */
			async function buildStoppingAfter(batches: number): Promise<void> {
				let committed = 0;
				const mutex = new ChainMutex();
				await buildIndexes(
					kv,
					async (fn) => {
						const result = await mutex.run(fn);
						committed++;
						return result;
					},
					indexedRegistry(),
					indexed,
					ws,
					() => committed >= batches,
				);
			}

			async function cursor(): Promise<IndexBuildCursor | undefined> {
				const tx = await kv.read();
				try {
					return await tx.get(metaIndexBuildCursor(ws, 'widget')) as IndexBuildCursor | undefined;
				} finally {
					tx.release();
				}
			}

			it('asks the store only for rows at or past the resume position', async () => {
				// The resume position bounds the SCAN, not just what is kept out of
				// it. Filtering a full prefix scan writes the right entries and asks
				// the store for the whole entity once per batch to do it, which is
				// quadratic in rows and invisible to every other assertion here.
				await seed();
				const counted = countingRowReads(kv, confirmedWsPrefix('widget', ws));
				const mutex = new ChainMutex();
				await buildIndexes(counted.kv, (fn) => mutex.run(fn), indexedRegistry(), indexed, ws);

				expect(await entries(kv)).toHaveLength(ROWS);
				// ROWS is one full batch plus a partial, so the walk resumes once and
				// re-reads exactly the row the resume position names: the lower bound
				// is inclusive on it, because a row key can be any string and one
				// built by appending a sentinel would skip a key beginning with it.
				expect(counted.rows()).toBeLessThanOrEqual(ROWS + 1);
			});

			it('resumes where it stopped instead of walking the entity again', async () => {
				await seed();
				await buildStoppingAfter(1);
				// One batch in: a position, no claim.
				expect((await cursor())?.k).toBe(keyAt(BUILD_BATCH - 1));
				expect(await entries(kv)).toHaveLength(BUILD_BATCH);

				// Rewrite the FIRST row without maintenance, which unindexes it. A
				// walk that restarted would re-index it at its new label; one that
				// resumes never looks at it again, which is what makes this
				// observable at all.
				await apply(kv, undefined, [ valueChange(keyAt(0), 'zzz', 999n) ]);

				const mutex = new ChainMutex();
				await buildIndexes(kv, (fn) => mutex.run(fn), indexedRegistry(), indexed, ws);

				const got = await entries(kv);
				// Row 0 holds no position at all — untouched by the resumed walk.
				// Under a restart it would have been re-indexed at 'zzz', i.e. last.
				expect(got).toHaveLength(ROWS - 1);
				expect(got.some(([ , rowKey ]) => rowKey === keyAt(0))).toBe(false);
				expect(got[0]![1]).toBe(keyAt(1));
				// And the walk finished, so the claim is made and the spent position
				// is gone.
				const marker = metaIndexBuilt(ws, 'widget', 'by_label', indexed.for('widget')[0]!.specHash);
				const tx = await kv.read();
				expect(await tx.has(marker)).toBe(true);
				tx.release();
				expect(await cursor()).toBeUndefined();
			});

			it('discards a position taken under a snapshot that has been replaced', async () => {
				await seed();
				await buildStoppingAfter(1);
				expect((await cursor())?.k).toBe(keyAt(BUILD_BATCH - 1));

				// A bootstrap swap installs a new snapshot: the generation moves and
				// the rows the cursor counted as done are replaced — unindexed, as a
				// swap writes them. A resume that trusted the stale position would
				// skip exactly those rows and then claim the index covers them.
				const swap = await kv.write();
				await swap.put(metaSnapshotGen(ws), 7);
				await swap.commit();
				await apply(
					kv,
					undefined,
					// Labels that sort AFTER every untouched row's, so where the
					// replaced rows end up in index order is what the assertion
					// reads — and, since the rewrite unindexed them, a stale resume
					// leaves them with no position at all.
					Array.from(
						{ length: BUILD_BATCH },
						(_, i) => valueChange(keyAt(i), `z${keyAt(i)}`, BigInt(1000 + i)),
					),
				);

				const mutex = new ChainMutex();
				await buildIndexes(kv, (fn) => mutex.run(fn), indexedRegistry(), indexed, ws);

				const got = await entries(kv);
				expect(got).toHaveLength(ROWS);
				// The walk restarted, so the replaced rows were re-indexed at their
				// new labels and now sort last. A resume from the stale position
				// would have left them at the front under their old ones.
				expect(got[0]![1]).toBe(keyAt(BUILD_BATCH));
				expect(got[got.length - 1]![1]).toBe(keyAt(BUILD_BATCH - 1));
			});
		});

		it("leaves another workspace's entries alone when one is wiped", async () => {
			await apply(kv, indexed, [ valueChange('w1', 'a', 1n) ]);
			// Same entity, different workspace — the index nests the workspace
			// inside its own prefix, so a careless prefix sweep would take both.
			const tx = await kv.write();
			await applyServerChanges(
				tx,
				registry,
				'ws2',
				[ valueChange('w9', 'a', 1n) ] as Parameters<typeof applyServerChanges>[3],
				new TouchSet(),
				indexed,
			);
			await tx.commit();

			const tx2 = await kv.write();
			await wipeConfirmedState(tx2, registry, ws, new TouchSet());
			await tx2.commit();

			expect((await entries(kv)).map(([ , rowKey ]) => rowKey)).toEqual([ 'w9' ]);
		});
	});
});
