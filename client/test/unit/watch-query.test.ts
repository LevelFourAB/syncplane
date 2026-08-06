// A watched query maintains its RESULT, not a copy of the entity: a commit
// splices the rows it named into the ordered match set and leaves the rest
// alone. The specs here pin the three things that makes observable —
//
//   - the answer is what a scan-and-filter would have produced, through every
//     transition a row can make (enter, leave, move, change in place);
//   - rows the commit did not name keep their object identity, which is what
//     proves nothing was re-read or re-decoded;
//   - a change the emitted page cannot see does not reach the callback at all.
//
// Correctness under the maintenance itself is checked the other way round, by
// the `debugVerifyTouches` hub: it re-derives the whole result from a scan
// after every commit and throws on any difference.

import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { compileQuery, type Query } from '../../src/query/index.js';
import type { Row } from '../../src/read.js';
import { defineSchema, entity, type Infer, int32, text } from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';

const registry = new EntityRegistry([ WidgetSchema ]);
const WS = 'ws1';
const flush = (): Promise<unknown> => new Promise((r) => setTimeout(r, 0));

function widgetBytes(w: MessageInitShape<typeof WidgetSchema> & { id: string }): Uint8Array {
	return toBinary(WidgetSchema, create(WidgetSchema, w));
}

function serverRow(
	key: string,
	version: bigint,
	w: MessageInitShape<typeof WidgetSchema> & { id: string },
): RowChange {
	return create(RowChangeSchema, {
		entity: 'widget',
		key,
		version,
		body: { case: 'value', value: widgetBytes(w) },
	});
}

function tombstone(key: string, version: bigint): RowChange {
	return create(RowChangeSchema, {
		entity: 'widget',
		key,
		version,
		body: { case: 'tombstone', value: true },
	});
}

async function applyServer(kv: MemoryKVStore, changes: RowChange[]): Promise<TouchSet> {
	const touched = new TouchSet();
	const tx = await kv.write();
	await applyServerChanges(tx, registry, WS, changes, touched);
	await tx.commit();
	return touched;
}

/** Counts walks of the confirmed row keyspace — what a full entity read costs. */
function countingStore(): { kv: MemoryKVStore; rowScans: () => number } {
	const kv = new MemoryKVStore();
	let rowScans = 0;
	const read = kv.read.bind(kv);
	vi.spyOn(kv, 'read').mockImplementation(async () => {
		const tx = await read();
		const inner = tx.scan.bind(tx);
		tx.scan = (prefix: string) => {
			if (prefix.startsWith('c/')) rowScans++;
			return inner(prefix);
		};
		return tx;
	});
	return { kv, rowScans: () => rowScans };
}

/**
 * A hub with the divergence check explicitly OFF. That check re-reads the whole
 * entity on every notify by design, which is exactly what the counting specs
 * measure — so they pin it rather than inherit it from the environment. The
 * maintenance itself is verified by the specs that leave the check on.
 */
function countingHub(kv: MemoryKVStore): WatcherHub {
	return new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: false });
}

/** A watcher over `q`, with every result it was handed, in order. */
async function watchQuery(
	hub: WatcherHub,
	q: Query<Widget>,
): Promise<{ seen: Row<Widget>[][]; unsub: () => void }> {
	const seen: Row<Widget>[][] = [];
	const unsub = hub.watch(
		WidgetSchema,
		WS,
		(rows) => seen.push(rows as Row<Widget>[]),
		compileQuery<Widget>(q),
	);
	await flush();
	return { seen, unsub };
}

const keysOf = (rows: Row<Widget>[] | undefined): string[] => rows?.map((r) => r.key) ?? [];

describe('maintained query results', () => {
	/** Every transition one row can make, against an ordered query. */
	it('keeps the order a scan would produce as rows enter, move and leave', async () => {
		const kv = new MemoryKVStore();
		// debugVerifyTouches on: every step below is also compared against a full
		// scan-and-filter, so the assertions pin the answer and the hub pins the
		// maintenance that produced it.
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });
		const { seen, unsub } = await watchQuery(hub, {
			where: (w) => w.count.gt(0),
			orderBy: (w) => [ w.count.asc() ],
		});
		expect(seen.at(-1)).toEqual([]);

		// Enter, out of order: each lands where the comparator puts it.
		for (const [ key, count ] of [ [ 'c', 30 ], [ 'a', 10 ], [ 'b', 20 ] ] as const) {
			await hub.notify(await applyServer(kv, [ serverRow(key, 1n, { id: key, count }) ]));
		}
		expect(keysOf(seen.at(-1))).toEqual([ 'a', 'b', 'c' ]);

		// Move: same row, new order key, so it leaves one position and takes another.
		await hub.notify(await applyServer(kv, [ serverRow('a', 2n, { id: 'a', count: 25 }) ]));
		expect(keysOf(seen.at(-1))).toEqual([ 'b', 'a', 'c' ]);

		// Leave by failing the predicate rather than by being deleted.
		await hub.notify(await applyServer(kv, [ serverRow('b', 3n, { id: 'b', count: 0 }) ]));
		expect(keysOf(seen.at(-1))).toEqual([ 'a', 'c' ]);

		// Re-enter: a row the result has never held has no history in it, and
		// still has to land in the right place.
		await hub.notify(await applyServer(kv, [ serverRow('b', 4n, { id: 'b', count: 5 }) ]));
		expect(keysOf(seen.at(-1))).toEqual([ 'b', 'a', 'c' ]);

		// Leave by deletion.
		await hub.notify(await applyServer(kv, [ tombstone('a', 5n) ]));
		expect(keysOf(seen.at(-1))).toEqual([ 'b', 'c' ]);
		unsub();
	});

	it('ties on the key exactly where the scan order does', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });
		const { seen, unsub } = await watchQuery(hub, { orderBy: (w) => [ w.count.asc() ] });

		// All equal on the order key: the tie-break is the row key, and it has to
		// hold whatever order the rows arrived in.
		for (const key of [ 'z', 'm', 'a' ]) {
			await hub.notify(await applyServer(kv, [ serverRow(key, 1n, { id: key, count: 1 }) ]));
		}
		expect(keysOf(seen.at(-1))).toEqual([ 'a', 'm', 'z' ]);
		unsub();
	});

	it('leaves untouched rows as the same objects', async () => {
		// Identity is the observable half of "the commit cost what it changed":
		// a re-read or a re-decode would produce new objects for rows that did
		// not move. It is also what lets a React list re-render one item.
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		for (const key of [ 'a', 'b', 'c' ]) {
			await applyServer(kv, [ serverRow(key, 1n, { id: key, count: 1 }) ]);
		}
		const { seen, unsub } = await watchQuery(hub, { orderBy: (w) => [ w.count.asc() ] });
		const before = seen.at(-1)!;

		await hub.notify(await applyServer(kv, [ serverRow('b', 2n, { id: 'b', count: 1 }) ]));
		const after = seen.at(-1)!;

		expect(keysOf(after)).toEqual([ 'a', 'b', 'c' ]);
		expect(after[0]).toBe(before[0]);
		expect(after[2]).toBe(before[2]);
		unsub();
	});

	it('reads only the rows a commit named', async () => {
		const { kv, rowScans } = countingStore();
		for (let i = 0; i < 20; i++) {
			await applyServer(kv, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
		}
		const hub = countingHub(kv);
		const { seen, unsub } = await watchQuery(hub, {
			where: (w) => w.count.gt(15),
			orderBy: (w) => [ w.count.desc() ],
		});
		const afterInitial = rowScans();

		await hub.notify(await applyServer(kv, [ serverRow('w3', 2n, { id: 'w3', count: 99 }) ]));

		expect(keysOf(seen.at(-1))).toEqual([ 'w3', 'w19', 'w18', 'w17', 'w16' ]);
		expect(rowScans()).toBe(afterInitial);
		unsub();
	});

	it('does not emit for a change the page cannot see', async () => {
		const { kv, rowScans } = countingStore();
		for (let i = 0; i < 10; i++) {
			await applyServer(kv, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
		}
		const hub = countingHub(kv);
		const { seen, unsub } = await watchQuery(hub, {
			orderBy: (w) => [ w.count.asc() ],
			limit: 3,
		});
		expect(keysOf(seen.at(-1))).toEqual([ 'w0', 'w1', 'w2' ]);
		const emits = seen.length;
		const scans = rowScans();

		// Past the page: the row moves within the match set, and no index the
		// page covers moves with it.
		await hub.notify(await applyServer(kv, [ serverRow('w7', 2n, { id: 'w7', count: 42 }) ]));
		expect(seen.length).toBe(emits);
		expect(rowScans()).toBe(scans);

		// Inside the page: delivered.
		await hub.notify(await applyServer(kv, [ serverRow('w1', 3n, { id: 'w1', count: -1 }) ]));
		expect(seen.length).toBe(emits + 1);
		expect(keysOf(seen.at(-1))).toEqual([ 'w1', 'w0', 'w2' ]);
		unsub();
	});

	it('fills a full page from the match set when a row is deleted out of it', async () => {
		// The case a page-only result cannot answer without going back to the
		// store: the row that replaces a deleted one was never in the page.
		const { kv, rowScans } = countingStore();
		for (let i = 0; i < 10; i++) {
			await applyServer(kv, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
		}
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });
		const { seen, unsub } = await watchQuery(hub, {
			orderBy: (w) => [ w.count.asc() ],
			limit: 3,
		});
		expect(keysOf(seen.at(-1))).toEqual([ 'w0', 'w1', 'w2' ]);
		const scans = rowScans();

		await hub.notify(await applyServer(kv, [ tombstone('w1', 2n) ]));

		expect(keysOf(seen.at(-1))).toEqual([ 'w0', 'w2', 'w3' ]);
		// debugVerifyTouches scans once per notify by design; what must NOT have
		// happened is a second scan to find w3.
		expect(rowScans()).toBe(scans + 1);
		unsub();
	});

	it('honours an offset the same way a scan would', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });
		for (let i = 0; i < 6; i++) {
			await applyServer(kv, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
		}
		const { seen, unsub } = await watchQuery(hub, {
			orderBy: (w) => [ w.count.asc() ],
			offset: 2,
			limit: 2,
		});
		expect(keysOf(seen.at(-1))).toEqual([ 'w2', 'w3' ]);

		// A row entering BELOW the offset still moves the page: everything slides.
		await hub.notify(await applyServer(kv, [ serverRow('x', 1n, { id: 'x', count: -1 }) ]));
		expect(keysOf(seen.at(-1))).toEqual([ 'w1', 'w2' ]);
		unsub();
	});

	it('shares one read between watchers over the same entity', async () => {
		// Two queries over one entity are two results but one read: the commit's
		// rows are fetched and decoded once, and each watcher decides admission
		// against the same objects.
		const { kv, rowScans } = countingStore();
		for (let i = 0; i < 6; i++) {
			await applyServer(kv, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
		}
		const hub = countingHub(kv);
		// Overlapping, so one row lands in both results and its identity is
		// comparable across them.
		const low = await watchQuery(hub, { where: (w) => w.count.lt(4) });
		const high = await watchQuery(hub, { where: (w) => w.count.gte(2) });
		const scans = rowScans();

		await hub.notify(await applyServer(kv, [ serverRow('w2', 2n, { id: 'w2', count: 3 }) ]));

		expect(keysOf(low.seen.at(-1))).toEqual([ 'w0', 'w1', 'w2', 'w3' ]);
		expect(keysOf(high.seen.at(-1))).toEqual([ 'w2', 'w3', 'w4', 'w5' ]);
		expect(rowScans()).toBe(scans);
		// One decode, one object: the changed row is the SAME row in both results.
		expect(low.seen.at(-1)?.[2]).toBe(high.seen.at(-1)?.[0]);
		low.unsub();
		high.unsub();
	});

	it('breaks a cross-workspace tie the way the scan order does', async () => {
		// compareRows ties on the row key alone, so two rows in different
		// workspaces that agree on every order key AND share a key fall through to
		// the scan's own order — workspace segment, then key. A maintained result
		// binary-searches against that order, so it has to reproduce it exactly.
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });
		const seen: Row<Widget>[][] = [];
		const unsub = hub.watch(
			WidgetSchema,
			undefined,
			(rows) => seen.push(rows as Row<Widget>[]),
			compileQuery<Widget>({ orderBy: (w) => [ w.count.asc() ] }),
		);
		await flush();

		for (const ws of [ 'wsB', 'wsA' ]) {
			const touched = new TouchSet();
			const tx = await kv.write();
			await applyServerChanges(
				tx,
				registry,
				ws,
				[ serverRow('a', 1n, { id: 'a', count: 1 }) ],
				touched,
			);
			await tx.commit();
			await hub.notify(touched);
		}

		expect(seen.at(-1)?.map((r) => r.workspaceId)).toEqual([ 'wsA', 'wsB' ]);
		unsub();
	});

	it('suppresses a commit that leaves the result byte-identical', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		const { seen, unsub } = await watchQuery(hub, {});
		await hub.notify(await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1', count: 1 }) ]));
		expect(seen).toHaveLength(2);

		// Same bytes AND same version: nothing a reader can observe moved.
		await hub.notify(await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1', count: 1 }) ]));
		expect(seen).toHaveLength(2);

		// Same bytes, new version: a compare-and-swap guard reads that, so it is
		// a change and has to be delivered.
		await hub.notify(await applyServer(kv, [ serverRow('w1', 2n, { id: 'w1', count: 1 }) ]));
		expect(seen).toHaveLength(3);
		expect(seen.at(-1)?.[0]?.version).toBe(2n);
		unsub();
	});
});

// --- index-seeded first reads --------------------------------------------------

const note = entity('note', {
	id: text().key(),
	state: text().indexed(),
	rank: int32(),
}, {});

type Note = Infer<typeof note>;
const noteBundle = defineSchema(note);
const NoteSchema = noteBundle.descMessage('note');
const noteRegistry = new EntityRegistry([ NoteSchema ]);

const NOTES: Note[] = [
	{ id: 'n1', state: 'open', rank: 1 },
	{ id: 'n2', state: 'done', rank: 2 },
	{ id: 'n3', state: 'open', rank: 3 },
	{ id: 'n4', state: 'done', rank: 4 },
];

async function seedNotes(kv: KVStore, indexes: IndexCache): Promise<void> {
	const tx = await kv.write();
	// A build only walks a workspace holding a whole installed snapshot, and the
	// cursor is what says so.
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		noteRegistry,
		WS,
		NOTES.map((row, i) =>
			create(RowChangeSchema, {
				entity: 'note',
				key: row.id,
				version: BigInt(i + 1),
				body: { case: 'value', value: toBinary(NoteSchema, create(NoteSchema, row)) },
			})
		),
		new TouchSet(),
		indexes,
	);
	await tx.commit();
}

describe('index-seeded watch results', () => {
	it('serves a covered first read from the index and keeps maintaining it', async () => {
		const { kv, rowScans } = countingStore();
		const indexes = new IndexCache(noteRegistry);
		await seedNotes(kv, indexes);
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), noteRegistry, indexes, WS);
		const before = rowScans();

		const hub = new WatcherHub(kv, noteRegistry, mutex, {
			indexes: (entityName) => indexes.for(entityName),
			debugVerifyTouches: false,
		});
		const seen: Row<Note>[][] = [];
		const unsub = hub.watch(
			NoteSchema,
			WS,
			(rows) => seen.push(rows as unknown as Row<Note>[]),
			compileQuery<Note>({ where: (n) => n.state.eq('open') }) as never,
		);
		await flush();

		expect(seen.at(-1)?.map((r) => r.key)).toEqual([ 'n1', 'n3' ]);
		// The whole point: the first read walked index positions and the rows
		// they name, never the entity.
		expect(rowScans()).toBe(before);

		// And the result it seeded is a maintained one, not a one-shot answer.
		const touched = new TouchSet();
		const tx = await kv.write();
		await applyServerChanges(
			tx,
			noteRegistry,
			WS,
			[ create(RowChangeSchema, {
				entity: 'note',
				key: 'n2',
				version: 9n,
				body: {
					case: 'value',
					value: toBinary(NoteSchema, create(NoteSchema, { id: 'n2', state: 'open', rank: 2 })),
				},
			}) ],
			touched,
			indexes,
		);
		await tx.commit();
		await hub.notify(touched);

		expect(seen.at(-1)?.map((r) => r.key)).toEqual([ 'n1', 'n2', 'n3' ]);
		expect(rowScans()).toBe(before);
		unsub();
	});

	it('scans when no index covers the query', async () => {
		const { kv, rowScans } = countingStore();
		const indexes = new IndexCache(noteRegistry);
		await seedNotes(kv, indexes);
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), noteRegistry, indexes, WS);
		const before = rowScans();

		const hub = new WatcherHub(kv, noteRegistry, mutex, {
			indexes: (entityName) => indexes.for(entityName),
			debugVerifyTouches: false,
		});
		const seen: Row<Note>[][] = [];
		// `rank` carries no index, so the planner declines and the scan — which
		// is the definition of the answer — serves it.
		const unsub = hub.watch(
			NoteSchema,
			WS,
			(rows) => seen.push(rows as unknown as Row<Note>[]),
			compileQuery<Note>({ where: (n) => n.rank.gt(2) }) as never,
		);
		await flush();

		expect(seen.at(-1)?.map((r) => r.key)).toEqual([ 'n3', 'n4' ]);
		expect(rowScans()).toBeGreaterThan(before);
		unsub();
	});
});
