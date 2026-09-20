// A local read merges the optimistic overlay, and an index-served one keeps
// the overlay it read so the next read of the same shape does not walk the
// pending queue again. What makes that safe is a generation token the STORE
// holds (keys.overlayGeneration): every committed transaction that touches the
// pair's `o/` rows or `ot/` fragments restamps it, so a reader that finds its
// own token knows nothing has moved.
//
// These pin the invalidation across CLIENTS, which is the case no in-process
// signal can cover: two SyncClients sharing one store stand in for two tabs,
// and the only thing that can tell the first one its copy is stale is the
// token the second one stamped.

import 'fake-indexeddb/auto';

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import {
	metaCursor,
	overlayGenerationFor,
	overlayKey,
	type OverlayRecord,
	overlayTextKey,
} from '../../src/keys.js';
import { IDBKVStore } from '../../src/kv/idb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { compactPending } from '../../src/optimistic/compact.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges, clearOverlayAndReplay } from '../../src/optimistic/rebase.js';
import type { Query } from '../../src/query/index.js';
import { readOverlay } from '../../src/read.js';
import { defineSchema, entity, type Infer, int32, text } from '../../src/schema/builder.js';
import type { QueryEvent } from '../../src/sync/events.js';
import { encodeFragmentFrom, encodeTextDoc, TextDoc } from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';

const note = entity('note', {
	id: text().key(),
	title: text(),
	state: text().indexed(),
	rank: int32(),
});
type Note = Infer<typeof note>;
const NoteSchema = defineSchema(note).descMessage('note');
const registry = new EntityRegistry([ NoteSchema ]);
const WS = 'ws1';
const CLIENT = 'c1';

const SEED: Note[] = [
	{ id: 'n1', title: 'one', state: 'open', rank: 1 },
	{ id: 'n2', title: 'two', state: 'open', rank: 2 },
	{ id: 'n3', title: 'three', state: 'done', rank: 3 },
];

/** Plants SEED as confirmed rows and builds the index over them — the state an
 * index-served read needs, and the only state an index covers. */
async function seed<S extends KVStore>(kv: S): Promise<S> {
	const indexes = new IndexCache(registry);
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		WS,
		SEED.map((row, i) =>
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
	const mutex = new ChainMutex();
	await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	return kv;
}

const seeded = (): Promise<MemoryKVStore> => seed(new MemoryKVStore());

const events: QueryEvent[] = [];

function client(kv: KVStore, verify = true): SyncClient<never> {
	return new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ NoteSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		// The scan path keeps no cache of its own, so comparing against it puts
		// every read these tests take under a check on the cached one. The one
		// test that counts requests turns it off: the comparison is a second
		// read of the same rows, and it is the uncached one.
		debugVerifyIndexReads: verify,
		onSyncEvent: (e) => {
			if (e.type === 'query') events.push(e);
		},
	}) as unknown as SyncClient<never>;
}

/**
 * Two clients over one store — two tabs, each with its own overlay cache —
 * with one overlay row already written.
 *
 * The row matters: an EMPTY overlay has no generation to cache under, so a
 * client whose first read found one would have kept nothing and every case
 * below would pass without the token doing anything. This one is `done`, so
 * it stays out of the results the tests read.
 */
async function twoClients(): Promise<{
	kv: MemoryKVStore;
	a: SyncClient<never>;
	b: SyncClient<never>;
}> {
	const kv = await seeded();
	const a = client(kv);
	const b = client(kv);
	await b.mutate.put(NoteSchema, { id: 'n8', title: 'eight', state: 'done', rank: 8 } as never);
	return { kv, a, b };
}

/** The keys `open` notes come back as, in rank order — an index-served read. */
async function open(c: SyncClient<never>): Promise<string[]> {
	events.length = 0;
	const q: Query<Note> = {
		where: (n) => n.state.eq('open'),
		orderBy: (n) => [ n.rank.asc() ],
		limit: 10,
	};
	const rows = await c.query(NoteSchema, q as never);
	// Otherwise these would pass against a planner that quietly declined, where
	// nothing they are about would have run.
	expect(events.at(-1)?.path).toBe('index');
	return (rows as unknown as { key: string }[]).map((r) => r.key);
}

const nine = { id: 'n9', title: 'nine', state: 'open', rank: 9 };

describe('overlay cache across clients sharing a store', () => {
	it('sees a row another client wrote after the cache was filled', async () => {
		const { a, b } = await twoClients();
		try {
			expect(await open(a)).toEqual([ 'n1', 'n2' ]);
			await b.mutate.put(NoteSchema, nine as never);
			expect(await open(a)).toEqual([ 'n1', 'n2', 'n9' ]);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it('sees a row another client deleted', async () => {
		const { a, b } = await twoClients();
		try {
			expect(await open(a)).toEqual([ 'n1', 'n2' ]);
			await b.mutate.del(NoteSchema, 'n1');
			expect(await open(a)).toEqual([ 'n2' ]);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it('sees a row another client patched out of the result', async () => {
		const { a, b } = await twoClients();
		try {
			expect(await open(a)).toEqual([ 'n1', 'n2' ]);
			await b.mutate.patch(NoteSchema, 'n2', { state: 'done' } as never);
			expect(await open(a)).toEqual([ 'n1' ]);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it('sees the overlay a rebase cleared and replayed', async () => {
		const { kv, a, b } = await twoClients();
		try {
			await b.mutate.put(NoteSchema, nine as never);
			expect(await open(a)).toEqual([ 'n1', 'n2', 'n9' ]);

			// What a pull does to the overlay: every row goes, and the mutations
			// the server has not acked are applied again — here as a different
			// value, so answering from the cleared copy is visible.
			const tx = await kv.write();
			await clearOverlayAndReplay(tx, registry, {}, WS, [
				putMutation(1n, { ...nine, rank: 0 }),
			], new TouchSet());
			await tx.commit();

			expect(await open(a)).toEqual([ 'n9', 'n1', 'n2' ]);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it('sees the overlay a compaction rewrote', async () => {
		const { kv, a, b } = await twoClients();
		try {
			await b.mutate.put(NoteSchema, nine as never);
			await b.mutate.patch(NoteSchema, 'n9', { rank: 0 } as never);
			expect(await open(a)).toEqual([ 'n9', 'n1', 'n2' ]);

			await compactPending(kv, registry, {}, WS, () => new Set<bigint>());

			// Folding two mutations into one rewrites the overlay row from the
			// queue without changing what it holds, so this is the other side of
			// the check: a rewrite that says nothing new must not say anything
			// wrong either.
			expect(await open(a)).toEqual([ 'n9', 'n1', 'n2' ]);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it('loses a row whose overlay a drained queue took away', async () => {
		const { kv, a, b } = await twoClients();
		try {
			await b.mutate.put(NoteSchema, nine as never);
			expect(await open(a)).toEqual([ 'n1', 'n2', 'n9' ]);

			// The shape an acked queue leaves behind: the overlay is cleared and
			// nothing is left to replay.
			const tx = await kv.write();
			await clearOverlayAndReplay(tx, registry, {}, WS, [], new TouchSet());
			await tx.commit();

			expect(await open(a)).toEqual([ 'n1', 'n2' ]);
		} finally {
			await a.close();
			await b.close();
		}
	});
});

/** One queued put of `row`, the shape a rebase replays. */
function putMutation(id: bigint, row: Note): Mutation {
	return create(MutationSchema, {
		clientId: CLIENT,
		mutationId: id,
		payload: {
			case: 'crud',
			value: buildPut(registry.get('note')!, NoteSchema, create(NoteSchema, row)),
		},
	});
}

/**
 * The text log is the overlay's other half: a fragment appended under `ot/`
 * changes the bytes a row folds to without touching the row itself, so it has
 * to restamp the same generation.
 *
 * Read through readOverlay rather than through a query, because the entity
 * carrying a collaborative-text field here declares no index — what is under
 * test is the fold the cache holds, not the planner above it.
 */
describe('overlay cache and the text log', () => {
	const readers = new EntityRegistry([ WidgetSchema ]);

	const read = async (kv: KVStore) => {
		const tx = await kv.read();
		try {
			return await readOverlay(tx, readers, 'widget', WS);
		} finally {
			tx.release();
		}
	};

	it('refolds a row after another writer appended a fragment', async () => {
		const kv = new MemoryKVStore();
		kv.trackGenerations(overlayGenerationFor);

		const doc = new TextDoc('peer-a');
		doc.insert(0, 'hello');
		const row: OverlayRecord = {
			v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body: encodeTextDoc(doc) })),
		};
		const tx = await kv.write();
		await tx.put(overlayKey('widget', WS, 'w1'), row);
		await tx.commit();

		const before = await read(kv);
		expect(before).toHaveLength(1);
		const foldedOnce = before[0]!.bytes!;
		expect(foldedOnce).toBeInstanceOf(Uint8Array);
		// The same read twice over an unchanged store: the cached copy, and the
		// same bytes.
		expect((await read(kv))[0]!.bytes).toBe(foldedOnce);

		// Another writer's keystroke: the row is untouched, the fragment is not.
		const range = doc.insert(5, '!');
		const fragment = encodeFragmentFrom(
			doc,
			Array.from({ length: range.count }, (_, i) => ({
				peer: range.peer,
				counter: range.start + i,
			})),
			[],
		);
		const tx2 = await kv.write();
		await tx2.put(overlayTextKey('widget', WS, 'w1', 'body', 1n, 0), fragment);
		await tx2.commit();

		const after = await read(kv);
		expect(after[0]!.bytes).not.toEqual(foldedOnce);
		expect(fromBinary(WidgetSchema, after[0]!.bytes!).body).toEqual(encodeTextDoc(doc));
	});

	it('reads the store again when it keeps no generation at all', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		await tx.put(
			overlayKey('widget', WS, 'w1'),
			{
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1' })),
			} satisfies OverlayRecord,
		);
		await tx.commit();

		// No mapping installed, so no token is stamped and nothing may be held:
		// a store an older build wrote reads as unknown, not as unchanged.
		const first = await read(kv);
		const tx2 = await kv.write();
		await tx2.put(
			overlayKey('widget', WS, 'w2'),
			{
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w2' })),
			} satisfies OverlayRecord,
		);
		await tx2.commit();

		expect(first).toHaveLength(1);
		expect(await read(kv)).toHaveLength(2);
	});
});

/**
 * What the cache is for, read off the store rather than off a clock: the
 * second of two identical reads must not ask IndexedDB for the overlay prefix
 * at all.
 *
 * A duration would say the same thing less reliably; the request is the work.
 */
describe('overlay cache request cost', () => {
	it('asks IndexedDB for the overlay prefix once across two identical reads', async () => {
		const kv = await seed(await IDBKVStore.open(`overlay-cache-${Date.now()}`));
		const a = client(kv, false);
		const b = client(kv, false);
		await b.mutate.put(NoteSchema, { id: 'n8', title: 'eight', state: 'done', rank: 8 } as never);

		const lowerBounds: string[] = [];
		const original = IDBObjectStore.prototype.getAll;
		IDBObjectStore.prototype.getAll = function(
			this: IDBObjectStore,
			query?: IDBValidKey | IDBKeyRange | null,
			count?: number,
		) {
			if (query instanceof IDBKeyRange && typeof query.lower === 'string') {
				lowerBounds.push(query.lower);
			}
			return original.call(this, query, count);
		};
		try {
			await open(a);
			const first = lowerBounds.filter((at) => at.startsWith(`o/note/${WS}/`)).length;
			lowerBounds.length = 0;
			await open(a);
			const second = lowerBounds.filter((at) => at.startsWith(`o/note/${WS}/`)).length;

			expect(first).toBeGreaterThan(0);
			expect(second).toBe(0);
		} finally {
			IDBObjectStore.prototype.getAll = original;
			await a.close();
			await b.close();
		}
	});
});
