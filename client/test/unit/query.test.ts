import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { rowTouchKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import { applyMutationLocally, type MirrorMap, rebase } from '../../src/optimistic/rebase.js';
import { and, compileQuery } from '../../src/query/index.js';
import type { Row } from '../../src/read.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe('client query', () => {
	it('runs a full query through the db facade', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({
			id: 'w1',
			label: 'tmp-a',
			count: 20,
			settings: { color: 'red' },
		});
		await client.db.widget.mutate.put({
			id: 'w2',
			label: 'keep',
			count: 15,
			settings: { color: 'red' },
		});
		await client.db.widget.mutate.put({
			id: 'w3',
			label: 'keep',
			count: 30,
			settings: { color: 'blue' },
		});
		await client.db.widget.mutate.put({
			id: 'w4',
			label: 'keep',
			count: 5,
			settings: { color: 'red' },
			tags: [ 'urgent' ],
		});

		const rows = await client.db.widget.query({
			where: (w) => and(w.settings.color.eq('red'), w.count.gt(10)),
			orderBy: (w) => [ w.count.desc() ],
		});
		expect(rows.map((r) => r.key)).toEqual([ 'w1', 'w2' ]);

		const limited = await client.db.widget.query({
			orderBy: (w) => [ w.count.asc() ],
			limit: 2,
			offset: 1,
		});
		expect(limited.map((r) => r.value.count)).toEqual([ 15, 20 ]);
		await client.close();
	});

	it('schema-first query matches the facade', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', count: 1 });
		await client.db.widget.mutate.put({ id: 'w2', count: 2 });

		const viaFacade = await client.db.widget.query({ where: (w) => w.count.gte(2) });
		const viaSchema = await client.query(WidgetSchema, { where: (w) => w.count.gte(2) });
		expect(viaSchema).toEqual(viaFacade);
		expect(viaSchema.map((r) => r.key)).toEqual([ 'w2' ]);
		await client.close();
	});

	it('watchQuery emits the initial snapshot, fires on matches and suppresses non-matches', async () => {
		const client = newClient();
		const snapshots: Row<Widget>[][] = [];
		const unsub = client.db.widget.watchQuery(
			{ where: (w) => w.count.gt(10) },
			(rows) => snapshots.push(rows),
		);
		await flush();
		expect(snapshots).toHaveLength(1);
		expect(snapshots[0]).toEqual([]);

		await client.db.widget.mutate.put({ id: 'w1', count: 20 });
		await flush();
		expect(snapshots).toHaveLength(2);
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'w1' ]);

		// Non-matching put touches the entity but leaves the result unchanged.
		await client.db.widget.mutate.put({ id: 'w2', count: 5 });
		await flush();
		expect(snapshots).toHaveLength(2);

		// Changing a matching row's content re-emits even though keys are equal.
		await client.db.widget.mutate.put({ id: 'w1', count: 21 });
		await flush();
		expect(snapshots).toHaveLength(3);
		expect(snapshots.at(-1)?.[0]?.value.count).toBe(21);

		unsub();
		await client.close();
	});

	it('plain watch fires only when the rows move', async () => {
		const client = newClient();
		let calls = 0;
		const unsub = client.db.widget.watch(() => calls++);
		await flush();
		const initial = calls;
		await client.db.widget.mutate.put({ id: 'w1', count: 1 });
		await flush();
		expect(calls).toBe(initial + 1);
		await client.db.widget.mutate.put({ id: 'w1', count: 1 }); // state-identical
		await flush();
		expect(calls).toBe(initial + 1);
		unsub();
		await client.close();
	});
});

// --- watchQuery across optimistic apply + rebase, at the WatcherHub level ---

const registry = new EntityRegistry([ WidgetSchema ]);
const noMirrors: MirrorMap = {};
const cursor = (n: number) => new Uint8Array([ n ]);

function putMutation(
	id: bigint,
	widget: MessageInitShape<typeof WidgetSchema> & { id: string },
): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'widget',
				key: widget.id,
				op: CrudOp.PUT,
				value: toBinary(WidgetSchema, create(WidgetSchema, widget)),
			},
		},
	});
}

function serverRow(
	key: string,
	version: bigint,
	widget: MessageInitShape<typeof WidgetSchema> & { id: string },
): RowChange {
	return create(RowChangeSchema, {
		entity: 'widget',
		key,
		version,
		body: { case: 'value', value: toBinary(WidgetSchema, create(WidgetSchema, widget)) },
	});
}

/** The workspace these specs drive. */
const WS = 'ws1';

async function enqueueLocally(kv: MemoryKVStore, ...mutations: Mutation[]): Promise<TouchSet> {
	const touched = new TouchSet();
	const tx = await kv.write();
	for (const m of mutations) {
		await enqueuePending(tx, WS, m);
		await applyMutationLocally(tx, registry, noMirrors, WS, m, touched);
	}
	await tx.commit();
	return touched;
}

describe('watchQuery across rebase', () => {
	it('drops a row from the result when the rebase moves it out of the filter', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		const touched = await enqueueLocally(kv, putMutation(1n, { id: 'w1', count: 20 }));
		expect(touched.tokens().has(rowTouchKey(WS, 'widget', 'w1'))).toBe(true);

		const snapshots: Row<Widget>[][] = [];
		const unsub = hub.watch(
			WidgetSchema,
			WS,
			(rows) => snapshots.push(rows as Row<Widget>[]),
			compileQuery<Widget>({ where: (w) => w.count.gt(10) }),
		);
		await flush();
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'w1' ]);

		// Server acks the mutation but its authoritative row has count 5.
		const rebased = await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('w1', 1n, { id: 'w1', count: 5 }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 1n,
		});
		await hub.notify(rebased.touched);
		expect(snapshots.at(-1)).toEqual([]);
		unsub();
	});

	it('re-fires a byte-identical rebase that only bumps the version', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('w1', 1n, { id: 'w1', count: 15 }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		const snapshots: Row<Widget>[][] = [];
		const unsub = hub.watch(
			WidgetSchema,
			WS,
			(rows) => snapshots.push(rows as Row<Widget>[]),
			compileQuery<Widget>({ where: (w) => w.count.gt(10) }),
		);
		await flush();
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'w1' ]);
		const count = snapshots.length;
		expect(snapshots.at(-1)?.[0]?.version).toBe(1n);

		// A new version with byte-identical row content must still re-fire:
		// Row.version drives compare-and-swap, so a version-only change is a
		// real change the watcher needs to see.
		const { touched } = await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('w1', 2n, { id: 'w1', count: 15 }) ],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});
		await hub.notify(touched);
		expect(snapshots.length).toBe(count + 1);
		expect(snapshots.at(-1)?.[0]?.version).toBe(2n);
		unsub();
	});

	it('delivers the real version when an optimistic create is server-acked', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		// Optimistic create: exists only as an overlay, so version 0n.
		await enqueueLocally(kv, putMutation(1n, { id: 'w1', count: 20 }));

		const snapshots: Row<Widget>[][] = [];
		const unsub = hub.watch(
			WidgetSchema,
			WS,
			(rows) => snapshots.push(rows as Row<Widget>[]),
			compileQuery<Widget>({ where: (w) => w.count.gt(10) }),
		);
		await flush();
		expect(snapshots.at(-1)?.[0]?.version).toBe(0n);
		const count = snapshots.length;

		// Server acks with byte-identical content but a real version; the overlay
		// is dropped for confirmed bytes. The watcher must re-fire so a CAS driven
		// off the result no longer sends expectedVersion 0n against a live row.
		const { touched } = await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('w1', 7n, { id: 'w1', count: 20 }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 1n,
		});
		await hub.notify(touched);
		expect(snapshots.length).toBe(count + 1);
		expect(snapshots.at(-1)?.[0]?.version).toBe(7n);
		unsub();
	});
});

describe('WatcherHub decode reuse', () => {
	// A commit reports only (workspace, entity), so a re-scan has to read every
	// row of the entity. It must not DECODE every row: the decoded value of a
	// row whose bytes did not move is reused, which is observable as object
	// identity across emits.
	it('reuses the decoded value of rows a commit did not touch', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		await enqueueLocally(
			kv,
			putMutation(1n, { id: 'w1', count: 20 }),
			putMutation(2n, { id: 'w2', count: 30 }),
		);

		const snapshots: Row<Widget>[][] = [];
		const unsub = hub.watch(
			WidgetSchema,
			WS,
			(rows) => snapshots.push(rows as Row<Widget>[]),
			compileQuery<Widget>({ where: (w) => w.count.gt(10) }),
		);
		await flush();
		const first = snapshots.at(-1);
		expect(first?.map((r) => r.key)).toEqual([ 'w1', 'w2' ]);

		const touched = await enqueueLocally(kv, putMutation(3n, { id: 'w1', count: 25 }));
		await hub.notify(touched);
		const second = snapshots.at(-1);
		expect(second?.map((r) => r.key)).toEqual([ 'w1', 'w2' ]);

		expect(second?.[1]?.value).toBe(first?.[1]?.value);
		expect(second?.[0]?.value).not.toBe(first?.[0]?.value);
		expect(second?.[0]?.value.count).toBe(25);
		unsub();
	});

	it('forgets a scope once its last watcher unsubscribes', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		await enqueueLocally(kv, putMutation(1n, { id: 'w1', count: 20 }));

		const seen: Row<Widget>[][] = [];
		const watch = () => hub.watch(WidgetSchema, WS, (rows) => seen.push(rows as Row<Widget>[]));

		const unsubA = watch();
		await flush();
		const before = seen.at(-1)?.[0]?.value;
		unsubA();

		const unsubB = watch();
		await flush();
		// Cache dropped with the last watcher, so this is a fresh decode rather
		// than a value retained for a scope nobody was watching.
		expect(seen.at(-1)?.[0]?.value).not.toBe(before);
		unsubB();
	});

	it('keeps a scope cache alive while another watcher still needs it', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		await enqueueLocally(kv, putMutation(1n, { id: 'w1', count: 20 }));

		const seen: Row<Widget>[][] = [];
		const unsubA = hub.watch(WidgetSchema, WS, () => {});
		const unsubB = hub.watch(WidgetSchema, WS, (rows) => seen.push(rows as Row<Widget>[]));
		await flush();
		const before = seen.at(-1)?.[0]?.value;

		unsubA();
		const touched = await enqueueLocally(kv, putMutation(2n, { id: 'w2', count: 5 }));
		await hub.notify(touched);
		expect(seen.at(-1)?.find((r) => r.key === 'w1')?.value).toBe(before);
		unsubB();
	});
});

describe('WatcherHub isolates throwing callbacks', () => {
	it('keeps notifying later watchers when an earlier one throws', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			await enqueueLocally(kv, putMutation(1n, { id: 'w1', count: 20 }));

			// Both watchers are on the same entity, so one notify() drives both. The
			// first throws; the dispatch loop must still reach the second.
			const unsubA = hub.watch(WidgetSchema, WS, () => {
				throw new Error('boom');
			});
			const seen: Row<Widget>[][] = [];
			const unsubB = hub.watch(WidgetSchema, WS, (rows) => seen.push(rows as Row<Widget>[]));
			await flush();
			expect(seen.length).toBe(1);

			const touched = await enqueueLocally(kv, putMutation(2n, { id: 'w2', count: 5 }));
			// A throwing watcher must not reject notify() — the commit already landed.
			await expect(hub.notify(touched)).resolves.toBeUndefined();
			expect(seen.length).toBe(2);
			expect(seen.at(-1)?.map((r) => r.key).sort()).toEqual([ 'w1', 'w2' ]);
			expect(errSpy).toHaveBeenCalled();

			unsubA();
			unsubB();
		} finally {
			errSpy.mockRestore();
		}
	});
});
