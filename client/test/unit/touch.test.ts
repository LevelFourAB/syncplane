import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import type { Coverage } from '../../src/coverage.js';
import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { rowTouchKey, touchKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import {
	applyMutationLocally,
	applyServerChanges,
	type MirrorMap,
	wipeConfirmedState,
} from '../../src/optimistic/rebase.js';
import { compileQuery } from '../../src/query/index.js';
import type { Row } from '../../src/read.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';

const registry = new EntityRegistry([ WidgetSchema ]);
const noMirrors: MirrorMap = {};
const WS = 'ws1';
const OTHER_WS = 'ws2';

const flush = () => new Promise((r) => setTimeout(r, 0));

function widgetBytes(w: MessageInitShape<typeof WidgetSchema> & { id: string }): Uint8Array {
	return toBinary(WidgetSchema, create(WidgetSchema, w));
}

function putMutation(
	id: bigint,
	w: MessageInitShape<typeof WidgetSchema> & { id: string },
): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: { entity: 'widget', key: w.id, op: CrudOp.PUT, value: widgetBytes(w) },
		},
	});
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

/** Applies server changes in one commit and returns exactly what they touched. */
async function applyServer(
	kv: MemoryKVStore,
	changes: RowChange[],
	workspaceId = WS,
): Promise<TouchSet> {
	const touched = new TouchSet();
	const tx = await kv.write();
	await applyServerChanges(tx, registry, workspaceId, changes, touched);
	await tx.commit();
	return touched;
}

describe('TouchSet', () => {
	it('reports one token per changed row', () => {
		const t = new TouchSet();
		t.row(WS, 'widget', 'w1');
		t.row(WS, 'widget', 'w2');
		expect(t.tokens()).toEqual(
			new Set([ rowTouchKey(WS, 'widget', 'w1'), rowTouchKey(WS, 'widget', 'w2') ]),
		);
	});

	it('collapses a pair to a wildcard when the whole entity changes', () => {
		const t = new TouchSet();
		t.row(WS, 'widget', 'w1');
		t.entity(WS, 'widget');
		expect(t.tokens()).toEqual(new Set([ touchKey(WS, 'widget') ]));
	});

	it('ignores a row reported after its pair went wildcard', () => {
		const t = new TouchSet();
		t.entity(WS, 'widget');
		t.row(WS, 'widget', 'w1');
		expect(t.tokens()).toEqual(new Set([ touchKey(WS, 'widget') ]));
	});

	it('keeps pairs independent — a wildcard in one workspace leaves another keyed', () => {
		const t = new TouchSet();
		t.row(WS, 'widget', 'w1');
		t.entity(OTHER_WS, 'widget');
		expect(t.tokens()).toEqual(
			new Set([ rowTouchKey(WS, 'widget', 'w1'), touchKey(OTHER_WS, 'widget') ]),
		);
	});

	it('is empty until something is recorded', () => {
		const t = new TouchSet();
		expect(t.isEmpty()).toBe(true);
		t.row(WS, 'widget', 'w1');
		expect(t.isEmpty()).toBe(false);
	});

	it('exposes pairs with their keys, and undefined keys for a wildcard', () => {
		const t = new TouchSet();
		t.row(WS, 'widget', 'w1');
		t.entity(OTHER_WS, 'widget');
		const pairs = [ ...t.pairs() ].sort((a, b) => a.workspaceId.localeCompare(b.workspaceId));
		expect(pairs).toEqual([
			{ workspaceId: WS, entity: 'widget', keys: new Set([ 'w1' ]) },
			{ workspaceId: OTHER_WS, entity: 'widget' },
		]);
	});

	describe('cross-tab tokens', () => {
		it('adds a compat wildcard beside the keyed tokens', () => {
			const t = new TouchSet();
			t.row(WS, 'widget', 'w1');
			expect(t.broadcastTokens()).toEqual(
				new Set([ rowTouchKey(WS, 'widget', 'w1'), touchKey(WS, 'widget') ]),
			);
		});

		it('round-trips keyed tokens, dropping the compat wildcard', () => {
			const t = new TouchSet();
			t.row(WS, 'widget', 'w1');
			t.row(WS, 'widget', 'w2');
			expect(TouchSet.fromTokens(t.broadcastTokens()).tokens()).toEqual(t.tokens());
		});

		it('keeps a genuine wildcard from a tab that reports no keys', () => {
			const t = new TouchSet();
			t.entity(WS, 'widget');
			const received = TouchSet.fromTokens(t.broadcastTokens());
			expect(received.tokens()).toEqual(new Set([ touchKey(WS, 'widget') ]));
			expect([ ...received.pairs() ][0]?.keys).toBeUndefined();
		});

		it('suppresses a wildcard only for the pair the keyed tokens cover', () => {
			const received = TouchSet.fromTokens([
				rowTouchKey(WS, 'widget', 'w1'),
				touchKey(WS, 'widget'),
				touchKey(OTHER_WS, 'widget'),
			]);
			expect(received.tokens()).toEqual(
				new Set([ rowTouchKey(WS, 'widget', 'w1'), touchKey(OTHER_WS, 'widget') ]),
			);
		});

		it('survives a row key that itself contains the token separator', () => {
			const key = 'a\u001fb\u001fc';
			const t = new TouchSet();
			t.row(WS, 'widget', key);
			const received = TouchSet.fromTokens(t.broadcastTokens());
			expect([ ...received.pairs() ]).toEqual([
				{ workspaceId: WS, entity: 'widget', keys: new Set([ key ]) },
			]);
		});
	});
});

describe('WatcherHub incremental notification', () => {
	/** Wraps a store so scans can be counted (and distinguished from point gets). */
	function countingStore(): { kv: MemoryKVStore; scans: () => number } {
		const kv = new MemoryKVStore();
		let scans = 0;
		const read = kv.read.bind(kv);
		vi.spyOn(kv, 'read').mockImplementation(async () => {
			const tx = await read();
			const inner = tx.scan.bind(tx);
			tx.scan = (prefix: string) => {
				// Only the confirmed/overlay row prefixes are entity scans; the point
				// reads of the incremental path use get().
				if (prefix.startsWith('c/') || prefix.startsWith('o/')) scans++;
				return inner(prefix);
			};
			return tx;
		});
		return { kv, scans: () => scans };
	}

	/**
	 * A hub with the divergence check explicitly OFF: that check scans on every
	 * notify by design, which is exactly what the counting specs measure. The
	 * incremental path itself is verified by the specs that leave it on.
	 */
	function countingHub(kv: MemoryKVStore): WatcherHub {
		return new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: false });
	}

	async function watched(
		kv: MemoryKVStore,
		hub: WatcherHub,
	): Promise<{ snapshots: Row<Widget>[][]; unsub: () => void }> {
		const snapshots: Row<Widget>[][] = [];
		const unsub = hub.watch(WidgetSchema, WS, (rows) => snapshots.push(rows as Row<Widget>[]));
		await flush();
		return { snapshots, unsub };
	}

	it('patches the snapshot from row tokens without rescanning', async () => {
		const { kv, scans } = countingStore();
		await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1', count: 1 }) ]);
		const hub = countingHub(kv);
		const { snapshots, unsub } = await watched(kv, hub);
		const afterInitial = scans();

		await hub.notify(await applyServer(kv, [ serverRow('w2', 2n, { id: 'w2', count: 2 }) ]));

		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'w1', 'w2' ]);
		expect(scans()).toBe(afterInitial);
		unsub();
	});

	it('rescans when the commit only names the entity', async () => {
		const { kv, scans } = countingStore();
		await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1', count: 1 }) ]);
		const hub = countingHub(kv);
		const { snapshots, unsub } = await watched(kv, hub);
		const afterInitial = scans();

		const touched = new TouchSet();
		const tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, touched);
		await tx.commit();
		await hub.notify(touched);

		expect(snapshots.at(-1)).toEqual([]);
		expect(scans()).toBeGreaterThan(afterInitial);
		unsub();
	});

	it('rescans once the touched rows outweigh the snapshot', async () => {
		const { kv, scans } = countingStore();
		const seed: RowChange[] = [];
		for (let i = 0; i < 12; i++) {
			seed.push(serverRow(`w${i}`, BigInt(i + 1), { id: `w${i}`, count: i }));
		}
		await applyServer(kv, seed);
		const hub = countingHub(kv);
		const { snapshots, unsub } = await watched(kv, hub);
		const afterInitial = scans();

		// 12 of 12 rows changed — well past both the floor and the fraction.
		const bulk = seed.map((r) =>
			serverRow(r.key, BigInt(r.key.slice(1)) + 100n, { id: r.key, count: 99 })
		);
		await hub.notify(await applyServer(kv, bulk));

		expect(scans()).toBeGreaterThan(afterInitial);
		expect(snapshots.at(-1)?.every((r) => r.value.count === 99)).toBe(true);
		unsub();
	});

	it('keeps the key order a full scan would produce as rows come and go', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		const { snapshots, unsub } = await watched(kv, hub);

		// Inserted out of order: each has to land at the position the scan's
		// lexicographic suffix order puts it in.
		for (const key of [ 'm', 'c', 'z', 'a', 'q' ]) {
			await hub.notify(await applyServer(kv, [ serverRow(key, 1n, { id: key }) ]));
		}
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'a', 'c', 'm', 'q', 'z' ]);

		await hub.notify(await applyServer(kv, [ tombstone('m', 2n) ]));
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'a', 'c', 'q', 'z' ]);

		await hub.notify(await applyServer(kv, [ tombstone('a', 3n), tombstone('z', 3n) ]));
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'c', 'q' ]);
		unsub();
	});

	it('re-emits a query watcher when only its coverage moved', async () => {
		// Coverage rides on the result array, so a change to it IS a change to
		// what the watcher reported — and the case that matters is the one where
		// nothing else moved: an entity finishing its first snapshot with no
		// matching rows. Suppressing that emit leaves a UI showing a spinner over
		// a result it now knows to be empty.
		const kv = new MemoryKVStore();
		let coverage: Coverage = { complete: false, bounds: [ 'hydrating' ] };
		const hub = new WatcherHub(kv, registry, new ChainMutex(), {
			coverageOf: () => coverage,
		});
		const seen: Coverage[] = [];
		const unsub = hub.watch(
			WidgetSchema,
			WS,
			(rows) => seen.push(rows.coverage),
			compileQuery<Widget>({ where: (w) => w.count.gt(100) }),
		);
		await flush();
		expect(seen).toHaveLength(1);

		// A commit the query does not match, with coverage unchanged: suppressed.
		await hub.notify(await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1', count: 1 }) ]));
		expect(seen).toHaveLength(1);

		// Same rows, coverage now complete: delivered.
		coverage = { complete: true, bounds: [] };
		await hub.notify(await applyServer(kv, [ serverRow('w2', 2n, { id: 'w2', count: 2 }) ]));
		expect(seen).toHaveLength(2);
		expect(seen.at(-1)).toEqual({ complete: true, bounds: [] });
		unsub();
	});

	it('orders a cross-workspace scan by workspace then key', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		const seen: Row<Widget>[][] = [];
		const unsub = hub.watch(
			WidgetSchema,
			undefined,
			(rows) => seen.push(rows as Row<Widget>[]),
		);
		await flush();

		await hub.notify(await applyServer(kv, [ serverRow('b', 1n, { id: 'b' }) ], OTHER_WS));
		await hub.notify(await applyServer(kv, [ serverRow('a', 1n, { id: 'a' }) ], WS));
		await hub.notify(await applyServer(kv, [ serverRow('z', 1n, { id: 'z' }) ], WS));

		expect(seen.at(-1)?.map((r) => [ r.workspaceId, r.key ])).toEqual([
			[ WS, 'a' ],
			[ WS, 'z' ],
			[ OTHER_WS, 'b' ],
		]);
		unsub();
	});

	it('alternating keyed and wildcard commits converge on the same view', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		const { snapshots, unsub } = await watched(kv, hub);

		await hub.notify(await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1', count: 1 }) ]));

		// Wildcard: everything confirmed goes, then a keyed insert on the emptied
		// scope, then another wildcard wipe.
		const wipe = async (): Promise<void> => {
			const touched = new TouchSet();
			const tx = await kv.write();
			await wipeConfirmedState(tx, registry, WS, touched);
			await tx.commit();
			await hub.notify(touched);
		};
		await wipe();
		expect(snapshots.at(-1)).toEqual([]);

		await hub.notify(await applyServer(kv, [ serverRow('w2', 2n, { id: 'w2', count: 2 }) ]));
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'w2' ]);

		await wipe();
		expect(snapshots.at(-1)).toEqual([]);
		unsub();
	});

	it('leaves the scope alone when only an unrelated workspace changed', async () => {
		const { kv, scans } = countingStore();
		await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1' }) ]);
		const hub = countingHub(kv);
		const { snapshots, unsub } = await watched(kv, hub);
		const beforeEmits = snapshots.length;
		const beforeScans = scans();

		await hub.notify(await applyServer(kv, [ serverRow('x', 1n, { id: 'x' }) ], OTHER_WS));

		// The scoped watcher was never woken, so nothing was read at all.
		expect(snapshots.length).toBe(beforeEmits);
		expect(scans()).toBe(beforeScans);
		unsub();
	});

	it('throws under debugVerifyTouches when a commit under-reports its rows', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });
		const { unsub } = await watched(kv, hub);
		await hub.notify(await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1' }) ]));

		// Write a row behind the hub's back, then report a DIFFERENT one.
		const tx = await kv.write();
		await applyServerChanges(tx, registry, WS, [ serverRow('w2', 2n, { id: 'w2' }) ], new TouchSet());
		await tx.commit();
		const lie = new TouchSet();
		lie.row(WS, 'widget', 'w1');

		await expect(hub.notify(lie)).rejects.toThrow(/diverged/);
		unsub();
	});

	/**
	 * A ChainMutex whose shared sections still release normally, but whose
	 * CALLER can be parked afterwards: the read has finished and the lock is
	 * free, yet the code that dispatches the result has not run. That gap is
	 * real — the hub dispatches outside the lock, and nothing sequences two
	 * callers across it (microtask order under ChainMutex, grant order under
	 * WebLockMutex).
	 */
	class ParkingMutex extends ChainMutex {
		private park?: Promise<void>;
		private release?: () => void;

		/** Parks the next shared section's caller until `unpark()`. */
		parkNext(): void {
			this.park = new Promise<void>((r) => (this.release = r));
		}

		unpark(): void {
			this.release?.();
		}

		override async runShared<T>(fn: () => Promise<T>): Promise<T> {
			const parked = this.park;
			this.park = undefined;
			const value = await super.runShared(fn);
			if (parked) await parked;
			return value;
		}
	}

	it('drops an initial snapshot overtaken by a later commit', async () => {
		const kv = new MemoryKVStore();
		await applyServer(kv, [ serverRow('w1', 1n, { id: 'w1' }) ]);
		const mutex = new ParkingMutex();
		const hub = new WatcherHub(kv, registry, mutex);

		// The initial scan sees only w1, then parks before its callback.
		mutex.parkNext();
		const snapshots: Row<Widget>[][] = [];
		const unsub = hub.watch(WidgetSchema, WS, (rows) => snapshots.push(rows as Row<Widget>[]));
		await flush();
		expect(snapshots).toEqual([]);

		// A wildcard commit lands meanwhile. It rescans into a FRESH snapshot, so
		// the parked one is not patched in place and stays at its older view.
		await applyServer(kv, [ serverRow('w2', 2n, { id: 'w2' }) ]);
		const wildcard = new TouchSet();
		wildcard.entity(WS, 'widget');
		await hub.notify(wildcard);
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'w1', 'w2' ]);

		mutex.unpark();
		await flush();

		// The stale initial emit must not land on top of the newer one.
		expect(snapshots.at(-1)?.map((r) => r.key)).toEqual([ 'w1', 'w2' ]);
		unsub();
	});

	it('replays optimistic writes onto the snapshot at row granularity', async () => {
		const kv = new MemoryKVStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex());
		const { snapshots, unsub } = await watched(kv, hub);

		const touched = new TouchSet();
		const tx = await kv.write();
		for (const m of [ putMutation(1n, { id: 'w1', count: 7 }) ]) {
			await enqueuePending(tx, WS, m);
			await applyMutationLocally(tx, registry, noMirrors, WS, m, touched);
		}
		await tx.commit();
		await hub.notify(touched);

		expect(snapshots.at(-1)?.map((r) => [ r.key, r.value.count, r.version ])).toEqual([
			// No confirmed base yet, so the optimistic row carries version 0n.
			[ 'w1', 7, 0n ],
		]);
		unsub();
	});
});
