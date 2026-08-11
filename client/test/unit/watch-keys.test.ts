// A keyed watch answers "what is at these keys" without ever deriving the
// entity around them. The specs here pin the three things that makes
// observable —
//
//   - the store is never scanned, at mount or on any commit, so the cost of a
//     row view is its own row and not the size of what it sits in;
//   - a callback carries only the keys that moved, so a consumer holding many
//     of them can re-render exactly the ones that did;
//   - "not read yet", "no such row" and "gone" are three different answers, and
//     each one reaches the callback.
//
// Correctness of the maintenance is checked the other way round, by the
// `debugVerifyTouches` hub: it point-reads every watched key after each commit
// and throws if what the watcher holds disagrees.

import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import type { Coverage } from '../../src/coverage.js';
import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import type { Row } from '../../src/read.js';
import { TouchSet } from '../../src/touch.js';
import { type RowRef, type ScopedKeyedRows, WatcherHub } from '../../src/watch.js';

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

async function applyServerIn(
	kv: MemoryKVStore,
	workspaceId: string,
	changes: RowChange[],
): Promise<TouchSet> {
	const touched = new TouchSet();
	const tx = await kv.write();
	await applyServerChanges(tx, registry, workspaceId, changes, touched);
	await tx.commit();
	return touched;
}

function applyServer(kv: MemoryKVStore, changes: RowChange[]): Promise<TouchSet> {
	return applyServerIn(kv, WS, changes);
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

/** One delivery: the keys it carried, mapped to the row (or `undefined`) each holds. */
type Delivery = Map<string, Row<Widget> | undefined>;

interface Watched {
	seen: Delivery[];
	handle: { close(): void; setKeys(keys: Iterable<string>): void };
}

/**
 * A watch over one workspace, addressed by bare key — the shape every spec in
 * the first block below reads in, with the refs the hub takes built here.
 */
async function watchKeys(hub: WatcherHub, keys: string[]): Promise<Watched> {
	const refsOf = (ks: Iterable<string>): { workspaceId: string; key: string }[] =>
		[ ...ks ].map((key) => ({ workspaceId: WS, key }));
	const seen: Delivery[] = [];
	const handle = hub.watchRefs(
		WidgetSchema,
		WS,
		refsOf(keys),
		(rows: ScopedKeyedRows<Widget>) => {
			seen.push(new Map(rows.map((r) => [ r.key, r.row ])));
		},
	);
	await flush();
	return {
		seen,
		handle: { close: () => handle.close(), setKeys: (ks) => handle.setRefs(refsOf(ks)) },
	};
}

/** The keys one delivery carried, sorted; `!` marks a key delivered as absent. */
function carried(delivery: Delivery | undefined): string[] {
	if (delivery === undefined) return [];
	return [ ...delivery ].map(([ key, row ]) => (row === undefined ? `${key}!` : key)).sort();
}

function verifyingHub(kv: MemoryKVStore): WatcherHub {
	return new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });
}

describe('keyed watches', () => {
	it('answers every requested key on the first callback, present or not', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);

		const { seen, handle } = await watchKeys(hub, [ 'a', 'ghost' ]);

		// An absent row is an ANSWER, not silence: a consumer cannot tell "no such
		// row" from "still coming" unless the watch says so.
		expect(seen).toHaveLength(1);
		expect(carried(seen[0])).toEqual([ 'a', 'ghost!' ]);
		expect(seen[0]!.get('a')?.value.count).toBe(1);
		handle.close();
	});

	it('never scans the entity, at mount or on a commit', async () => {
		const { kv, rowScans } = countingStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: false });
		for (let i = 0; i < 50; i++) {
			await applyServer(kv, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
		}

		const { seen, handle } = await watchKeys(hub, [ 'w7' ]);
		expect(rowScans()).toBe(0);

		await hub.notify(await applyServer(kv, [ serverRow('w7', 2n, { id: 'w7', count: 99 }) ]));
		expect(rowScans()).toBe(0);
		expect(seen.at(-1)!.get('w7')?.value.count).toBe(99);
		handle.close();
	});

	it('carries only the watched keys a commit named', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [
			serverRow('a', 1n, { id: 'a', count: 1 }),
			serverRow('b', 1n, { id: 'b', count: 2 }),
		]);
		const { seen, handle } = await watchKeys(hub, [ 'a', 'b' ]);
		const firstA = seen[0]!.get('a');

		await hub.notify(await applyServer(kv, [ serverRow('b', 2n, { id: 'b', count: 20 }) ]));

		expect(carried(seen.at(-1))).toEqual([ 'b' ]);
		expect(seen.at(-1)!.get('b')?.value.count).toBe(20);
		// `a` was not re-read, so the consumer still holds the object it had —
		// which is what lets a list re-render one row rather than all of them.
		expect(firstA).toBeDefined();
		handle.close();
	});

	it('stays silent for a commit that touches no watched key', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		const { seen, handle } = await watchKeys(hub, [ 'a' ]);
		const before = seen.length;

		await hub.notify(await applyServer(kv, [ serverRow('elsewhere', 1n, { id: 'elsewhere' }) ]));

		expect(seen).toHaveLength(before);
		handle.close();
	});

	it('reports a deleted row as absent', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		const { seen, handle } = await watchKeys(hub, [ 'a' ]);

		await hub.notify(await applyServer(kv, [ tombstone('a', 2n) ]));

		expect(carried(seen.at(-1))).toEqual([ 'a!' ]);
		handle.close();
	});

	it('re-reads every watched key when a commit names an entity without its rows', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [
			serverRow('a', 1n, { id: 'a', count: 1 }),
			serverRow('b', 1n, { id: 'b', count: 2 }),
		]);
		const { seen, handle } = await watchKeys(hub, [ 'a', 'b' ]);

		// Both rows change, but the commit only says "this entity moved" — a
		// wholesale overlay clear or a bootstrap swap. Every watched key is then
		// suspect.
		await applyServer(kv, [
			serverRow('a', 2n, { id: 'a', count: 10 }),
			serverRow('b', 2n, { id: 'b', count: 20 }),
		]);
		const wildcard = new TouchSet();
		wildcard.entity(WS, 'widget');
		await hub.notify(wildcard);

		expect(carried(seen.at(-1))).toEqual([ 'a', 'b' ]);
		expect(seen.at(-1)!.get('a')?.value.count).toBe(10);
		expect(seen.at(-1)!.get('b')?.value.count).toBe(20);
		handle.close();
	});

	it('reads only the keys setKeys adds, and drops the ones it removes', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [
			serverRow('a', 1n, { id: 'a', count: 1 }),
			serverRow('b', 1n, { id: 'b', count: 2 }),
		]);
		const { seen, handle } = await watchKeys(hub, [ 'a' ]);
		expect(carried(seen.at(-1))).toEqual([ 'a' ]);

		handle.setKeys([ 'a', 'b' ]);
		await flush();
		// `a` was already held, so growing the set costs the new key and nothing
		// for the ones already in it.
		expect(carried(seen.at(-1))).toEqual([ 'b' ]);

		handle.setKeys([ 'b' ]);
		await hub.notify(await applyServer(kv, [ serverRow('a', 2n, { id: 'a', count: 10 }) ]));
		const after = seen.length;
		await flush();
		// `a` is no longer watched, so a change to it reaches nothing.
		expect(seen).toHaveLength(after);
		handle.close();
	});

	it('coalesces the keys added within one turn into a single delivery', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [
			serverRow('a', 1n, { id: 'a', count: 1 }),
			serverRow('b', 1n, { id: 'b', count: 2 }),
			serverRow('c', 1n, { id: 'c', count: 3 }),
		]);
		const { seen, handle } = await watchKeys(hub, []);
		expect(seen).toHaveLength(0);

		// What a list of row views mounting together does: one call each.
		handle.setKeys([ 'a' ]);
		handle.setKeys([ 'a', 'b' ]);
		handle.setKeys([ 'a', 'b', 'c' ]);
		await flush();

		expect(seen).toHaveLength(1);
		expect(carried(seen[0])).toEqual([ 'a', 'b', 'c' ]);
		handle.close();
	});

	it('fires on a coverage change alone, so an absent row stops meaning "coming"', async () => {
		const kv = new MemoryKVStore();
		let coverage: Coverage = { complete: false, bounds: [ 'hydrating' ] };
		const hub = new WatcherHub(kv, registry, new ChainMutex(), {
			debugVerifyTouches: true,
			coverageOf: () => coverage,
		});
		const seen: { keys: string[]; coverage: Coverage }[] = [];
		const handle = hub.watchRefs(
			WidgetSchema,
			WS,
			[ { workspaceId: WS, key: 'ghost' } ],
			(rows, c) => {
				seen.push({ keys: rows.map((r) => r.key), coverage: c });
			},
		);
		await flush();
		expect(seen.at(-1)!.coverage.bounds).toEqual([ 'hydrating' ]);

		// The entity finishes arriving. Nothing the watcher holds moved, but what
		// its absent key MEANS did.
		coverage = { complete: true, bounds: [] };
		await hub.notify(await applyServer(kv, [ serverRow('other', 1n, { id: 'other' }) ]));

		expect(seen.at(-1)!.keys).toEqual([]);
		expect(seen.at(-1)!.coverage.complete).toBe(true);
		handle.close();
	});

	it('delivers nothing after close', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServer(kv, [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		const { seen, handle } = await watchKeys(hub, [ 'a' ]);
		handle.close();
		const before = seen.length;

		await hub.notify(await applyServer(kv, [ serverRow('a', 2n, { id: 'a', count: 2 }) ]));

		expect(seen).toHaveLength(before);
	});
});

// The same watch with no workspace scope: every ref names its own workspace, so
// the same key in two of them is two rows, and a commit in one says nothing
// about the others.
describe('cross-workspace keyed watches', () => {
	/** One delivery, as `<workspace>/<key>` entries; `!` marks a ref delivered as absent. */
	function carriedRefs(rows: ScopedKeyedRows<Widget> | undefined): string[] {
		if (rows === undefined) return [];
		return rows
			.map((r) => `${r.workspaceId}/${r.key}${r.row === undefined ? '!' : ''}`)
			.sort();
	}

	async function watchAll(hub: WatcherHub, refs: RowRef[]) {
		const seen: ScopedKeyedRows<Widget>[] = [];
		const handle = hub.watchRefs(
			WidgetSchema,
			undefined,
			refs,
			(rows: ScopedKeyedRows<Widget>) => {
				seen.push(rows);
			},
		);
		await flush();
		return { seen, handle };
	}

	it('keeps the same key in two workspaces apart', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServerIn(kv, 'ws1', [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		await applyServerIn(kv, 'ws2', [ serverRow('a', 1n, { id: 'a', count: 2 }) ]);

		const { seen, handle } = await watchAll(hub, [
			{ workspaceId: 'ws1', key: 'a' },
			{ workspaceId: 'ws2', key: 'a' },
			{ workspaceId: 'ws3', key: 'a' },
		]);

		expect(carriedRefs(seen[0])).toEqual([ 'ws1/a', 'ws2/a', 'ws3/a!' ]);
		const byWorkspace = new Map(seen[0]!.map((r) => [ r.workspaceId, r.row?.value.count ]));
		expect(byWorkspace.get('ws1')).toBe(1);
		expect(byWorkspace.get('ws2')).toBe(2);
		handle.close();
	});

	it('carries only the ref a commit moved, in the workspace it moved it', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServerIn(kv, 'ws1', [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		await applyServerIn(kv, 'ws2', [ serverRow('a', 1n, { id: 'a', count: 2 }) ]);
		const { seen, handle } = await watchAll(hub, [
			{ workspaceId: 'ws1', key: 'a' },
			{ workspaceId: 'ws2', key: 'a' },
		]);
		const first = seen[0]!.find((r) => r.workspaceId === 'ws1')!.row;

		await hub.notify(
			await applyServerIn(kv, 'ws2', [ serverRow('a', 2n, { id: 'a', count: 20 }) ]),
		);

		expect(carriedRefs(seen.at(-1))).toEqual([ 'ws2/a' ]);
		expect(seen.at(-1)![0]!.row?.value.count).toBe(20);
		// ws1's row was never re-read, so a consumer still holds the object it had.
		expect(first).toBeDefined();
		handle.close();
	});

	it('re-reads only the touched workspace when a commit names an entity wholesale', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServerIn(kv, 'ws1', [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		await applyServerIn(kv, 'ws2', [ serverRow('b', 1n, { id: 'b', count: 2 }) ]);
		const { seen, handle } = await watchAll(hub, [
			{ workspaceId: 'ws1', key: 'a' },
			{ workspaceId: 'ws2', key: 'b' },
		]);

		await applyServerIn(kv, 'ws2', [ serverRow('b', 2n, { id: 'b', count: 20 }) ]);
		// A wildcard says "something in this entity moved" without naming rows. It
		// makes every ref in ITS workspace suspect and none of the others.
		const wildcard = new TouchSet();
		wildcard.entity('ws2', 'widget');
		await hub.notify(wildcard);

		expect(carriedRefs(seen.at(-1))).toEqual([ 'ws2/b' ]);
		expect(seen.at(-1)![0]!.row?.value.count).toBe(20);
		handle.close();
	});

	it('stays silent for a commit in a workspace it holds no ref in', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServerIn(kv, 'ws1', [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		const { seen, handle } = await watchAll(hub, [ { workspaceId: 'ws1', key: 'a' } ]);
		const before = seen.length;

		await hub.notify(
			await applyServerIn(kv, 'ws2', [ serverRow('a', 1n, { id: 'a', count: 9 }) ]),
		);

		expect(seen).toHaveLength(before);
		handle.close();
	});

	it('reads only the refs setRefs adds, and drops the ones it removes', async () => {
		const kv = new MemoryKVStore();
		const hub = verifyingHub(kv);
		await applyServerIn(kv, 'ws1', [ serverRow('a', 1n, { id: 'a', count: 1 }) ]);
		await applyServerIn(kv, 'ws2', [ serverRow('a', 1n, { id: 'a', count: 2 }) ]);
		const { seen, handle } = await watchAll(hub, [ { workspaceId: 'ws1', key: 'a' } ]);
		expect(carriedRefs(seen.at(-1))).toEqual([ 'ws1/a' ]);

		handle.setRefs([ { workspaceId: 'ws1', key: 'a' }, { workspaceId: 'ws2', key: 'a' } ]);
		await flush();
		expect(carriedRefs(seen.at(-1))).toEqual([ 'ws2/a' ]);

		handle.setRefs([ { workspaceId: 'ws2', key: 'a' } ]);
		await hub.notify(
			await applyServerIn(kv, 'ws1', [ serverRow('a', 2n, { id: 'a', count: 10 }) ]),
		);
		const after = seen.length;
		await flush();
		expect(seen).toHaveLength(after);
		handle.close();
	});

	it('reads refs spanning several workspaces in one batch', async () => {
		const { kv, rowScans } = countingStore();
		const hub = new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: false });
		for (const ws of [ 'ws1', 'ws2', 'ws3' ]) {
			for (let i = 0; i < 20; i++) {
				await applyServerIn(kv, ws, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
			}
		}
		let reads = 0;
		const read = kv.read.bind(kv);
		vi.spyOn(kv, 'read').mockImplementation(async () => {
			reads++;
			return read();
		});

		const { seen, handle } = await watchAll(hub, [
			{ workspaceId: 'ws1', key: 'w3' },
			{ workspaceId: 'ws2', key: 'w7' },
			{ workspaceId: 'ws3', key: 'w11' },
		]);

		// One transaction and no scan for the whole set, however many workspaces
		// the refs span.
		expect(reads).toBe(1);
		expect(rowScans()).toBe(0);
		expect(carriedRefs(seen[0])).toEqual([ 'ws1/w3', 'ws2/w7', 'ws3/w11' ]);
		handle.close();
	});
});
