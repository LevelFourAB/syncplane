// The rows of a scan scope are shared by every watcher on it, and a scan puts
// the WHOLE entity in that map — so something has to take back out of it
// everything the queries reject. The specs here pin what that costs and what it
// leaves behind:
//
//   - a commit applied incrementally does not walk the shared map, so the price
//     of a write does not grow with the size of the live results;
//   - the map still comes back down to what the watchers hold, both after a
//     rescan filled it and over a long run of ordinary commits;
//   - a scope goes away with its last watcher, and one watcher closing releases
//     the rows only it held.

import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { compileQuery, type Query } from '../../src/query/index.js';
import { readMergedRowsWithVersion, type Row } from '../../src/read.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub, type WatchHandle } from '../../src/watch.js';

const registry = new EntityRegistry([ WidgetSchema ]);
const WS = 'ws1';
const ROWS = 200;
const flush = (): Promise<unknown> => new Promise((r) => setTimeout(r, 0));

function serverRow(
	key: string,
	version: bigint,
	w: MessageInitShape<typeof WidgetSchema> & { id: string },
): RowChange {
	return create(RowChangeSchema, {
		entity: 'widget',
		key,
		version,
		body: { case: 'value', value: toBinary(WidgetSchema, create(WidgetSchema, w)) },
	});
}

async function applyServer(kv: MemoryKVStore, changes: RowChange[]): Promise<TouchSet> {
	const touched = new TouchSet();
	const tx = await kv.write();
	await applyServerChanges(tx, registry, WS, changes, touched);
	await tx.commit();
	return touched;
}

/** A store holding ROWS widgets, `count` running from 0 upwards. */
async function seeded(): Promise<MemoryKVStore> {
	const kv = new MemoryKVStore();
	for (let i = 0; i < ROWS; i++) {
		await applyServer(kv, [ serverRow(`w${i}`, 1n, { id: `w${i}`, count: i }) ]);
	}
	return kv;
}

const hubOf = (kv: MemoryKVStore): WatcherHub =>
	new WatcherHub(kv, registry, new ChainMutex(), { debugVerifyTouches: true });

async function watchQuery(
	hub: WatcherHub,
	q: Query<Widget>,
): Promise<{ seen: Row<Widget>[][]; unsub: WatchHandle }> {
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

/** The one scope every watcher in these specs shares. */
function scope(hub: WatcherHub): { rows: number; fullPrunes: number } {
	const stats = [ ...hub.scopeStats().values() ];
	expect(stats.length).toBeLessThanOrEqual(1);
	return stats[0] ?? { rows: 0, fullPrunes: 0 };
}

describe('shared row pruning', () => {
	it('does not walk the shared rows on a commit applied as a delta', async () => {
		const kv = await seeded();
		const hub = hubOf(kv);
		// A watcher whose result is a handful of rows out of an entity the first
		// read has to scan: the map starts at ROWS and is cut down to the matches.
		const { unsub } = await watchQuery(hub, {
			where: (w) => w.count.gte(ROWS - 5),
			orderBy: (w) => [ w.count.asc() ],
		});
		const seedPrunes = scope(hub).fullPrunes;

		for (let i = 0; i < 40; i++) {
			await hub.notify(await applyServer(kv, [ serverRow(`w${i}`, 2n, { id: `w${i}`, count: i }) ]));
		}

		// Forty commits, none of which grew the map: none of them may have paid
		// for a walk of it.
		expect(scope(hub).fullPrunes).toBe(seedPrunes);
		unsub.close();
	});

	it('keeps the shared rows down to what the watchers hold', async () => {
		const kv = await seeded();
		const hub = hubOf(kv);
		const { unsub } = await watchQuery(hub, {
			where: (w) => w.count.gte(ROWS - 5),
			orderBy: (w) => [ w.count.asc() ],
		});
		// The scan behind the first read held the whole entity.
		expect(scope(hub).rows).toBe(5);

		// A commit reporting the entity without its rows forces a rescan, which
		// fills the map again — and the rows the query rejects have to come back
		// out of it.
		const wildcard = new TouchSet();
		wildcard.entity(WS, 'widget');
		await hub.notify(wildcard);
		expect(scope(hub).rows).toBe(5);

		// And a long run of ordinary commits, each naming a row the query does not
		// admit, must not accumulate them.
		for (let i = 0; i < 60; i++) {
			await hub.notify(await applyServer(kv, [ serverRow(`w${i}`, 3n, { id: `w${i}`, count: i }) ]));
		}
		expect(scope(hub).rows).toBeLessThan(30);
		unsub.close();
	});

	it('releases the rows a closing watcher alone held, and the scope with the last one', async () => {
		const kv = await seeded();
		const hub = hubOf(kv);
		const low = await watchQuery(hub, { where: (w) => w.count.lt(5) });
		const high = await watchQuery(hub, { where: (w) => w.count.gte(ROWS - 5) });
		expect(scope(hub).rows).toBe(10);

		high.unsub.close();
		expect(scope(hub).rows).toBe(5);

		low.unsub.close();
		expect(hub.scopeStats().size).toBe(0);
	});

	it('drops the rows of refs a keyed watcher stops watching', async () => {
		const kv = await seeded();
		const hub = hubOf(kv);
		const refs = [ 'w0', 'w1', 'w2', 'w3' ].map((key) => ({ workspaceId: WS, key }));
		const handle = hub.watchRefs(WidgetSchema, WS, refs, () => {});
		await flush();
		expect(scope(hub).rows).toBe(4);

		handle.setRefs(refs.slice(0, 1));
		expect(scope(hub).rows).toBe(1);

		handle.close();
		expect(hub.scopeStats().size).toBe(0);
	});

	it('keeps the shared rows to the union of many keyed watchers on every commit path', async () => {
		const kv = await seeded();
		const hub = hubOf(kv);
		// One watcher per row, as a list of row views mounts them, plus a second
		// watcher on w0 and a query sharing the scope.
		const counts = new Map<string, number | undefined>();
		const handles = Array.from(
			{ length: 20 },
			(_, i) =>
				hub.watchRefs(WidgetSchema, WS, [ { workspaceId: WS, key: `w${i}` } ], (rows) => {
					for (const r of rows) counts.set(r.key, (r.row?.value as Widget | undefined)?.count);
				}),
		);
		const twin = hub.watchRefs(WidgetSchema, WS, [ { workspaceId: WS, key: 'w0' } ], () => {});
		const high = await watchQuery(hub, { where: (w) => w.count.gte(ROWS - 5) });
		expect(scope(hub).rows).toBe(25);

		// Read back: a watched row and a row no watcher holds.
		await hub.notify(
			await applyServer(kv, [
				serverRow('w5', 2n, { id: 'w5', count: 50 }),
				serverRow('w150', 2n, { id: 'w150', count: 150 }),
			]),
		);
		expect(counts.get('w5')).toBe(50);
		expect(scope(hub).rows).toBe(25);

		// Handed over by the writer: the same commit shape, without the read.
		await applyServer(kv, [
			serverRow('w6', 3n, { id: 'w6', count: 60 }),
			serverRow('w151', 3n, { id: 'w151', count: 151 }),
		]);
		const handed = new TouchSet();
		const tx = await kv.read();
		const keys = [ 'w6', 'w151' ];
		const views = await readMergedRowsWithVersion(
			tx,
			registry,
			'widget',
			keys.map((key) => ({ workspaceId: WS, key })),
		);
		tx.release();
		keys.forEach((key, i) => handed.rowWithView(WS, 'widget', key, views[i]));
		const deliver = await hub.notifyWithRows(handed);
		expect(deliver).toBeDefined();
		await deliver!();
		expect(counts.get('w6')).toBe(60);
		expect(scope(hub).rows).toBe(25);

		// A row two watchers hold stays until the second one closes.
		twin.close();
		expect(scope(hub).rows).toBe(25);
		handles[0]!.close();
		expect(scope(hub).rows).toBe(24);

		for (const h of handles) h.close();
		high.unsub.close();
		expect(hub.scopeStats().size).toBe(0);
	});
});
