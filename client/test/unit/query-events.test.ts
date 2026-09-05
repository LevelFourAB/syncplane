// What `onSyncEvent` reports about local reads.
//
// The event exists for one question a result cannot answer: did this query walk
// an index or the whole entity? So every case here pins `path` and, when it
// scanned, WHICH of the reasons put it there — a decline reported as the wrong
// reason sends an app to fix the wrong thing.

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex, type Mutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import type { Query } from '../../src/query/index.js';
import { defineSchema, entity, type Infer, int32, text } from '../../src/schema/builder.js';
import type { QueryEvent, SyncEvent } from '../../src/sync/events.js';
import { TouchSet } from '../../src/touch.js';

const issue = entity('issue', {
	id: text().key(),
	title: text(),
	state: text().indexed(),
	rank: int32(),
}, {
	indexes: [ { name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] } ],
});

// The second entity declares nothing, which is the only way to reach the
// `no-index` decline: an entity with indexes reports one of the later ones.
const note = entity('note', { id: text().key(), body: text() });

type Issue = Infer<typeof issue>;
const bundle = defineSchema(issue, note);
const IssueSchema = bundle.descMessage('issue');
const NoteSchema = bundle.descMessage('note');
const registry = new EntityRegistry([ IssueSchema, NoteSchema ]);

const WS = 'ws1';

const SEED: Issue[] = [
	{ id: 'i1', title: 'one', state: 'open', rank: 5 },
	{ id: 'i2', title: 'two', state: 'open', rank: 9 },
	{ id: 'i3', title: 'three', state: 'done', rank: 3 },
];

/** Plants SEED as confirmed rows — the only state an index covers. */
async function seedConfirmed(kv: KVStore, indexes: IndexCache): Promise<void> {
	const tx = await kv.write();
	// The cursor is what marks the workspace as holding a whole installed
	// snapshot; without one a build declines (see readInstalledSnapshot).
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		WS,
		SEED.map((row, i) =>
			create(RowChangeSchema, {
				entity: 'issue',
				key: row.id,
				version: BigInt(i + 1),
				body: { case: 'value', value: toBinary(IssueSchema, create(IssueSchema, row)) },
			})
		),
		new TouchSet(),
		indexes,
	);
	await tx.commit();
}

/**
 * A client over a seeded store, whose events land in the returned array.
 * `onEvent` runs after each one is recorded, for the case where the app's
 * handler misbehaves.
 */
function clientOver(kv: KVStore, onEvent?: (event: SyncEvent) => void) {
	const events: SyncEvent[] = [];
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ IssueSchema, NoteSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		onSyncEvent: (e) => {
			events.push(e);
			onEvent?.(e);
		},
	}) as unknown as SyncClient<never>;
	const queries = () => events.filter((e): e is QueryEvent => e.type === 'query');
	return { client, events, queries };
}

/** A client over SEED, with its indexes built or not. */
async function clientWith(built: boolean, onEvent?: (event: SyncEvent) => void) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seedConfirmed(kv, indexes);
	if (built) {
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	}
	return clientOver(kv, onEvent);
}

/**
 * A client holding `rows` issues in one state, built index and all — enough of
 * them that one range covers more positions than a walk will read rows for.
 * Written in batches, which keeps the memory store's snapshot copying from
 * dominating the setup.
 */
async function bigClient(rows: number) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	const cursor = await kv.write();
	await cursor.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await cursor.commit();
	for (let start = 0; start < rows; start += 1000) {
		const tx = await kv.write();
		const changes = [];
		for (let i = start; i < Math.min(start + 1000, rows); i++) {
			changes.push(create(RowChangeSchema, {
				entity: 'issue',
				key: `i${i}`,
				version: BigInt(i + 1),
				body: {
					case: 'value',
					value: toBinary(
						IssueSchema,
						create(IssueSchema, { id: `i${i}`, title: 'many', state: 'open', rank: i }),
					),
				},
			}));
		}
		await applyServerChanges(tx, registry, WS, changes, new TouchSet(), indexes);
		await tx.commit();
	}
	const mutex = new ChainMutex();
	await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	return clientOver(kv);
}

/**
 * Types a query against the builder's shape and erases it for the call. The
 * cast is the price of a runtime-built schema; see index-query.test.ts.
 */
const q = (query: Query<Issue>): never => query as never;

const run = (client: SyncClient<never>, query: Query<Issue>): Promise<unknown> =>
	client.query(IssueSchema, q(query));

describe('query events', () => {
	it('reports the index that served a query, and what the walk cost', async () => {
		const { client, queries } = await clientWith(true);
		try {
			await run(client, { where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ] });
		} finally {
			await client.close();
		}
		expect(queries()).toHaveLength(1);
		const event = queries()[0]!;
		expect(event).toMatchObject({
			type: 'query',
			origin: 'query',
			entity: 'issue',
			workspaceId: WS,
			path: 'index',
			index: 'by_state_rank',
			rows: 2,
			total: 2,
		});
		// Two matching rows sit behind two positions: the walk found the range and
		// nothing beyond it.
		expect(event.candidates).toBe(2);
		expect(event.declined).toBeUndefined();
		expect(event.durationMs).toBeGreaterThanOrEqual(0);
		expect(event.waitMs).toBeLessThanOrEqual(event.durationMs);
		expect(typeof event.at).toBe('number');
	});

	it('reports a scan as a scan, with the reason it fell back to one', async () => {
		const { client, queries } = await clientWith(false);
		try {
			await run(client, { where: (i) => i.state.eq('open') });
		} finally {
			await client.close();
		}
		expect(queries()[0]).toMatchObject({
			path: 'scan',
			declined: 'not-built',
			rows: 2,
			total: 2,
		});
		// The fields that belong to the index path are absent, not undefined: an
		// aggregation over `candidates` must not see a walk that never happened.
		expect(queries()[0]).not.toHaveProperty('index');
		expect(queries()[0]).not.toHaveProperty('candidates');
	});

	it('separates the reasons a query never reached an index', async () => {
		const { client, queries } = await clientWith(true);
		try {
			// No index on the entity at all.
			await client.query(NoteSchema, {} as never);
			// Indexed entity, but no index covers a filter on `title`.
			await run(client, { where: (i) => i.title.eq('one') });
			// Every workspace at once: one index walk per workspace, and the read
			// names no single workspace.
			await client.queryAll(IssueSchema, q({ where: (i) => i.state.eq('open') }));
		} finally {
			await client.close();
		}
		expect(queries().map((e) => [ e.entity, e.path, e.declined ])).toEqual([
			[ 'note', 'scan', 'no-index' ],
			[ 'issue', 'scan', 'no-plan' ],
			[ 'issue', 'index', undefined ],
		]);
		expect(queries()[2]!.workspaceId).toBeUndefined();
	});

	it('reports the budget decline, which no result would show', async () => {
		// The cliff this event exists for: the same query, the same index, the
		// same correct rows — and past a size threshold it is a full scan. No
		// ordering, so every match's row must be read to re-create key order,
		// and it is the row reads that overrun the ceiling.
		const { client, queries } = await bigClient(10_100);
		try {
			await run(client, { where: (i) => i.state.eq('open') });
		} finally {
			await client.close();
		}
		expect(queries()[0]).toMatchObject({ path: 'scan', declined: 'budget' });
		// The walk stopped at the ceiling, so its count carries no information the
		// reason does not.
		expect(queries()[0]).not.toHaveProperty('candidates');
	});

	it('serves an ordered page from a range wider than the budget', async () => {
		// The counterpart that keeps the ceiling honest: the same range, but an
		// ordered plan with a limit settles its page after a handful of ROW
		// reads and then only counts positions toward `total`. Counting is
		// O(positions of the range) with no row reads at all, so the ceiling —
		// which exists to stop runaway reads — must not apply to it, or a busy
		// entity's most selective indexed query becomes a permanent full scan.
		const { client, queries } = await bigClient(10_100);
		try {
			await run(client, {
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
				limit: 5,
			});
		} finally {
			await client.close();
		}
		expect(queries()[0]).toMatchObject({
			path: 'index',
			index: 'by_state_rank',
			rows: 5,
			total: 10_100,
		});
	});

	it('counts the page it delivered, and the matches behind it', async () => {
		const { client, queries } = await clientWith(true);
		try {
			await run(client, {
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
				limit: 1,
			});
		} finally {
			await client.close();
		}
		expect(queries()[0]).toMatchObject({ path: 'index', rows: 1, total: 2 });
	});

	it('reports a watcher first read once, and not again per commit', async () => {
		const { client, queries } = await clientWith(true);
		const seen: number[] = [];
		try {
			const handle = client.watchQuery(
				IssueSchema,
				q({ where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ] }),
				(rows) => seen.push(rows.length),
			);
			await vi.waitUntil(() => seen.length > 0);
			expect(queries()).toHaveLength(1);
			expect(queries()[0]).toMatchObject({
				origin: 'watch',
				entity: 'issue',
				path: 'index',
				index: 'by_state_rank',
				// A watcher takes the whole match set, which is what it maintains.
				rows: 2,
				total: 2,
			});
			// A commit the watcher reports on is maintained, not re-queried.
			await client.mutate.put(
				IssueSchema,
				{ id: 'i4', title: 'four', state: 'open', rank: 7 } as never,
			);
			await vi.waitUntil(() => seen.length > 1);
			expect(queries()).toHaveLength(1);
			handle.close();
		} finally {
			await client.close();
		}
	});

	it('reports an unqueried watch as the whole-entity read it is', async () => {
		const { client, queries } = await clientWith(true);
		const seen: number[] = [];
		try {
			const handle = client.watch(IssueSchema, (rows) => seen.push(rows.length));
			await vi.waitUntil(() => seen.length > 0);
			expect(queries()[0]).toMatchObject({
				origin: 'watch',
				path: 'scan',
				declined: 'no-query',
				rows: 3,
				total: 3,
			});
			handle.close();
		} finally {
			await client.close();
		}
	});

	it('charges a blocking commit to waitMs rather than to the query', async () => {
		// The number this separation exists for: an uncontended read and one that
		// queued behind a writer differ only in waitMs, so a latency threshold over
		// `durationMs - waitMs` does not fire on write load.
		const HOLD_MS = 50;
		const { client, queries } = await clientWith(true);
		const mutex = (client as unknown as { kvMutex: Mutex }).kvMutex;
		try {
			await run(client, { where: (i) => i.state.eq('open') });
			// Hold the store exclusively, as a rebase or an index-build batch does.
			let release!: () => void;
			const held = new Promise<void>((resolve) => (release = resolve));
			const writing = mutex.run(() => held);
			const reading = run(client, { where: (i) => i.state.eq('open') });
			await new Promise((resolve) => setTimeout(resolve, HOLD_MS));
			release();
			await writing;
			expect(await (reading as Promise<unknown[]>)).toHaveLength(2);
		} finally {
			await client.close();
		}
		const [ free, blocked ] = queries() as [QueryEvent, QueryEvent];
		expect(blocked.waitMs).toBeGreaterThanOrEqual(HOLD_MS * 0.5);
		expect(blocked.durationMs).toBeGreaterThanOrEqual(blocked.waitMs);
		// The read itself did the same work both times; only the wait moved.
		expect(blocked.durationMs - blocked.waitMs).toBeLessThan(HOLD_MS * 0.5);
		expect(free.waitMs).toBeLessThan(HOLD_MS * 0.5);
	});

	it('reports the background build, after which queries stop scanning', async () => {
		// Seeded but never built: the state a bootstrap swap leaves behind, which
		// the background build is responsible for ending — and the event is the
		// only way an app can see it end. Driven through the same private entry
		// start() and the swap use, without a coordinator in the way.
		const { client, events, queries } = await clientWith(false);
		try {
			(client as unknown as { startIndexBuild(ws?: string): void }).startIndexBuild(WS);
			await vi.waitUntil(() => events.some((e) => e.type === 'indexing'));
			const built = events.find((e) => e.type === 'indexing')!;
			expect(built).toMatchObject({ type: 'indexing', workspaceId: WS, rows: 3 });
			expect(built.durationMs).toBeGreaterThanOrEqual(0);
			await run(client, { where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ] });
			expect(queries()[0]).toMatchObject({ path: 'index', index: 'by_state_rank' });
		} finally {
			await client.close();
		}
	});

	it('reports a build that found everything built, as the zero-row pass it was', async () => {
		const { client, events } = await clientWith(true);
		try {
			(client as unknown as { startIndexBuild(ws?: string): void }).startIndexBuild(WS);
			await vi.waitUntil(() => events.some((e) => e.type === 'indexing'));
			expect(events.find((e) => e.type === 'indexing')).toMatchObject({
				workspaceId: WS,
				rows: 0,
			});
		} finally {
			await client.close();
		}
	});

	it('answers the query even when the handler throws', async () => {
		const { client, queries } = await clientWith(true, () => {
			throw new Error('handler blew up');
		});
		// The throw is contained and reported through console.error; the point
		// here is that the caller still gets its rows.
		const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			const rows = await run(client, { where: (i) => i.state.eq('open') });
			expect(rows as unknown[]).toHaveLength(2);
			expect(queries()).toHaveLength(1);
		} finally {
			quiet.mockRestore();
			await client.close();
		}
	});
});
