// An ordered index walk that stops at a limit must cut where the SCAN would
// cut. The scan breaks a tie on the order keys by row key, ascending; an index
// whose entry carries components past the order keys orders the same tie group
// by those components instead. Each case runs against identical data with the
// index built and without, and the two must agree.

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { queryByIndex } from '../../src/query/execute.js';
import { compileQuery, type Query, type QueryCursor } from '../../src/query/index.js';
import { prepareQuery } from '../../src/query/wire.js';
import { defineSchema, entity, type Infer, int32, text } from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

// `title` is a component the order keys do not name, and its order inside a
// day is the reverse of the row-key order.
const journal = entity('journal', {
	id: text().key(),
	day: int32(),
	title: text(),
}, {
	indexes: [
		{ name: 'by_day_title', key: [ { path: 'day' }, { path: 'title' } ] },
	],
});

type Journal = Infer<typeof journal>;
const bundle = defineSchema(journal);
const JournalSchema = bundle.descMessage('journal');
const registry = new EntityRegistry([ JournalSchema ]);
const WS = 'ws1';

const SEED: Journal[] = [
	{ id: 'j1', day: 1, title: 'z' },
	{ id: 'j2', day: 1, title: 'y' },
	{ id: 'j3', day: 1, title: 'x' },
	{ id: 'j4', day: 2, title: 'b' },
	{ id: 'j5', day: 2, title: 'a' },
	{ id: 'j6', day: 3, title: 'm' },
];

async function seeded(built: boolean, rows: Journal[] = SEED) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		WS,
		rows.map((row, i) =>
			create(RowChangeSchema, {
				entity: 'journal',
				key: row.id,
				version: BigInt(i + 1),
				body: { case: 'value', value: toBinary(JournalSchema, create(JournalSchema, row)) },
			})
		),
		new TouchSet(),
		indexes,
	);
	await tx.commit();
	if (built) {
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	}
	return { kv, indexes };
}

/** A client over the seeded store, with `overlay` written as pending puts. */
async function clientWith(built: boolean, overlay: Journal[] = [], rows: Journal[] = SEED) {
	const { kv } = await seeded(built, rows);
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ JournalSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	}) as unknown as SyncClient<never>;
	for (const row of overlay) await client.mutate.put(JournalSchema, row as never);
	return client;
}

const query = (client: SyncClient<never>, q: Query<Journal>): Promise<RowSet<Journal>> =>
	client.query(JournalSchema, q as never) as unknown as Promise<RowSet<Journal>>;

async function run(built: boolean, q: Query<Journal>, overlay?: Journal[]) {
	const client = await clientWith(built, overlay);
	try {
		const rows = await query(client, q);
		return { keys: rows.map((r) => r.key), total: rows.total, hasMore: rows.hasMore };
	} finally {
		await client.close();
	}
}

/** Whether the index path answered, rather than declining to the scan. */
async function served(q: Query<Journal>): Promise<boolean> {
	const { kv, indexes } = await seeded(true);
	const tx = await kv.read();
	try {
		const out = await queryByIndex(
			tx,
			registry,
			registry.get('journal')!,
			JournalSchema,
			indexes.for('journal'),
			prepareQuery(JournalSchema, compileQuery(q)),
			WS,
		);
		return out !== undefined;
	} finally {
		tx.release();
	}
}

function same(name: string, q: Query<Journal>, want: string[], overlay?: Journal[]): void {
	it(name, async () => {
		expect(await served(q)).toBe(true);
		const scanned = await run(false, q, overlay);
		const indexed = await run(true, q, overlay);
		expect(scanned.keys).toEqual(want);
		expect(indexed).toEqual(scanned);
	});
}

describe('an ordered walk over an index with components past the order keys', () => {
	same('cuts a tie group by row key at the limit', {
		orderBy: (j) => [ j.day.asc() ],
		limit: 2,
	}, [ 'j1', 'j2' ]);

	same('cuts a tie group by row key past an offset', {
		orderBy: (j) => [ j.day.asc() ],
		offset: 3,
		limit: 1,
	}, [ 'j4' ]);

	same('cuts a tie group by row key inside a range', {
		where: (j) => j.day.gt(1),
		orderBy: (j) => [ j.day.asc() ],
		limit: 1,
	}, [ 'j4' ]);

	same('cuts a tie group by row key with the total opted out', {
		orderBy: (j) => [ j.day.asc() ],
		limit: 1,
		total: false,
	}, [ 'j1' ]);

	same('cuts a tie group by row key when a residual re-checks each row', {
		where: (j) => j.title.neq('y'),
		orderBy: (j) => [ j.day.asc() ],
		limit: 1,
	}, [ 'j1' ]);

	// A pending row grows the budget by one, which moves the boundary deeper
	// into the tie group without reaching its end.
	same(
		'cuts a tie group by row key through a pending overlay',
		{
			orderBy: (j) => [ j.day.asc() ],
			limit: 1,
		},
		[ 'j1' ],
		[ { id: 'j9', day: 5, title: 'q' } ],
	);

	it('walks the same pages from the index as from the scan', async () => {
		const pageAll = async (built: boolean): Promise<string[]> => {
			const client = await clientWith(built);
			try {
				const keys: string[] = [];
				let after: QueryCursor | undefined;
				for (;;) {
					const page = await query(client, { orderBy: (j) => [ j.day.asc() ], limit: 2, after });
					keys.push(...page.map((r) => r.key));
					if (!page.hasMore) break;
					after = page.endCursor;
				}
				return keys;
			} finally {
				await client.close();
			}
		};
		expect(await pageAll(false)).toEqual([ 'j1', 'j2', 'j3', 'j4', 'j5', 'j6' ]);
		expect(await pageAll(true)).toEqual(await pageAll(false));
	});

	it('counts a page bounded by both cursors as the scan does', async () => {
		const bounded = async (built: boolean) => {
			const client = await clientWith(built);
			try {
				const all = await query(client, { orderBy: (j) => [ j.day.asc() ] });
				const after = (await query(client, { orderBy: (j) => [ j.day.asc() ], limit: 1 })).endCursor;
				const before = (await query(client, { orderBy: (j) => [ j.day.asc() ], offset: 1, limit: 1 }))
					.endCursor;
				const page = await query(client, { orderBy: (j) => [ j.day.asc() ], limit: 1, after, before });
				return {
					all: all.length,
					keys: page.map((r) => r.key),
					total: page.total,
					hasMore: page.hasMore,
				};
			} finally {
				await client.close();
			}
		};
		expect(await bounded(true)).toEqual(await bounded(false));
	});

	// The seek keeps the before cursor's whole tie group inside the range. The
	// page ends in day 1, so k2 closes that tie group and settles the walk; in
	// index order k4 and k5 (at and past the before cursor) follow it inside
	// day 2, and the walk must not count them.
	it('leaves the rows past a before cursor out of the total', async () => {
		const rows: Journal[] = [
			{ id: 'k0', day: 0, title: 'a' },
			{ id: 'k1', day: 1, title: 'a' },
			{ id: 'k2', day: 2, title: 'a' },
			{ id: 'k3', day: 2, title: 'z' },
			{ id: 'k4', day: 2, title: 'b' },
			{ id: 'k5', day: 2, title: 'c' },
		];
		const order: Query<Journal> = { orderBy: (j) => [ j.day.asc() ] };
		const bounded = async (built: boolean) => {
			const client = await clientWith(built, [], rows);
			try {
				const after = (await query(client, { ...order, limit: 1 })).endCursor;
				const before = (await query(client, { ...order, offset: 4, limit: 1 })).endCursor;
				const page = await query(client, { ...order, limit: 1, after, before });
				return { keys: page.map((r) => r.key), total: page.total, hasMore: page.hasMore };
			} finally {
				await client.close();
			}
		};
		const scanned = await bounded(false);
		expect(scanned.keys).toEqual([ 'k1' ]);
		expect(await bounded(true)).toEqual(scanned);
	});
});
