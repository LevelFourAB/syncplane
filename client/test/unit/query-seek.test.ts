// What a cursor costs the index walk, as opposed to what it returns.
//
// Every case here pages inside ONE tie group — hundreds of rows sharing the
// order key, separated only by the row-key tiebreak — because that is where a
// seek bound that stops before the row key lands at the START of the group and
// re-reads every page before this one. The rows are pinned against the offset
// path, which cannot be wrong for the wrong reason, and the candidate count
// from the query event is what says the walk did not read them.

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
import type { Query, QueryCursor } from '../../src/query/index.js';
import { defineSchema, entity, type Infer, int32, text } from '../../src/schema/builder.js';
import type { QueryEvent, SyncEvent } from '../../src/sync/events.js';
import { TouchSet } from '../../src/touch.js';

// The two index shapes the seek distinguishes. `narrow` ends exactly where the
// ordering does, so a cursor's whole entry — row key included — is one of its
// entries; `wide` carries a component past it, and a bound running into the row
// key would be a prefix of nothing there.
const narrow = entity('narrow', {
	id: text().key(),
	title: text(),
	state: text(),
	rank: int32(),
}, {
	indexes: [ { name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] } ],
});

const wide = entity('wide', {
	id: text().key(),
	title: text(),
	state: text(),
	rank: int32(),
}, {
	indexes: [ {
		name: 'by_state_rank_title',
		key: [ { path: 'state' }, { path: 'rank', desc: true }, { path: 'title' } ],
	} ],
});

type Row = Infer<typeof narrow>;
const bundle = defineSchema(narrow, wide);
const NarrowSchema = bundle.descMessage('narrow');
const WideSchema = bundle.descMessage('wide');
const registry = new EntityRegistry([ NarrowSchema, WideSchema ]);

const WS = 'ws1';

/** Rows in the one tie group every case pages through. */
const TIE = 400;
/** Rows outside the equality prefix, so the range is a part of the index. */
const OTHER = 20;
const LIMIT = 10;

/** The key of the nth row of the tie group, zero-padded so key order is n
 * order. */
const keyAt = (n: number): string => `i${String(n).padStart(3, '0')}`;

/**
 * A client over both entities, seeded and built.
 *
 * Every row of the tie group holds the SAME rank, so the ordering is decided by
 * the row key alone — which is the half a seek bound may or may not carry.
 */
async function seeded() {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	const meta = await kv.write();
	// A build only walks a workspace holding a whole installed snapshot, and the
	// cursor is what says so (see readInstalledSnapshot).
	await meta.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await meta.commit();

	let version = 0n;
	for (const [ name, schema ] of [ [ 'narrow', NarrowSchema ], [ 'wide', WideSchema ] ] as const) {
		for (let start = 0; start < TIE + OTHER; start += 200) {
			const tx = await kv.write();
			const changes = [];
			for (let i = start; i < Math.min(start + 200, TIE + OTHER); i++) {
				const row: Row = {
					id: keyAt(i),
					title: 'same',
					state: i < TIE ? 'open' : 'closed',
					rank: 1,
				};
				changes.push(create(RowChangeSchema, {
					entity: name,
					key: row.id,
					version: ++version,
					body: { case: 'value', value: toBinary(schema, create(schema, row)) },
				}));
			}
			await applyServerChanges(tx, registry, WS, changes, new TouchSet(), indexes);
			await tx.commit();
		}
	}
	const mutex = new ChainMutex();
	await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);

	const events: SyncEvent[] = [];
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ NarrowSchema, WideSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		onSyncEvent: (e) => void events.push(e),
	}) as unknown as SyncClient<never>;
	/** Index positions the last query walked; undefined if it scanned. */
	const walked = (): number | undefined => {
		const last = events.filter((e): e is QueryEvent => e.type === 'query').at(-1)!;
		expect(last.path).toBe('index');
		return last.candidates;
	};
	return { client, walked };
}

/**
 * Runs one query through the client's normal read path. The cast is the price
 * of a runtime-built schema; see index-query.test.ts.
 */
const run = (
	client: SyncClient<never>,
	schema: typeof NarrowSchema,
	q: Query<Row>,
): Promise<RowSet<Row>> => client.query(schema, q as never) as unknown as Promise<RowSet<Row>>;

/** The page every case takes, without its bounds. */
const triage: Query<Row> = {
	where: (r) => r.state.eq('open'),
	orderBy: (r) => [ r.rank.desc() ],
};

const keysOf = (rows: RowSet<Row>) => rows.map((r) => r.key);

/** The cursor of the nth row of the tie group. */
async function cursorAt(
	client: SyncClient<never>,
	schema: typeof NarrowSchema,
	n: number,
): Promise<QueryCursor> {
	const page = await run(client, schema, { ...triage, limit: 1, offset: n });
	expect(keysOf(page)).toEqual([ keyAt(n) ]);
	return page.endCursor!;
}

describe('paging inside a tie group', () => {
	it('costs the page rather than the depth when the index ends at the order keys', async () => {
		const { client, walked } = await seeded();
		try {
			const shallow = await cursorAt(client, NarrowSchema, 39);
			const deep = await cursorAt(client, NarrowSchema, 239);

			const early = await run(client, NarrowSchema, { ...triage, limit: LIMIT, after: shallow });
			const earlyWalk = walked()!;
			const late = await run(client, NarrowSchema, { ...triage, limit: LIMIT, after: deep });
			const lateWalk = walked()!;

			// The rows the offset path returns, which cannot be right by accident.
			expect(keysOf(early)).toEqual(
				keysOf(await run(client, NarrowSchema, { ...triage, limit: LIMIT, offset: 40 })),
			);
			expect(keysOf(late)).toEqual(
				keysOf(await run(client, NarrowSchema, { ...triage, limit: LIMIT, offset: 240 })),
			);
			expect(keysOf(late)[0]).toBe(keyAt(240));
			// The bound cut nothing off the match set: `total` still counts every
			// row past the cursor.
			expect(late.total).toBe(TIE - 240);

			// The deep page reads no more of the index than the shallow one — the
			// property that makes a 20-page walk O(pages) instead of O(pages²) —
			// and both read a page's worth rather than a tie group's.
			expect(lateWalk).toBe(earlyWalk);
			expect(lateWalk).toBeLessThan(TIE / 2);
		} finally {
			await client.close();
		}
	});

	it('still answers from the tie group when the index carries a further component', async () => {
		const { client, walked } = await seeded();
		try {
			const deep = await cursorAt(client, WideSchema, 239);
			const late = await run(client, WideSchema, { ...triage, limit: LIMIT, after: deep });
			const lateWalk = walked()!;

			expect(keysOf(late)).toEqual(
				keysOf(await run(client, WideSchema, { ...triage, limit: LIMIT, offset: 240 })),
			);
			expect(late.total).toBe(TIE - 240);
			// The seek could only bound the components, so the walk crossed every
			// position ahead of the cursor and `within` rejected them. Correct, and
			// the cost the narrow index above does not pay.
			expect(lateWalk).toBeGreaterThan(239);
		} finally {
			await client.close();
		}
	});

	it('ends a before page at the cursor rather than at the end of its tie group', async () => {
		const { client, walked } = await seeded();
		try {
			const at200 = await cursorAt(client, NarrowSchema, 200);
			const previous = await run(client, NarrowSchema, {
				...triage,
				limit: LIMIT,
				before: at200,
			});
			const walk = walked();

			expect(keysOf(previous)).toEqual(
				keysOf(await run(client, NarrowSchema, { ...triage, limit: LIMIT, offset: 190 })),
			);
			expect(previous.hasMore).toBe(true);
			// Exactly the positions below the bound: the boundary row is outside an
			// exclusive end, and so is every row of its tie group after it.
			expect(walk).toBe(200);
		} finally {
			await client.close();
		}
	});

	it('reads one position for a cursor on the last row of the tie group', async () => {
		const { client, walked } = await seeded();
		try {
			const last = await cursorAt(client, NarrowSchema, TIE - 1);
			const page = await run(client, NarrowSchema, { ...triage, limit: LIMIT, after: last });

			expect(keysOf(page)).toEqual([]);
			expect(page.total).toBe(0);
			expect(page.hasMore).toBe(false);
			// The boundary row itself, which the seek admits and `within` rejects.
			expect(walked()).toBe(1);
		} finally {
			await client.close();
		}
	});
});
