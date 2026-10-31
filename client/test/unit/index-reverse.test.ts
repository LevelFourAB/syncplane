// An ordering that runs against every component it uses is served by reading
// the index backward. That must return exactly what the scan returns, with
// three things in the way: the scan breaks a tie on the row key ascending
// while a backward read meets each tie group in descending key order, a NaN
// or missing value sorts last in either direction while its tag does not
// invert, and the walk has to stop near the limit rather than read the range.
// Every case runs against identical data with the indexes built and without.

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
import { type IndexReport, queryByIndex } from '../../src/query/execute.js';
import { compileQuery, type Query, type QueryCursor } from '../../src/query/index.js';
import { prepareQuery } from '../../src/query/wire.js';
import {
	defineSchema,
	double,
	entity,
	type Infer,
	int32,
	message,
	text,
} from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

// Every index is ascending except by_title_desc, and every query below orders
// against it. Ordering by `day` alone reads by_day_title, whose `title` sits
// past the order key; the single-component indexes are the ones the order
// keys reach the end of.
const journal = entity('journal', {
	id: text().key(),
	day: int32(),
	title: text(),
	// Unset on some rows, so `meta.*` resolves to the missing rank there.
	meta: message({ due: int32(), score: double() }),
}, {
	indexes: [
		{ name: 'by_due', key: [ { path: 'meta.due' } ] },
		{ name: 'by_score', key: [ { path: 'meta.score' } ] },
		{ name: 'by_title_desc', key: [ { path: 'title', desc: true } ] },
		{ name: 'by_day_title', key: [ { path: 'day' }, { path: 'title' } ] },
	],
});

type Journal = Infer<typeof journal>;
type SeedJournal = Omit<Journal, 'meta'> & { meta?: Journal['meta'] };
const bundle = defineSchema(journal);
const JournalSchema = bundle.descMessage('journal');
const registry = new EntityRegistry([ JournalSchema ]);
const WS = 'ws1';

const ROWS = 60;

// Three rows per day, so every page boundary can fall inside a tie group; day 0
// is what `day > 0` leaves out. Titles run against the row keys.
const SEED: SeedJournal[] = Array.from({ length: ROWS }, (_, i) => {
	const id = `j${String(i).padStart(2, '0')}`;
	const row: SeedJournal = { id, day: i % 20, title: `t${String(ROWS - i).padStart(2, '0')}` };
	if (i % 7 !== 0) row.meta = { due: i % 11, score: i % 13 === 0 ? Number.NaN : (i % 9) - 4 };
	return row;
});

async function seeded(built: boolean) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		WS,
		SEED.map((row, i) =>
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
async function clientWith(built: boolean, overlay: SeedJournal[] = []) {
	const { kv } = await seeded(built);
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

async function run(built: boolean, q: Query<Journal>, overlay?: SeedJournal[]) {
	const client = await clientWith(built, overlay);
	try {
		const rows = await query(client, q);
		return { keys: rows.map((r) => r.key), total: rows.total, hasMore: rows.hasMore };
	} finally {
		await client.close();
	}
}

/** What the index path did with `q`: the index it read and the positions it
 * walked, or undefined when it declined to the scan. */
async function walkOf(q: Query<Journal>): Promise<IndexReport | undefined> {
	const { kv, indexes } = await seeded(true);
	const tx = await kv.read();
	try {
		const report: IndexReport = { candidates: 0 };
		const out = await queryByIndex(
			tx,
			registry,
			registry.get('journal')!,
			JournalSchema,
			indexes.for('journal'),
			prepareQuery(JournalSchema, compileQuery(q)),
			WS,
			undefined,
			report,
		);
		return out === undefined ? undefined : report;
	} finally {
		tx.release();
	}
}

/**
 * Asserts the index serves `q` from `index`, answers as the scan does, and —
 * when `walked` is set — reads at most that many positions to do it.
 */
function same(
	name: string,
	q: Query<Journal>,
	opts: { index: string; walked?: number; overlay?: SeedJournal[] },
): void {
	it(name, async () => {
		const report = await walkOf(q);
		expect(report?.index).toBe(opts.index);
		if (opts.walked !== undefined) expect(report!.candidates).toBeLessThanOrEqual(opts.walked);
		const scanned = await run(false, q, opts.overlay);
		const indexed = await run(true, q, opts.overlay);
		expect(indexed).toEqual(scanned);
	});
}

describe('an ordering served by reading the index backward', () => {
	// A walk that stops near the limit reads the offset and the page, the rest
	// of the tie group the page ends in (at most two more rows here), the batch
	// that finds where that group ends (at most two positions past it), and the
	// position that settles the walk: offset + limit + 5 at most. The range
	// holds 60.
	same('stops after the tie group the limit falls in', {
		orderBy: (j) => [ j.day.desc() ],
		limit: 5,
	}, { index: 'by_day_title', walked: 10 });

	same('the latest journals: a range, read backward to the limit', {
		where: (j) => j.day.gt(0),
		orderBy: (j) => [ j.day.desc() ],
		limit: 30,
	}, { index: 'by_day_title', walked: 35 });

	same('past an offset', {
		orderBy: (j) => [ j.day.desc() ],
		offset: 7,
		limit: 4,
	}, { index: 'by_day_title', walked: 16 });

	same('with the total opted out', {
		orderBy: (j) => [ j.day.desc() ],
		limit: 5,
		total: false,
	}, { index: 'by_day_title', walked: 10 });

	same('with a residual that rejects rows', {
		where: (j) => j.title.neq('t59'),
		orderBy: (j) => [ j.day.desc() ],
		limit: 5,
	}, { index: 'by_day_title' });

	same('an ascending order on a descending component', {
		orderBy: (j) => [ j.title.asc() ],
		limit: 5,
	}, { index: 'by_title_desc', walked: 10 });

	same('two order keys, both against the index', {
		orderBy: (j) => [ j.day.desc(), j.title.desc() ],
		limit: 5,
	}, { index: 'by_day_title', walked: 10 });

	same('puts missing values after every real one', {
		orderBy: (j) => [ j.meta.due.desc() ],
		limit: 55,
	}, { index: 'by_due' });

	same('stops among the real values before reaching the missing ones', {
		orderBy: (j) => [ j.meta.due.desc() ],
		limit: 3,
	}, { index: 'by_due' });

	same('puts NaN after every real value and before missing', {
		orderBy: (j) => [ j.meta.score.desc() ],
	}, { index: 'by_score' });

	same('cuts a page inside the NaN rank', {
		orderBy: (j) => [ j.meta.score.desc() ],
		limit: 47,
	}, { index: 'by_score' });

	same('cuts a page inside the missing rank', {
		orderBy: (j) => [ j.meta.score.desc() ],
		limit: 55,
	}, { index: 'by_score' });

	same('merges a pending overlay', {
		orderBy: (j) => [ j.day.desc() ],
		limit: 5,
	}, {
		index: 'by_day_title',
		overlay: [
			{ id: 'j99', day: 30, title: 'new' },
			{ id: 'j19', day: 0, title: 'moved down' },
		],
	});

	for (const step of [ 1, 4, 7 ]) {
		it(`walks the same pages by cursor as the scan does, ${step} at a time`, async () => {
			const order: Query<Journal> = { orderBy: (j) => [ j.day.desc() ], limit: step };
			const forward = async (built: boolean): Promise<string[]> => {
				const client = await clientWith(built);
				try {
					const keys: string[] = [];
					let after: QueryCursor | undefined;
					for (;;) {
						const page = await query(client, { ...order, after });
						keys.push(...page.map((r) => r.key));
						if (!page.hasMore) return keys;
						after = page.endCursor;
					}
				} finally {
					await client.close();
				}
			};
			const backward = async (built: boolean): Promise<string[]> => {
				const client = await clientWith(built);
				try {
					const last = await query(client, { ...order, offset: ROWS - step });
					const keys = last.map((r) => r.key);
					let before = last.startCursor;
					for (;;) {
						const page = await query(client, { ...order, before });
						keys.unshift(...page.map((r) => r.key));
						if (!page.hasMore) return keys;
						before = page.startCursor;
					}
				} finally {
					await client.close();
				}
			};
			const scanned = await forward(false);
			expect(scanned).toHaveLength(ROWS);
			expect(await forward(true)).toEqual(scanned);
			expect(await backward(true)).toEqual(scanned);
		});
	}
});
