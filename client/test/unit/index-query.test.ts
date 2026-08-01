// Index-served queries must return EXACTLY what the scan returns — same rows,
// same order, same versions. So every case here runs twice against identical
// data, once with the index built and once without, and compares the two.
//
// That is the whole correctness argument for the client index: the scan path is
// the definition, the index path is an optimization over it, and the planner is
// free to decline anything as long as it never answers differently.

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { queryByIndex } from '../../src/query/execute.js';
import { and, compileQuery, type Query } from '../../src/query/index.js';
import { prepareQuery } from '../../src/query/wire.js';
import type { Row } from '../../src/read.js';
import {
	bool,
	defineSchema,
	entity,
	type Infer,
	int32,
	list,
	text,
} from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

// Both declaration surfaces on one entity: `state` carries a field mark (one
// component), `by_state_rank` is a declared composite over (state, rank desc) —
// the filter-plus-different-sort shape a single marked field cannot serve at
// O(limit).
// `labels` adds the third shape: a MULTI-ENTRY index, where one row takes one
// position per element. Everything below applies to it unchanged — which is
// the point, since a fan-out index is where an index and a scan are most
// likely to drift apart.
const issue = entity('issue', {
	id: text().key(),
	title: text(),
	state: text().indexed(),
	rank: int32(),
	done: bool(),
	labels: list(text()).indexed(),
}, {
	indexes: [
		{ name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] },
		// A fan-out component pinned by the equality prefix, followed by an
		// ordinary one: the shape that serves "labelled X, ordered by rank".
		{ name: 'by_label_rank', key: [ { path: 'labels' }, { path: 'rank', desc: true } ] },
	],
});

type Issue = Infer<typeof issue>;
const bundle = defineSchema(issue);
const IssueSchema = bundle.descMessage('issue');
const registry = new EntityRegistry([ IssueSchema ]);
const WS = 'ws1';

const SEED: Issue[] = [
	{ id: 'i1', title: 'one', state: 'open', rank: 5, done: false, labels: [ 'bug', 'ui' ] },
	{ id: 'i2', title: 'two', state: 'open', rank: 9, done: true, labels: [ 'ui' ] },
	{ id: 'i3', title: 'three', state: 'done', rank: 3, done: true, labels: [ 'bug' ] },
	// No labels at all: it must still be reachable through by_label_rank, which
	// is what the missing tag on an empty list is for.
	{ id: 'i4', title: 'four', state: 'open', rank: 1, done: false, labels: [] },
	// A duplicate element collapses to one entry — the row must appear once.
	{ id: 'i5', title: 'five', state: 'done', rank: 7, done: false, labels: [ 'ui', 'ui' ] },
	// Implicit-presence empty string. Matching it is not the same as matching a
	// missing value, and an index must not conflate the two.
	{ id: 'i6', title: 'six', state: '', rank: 4, done: false, labels: [ 'bug', 'ui' ] },
];

/** Plants rows as CONFIRMED state — the only state an index covers. */
async function seedConfirmed(kv: KVStore, indexes: IndexCache): Promise<void> {
	const tx = await kv.write();
	// A build walks only a workspace whose rows are a whole installed snapshot,
	// and the cursor is what says so — without one these rows look like a
	// bootstrap still filling the store (see readInstalledSnapshot).
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
 * A client over a store seeded with SEED, with its indexes built or not.
 *
 * `built` is the ONLY difference between the two arms — same schema, same rows,
 * same query — which is what makes a divergence a bug rather than a difference
 * in setup.
 */
async function clientWith(built: boolean, overlay?: Issue[]) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seedConfirmed(kv, indexes);
	if (built) {
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	}
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ IssueSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	}) as unknown as SyncClient<never>;
	// Local writes land in the optimistic overlay, which the index deliberately
	// does not cover — so these exercise the confirmed/overlay merge.
	for (const row of overlay ?? []) await client.mutate.put(IssueSchema, row as never);
	return client;
}

/**
 * Runs one query through the client's normal read path.
 *
 * The cast is the price of a runtime-built schema: `Issue` is inferred from the
 * builder, while the descriptor the client holds is only statically a
 * `DescMessage`, so the two cannot be related without a nominal link the
 * builder does not create.
 */
const run = (client: SyncClient<never>, q: Query<Issue>): Promise<Row<Issue>[]> =>
	client.query(IssueSchema, q as never) as unknown as Promise<Row<Issue>[]>;

/** The two arms of one query, as comparable plain data. */
async function both(q: Query<Issue>, overlay?: Issue[]) {
	const shape = (rows: Row<Issue>[]) =>
		rows.map((r) => ({ key: r.key, version: r.version, state: r.value.state, rank: r.value.rank }));
	const scanned = await clientWith(false, overlay);
	const indexed = await clientWith(true, overlay);
	try {
		return {
			scanned: shape(await run(scanned, q)),
			indexed: shape(await run(indexed, q)),
		};
	} finally {
		await scanned.close();
		await indexed.close();
	}
}

/** Asserts the two arms agree, and pins what they agree ON. */
function same(name: string, q: Query<Issue>, want: string[], overlay?: Issue[]): void {
	it(name, async () => {
		const { scanned, indexed } = await both(q, overlay);
		expect(indexed).toEqual(scanned);
		expect(scanned.map((r) => r.key)).toEqual(want);
	});
}

/**
 * Whether the index path actually served a query, rather than quietly
 * declining. Without this the differential tests above would pass trivially
 * with a planner that never chose anything.
 */
async function served(built: boolean, q: Query<Issue>): Promise<boolean> {
	return await servedIn(WS, q, built);
}

/** served(), for a named workspace or for the cross-workspace read. */
async function servedIn(
	workspaceId: string | undefined,
	q: Query<Issue>,
	built = true,
): Promise<boolean> {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seedConfirmed(kv, indexes);
	if (built) {
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	}
	const tx = await kv.read();
	try {
		const out = await queryByIndex(
			tx,
			registry,
			registry.get('issue')!,
			IssueSchema,
			indexes.for('issue'),
			prepareQuery(IssueSchema, compileQuery(q)),
			workspaceId,
		);
		return out !== undefined;
	} finally {
		tx.release();
	}
}

describe('the index path is actually taken', () => {
	it('serves a query the plan covers, once built', async () => {
		expect(await served(true, { where: (i) => i.state.eq('open') })).toBe(true);
		expect(
			await served(true, {
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
			}),
		).toBe(true);
	});

	it('declines before the build has marked the index', async () => {
		// Maintenance alone never claims coverage: entries exist only for rows
		// written since the index was declared.
		expect(await served(false, { where: (i) => i.state.eq('open') })).toBe(false);
	});

	it('serves membership over a repeated field', async () => {
		expect(await served(true, { where: (i) => i.labels.has('ui') })).toBe(true);
		expect(
			await served(true, {
				where: (i) => i.labels.has('ui'),
				orderBy: (i) => [ i.rank.desc() ],
			}),
		).toBe(true);
	});

	it('declines a query no index covers', async () => {
		expect(await served(true, { where: (i) => i.title.contains('x') })).toBe(false);
		expect(await served(true, { where: (i) => i.done.eq(true) })).toBe(false);
	});

	it('declines a cross-workspace query', async () => {
		// The index nests the workspace inside its prefix, and its tie-break
		// across workspaces differs from the scan path's; see execute.ts.
		expect(await servedIn(undefined, { where: (i) => i.state.eq('open') })).toBe(false);
		expect(await servedIn(WS, { where: (i) => i.state.eq('open') })).toBe(true);
	});
});

describe('index-served local queries', () => {
	same('equality on an indexed field', { where: (i) => i.state.eq('open') }, [ 'i1', 'i2', 'i4' ]);

	same('equality plus the ordering the composite index supplies', {
		where: (i) => i.state.eq('open'),
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i2', 'i1', 'i4' ]);

	same('equality, ordering and a limit', {
		where: (i) => i.state.eq('open'),
		orderBy: (i) => [ i.rank.desc() ],
		limit: 2,
	}, [ 'i2', 'i1' ]);

	same('an offset past the first page', {
		where: (i) => i.state.eq('open'),
		orderBy: (i) => [ i.rank.desc() ],
		offset: 1,
		limit: 1,
	}, [ 'i1' ]);

	same('an ordering with no predicate', {
		orderBy: (i) => [ i.state.asc() ],
	}, [ 'i6', 'i3', 'i5', 'i1', 'i2', 'i4' ]);

	same('an inequality bounds the range', {
		where: (i) => i.rank.gt(4),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i1', 'i5', 'i2' ]);

	same('a residual term is re-checked after the range', {
		where: (i) => and(i.state.eq('open'), i.done.eq(true)),
	}, [ 'i2' ]);

	same('matching the empty string is not matching nothing', {
		where: (i) => i.state.eq(''),
	}, [ 'i6' ]);

	same('contains is never index-servable and falls back', {
		where: (i) => i.title.contains('hre'),
	}, [ 'i3' ]);

	same('a predicate matching nothing', { where: (i) => i.state.eq('archived') }, []);

	// Fan-out: one row, several index entries. Every case here is a place the
	// index could return a row twice, or miss one whose list is empty.
	same('membership over a repeated field', {
		where: (i) => i.labels.has('bug'),
	}, [ 'i1', 'i3', 'i6' ]);

	same('a row whose element repeats is still returned once', {
		where: (i) => i.labels.has('ui'),
	}, [ 'i1', 'i2', 'i5', 'i6' ]);

	same('membership plus the ordering the fan-out index supplies', {
		where: (i) => i.labels.has('ui'),
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i2', 'i5', 'i1', 'i6' ]);

	same('membership, ordering and a limit', {
		where: (i) => i.labels.has('ui'),
		orderBy: (i) => [ i.rank.desc() ],
		limit: 2,
	}, [ 'i2', 'i5' ]);

	same('hasAny returns each matching row once, across several ranges', {
		where: (i) => i.labels.hasAny([ 'bug', 'ui' ]),
	}, [ 'i1', 'i2', 'i3', 'i5', 'i6' ]);

	same('hasAll re-checks what the range cannot pin', {
		where: (i) => i.labels.hasAll([ 'bug', 'ui' ]),
	}, [ 'i1', 'i6' ]);

	same('isEmpty finds the row with no elements', {
		where: (i) => i.labels.isEmpty(),
	}, [ 'i4' ]);

	same('a predicate on another field still sees rows with an empty list', {
		// by_label_rank pins nothing here, so the range spans every entry — and
		// i4, whose list is empty, must be in it.
		where: (i) => i.state.eq('open'),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i4', 'i1', 'i2' ]);

	same('no predicate and no ordering', {}, [ 'i1', 'i2', 'i3', 'i4', 'i5', 'i6' ]);

	describe('with an optimistic overlay', () => {
		// The overlay is authoritative for a row it holds, so a confirmed row it
		// shadows must drop out of the index stream and reappear — if it still
		// matches — from the overlay. Getting this wrong duplicates or loses rows,
		// and only the confirmed/overlay merge can catch it.
		same(
			'a local edit that moves a row within the index',
			{
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
			},
			[ 'i1', 'i2', 'i4' ],
			[
				// i2 keeps matching but sorts below i1 now.
				{ id: 'i2', title: 'two', state: 'open', rank: 2, done: true, labels: [ 'ui' ] },
			],
		);

		same(
			'a local edit that removes a row from the result',
			{
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
			},
			[ 'i1', 'i4' ],
			[
				{ id: 'i2', title: 'two', state: 'done', rank: 9, done: true, labels: [ 'ui' ] },
			],
		);

		same(
			'a local edit that adds a row to the result',
			{
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
			},
			[ 'i2', 'i3', 'i1', 'i4' ],
			[
				{ id: 'i3', title: 'three', state: 'open', rank: 6, done: true, labels: [ 'bug' ] },
			],
		);

		same(
			'a row that exists only optimistically',
			{
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
			},
			[ 'i9', 'i2', 'i1', 'i4' ],
			[
				{ id: 'i9', title: 'nine', state: 'open', rank: 99, done: false, labels: [ 'ui' ] },
			],
		);

		same(
			'a local edit to the indexed list, which the confirmed entries do not know about',
			{ where: (i) => i.labels.has('bug') },
			[ 'i1', 'i2', 'i3', 'i6' ],
			[
				// i2 gains "bug" only optimistically. The index covers confirmed
				// rows alone, so this can only come from the overlay merge.
				{ id: 'i2', title: 'two', state: 'open', rank: 9, done: true, labels: [ 'ui', 'bug' ] },
			],
		);

		same(
			'a local edit that drops the element the range was probed for',
			{ where: (i) => i.labels.has('bug') },
			[ 'i3', 'i6' ],
			[
				// i1's confirmed entry under "bug" is still there; the overlay
				// shadows the row, so it must not come back from the range.
				{ id: 'i1', title: 'one', state: 'open', rank: 5, done: false, labels: [ 'ui' ] },
			],
		);

		same(
			'an overlay row still counts against a limit',
			{
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
				limit: 2,
			},
			[ 'i9', 'i2' ],
			[
				{ id: 'i9', title: 'nine', state: 'open', rank: 99, done: false, labels: [ 'ui' ] },
			],
		);
	});
});
