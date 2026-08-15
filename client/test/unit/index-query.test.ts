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
import type { RowSet } from '../../src/coverage.js';
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
import { and, compileQuery, not, or, type Query, type QueryCursor } from '../../src/query/index.js';
import { prepareQuery } from '../../src/query/wire.js';
import type { Row } from '../../src/read.js';
import {
	bool,
	defineSchema,
	entity,
	type Infer,
	int32,
	list,
	map,
	message,
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
	// A leaf behind a message that can be unset — the shape whose index carries
	// the missing rank, which is what presence plans against.
	meta: message({ due: int32() }),
	// User-defined fields: `custom.*.<side>` fans out over EVERY entry, which is
	// the fourth shape — one entry per (key, value) pair rather than per element.
	custom: map('string', message({ number: int32(), tag: text() })),
}, {
	indexes: [
		{ name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] },
		{ name: 'by_due', key: [ { path: 'meta.due' } ] },
		// A fan-out component pinned by the equality prefix, followed by an
		// ordinary one: the shape that serves "labelled X, ordered by rank".
		{ name: 'by_label_rank', key: [ { path: 'labels' }, { path: 'rank', desc: true } ] },
		// The map fan-out, key first: a probe on ONE custom field is a
		// contiguous range, and the value orders within it.
		{
			name: 'cf_number',
			key: [ { path: 'custom.*.key' }, { path: 'custom.*.value.number' } ],
		},
		{
			name: 'cf_num_rank',
			key: [
				{ path: 'custom.*.key' },
				{ path: 'custom.*.value.number' },
				{ path: 'rank', desc: true },
			],
		},
		// The mirror layout: value first, so "which custom field holds 5" is a
		// range read rather than a scan.
		{
			name: 'cf_by_number',
			key: [ { path: 'custom.*.value.number' }, { path: 'custom.*.key' } ],
		},
	],
});

type Issue = Infer<typeof issue>;
/** A seed row: `meta` may be absent — the builder's inferred shape requires
 * it, but a row without one is exactly what the missing rank encodes. */
type SeedIssue = Omit<Issue, 'meta'> & { meta?: Issue['meta'] };
const bundle = defineSchema(issue);
const IssueSchema = bundle.descMessage('issue');
const registry = new EntityRegistry([ IssueSchema ]);
const WS = 'ws1';

const SEED: SeedIssue[] = [
	{
		id: 'i1',
		title: 'one',
		state: 'open',
		rank: 5,
		done: false,
		labels: [ 'bug', 'ui' ],
		custom: { priority: { number: 5, tag: 'hi' }, team: { number: 0, tag: 'platform' } },
	},
	{
		id: 'i2',
		title: 'two',
		state: 'open',
		rank: 9,
		done: true,
		labels: [ 'ui' ],
		meta: { due: 2 },
		custom: {},
	},
	{
		id: 'i3',
		title: 'three',
		state: 'done',
		rank: 3,
		done: true,
		labels: [ 'bug' ],
		custom: { priority: { number: 1, tag: '' } },
	},
	// No labels at all: it must still be reachable through by_label_rank, which
	// is what the missing tag on an empty list is for. No custom fields either,
	// which is the same story for cf_number.
	{ id: 'i4', title: 'four', state: 'open', rank: 1, done: false, labels: [], custom: {} },
	// A duplicate element collapses to one entry — the row must appear once.
	{
		id: 'i5',
		title: 'five',
		state: 'done',
		rank: 7,
		done: false,
		labels: [ 'ui', 'ui' ],
		meta: { due: 8 },
		custom: { size: { number: 5, tag: '' } },
	},
	// Implicit-presence empty string. Matching it is not the same as matching a
	// missing value, and an index must not conflate the two.
	{
		id: 'i6',
		title: 'six',
		state: '',
		rank: 4,
		done: false,
		labels: [ 'bug', 'ui' ],
		custom: {},
	},
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
async function clientWith(built: boolean, overlay?: SeedIssue[], deletes?: string[]) {
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
	for (const key of deletes ?? []) await client.mutate.del(IssueSchema, key);
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
const run = (client: SyncClient<never>, q: Query<Issue>): Promise<RowSet<Issue>> =>
	client.query(IssueSchema, q as never) as unknown as Promise<RowSet<Issue>>;

/** The two arms of one query, as comparable plain data. */
async function both(q: Query<Issue>, overlay?: SeedIssue[], deletes?: string[]) {
	// `total` is part of the answer, so it is part of what the two arms have to
	// agree on: an index that early-exits still has to count what it skipped.
	const shape = (rows: RowSet<Issue>) => ({
		rows: rows.map((r) => ({
			key: r.key,
			version: r.version,
			state: r.value.state,
			rank: r.value.rank,
		})),
		total: rows.total,
	});
	const scanned = await clientWith(false, overlay, deletes);
	const indexed = await clientWith(true, overlay, deletes);
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
function same(name: string, q: Query<Issue>, want: string[], overlay?: SeedIssue[]): void {
	it(name, async () => {
		const { scanned, indexed } = await both(q, overlay);
		expect(indexed).toEqual(scanned);
		expect(scanned.rows.map((r) => r.key)).toEqual(want);
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

	it('serves a predicate on one custom field, and declines a bare ordering', async () => {
		// Without this the differential cases above would pass trivially on a
		// planner that never chose the map index at all.
		expect(await served(true, { where: (i) => i.custom.at('priority').number.eq(5) }))
			.toBe(true);
		expect(
			await served(true, {
				where: (i) => i.custom.at('priority').number.gt(0),
				orderBy: (i) => [ i.custom.at('priority').number.asc() ],
			}),
		).toBe(true);
		// Nothing pins the entry key, so rows lacking it sit outside every range
		// and must sort last — which the executor has no second pass to do.
		expect(await served(true, { orderBy: (i) => [ i.custom.at('priority').number.asc() ] }))
			.toBe(false);
		expect(await served(true, { where: (i) => i.custom.at('priority').isNotSet() })).toBe(false);
	});

	it('serves a cross-key any() off the mirror layout', async () => {
		// Without this the differential cases would pass trivially on a planner
		// that never chose cf_by_number at all and scanned every time.
		expect(await served(true, { where: (i) => i.custom.any((e) => e.value.number.eq(5)) }))
			.toBe(true);
		expect(
			await served(true, {
				where: (i) => i.custom.any((e) => and(e.key.eq('priority'), e.value.number.eq(5))),
			}),
		).toBe(true);
		// startsWith over keys is not an equality, and no range bounds an
		// existential from inside — so this one honestly falls back.
		expect(await served(true, { where: (i) => i.custom.any((e) => e.key.startsWith('pri')) }))
			.toBe(false);
	});

	it('declines a query no index covers', async () => {
		expect(await served(true, { where: (i) => i.title.contains('x') })).toBe(false);
		expect(await served(true, { where: (i) => i.done.eq(true) })).toBe(false);
	});

	it('serves a top-level or whose every branch has an index', async () => {
		expect(await served(true, { where: (i) => or(i.state.eq('done'), i.labels.has('bug')) }))
			.toBe(true);
		// A branch no index serves declines the whole union: a union that scans
		// for one branch has become the scan with extra steps.
		expect(await served(true, { where: (i) => or(i.state.eq('done'), i.done.eq(true)) }))
			.toBe(false);
	});

	it('serves presence against a component that can hold the missing rank', async () => {
		expect(await served(true, { where: (i) => i.meta.due.isNotSet() })).toBe(true);
		expect(await served(true, { where: (i) => i.meta.due.isSet() })).toBe(true);
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

	// A top-level or: one probe set per branch, on DIFFERENT indexes, deduped
	// by row key. i3 matches both branches — the row the dedupe exists for.
	same('an or of indexed branches unions their probes', {
		where: (i) => or(i.state.eq('done'), i.labels.has('bug')),
	}, [ 'i1', 'i3', 'i5', 'i6' ]);

	same("a union's residual is the whole or, not the branch's", {
		// i1, i3 and i6 sit in the labels range and FAIL the first branch —
		// they must come back through the or, not be dropped by a branch-local
		// check.
		where: (i) => or(and(i.state.eq('open'), i.done.eq(true)), i.labels.has('bug')),
	}, [ 'i1', 'i2', 'i3', 'i6' ]);

	same('an ordered or is re-sorted by the caller', {
		where: (i) => or(i.state.eq('done'), i.labels.has('bug')),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i3', 'i6', 'i1', 'i5' ]);

	same(
		'a union merges the overlay like any other indexed read',
		{
			where: (i) => or(i.state.eq('done'), i.labels.has('bug')),
		},
		[ 'i1', 'i5', 'i6' ],
		[
			// i3 no longer matches either branch; the confirmed positions that
			// still name it are shadowed by the overlay.
			{ id: 'i3', title: 'three', state: 'open', rank: 3, done: true, labels: [], custom: {} },
		],
	);

	same('two inequalities intersect into one bounded range', {
		where: (i) => and(i.meta.due.gte(2), i.meta.due.lt(8)),
	}, [ 'i2' ]);

	same('bounds that contradict intersect into an empty range', {
		where: (i) => and(i.meta.due.gte(8), i.meta.due.lt(2)),
	}, []);

	// Presence: the by_due index carries the missing rank for every row whose
	// `meta` is unset, and both the encoder's "missing" and the evaluator's
	// isNotSet come from the same resolution rule.
	same('isNotSet pins the missing rank', {
		where: (i) => i.meta.due.isNotSet(),
	}, [ 'i1', 'i3', 'i4', 'i6' ]);

	same('isSet bounds the set ranks', {
		where: (i) => i.meta.due.isSet(),
	}, [ 'i2', 'i5' ]);

	// Map fan-out. cf_number stores (entry key, number) per entry, so a
	// predicate naming one custom field is a contiguous range — and every case
	// here is a place that range could drop a row it should not.
	same('equality on one custom field', {
		where: (i) => i.custom.at('priority').number.eq(5),
	}, [ 'i1' ]);

	same('an inequality inside one pinned custom field', {
		where: (i) => i.custom.at('priority').number.gt(0),
		orderBy: (i) => [ i.custom.at('priority').number.asc() ],
	}, [ 'i3', 'i1' ]);

	same('a custom field on a different key does not answer this one', {
		// i5 holds `size` = 5, not `priority` — the key component is what keeps
		// the two apart.
		where: (i) => i.custom.at('priority').number.eq(5),
	}, [ 'i1' ]);

	same('a custom field no row holds', {
		where: (i) => i.custom.at('absent').number.eq(1),
	}, []);

	same('isNotSet must not be answered from a key-pinned range', {
		// Every row lacking that entry matches, including rows with no custom
		// fields at all — which sit outside any key-pinned range.
		where: (i) => i.custom.at('priority').isNotSet(),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i4', 'i6', 'i5', 'i2' ]);

	same('a custom-field predicate with an ordering on an ordinary field', {
		where: (i) => i.custom.at('priority').number.gt(0),
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i1', 'i3' ]);

	same('ordering by a custom field with no predicate falls back to the scan', {
		// Nothing pins the entry key, so the index cannot be walked in this
		// order — rows lacking the key sit outside any range and must sort last.
		orderBy: (i) => [ i.custom.at('priority').number.asc() ],
	}, [ 'i3', 'i1', 'i2', 'i4', 'i5', 'i6' ]);

	same('isEmpty over the whole map', {
		where: (i) => i.custom.isEmpty(),
	}, [ 'i2', 'i4', 'i6' ]);

	// Cross-key: the mirror axis, served by cf_by_number's [value, key] layout.
	// No addressed path can ask these — they name a VALUE and leave the field
	// open, which is the whole point of the entry scope.
	same('which custom field holds a value, across keys', {
		where: (i) => i.custom.any((e) => e.value.number.eq(5)),
	}, [ 'i1', 'i5' ]);

	same('a value no custom field holds', {
		where: (i) => i.custom.any((e) => e.value.number.eq(99)),
	}, []);

	same('key and value conjoined describe ONE entry', {
		where: (i) => i.custom.any((e) => and(e.key.eq('priority'), e.value.number.eq(5))),
	}, [ 'i1' ]);

	same('key and value from different entries do not describe one', {
		// i5 holds size = 5 and i1 priority = 5, so neither row has an entry
		// keyed `team` holding 5 — a range that folded the two would return both.
		where: (i) => i.custom.any((e) => and(e.key.eq('team'), e.value.number.eq(5))),
	}, []);

	same('which rows have any of these custom fields set', {
		where: (i) => i.custom.any((e) => e.key.in([ 'size', 'absent' ])),
	}, [ 'i5' ]);

	same('a prefix over entry keys, which an addressed path cannot ask', {
		where: (i) => i.custom.any((e) => e.key.startsWith('pri')),
	}, [ 'i1', 'i3' ]);

	same('not(any) over a map reads as "no entry matches"', {
		// Rows with no custom fields at all match, and they sit outside every
		// value range — the same trap isNotSet sets for a key-pinned range.
		where: (i) => not(i.custom.any((e) => e.value.number.eq(5))),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i4', 'i3', 'i6', 'i2' ]);

	same('cross-key with an ordering the index does not supply', {
		where: (i) => i.custom.any((e) => e.value.number.eq(5)),
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i5', 'i1' ]);

	same('both axes at once: one entry pinned by key, another found by value', {
		where: (i) =>
			and(
				i.custom.at('priority').number.eq(5),
				i.custom.any((e) => e.value.tag.eq('platform')),
			),
	}, [ 'i1' ]);

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
				{ id: 'i2', title: 'two', state: 'open', rank: 2, done: true, labels: [ 'ui' ], custom: {} },
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
				{ id: 'i2', title: 'two', state: 'done', rank: 9, done: true, labels: [ 'ui' ], custom: {} },
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
				{ id: 'i3', title: 'three', state: 'open', rank: 6, done: true, labels: [ 'bug' ], custom: {} },
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
				{ id: 'i9', title: 'nine', state: 'open', rank: 99, done: false, labels: [ 'ui' ], custom: {} },
			],
		);

		same(
			'a local edit to the indexed list, which the confirmed entries do not know about',
			{ where: (i) => i.labels.has('bug') },
			[ 'i1', 'i2', 'i3', 'i6' ],
			[
				// i2 gains "bug" only optimistically. The index covers confirmed
				// rows alone, so this can only come from the overlay merge.
				{
					id: 'i2',
					title: 'two',
					state: 'open',
					rank: 9,
					done: true,
					labels: [ 'ui', 'bug' ],
					custom: {},
				},
			],
		);

		same(
			'a local edit that drops the element the range was probed for',
			{ where: (i) => i.labels.has('bug') },
			[ 'i3', 'i6' ],
			[
				// i1's confirmed entry under "bug" is still there; the overlay
				// shadows the row, so it must not come back from the range.
				{ id: 'i1', title: 'one', state: 'open', rank: 5, done: false, labels: [ 'ui' ], custom: {} },
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
				{ id: 'i9', title: 'nine', state: 'open', rank: 99, done: false, labels: [ 'ui' ], custom: {} },
			],
		);

		// The shapes below settle inside a range that still holds shadowed
		// positions, which is where the count shortcut answers the remainder —
		// with the overlay's confirmed positions taken back out (countAssist in
		// query/execute.ts). Every way a shadowed position relates to the counted
		// range gets a case: in it, out of it, absent, and tombstoned.
		same(
			'a shadowed row in the counted remainder stays out of the total',
			{ where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ], limit: 1 },
			[ 'i2' ],
			[
				// i1 leaves the match set only optimistically; its confirmed
				// position under "open" is still in the index the count reads.
				{ id: 'i1', title: 'one', state: 'wip', rank: 5, done: false, labels: [ 'ui' ], custom: {} },
			],
		);

		same(
			'a shadowed row outside the counted range is not subtracted',
			{ where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ], limit: 1 },
			[ 'i2' ],
			[
				// i3's confirmed position sits under "done" — a range the count
				// never covers, so nothing of it may come back out.
				{
					id: 'i3',
					title: 'three',
					state: 'done',
					rank: 8,
					done: true,
					labels: [ 'bug' ],
					custom: { priority: { number: 1, tag: '' } },
				},
			],
		);

		same(
			'an overlay edit that keeps matching, through the fan-out index',
			{ where: (i) => i.labels.has('ui'), orderBy: (i) => [ i.rank.desc() ], limit: 1 },
			[ 'i2' ],
			[
				// i5 still matches from the overlay while its confirmed "ui"
				// position is in the counted remainder — the row must appear once
				// and be counted once.
				{ id: 'i5', title: 'five', state: 'done', rank: 8, done: false, labels: [ 'ui' ], custom: {} },
			],
		);

		same(
			'an overlay-only insert on top of the counted remainder',
			{ where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ], limit: 1 },
			[ 'i9' ],
			[
				// No confirmed base, so nothing to subtract — the row only adds.
				{ id: 'i9', title: 'nine', state: 'open', rank: 99, done: false, labels: [], custom: {} },
			],
		);

		it('a local delete stays out of the total the count reports', async () => {
			const q: Query<Issue> = {
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
				limit: 1,
			};
			const { scanned, indexed } = await both(q, undefined, [ 'i1' ]);
			expect(indexed).toEqual(scanned);
			expect(scanned.rows.map((r) => r.key)).toEqual([ 'i2' ]);
			expect(scanned.total).toBe(2);
		});

		it('answers the remainder with a count, not a walk, through a pending overlay', async () => {
			// The differential cases above stay green whichever path answers; this
			// is the receipt that the shortcut actually fired — without it a typo
			// in countAssist would quietly put every overlay read back on the walk.
			const kv = new MemoryKVStore();
			const indexes = new IndexCache(registry);
			await seedConfirmed(kv, indexes);
			const mutex = new ChainMutex();
			await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
			const client = new SyncClient({
				workspaceId: WS,
				kvStore: kv,
				entities: [ IssueSchema ],
				streamEnabled: false,
				pollIntervalMs: 0,
			}) as unknown as SyncClient<never>;
			try {
				await client.mutate.put(IssueSchema, {
					id: 'i1',
					title: 'one',
					state: 'wip',
					rank: 5,
					done: false,
					labels: [ 'ui' ],
					custom: {},
				} as never);
				const tx = await kv.read();
				let counted = 0;
				const counting = new Proxy(tx, {
					get(target, prop, receiver) {
						if (prop === 'indexCount') {
							return (lo?: string, hi?: string) => {
								counted++;
								return target.indexCount(lo, hi);
							};
						}
						const value = Reflect.get(target, prop, receiver);
						return typeof value === 'function' ? value.bind(target) : value;
					},
				});
				try {
					const ask = (q: Query<Issue>) =>
						queryByIndex(
							counting,
							registry,
							registry.get('issue')!,
							IssueSchema,
							indexes.for('issue'),
							prepareQuery(IssueSchema, compileQuery(q)),
							WS,
						);
					const page = await ask({
						where: (i) => i.state.eq('open'),
						orderBy: (i) => [ i.rank.desc() ],
						limit: 1,
					});
					expect(counted).toBe(1);
					expect(page?.rows.map((r) => r.row.key)).toEqual([ 'i2' ]);
					expect(page?.total).toBe(2);
					expect(page?.hasMore).toBe(true);

					// total: false owes no count at all — same page, same hasMore,
					// no indexCount request and no total.
					const uncounted = await ask({
						where: (i) => i.state.eq('open'),
						orderBy: (i) => [ i.rank.desc() ],
						limit: 1,
						total: false,
					});
					expect(counted).toBe(1);
					expect(uncounted?.rows.map((r) => r.row.key)).toEqual([ 'i2' ]);
					expect(uncounted?.total).toBeUndefined();
					expect(uncounted?.hasMore).toBe(true);
				} finally {
					tx.release();
				}
			} finally {
				await client.close();
			}
		});
	});

	describe('with the total opted out', () => {
		// `total: false` changes what the result reports, never which rows: the
		// differential still compares the two arms whole, and both must land on
		// an undefined total.
		same(
			'returns the same rows and no total',
			{ where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ], limit: 1, total: false },
			[ 'i2' ],
		);

		same(
			'through a pending overlay',
			{ where: (i) => i.state.eq('open'), orderBy: (i) => [ i.rank.desc() ], limit: 1, total: false },
			[ 'i2' ],
			[
				{ id: 'i1', title: 'one', state: 'wip', rank: 5, done: false, labels: [ 'ui' ], custom: {} },
			],
		);

		it('reports an exact hasMore without counting', async () => {
			const client = await clientWith(true);
			try {
				const q: Query<Issue> = {
					where: (i) => i.state.eq('open'),
					orderBy: (i) => [ i.rank.desc() ],
					limit: 2,
					total: false,
				};
				const first = await run(client, q);
				expect(first.map((r) => r.key)).toEqual([ 'i2', 'i1' ]);
				expect(first.total).toBeUndefined();
				expect(first.hasMore).toBe(true);
				const rest = await run(client, { ...q, after: first.endCursor });
				expect(rest.map((r) => r.key)).toEqual([ 'i4' ]);
				expect(rest.total).toBeUndefined();
				expect(rest.hasMore).toBe(false);
			} finally {
				await client.close();
			}
		});
	});

	describe('keyset paging', () => {
		// by_state_rank is [state, rank desc], so `state == open` pins the
		// leading component and `rank desc` is served by the one after it — the
		// shape that makes a plan ordered, which is the only shape the seek
		// applies to. What the seek must not change is the answer, so both arms
		// walk the same pages.
		const open: Query<Issue> = {
			where: (i) => i.state.eq('open'),
			orderBy: (i) => [ i.rank.desc() ],
			limit: 2,
		};

		/** Every page of `open`, walked forward with cursors. */
		async function pageAll(built: boolean): Promise<string[]> {
			const client = await clientWith(built);
			try {
				const keys: string[] = [];
				let after: QueryCursor | undefined;
				for (;;) {
					const page = await run(client, { ...open, after });
					keys.push(...page.map((r) => r.key));
					if (!page.hasMore) break;
					after = page.endCursor;
				}
				return keys;
			} finally {
				await client.close();
			}
		}

		it('walks the same pages from the index as from the scan', async () => {
			expect(await pageAll(true)).toEqual(await pageAll(false));
			expect(await pageAll(true)).toEqual([ 'i2', 'i1', 'i4' ]);
		});

		it('pages backward the same way from both', async () => {
			const back = async (built: boolean) => {
				const client = await clientWith(built);
				try {
					const last = await run(client, { ...open, offset: 1 });
					const prev = await run(client, { ...open, before: last.startCursor });
					return { last: last.map((r) => r.key), prev: prev.map((r) => r.key) };
				} finally {
					await client.close();
				}
			};
			expect(await back(true)).toEqual(await back(false));
			expect(await back(true)).toEqual({ last: [ 'i1', 'i4' ], prev: [ 'i2' ] });
		});

		it('still serves the query from the index once a cursor is set', async () => {
			// Without this the parity above would hold trivially, with the index
			// path quietly declining every cursor query.
			const client = await clientWith(true);
			let after: QueryCursor | undefined;
			try {
				after = (await run(client, open)).endCursor;
			} finally {
				await client.close();
			}
			expect(await served(true, { ...open, after })).toBe(true);
		});
	});
});

/**
 * Type-level pins for the total opt-out: `total: false` in the query literal
 * types the result's total as undefined, and its absence keeps `number`.
 * Checked by `tsc --noEmit`, never executed.
 */
async function _totalTypes(client: SyncClient<never>): Promise<void> {
	const counted = await client.query(IssueSchema, { limit: 1 });
	const countedTotal: number = counted.total;
	const uncounted = await client.query(IssueSchema, { limit: 1, total: false });
	const uncountedTotal: undefined = uncounted.total;
	// @ts-expect-error the total was opted out, so it is not a number
	const wrong: number = uncounted.total;
	client.watchQuery(IssueSchema, { limit: 1, total: false }, (rows) => {
		const watched: undefined = rows.total;
		void watched;
	});
	void [ countedTotal, uncountedTotal, wrong ];
}
void _totalTypes;
