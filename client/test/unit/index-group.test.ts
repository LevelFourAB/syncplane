// A grouped read is served two ways — a skip-scan over an index, and a bucketed
// scan of the merged view — and the two must agree group for group and count
// for count. So every case here runs twice against identical data, once with
// the index built and once without, and compares the two.
//
// That is the whole correctness argument, the same one index-query.test.ts
// makes for rows: the scan is the definition, the skip-scan is an optimization
// over it, and the planner is free to decline anything as long as it never
// answers differently.

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
import { groupByIndex, type IndexReport } from '../../src/query/execute.js';
import { compileGroupQuery, type Group, type GroupQuery } from '../../src/query/index.js';
import { prepareGroupQuery } from '../../src/query/wire.js';
import {
	defineSchema,
	double,
	entity,
	type Infer,
	int32,
	list,
	map,
	message,
	text,
} from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

const issue = entity('issue', {
	id: text().key(),
	title: text(),
	state: text().indexed(),
	rank: int32(),
	score: double(),
	labels: list(text()).indexed(),
	custom: map('string', message({ number: int32(), tag: text() })),
}, {
	indexes: [
		{ name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] },
		{ name: 'by_label_rank', key: [ { path: 'labels' }, { path: 'rank', desc: true } ] },
		{ name: 'by_rank_desc', key: [ { path: 'rank', desc: true } ] },
		{
			name: 'cf_number',
			key: [ { path: 'custom.*.key' }, { path: 'custom.*.value.number' } ],
		},
	],
});

type Issue = Infer<typeof issue>;
const bundle = defineSchema(issue);
const IssueSchema = bundle.descMessage('issue');
const registry = new EntityRegistry([ IssueSchema ]);
const WS = 'ws1';

const SEED: Issue[] = [
	{
		id: 'i1',
		title: 'one',
		state: 'open',
		rank: 5,
		score: 1.5,
		labels: [ 'bug', 'ui' ],
		custom: { priority: { number: 5, tag: '' } },
	},
	{ id: 'i2', title: 'two', state: 'open', rank: 9, score: 0, labels: [ 'ui' ], custom: {} },
	{
		id: 'i3',
		title: 'three',
		state: 'done',
		rank: 3,
		score: Number.NaN,
		labels: [ 'bug' ],
		custom: { priority: { number: 1, tag: '' } },
	},
	// No labels at all: the row belongs to the no-value group rather than to
	// none, which is what keeps a facet's counts covering every matching row.
	{ id: 'i4', title: 'four', state: 'open', rank: 1, score: 2, labels: [], custom: {} },
	// A duplicate element collapses to one entry, so the row contributes once.
	{
		id: 'i5',
		title: 'five',
		state: 'done',
		rank: 9,
		score: 1.5,
		labels: [ 'ui', 'ui' ],
		custom: { priority: { number: 5, tag: '' } },
	},
	// Implicit-presence empty string: a group of its own, never conflated with
	// the no-value group.
	{ id: 'i6', title: 'six', state: '', rank: 4, score: 0, labels: [ 'bug' ], custom: {} },
];

/** Plants rows as CONFIRMED state — the only state an index covers. */
async function seedConfirmed(kv: KVStore, indexes: IndexCache): Promise<void> {
	const tx = await kv.write();
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

async function clientWith(built: boolean, overlay?: Issue[], deletes?: string[]) {
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
	for (const row of overlay ?? []) await client.mutate.put(IssueSchema, row as never);
	for (const key of deletes ?? []) await client.mutate.del(IssueSchema, key);
	return client;
}

const byState: GroupQuery<Issue> = { by: (i) => i.state };
/** A path nothing can be grouped by: a whole map identifies no value. */
const byCustom = { by: (i: { custom: unknown }) => i.custom };

/** Groups as comparable plain data; `undefined` is the no-value group. */
const shape = (groups: Group[]) => groups.map((g) => [ g.value, g.count ] as const);

/** The two arms of one grouped read, which must be identical. */
async function both(q: GroupQuery<Issue>, overlay?: Issue[], deletes?: string[]) {
	const scanned = await clientWith(false, overlay, deletes);
	const indexed = await clientWith(true, overlay, deletes);
	try {
		return {
			scanned: shape(await scanned.groupCount(IssueSchema, q as never)),
			indexed: shape(await indexed.groupCount(IssueSchema, q as never)),
		};
	} finally {
		await scanned.close();
		await indexed.close();
	}
}

/** Asserts the two arms agree, and returns what they agreed on. */
async function agree(q: GroupQuery<Issue>, overlay?: Issue[], deletes?: string[]) {
	const { scanned, indexed } = await both(q, overlay, deletes);
	expect(indexed).toEqual(scanned);
	return scanned;
}

describe('grouped reads', () => {
	it('counts by a leading indexed component', async () => {
		expect(await agree({ by: (i) => i.state })).toEqual([
			[ '', 1 ],
			[ 'done', 2 ],
			[ 'open', 3 ],
		]);
	});

	it('counts a repeated path once per distinct element', async () => {
		// i5 lists "ui" twice and contributes one; i4 has no labels at all and
		// lands in the no-value group.
		expect(await agree({ by: (i) => i.labels })).toEqual([
			[ 'bug', 3 ],
			[ 'ui', 3 ],
			[ undefined, 1 ],
		]);
	});

	it('orders real values before NaN before the no-value group', async () => {
		// `score` is implicit-presence, so nothing here is missing — the NaN is
		// what has to sort last.
		expect(await agree({ by: (i) => i.score })).toEqual([
			[ 0, 2 ],
			[ 1.5, 2 ],
			[ 2, 1 ],
			[ Number.NaN, 1 ],
		]);
	});

	it('counts under a predicate the range answers whole', async () => {
		expect(await agree({ by: (i) => i.rank, where: (i) => i.state.eq('open') })).toEqual([
			[ 1, 1 ],
			[ 5, 1 ],
			[ 9, 1 ],
		]);
	});

	it('counts under a residual the range cannot express', async () => {
		expect(await agree({ by: (i) => i.state, where: (i) => i.title.startsWith('t') })).toEqual([
			[ 'done', 1 ],
			[ 'open', 1 ],
		]);
	});

	it('reads groups in ascending order off a DESCENDING component', async () => {
		expect(await agree({ by: (i) => i.rank })).toEqual([
			[ 1, 1 ],
			[ 3, 1 ],
			[ 4, 1 ],
			[ 5, 1 ],
			[ 9, 2 ],
		]);
	});

	it('groups a map entry addressed by key', async () => {
		expect(await agree({ by: (i) => i.custom.at('priority').number })).toEqual([
			[ 1, 1 ],
			[ 5, 2 ],
			[ undefined, 3 ],
		]);
	});

	it('caps the groups returned at `limit`, from the first in value order', async () => {
		expect(await agree({ by: (i) => i.state, limit: 2 })).toEqual([
			[ '', 1 ],
			[ 'done', 2 ],
		]);
	});

	it('reports no group for a value no matching row holds', async () => {
		// The group DOMAIN is the app's; a read over rows can only report the
		// values that are there.
		expect(await agree({ by: (i) => i.state, where: (i) => i.title.eq('nothing') })).toEqual([]);
	});

	it('counts the optimistic overlay, not the confirmed rows it shadows', async () => {
		const moved: Issue[] = [ { ...SEED[0]!, state: 'done' } ];
		expect(await agree({ by: (i) => i.state }, moved)).toEqual([
			[ '', 1 ],
			[ 'done', 3 ],
			[ 'open', 2 ],
		]);
	});

	it('drops a locally deleted row from every group', async () => {
		expect(await agree({ by: (i) => i.state }, [], [ 'i2' ])).toEqual([
			[ '', 1 ],
			[ 'done', 2 ],
			[ 'open', 2 ],
		]);
	});

	it('names the distinct values, with no counts', async () => {
		const client = await clientWith(true);
		try {
			expect(await client.distinct(IssueSchema, byState as never)).toEqual([ '', 'done', 'open' ]);
		} finally {
			await client.close();
		}
	});

	// Every case above compares two arms, which would pass just as happily if
	// the index arm declined every time and both ran the scan. These pin that it
	// does not — and that the shapes it declines are the ones it should.
	describe('the walk the two arms are meant to differ by', () => {
		const served = async (q: GroupQuery<Issue>): Promise<string | undefined> => {
			const kv = new MemoryKVStore();
			const indexes = new IndexCache(registry);
			await seedConfirmed(kv, indexes);
			const mutex = new ChainMutex();
			await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
			const compiled = prepareGroupQuery(IssueSchema, compileGroupQuery(q as never, false));
			const tx = await kv.read();
			try {
				const report: IndexReport = { candidates: 0 };
				await groupByIndex(
					tx,
					registry,
					registry.bySchema(IssueSchema),
					IssueSchema,
					indexes.for('issue'),
					compiled,
					compiled.multi,
					WS,
					report,
				);
				return report.index;
			} finally {
				tx.release();
			}
		};

		it('partitions on a leading component', async () => {
			expect(await served({ by: (i) => i.state })).toBe('by_state_rank');
			expect(await served({ by: (i) => i.labels })).toBe('by_label_rank');
			expect(await served({ by: (i) => i.rank })).toBe('by_rank_desc');
		});

		it('partitions on the component after the equality prefix', async () => {
			// The value component only partitions once the KEY component beside it
			// is pinned; unpinned it spans that value under every custom field.
			expect(
				await served({
					by: (i) => i.custom.at('priority').number,
					where: (i) => i.custom.at('priority').isSet(),
				}),
			).toBe('cf_number');
			expect(await served({ by: (i) => i.custom.at('priority').number })).toBeUndefined();
		});

		it('declines a path no index leads with', async () => {
			expect(await served({ by: (i) => i.title })).toBeUndefined();
		});

		it('declines while anything is in the optimistic overlay', async () => {
			// The overlay merge would mean subtracting each shadowed row's
			// positions from whichever group they fall in; the scan does it for
			// free.
			const client = await clientWith(true, [ { ...SEED[0]!, state: 'done' } ]);
			try {
				expect(shape(await client.groupCount(IssueSchema, byState as never)))
					.toEqual([ [ '', 1 ], [ 'done', 3 ], [ 'open', 2 ] ]);
			} finally {
				await client.close();
			}
		});
	});

	it('rejects a path nothing can be grouped by', async () => {
		const client = await clientWith(true);
		try {
			await expect(client.groupCount(IssueSchema, byCustom as never))
				.rejects.toThrow(/not comparable/);
		} finally {
			await client.close();
		}
	});
});
