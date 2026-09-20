// A grouped ROW read is served two ways — a skip-scan that reads each group's
// head straight off an index, and a scan that buckets the merged view and sorts
// every bucket — and the two must agree group for group and row for row. So
// every case here runs twice against identical data, once with the index built
// and once without, and compares the two.
//
// Same argument as index-group.test.ts, one step further: the scan is the
// definition, the walk is an optimization over it, and the planner may decline
// anything as long as it never answers differently.

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
import {
	groupRowsByIndex,
	type IndexReport,
	withIndexCandidateBudget,
} from '../../src/query/execute.js';
import {
	and,
	compileGroupRowsQuery,
	type GroupRowsQuery,
	type RowGroup,
} from '../../src/query/index.js';
import { prepareGroupRowsQuery } from '../../src/query/wire.js';
import { defineSchema, entity, type Infer, int32, list, text } from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

const issue = entity('issue', {
	id: text().key(),
	title: text().searchable(),
	state: text().indexed(),
	rank: int32(),
	labels: list(text()).indexed(),
}, {
	indexes: [
		{ name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] },
		{ name: 'by_label_rank', key: [ { path: 'labels' }, { path: 'rank', desc: true } ] },
	],
});

type Issue = Infer<typeof issue>;
const bundle = defineSchema(issue);
const IssueSchema = bundle.descMessage('issue');
const registry = new EntityRegistry([ IssueSchema ]);
const WS = 'ws1';

const SEED: Issue[] = [
	{ id: 'i1', title: 'one', state: 'open', rank: 5, labels: [ 'bug', 'ui' ] },
	{ id: 'i2', title: 'two', state: 'open', rank: 9, labels: [ 'ui' ] },
	{ id: 'i3', title: 'three', state: 'done', rank: 3, labels: [ 'bug' ] },
	// No labels at all: one group of its own, which a board column for
	// "unlabelled" is exactly.
	{ id: 'i4', title: 'four', state: 'open', rank: 1, labels: [] },
	// Ties with i2 on rank, so the row key is what orders the two.
	{ id: 'i5', title: 'five', state: 'done', rank: 9, labels: [ 'ui' ] },
	{ id: 'i6', title: 'six', state: '', rank: 4, labels: [ 'bug' ] },
];

/** Plants rows as CONFIRMED state — the only state an index covers. */
async function seedConfirmed(
	kv: KVStore,
	indexes: IndexCache,
	rows: Issue[] = SEED,
): Promise<void> {
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		WS,
		rows.map((row, i) =>
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

async function clientWith(built: boolean, overlay?: Issue[], deletes?: string[], rows?: Issue[]) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seedConfirmed(kv, indexes, rows);
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

/** Groups as comparable plain data; `undefined` is the no-value group. */
const shape = (groups: readonly RowGroup<unknown>[]) =>
	groups.map((g) => [ g.value, g.rows.map((r) => r.key).join(','), g.total, g.hasMore ] as const);

/** Asserts the index walk and the scan agree, and returns what they agreed on. */
async function agree(
	q: GroupRowsQuery<Issue>,
	overlay?: Issue[],
	deletes?: string[],
	rows?: Issue[],
) {
	const scanned = await clientWith(false, overlay, deletes, rows);
	const indexed = await clientWith(true, overlay, deletes, rows);
	try {
		const fromScan = shape(await scanned.groupRows(IssueSchema, q as never));
		expect(shape(await indexed.groupRows(IssueSchema, q as never))).toEqual(fromScan);
		return fromScan;
	} finally {
		await scanned.close();
		await indexed.close();
	}
}

// The same pricing groupByIndex applies: a group that owes a total, or a
// residual that can reject any of its rows, means the walk reads every row of
// every group — so the choice between it and the scan is made before the first
// read rather than after a group outruns the ceiling.
describe('pricing a residual walk against the scan', () => {
	const CEILING = 3;

	const BULK: Issue[] = [ [ 'a', 2 ], [ 'm', 6 ], [ 'z', 32 ] ].flatMap(([ state, n ], group) =>
		Array.from({ length: n as number }, (_, i) => ({
			id: `${state as string}${i}`,
			title: `t-${state as string}-${i}`,
			state: state as string,
			rank: group * 100 + i,
			labels: [],
		}))
	);

	/** What groupRowsByIndex reports about one read of the bulk seed. */
	async function receipt(q: GroupRowsQuery<Issue>): Promise<IndexReport> {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await seedConfirmed(kv, indexes, BULK);
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		const compiled = prepareGroupRowsQuery(IssueSchema, compileGroupRowsQuery(q as never));
		const tx = await kv.read();
		const report: IndexReport = { candidates: 0 };
		try {
			await groupRowsByIndex(
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
		} finally {
			tx.release();
		}
		return report;
	}

	it('walks past the ceiling when the range is a minority of the index', async () => {
		const restore = withIndexCandidateBudget(CEILING);
		try {
			const q: GroupRowsQuery<Issue> = {
				by: (i) => i.rank,
				where: (i) => and(i.state.eq('m'), i.title.startsWith('t')),
				perGroup: 1,
			};
			const report = await receipt(q);
			expect(report.declined).toBeUndefined();
			expect(report.index).toBe('by_state_rank');
			expect(report.candidates).toBe(6);
			expect(await agree(q, [], [], BULK)).toEqual(
				[ 0, 1, 2, 3, 4, 5 ].map((i) => [ 100 + i, `m${i}`, 1, false ]),
			);
		} finally {
			restore();
		}
	});

	it('falls to the scan without reading a row when the range is the whole index', async () => {
		const restore = withIndexCandidateBudget(CEILING);
		try {
			const q: GroupRowsQuery<Issue> = {
				by: (i) => i.state,
				where: (i) => i.title.startsWith('t'),
				orderBy: (i) => [ i.rank.desc() ],
				perGroup: 1,
			};
			const report = await receipt(q);
			expect(report.declined).toBe('budget');
			expect(report.candidates).toBe(0);
			expect(await agree(q, [], [], BULK)).toEqual([
				[ 'a', 'a1', 2, true ],
				[ 'm', 'm5', 6, true ],
				[ 'z', 'z31', 32, true ],
			]);
		} finally {
			restore();
		}
	});
});

describe('grouped row reads', () => {
	it('returns each group head in the requested order', async () => {
		expect(
			await agree({ by: (i) => i.state, orderBy: (i) => [ i.rank.desc() ], perGroup: 2 }),
		).toEqual([
			[ '', 'i6', 1, false ],
			[ 'done', 'i5,i3', 2, false ],
			[ 'open', 'i2,i1', 3, true ],
		]);
	});

	it('breaks a tie inside a group on the row key', async () => {
		expect(
			await agree({ by: (i) => i.labels, orderBy: (i) => [ i.rank.desc() ], perGroup: 2 }),
		).toEqual([
			[ 'bug', 'i1,i6', 3, true ],
			[ 'ui', 'i2,i5', 3, true ],
			[ undefined, 'i4', 1, false ],
		]);
	});

	it('orders by the row key when nothing else is asked for', async () => {
		expect(await agree({ by: (i) => i.state, perGroup: 2 })).toEqual([
			[ '', 'i6', 1, false ],
			[ 'done', 'i3,i5', 2, false ],
			[ 'open', 'i1,i2', 3, true ],
		]);
	});

	it('drops the total when the query opts out of counting', async () => {
		expect(
			await agree({
				by: (i) => i.state,
				orderBy: (i) => [ i.rank.desc() ],
				perGroup: 1,
				total: false,
			}),
		).toEqual([
			[ '', 'i6', undefined, false ],
			[ 'done', 'i5', undefined, true ],
			[ 'open', 'i2', undefined, true ],
		]);
	});

	it('takes the head under a predicate the range answers whole', async () => {
		expect(
			await agree({
				by: (i) => i.state,
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.rank.desc() ],
				perGroup: 2,
			}),
		).toEqual([ [ 'open', 'i2,i1', 3, true ] ]);
	});

	it('takes the head under a residual the range cannot express', async () => {
		expect(
			await agree({
				by: (i) => i.state,
				where: (i) => i.title.startsWith('t'),
				orderBy: (i) => [ i.rank.desc() ],
				perGroup: 1,
			}),
		).toEqual([
			[ 'done', 'i3', 1, false ],
			[ 'open', 'i2', 1, false ],
		]);
	});

	it('takes the head of an ordering no index serves', async () => {
		// The index orders by rank; this asks for the title, so the walk declines
		// and the scan answers. Both arms still agree, which is what makes
		// declining free.
		expect(
			await agree({ by: (i) => i.state, orderBy: (i) => [ i.title.asc() ], perGroup: 1 }),
		).toEqual([
			[ '', 'i6', 1, false ],
			[ 'done', 'i5', 2, true ],
			[ 'open', 'i4', 3, true ],
		]);
	});

	it('ranks the rows of a group by relevance', async () => {
		// The rank binds to the predicate's match condition, as it does in a row
		// query — no index can supply it, so this is the scan on both arms.
		expect(
			await agree({
				by: (i) => i.state,
				where: (i) => i.match('t'),
				orderBy: (i) => [ i.relevance() ],
				perGroup: 2,
			}),
		).toEqual([
			[ 'done', 'i3', 1, false ],
			[ 'open', 'i2', 1, false ],
		]);
	});

	it('caps the groups returned at `limit`, from the first in value order', async () => {
		expect(
			await agree({
				by: (i) => i.state,
				orderBy: (i) => [ i.rank.desc() ],
				perGroup: 1,
				limit: 2,
			}),
		).toEqual([
			[ '', 'i6', 1, false ],
			[ 'done', 'i5', 2, true ],
		]);
	});

	it('reports no group for a value no matching row holds', async () => {
		expect(
			await agree({ by: (i) => i.state, where: (i) => i.title.eq('nothing'), perGroup: 3 }),
		).toEqual([]);
	});

	it('reads the optimistic overlay, not the confirmed rows it shadows', async () => {
		const moved: Issue[] = [ { ...SEED[1]!, state: 'done' } ];
		expect(
			await agree({ by: (i) => i.state, orderBy: (i) => [ i.rank.desc() ], perGroup: 1 }, moved),
		).toEqual([
			[ '', 'i6', 1, false ],
			[ 'done', 'i2', 3, true ],
			[ 'open', 'i1', 2, true ],
		]);
	});

	it('drops a locally deleted row from every group', async () => {
		expect(
			await agree(
				{ by: (i) => i.state, orderBy: (i) => [ i.rank.desc() ], perGroup: 2 },
				[],
				[ 'i2' ],
			),
		).toEqual([
			[ '', 'i6', 1, false ],
			[ 'done', 'i5,i3', 2, false ],
			[ 'open', 'i1,i4', 2, false ],
		]);
	});

	// Every case above compares two arms, which would pass just as happily if the
	// index arm declined every time and both ran the scan. These pin that it does
	// not — and that the shapes it declines are the ones it should.
	describe('the walk the two arms are meant to differ by', () => {
		const served = async (q: GroupRowsQuery<Issue>): Promise<string | undefined> => {
			const kv = new MemoryKVStore();
			const indexes = new IndexCache(registry);
			await seedConfirmed(kv, indexes);
			const mutex = new ChainMutex();
			await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
			const compiled = prepareGroupRowsQuery(IssueSchema, compileGroupRowsQuery(q as never));
			const tx = await kv.read();
			try {
				const report: IndexReport = { candidates: 0 };
				await groupRowsByIndex(
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

		it('serves the head from the components past the partition', async () => {
			expect(
				await served({ by: (i) => i.state, orderBy: (i) => [ i.rank.desc() ], perGroup: 2 }),
			).toBe('by_state_rank');
			expect(
				await served({ by: (i) => i.labels, orderBy: (i) => [ i.rank.desc() ], perGroup: 2 }),
			).toBe('by_label_rank');
		});

		it('declines an unordered read while a component sits past the partition', async () => {
			// An unordered read wants row-key order, which a position ends with —
			// but inside a group of `by_state_rank` the rank comes first. An index
			// stopping at `state` would serve it, and declaring one is pointless:
			// being a prefix of `by_state_rank`, it is dropped as redundant.
			expect(await served({ by: (i) => i.state, perGroup: 2 })).toBeUndefined();
		});

		it('declines an ordering the stored components do not supply', async () => {
			// The component stores rank DESCENDING, so a forward walk of a group
			// yields the last rows of an ascending ordering first.
			expect(await served({ by: (i) => i.state, orderBy: (i) => [ i.rank.asc() ], perGroup: 2 }))
				.toBeUndefined();
			expect(await served({ by: (i) => i.state, orderBy: (i) => [ i.title.asc() ], perGroup: 2 }))
				.toBeUndefined();
		});

		it('declines a path no index leads with', async () => {
			expect(await served({ by: (i) => i.title, perGroup: 2 })).toBeUndefined();
		});

		it('declines while anything is in the optimistic overlay', async () => {
			const client = await clientWith(true, [ { ...SEED[0]!, state: 'done' } ]);
			try {
				expect(
					shape(
						await client.groupRows(
							IssueSchema,
							{ by: (i: Issue) => i.state, perGroup: 1 } as never,
						),
					),
				).toEqual([
					[ '', 'i6', 1, false ],
					[ 'done', 'i1', 3, true ],
					[ 'open', 'i2', 2, true ],
				]);
			} finally {
				await client.close();
			}
		});
	});

	it('rejects a path nothing can be grouped by', async () => {
		const client = await clientWith(true);
		try {
			await expect(
				client.groupRows(IssueSchema, { by: (i: Issue) => i.labels[0], perGroup: 1 } as never),
			).rejects.toThrow();
		} finally {
			await client.close();
		}
	});
});
