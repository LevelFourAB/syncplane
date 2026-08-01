/**
 * What a secondary index buys a local query, and what it costs the write path.
 *
 * The Go mirror is engine/server/index_bench_test.go and measures the same
 * trade at the same sizes: a selective read against a growing entity, and the
 * per-write maintenance that pays for it. Which side a cost lands on is often
 * the only thing that differs between the two.
 *
 * Sizes are the number of confirmed rows; a FIXED number of them match.
 *
 * WHAT THESE NUMBERS DO AND DO NOT SHOW. They run on MemoryKVStore, whose scans
 * materialize and sort every key in the store on every call — `scanRange` there
 * is O(store), not O(range). So what is measured is the decode and evaluation
 * the index avoids (a steady ~6x), NOT the range read itself. The asymptotic
 * win — O(result) instead of O(entity) — needs a backend whose ranges are real,
 * which IndexedDB's `IDBKeyRange.bound` is and the memory store is not. Read
 * these as a floor on the benefit, and read the shape off
 * engine/server/index_bench_test.go, which runs against SQLite.
 *
 * The write side is the honest other half: maintenance re-encodes and re-stores
 * a row's whole entry set on every confirmed write, and it shows.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench
 */

import { create, toBinary } from '@bufbuild/protobuf';
import { bench, describe } from 'vitest';

import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { runQuery } from '../../src/query/evaluate.js';
import { queryByIndex } from '../../src/query/execute.js';
import { compileQuery } from '../../src/query/index.js';
import { prepareQuery } from '../../src/query/wire.js';
import { scanDecoded } from '../../src/read.js';
import { entity, type Infer, int32, text } from '../../src/schema/builder.js';
import { defineSchema } from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

const SIZES = [ 1000, 10000, 50000 ];
const MATCHES = 10;
const WS = 'ws1';

const issue = entity('issue', {
	id: text().key(),
	title: text(),
	state: text().indexed(),
	rank: int32(),
}, {
	indexes: [ { name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] } ],
});

type Issue = Infer<typeof issue>;
const IssueSchema = defineSchema(issue).descMessage('issue');
const registry = new EntityRegistry([ IssueSchema ]);

const QUERY = prepareQuery(
	IssueSchema,
	compileQuery<Issue>({
		where: (i) => i.state.eq('hit'),
		orderBy: (i) => [ i.rank.desc() ],
		limit: 10,
	}),
);

/** A store of n confirmed rows, MATCHES of which the query selects. */
async function seeded(n: number, built: boolean): Promise<{ kv: KVStore; indexes: IndexCache }> {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	// Batched: one transaction per 1000 rows keeps the memory store's snapshot
	// copying from dominating setup at 50k.
	for (let start = 0; start < n; start += 1000) {
		const tx = await kv.write();
		const changes = [];
		for (let i = start; i < Math.min(start + 1000, n); i++) {
			// The matching rows are spread across the key space rather than
			// clustered, so the scan cannot get lucky and stop early.
			const state = i % Math.floor(n / MATCHES) === 0 ? 'hit' : 'miss';
			changes.push(create(RowChangeSchema, {
				entity: 'issue',
				key: `i${String(i).padStart(7, '0')}`,
				version: BigInt(i + 1),
				body: {
					case: 'value',
					value: toBinary(
						IssueSchema,
						create(IssueSchema, { id: `i${i}`, title: 'issue', state, rank: i % 1000 }),
					),
				},
			}));
		}
		await applyServerChanges(tx, registry, WS, changes, new TouchSet(), indexes);
		await tx.commit();
	}
	if (built) {
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	}
	return { kv, indexes };
}

for (const n of SIZES) {
	describe(`local query over ${n} rows`, () => {
		let scanStore: Awaited<ReturnType<typeof seeded>>;
		let indexStore: Awaited<ReturnType<typeof seeded>>;

		bench('scan', async () => {
			scanStore ??= await seeded(n, false);
			const tx = await scanStore.kv.read();
			try {
				const def = registry.get('issue')!;
				runQuery(await scanDecoded(tx, registry, def, IssueSchema, WS), QUERY, IssueSchema);
			} finally {
				tx.release();
			}
		});

		bench('index', async () => {
			indexStore ??= await seeded(n, true);
			const tx = await indexStore.kv.read();
			try {
				await queryByIndex(
					tx,
					registry,
					registry.get('issue')!,
					IssueSchema,
					indexStore.indexes.for('issue'),
					QUERY,
					WS,
				);
			} finally {
				tx.release();
			}
		});
	});
}

describe('confirmed row write', () => {
	// The other half of the trade: maintenance runs on every confirmed row
	// write, so the read speed above is paid for here.
	const change = (i: number) =>
		create(RowChangeSchema, {
			entity: 'issue',
			key: `i${String(i).padStart(7, '0')}`,
			version: BigInt(i + 1),
			body: {
				case: 'value',
				value: toBinary(
					IssueSchema,
					create(IssueSchema, { id: `i${i}`, title: 'issue', state: 'hit', rank: i }),
				),
			},
		});

	let plain = 0;
	bench('without index maintenance', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		await applyServerChanges(tx, registry, WS, [ change(plain++) ], new TouchSet());
		await tx.commit();
	});

	let maintained = 0;
	bench('with index maintenance', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const tx = await kv.write();
		await applyServerChanges(tx, registry, WS, [ change(maintained++) ], new TouchSet(), indexes);
		await tx.commit();
	});
});
