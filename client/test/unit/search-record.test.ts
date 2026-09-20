// The search-index record (`x/<entity>/<ws>/<key>`): a row's term postings,
// kept beside the row rather than on it so a write that leaves the searchable
// text alone leaves the postings alone.
//
// Two properties are load-bearing and neither is visible in a query result. A
// rewrite that does not change the terms must not touch the record — the whole
// point of splitting it off — and every path that removes a row must remove it,
// since a posting outliving its row would answer for a row nothing can read.
// Both are asserted against the keys the store is actually asked to write.

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import {
	metaCursor,
	metaSnapshotGen,
	searchIndexKey,
	type SearchIndexRecord,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes, markIndexesBuilt } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import {
	applyBootstrapRows,
	applyServerChanges,
	wipeConfirmedState,
} from '../../src/optimistic/rebase.js';
import type { Query } from '../../src/query/index.js';
import { defineSchema, entity, type Infer, text } from '../../src/schema/builder.js';
import type { QueryEvent } from '../../src/sync/events.js';
import { TouchSet } from '../../src/touch.js';

const article = entity('article', {
	id: text().key(),
	title: text().searchable(),
	state: text(),
});

type Article = Infer<typeof article>;
const bundle = defineSchema(article);
const ArticleSchema = bundle.descMessage('article');
const registry = new EntityRegistry([ ArticleSchema ]);
const WS = 'ws1';

const change = (id: string, title: string, state: string, version: bigint) =>
	create(RowChangeSchema, {
		entity: 'article',
		key: id,
		version,
		body: {
			case: 'value',
			value: toBinary(ArticleSchema, create(ArticleSchema, { id, title, state })),
		},
	});

const tombstone = (id: string, version: bigint) =>
	create(RowChangeSchema, {
		entity: 'article',
		key: id,
		version,
		body: { case: 'tombstone', value: true },
	});

/** Records every key the store is asked to put or delete, so an assertion can
 * be about the writes rather than about what they left behind. */
function recordingWrites(inner: KVStore): { kv: KVStore; puts: string[]; dels: string[] } {
	const puts: string[] = [];
	const dels: string[] = [];
	const kv: KVStore = {
		read: () => inner.read(),
		write: async () => {
			const tx = await inner.write();
			const put = tx.put.bind(tx);
			const del = tx.del.bind(tx);
			tx.put = (k, v) => {
				puts.push(k);
				return put(k, v);
			};
			tx.del = (k) => {
				dels.push(k);
				return del(k);
			};
			return tx;
		},
		close: () => inner.close(),
	};
	return { kv, puts, dels };
}

async function apply(kv: KVStore, indexes: IndexCache, changes: ReturnType<typeof change>[]) {
	const tx = await kv.write();
	await applyServerChanges(tx, registry, WS, changes, new TouchSet(), indexes);
	await tx.commit();
}

async function keysUnder(kv: KVStore, prefix: string): Promise<string[]> {
	const out: string[] = [];
	const tx = await kv.read();
	for await (const [ k ] of tx.scan(prefix)) out.push(k);
	tx.release();
	return out;
}

async function searchRecord(kv: KVStore, id: string): Promise<SearchIndexRecord | undefined> {
	const tx = await kv.read();
	const record = await tx.get(searchIndexKey('article', WS, id)) as SearchIndexRecord | undefined;
	tx.release();
	return record;
}

const xKey = (id: string) => searchIndexKey('article', WS, id);

describe('the search-index record', () => {
	it('is left alone by a change that does not move the terms', async () => {
		const { kv, puts } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		expect(puts).toContain(xKey('a1'));
		const before = await searchRecord(kv, 'a1');

		puts.length = 0;
		await apply(kv, indexes, [ change('a1', 'milk cow', 'live', 2n) ]);
		expect(puts).not.toContain(xKey('a1'));
		expect(puts).toContain('c/article/ws1/a1');
		expect(await searchRecord(kv, 'a1')).toEqual(before);
	});

	it('is left alone when the same terms arrive in another order', async () => {
		const { kv, puts } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);

		puts.length = 0;
		await apply(kv, indexes, [ change('a1', 'cow milk', 'draft', 2n) ]);
		expect(puts).not.toContain(xKey('a1'));
	});

	it('is rewritten by a change that moves the terms', async () => {
		const { kv, puts } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		const before = await searchRecord(kv, 'a1');

		puts.length = 0;
		await apply(kv, indexes, [ change('a1', 'milk goat', 'draft', 2n) ]);
		expect(puts).toContain(xKey('a1'));
		expect(await searchRecord(kv, 'a1')).not.toEqual(before);
	});

	it('follows a row whose searchable text empties out', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		await apply(kv, indexes, [ change('a1', '', 'draft', 2n) ]);
		expect(await searchRecord(kv, 'a1')).toBeUndefined();
	});

	it('is deleted by a tombstone', async () => {
		const { kv, dels } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);

		dels.length = 0;
		const tx = await kv.write();
		await applyServerChanges(tx, registry, WS, [ tombstone('a1', 2n) ], new TouchSet(), indexes);
		await tx.commit();
		expect(dels).toContain(xKey('a1'));
		expect(await searchRecord(kv, 'a1')).toBeUndefined();
		expect(await keysUnder(kv, 'x/')).toEqual([]);
	});

	it('is swept with the confirmed rows of a workspace', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		expect(await keysUnder(kv, 'x/')).not.toEqual([]);

		const tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, new TouchSet());
		await tx.commit();
		expect(await keysUnder(kv, 'x/')).toEqual([]);
		expect(await keysUnder(kv, 'c/')).toEqual([]);
	});
});

/**
 * A search query served by the index, with the built markers claimed rather
 * than walked for — so the answer comes from the postings the seeding path
 * wrote, not from a build that rewrote them.
 */
async function queryServedByIndex(
	kv: KVStore,
	indexes: IndexCache,
	q: Query<Article>,
): Promise<{ keys: string[]; receipt: QueryEvent }> {
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await markIndexesBuilt(tx, indexes, WS, [ 'article' ]);
	await tx.commit();
	return await queryThrough(kv, q);
}

async function queryThrough(
	kv: KVStore,
	q: Query<Article>,
): Promise<{ keys: string[]; receipt: QueryEvent }> {
	const events: QueryEvent[] = [];
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ ArticleSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		onSyncEvent: (e) => {
			if (e.type === 'query') events.push(e);
		},
	}) as unknown as SyncClient<never>;
	try {
		const rows = await client.query(ArticleSchema, q as never) as unknown as RowSet<Article>;
		return { keys: rows.map((r) => r.key), receipt: events.at(-1)! };
	} finally {
		await client.close();
	}
}

const SEED = [
	change('a1', 'milk cow', 'draft', 1n),
	change('a2', 'water', 'live', 2n),
	change('a3', 'milk goat', 'live', 3n),
];

const MILK: Query<Article> = { where: (a) => a.match('milk') };

describe('search queries over the split record', () => {
	it('finds rows written by a pull apply', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, SEED);
		const { keys, receipt } = await queryServedByIndex(kv, indexes, MILK);
		expect(receipt.index).toBe('__search');
		expect(keys).toEqual([ 'a1', 'a3' ]);
	});

	it('finds rows written by a bootstrap', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const tx = await kv.write();
		await applyBootstrapRows(tx, registry, WS, SEED, new TouchSet(), indexes);
		await tx.commit();
		const { keys, receipt } = await queryServedByIndex(kv, indexes, MILK);
		expect(receipt.index).toBe('__search');
		expect(keys).toEqual([ 'a1', 'a3' ]);
	});

	it('finds rows the background build walked', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		// Seeded WITHOUT the index, which is the state a staged bootstrap swap
		// leaves: rows with no positions and no search records at all.
		const seed = await kv.write();
		await applyServerChanges(seed, registry, WS, SEED, new TouchSet());
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(metaSnapshotGen(WS), 1);
		await seed.commit();
		expect(await keysUnder(kv, 'x/')).toEqual([]);

		const mutex = new ChainMutex();
		const built = await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		expect(built.ready).toBe(true);
		expect(await keysUnder(kv, 'x/')).toEqual([ xKey('a1'), xKey('a2'), xKey('a3') ]);

		const { keys, receipt } = await queryThrough(kv, MILK);
		expect(receipt.index).toBe('__search');
		expect(keys).toEqual([ 'a1', 'a3' ]);
	});
});
