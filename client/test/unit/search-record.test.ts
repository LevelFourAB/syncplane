// The search-index record (`x/<scope><key>`): a row's term postings,
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
	indexPositionPrefix,
	legacySearchIndexWsPrefix,
	metaCursor,
	metaIndexBuilt,
	metaSnapshotGen,
	SEARCH_INDEX_ROOT,
	searchIndexKey,
	type SearchIndexRecord,
	searchIndexWsPrefix,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes, markIndexesBuilt } from '../../src/optimistic/index-build.js';
import { IndexCache, searchIndexOf } from '../../src/optimistic/index-maintain.js';
import {
	applyBootstrapRows,
	applyServerChanges,
	dropConfirmedEntities,
	wipeConfirmedState,
} from '../../src/optimistic/rebase.js';
import type { Query } from '../../src/query/index.js';
import { fingerprint } from '../../src/query/indexkey.js';
import { SEARCH_INDEX_NAME, SEARCH_TERMS_PER_ROW } from '../../src/query/searchindex.js';
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

/** Where position layouts up to 3 kept the search record of `id`. */
const legacyKey = (id: string) => `x/article/${WS}/${id}`;

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

/** The prefix every search position of `article` in WS starts with. */
function searchPrefix(indexes: IndexCache): string {
	const idx = searchIndexOf(indexes.for('article'))!;
	return indexPositionPrefix('article', SEARCH_INDEX_NAME, idx.specHash, WS);
}

/** A title with more distinct terms than one row stores postings for. */
const oversized = (tail: string) =>
	Array.from({ length: SEARCH_TERMS_PER_ROW + 20 }, (_, i) => `unique${i}`).join(' ') + tail;

describe('search positions', () => {
	it('carry the term alone, so rows sharing a term share its position', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [
			change('a1', 'milk cow', 'draft', 1n),
			change('a2', 'cow milk', 'live', 2n),
		]);
		const prefix = searchPrefix(indexes);
		const milk = `${prefix}\u0001milk\u0000\u0000`;
		const cow = `${prefix}\u0001cow\u0000\u0000`;
		expect((await searchRecord(kv, 'a1'))!.ix).toEqual([ milk, cow ]);
		expect((await searchRecord(kv, 'a2'))!.ix).toEqual([ cow, milk ]);
	});

	it('store the overflow marker alone for every oversized row', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [
			change('b1', oversized(' needle'), 'draft', 1n),
			change('b2', oversized(''), 'draft', 2n),
		]);
		const overflow = `${searchPrefix(indexes)}\u0002`;
		expect((await searchRecord(kv, 'b1'))!.ix).toEqual([ overflow ]);
		expect((await searchRecord(kv, 'b2'))!.ix).toEqual([ overflow ]);
	});
});

describe('search queries over shared positions', () => {
	const ROWS = [
		change('a1', 'milk cow', 'draft', 1n),
		change('a2', 'cow milk', 'live', 2n),
		change('a3', 'milk cow', 'live', 3n),
		change('a4', 'water', 'live', 4n),
		change('b1', oversized(' needle milk'), 'draft', 5n),
		change('b2', oversized(''), 'draft', 6n),
	];

	async function served(q: Query<Article>): Promise<string[]> {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, ROWS);
		const { keys, receipt } = await queryServedByIndex(kv, indexes, q);
		expect(receipt.index).toBe('__search');
		return keys;
	}

	it('finds every row holding one term', async () => {
		expect(await served({ where: (a) => a.match('milk ') })).toEqual([ 'a1', 'a2', 'a3', 'b1' ]);
	});

	it('finds every row holding all of several terms', async () => {
		expect(await served({ where: (a) => a.match('cow milk ') })).toEqual([ 'a1', 'a2', 'a3' ]);
	});

	it('pages the rows of a shared term in key order', async () => {
		expect(await served({ where: (a) => a.match('cow '), limit: 2 })).toEqual([ 'a1', 'a2' ]);
		expect(await served({ where: (a) => a.match('cow '), offset: 2, limit: 2 })).toEqual([ 'a3' ]);
	});

	it('finds a term past the cap in only the oversized row that holds it', async () => {
		expect(await served({ where: (a) => a.match('needle') })).toEqual([ 'b1' ]);
	});
});

describe('a store written under an older position layout', () => {
	it('is searched by scan until the build rewrites its positions', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const decl = searchIndexOf(indexes.for('article'))!.search!;
		// The previous layout, spelled out rather than derived: what is under
		// test is that positions and markers written by it are not trusted.
		// It hashed `searchv1` into the spec, rooted each prefix at `i/`
		// with 16 hex digits and a slash, and ended every search entry with
		// the escaped, terminated row key.
		const oldSpec = fingerprint(`searchv1|${decl.spec}|cap:${SEARCH_TERMS_PER_ROW}`);
		const oldPrefix = `i/${fingerprint(`2/article/${SEARCH_INDEX_NAME}/${oldSpec}/${WS}`)}/`;
		const oldMarker = `meta/ws/${WS}/index/article/${SEARCH_INDEX_NAME}@${oldSpec}~2`;
		const oldTerm = (term: string, key: string) =>
			`${oldPrefix}\u0001${term}\u0000\u0000${key}\u0000\u0000`;

		const seed = await kv.write();
		await applyServerChanges(seed, registry, WS, SEED, new TouchSet());
		await seed.put(legacyKey('a1'), { ix: [ oldTerm('milk', 'a1'), oldTerm('cow', 'a1') ] });
		await seed.put(legacyKey('a2'), { ix: [ oldTerm('water', 'a2') ] });
		await seed.put(legacyKey('a3'), { ix: [ oldTerm('milk', 'a3'), oldTerm('goat', 'a3') ] });
		await seed.put(oldMarker, true);
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(metaSnapshotGen(WS), 1);
		await seed.commit();

		const before = await queryThrough(kv, MILK);
		expect(before.receipt.index).toBeUndefined();
		expect(before.keys).toEqual([ 'a1', 'a3' ]);

		const mutex = new ChainMutex();
		const built = await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		expect(built).toEqual({ rows: 3, ready: true });

		const prefix = searchPrefix(indexes);
		for (const id of [ 'a1', 'a2', 'a3' ]) {
			const ix = (await searchRecord(kv, id))!.ix;
			expect(ix.every((p) => p.startsWith(prefix) && !p.startsWith(oldPrefix))).toBe(true);
		}
		const after = await queryThrough(kv, MILK);
		expect(after.receipt.index).toBe('__search');
		expect(after.keys).toEqual([ 'a1', 'a3' ]);

		expect(await keysUnder(kv, legacySearchIndexWsPrefix('article', WS))).toEqual([]);
		const r = await kv.read();
		expect(await r.has(oldMarker)).toBe(false);
		expect(
			await r.has(
				metaIndexBuilt(
					WS,
					'article',
					SEARCH_INDEX_NAME,
					searchIndexOf(indexes.for('article'))!.specHash,
				),
			),
		).toBe(true);
		r.release();
	});
});

/** A fingerprint as the 8 characters, one per byte, a position prefix holds. */
function fingerprintBytes(parts: string): string {
	const hex = fingerprint(parts);
	let out = '';
	for (let i = 0; i < hex.length; i += 2) {
		out += String.fromCharCode(Number.parseInt(hex.slice(i, i + 2), 16));
	}
	return out;
}

describe('a store written under position layout 3', () => {
	// Layout 3 composed positions exactly as the current layout does, with a 3
	// hashed in, and kept the search record at `x/<entity>/<ws>/<key>`. Spelled
	// out rather than derived, like the layout 2 case above.
	async function seedLayout3(kv: KVStore, indexes: IndexCache) {
		const spec = searchIndexOf(indexes.for('article'))!.specHash;
		const prefix = fingerprintBytes(`3/article/${SEARCH_INDEX_NAME}/${spec}/${WS}`);
		const term = (t: string) => `${prefix}\u0001${t}\u0000\u0000`;
		const marker = `meta/ws/${WS}/index/article/${SEARCH_INDEX_NAME}@${spec}~3`;
		const seed = await kv.write();
		await applyServerChanges(seed, registry, WS, SEED, new TouchSet());
		await seed.put(legacyKey('a1'), { ix: [ term('milk'), term('cow') ] });
		await seed.put(legacyKey('a2'), { ix: [ term('water') ] });
		await seed.put(legacyKey('a3'), { ix: [ term('milk'), term('goat') ] });
		// A record whose row a tombstone has since removed. Only the build's
		// sweep can find it: no row is left to lead to it.
		await seed.put(legacyKey('a9'), { ix: [ term('milk') ] });
		await seed.put(marker, true);
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(metaSnapshotGen(WS), 1);
		await seed.commit();
		return marker;
	}

	it('is searched by scan until the build, and from the new records after it', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const oldMarker = await seedLayout3(kv, indexes);

		const before = await queryThrough(kv, MILK);
		expect(before.receipt.index).toBeUndefined();
		expect(before.keys).toEqual([ 'a1', 'a3' ]);

		const mutex = new ChainMutex();
		const built = await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		expect(built).toEqual({ rows: 3, ready: true });

		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toEqual([ xKey('a1'), xKey('a2'), xKey('a3') ]);
		const r = await kv.read();
		expect(await r.has(oldMarker)).toBe(false);
		r.release();

		const after = await queryThrough(kv, MILK);
		expect(after.receipt.index).toBe('__search');
		expect(after.keys).toEqual([ 'a1', 'a3' ]);
	});

	it('loses its old records to a wipe of the workspace', async () => {
		const kv = new MemoryKVStore();
		await seedLayout3(kv, new IndexCache(registry));
		const tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, new TouchSet());
		await tx.commit();
		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toEqual([]);
	});

	it('loses its old records to a drop of the entity', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await seedLayout3(kv, indexes);
		const tx = await kv.write();
		await dropConfirmedEntities(tx, WS, [ 'article' ], new TouchSet(), indexes);
		await tx.commit();
		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toEqual([]);
	});
});

describe('the search-index record key', () => {
	it('is the root, an 8-character scope, then the row key verbatim', () => {
		const prefix = searchIndexWsPrefix('article', WS);
		const key = searchIndexKey('article', WS, 'a/1');
		expect(key).toBe(`${prefix}a/1`);
		expect(prefix.startsWith(SEARCH_INDEX_ROOT)).toBe(true);
		const scope = prefix.slice(SEARCH_INDEX_ROOT.length);
		expect(scope).toHaveLength(8);
		expect([ ...scope ].every((c) => c.charCodeAt(0) <= 0xff)).toBe(true);
		// The high bit is what keeps the scope apart from an old `x/<entity>/`
		// key, whose third character is an ASCII entity name. Checked over many
		// pairs, since half of all fingerprints have it set by chance.
		for (let i = 0; i < 32; i++) {
			const p = searchIndexWsPrefix(`e${i}`, `w${i}`);
			expect(p.charCodeAt(SEARCH_INDEX_ROOT.length)).toBeGreaterThanOrEqual(0x80);
		}
	});

	it('scopes pairs whose names share a prefix apart', () => {
		const pairs = [
			[ 'article', 'ws1' ],
			[ 'article', 'ws10' ],
			[ 'articles', 'ws1' ],
			[ 'articles', 'ws10' ],
		] as const;
		const prefixes = pairs.map(([ e, w ]) => searchIndexWsPrefix(e, w));
		expect(new Set(prefixes).size).toBe(pairs.length);
		const legacy = pairs.map(([ e, w ]) => legacySearchIndexWsPrefix(e, w));
		for (const p of prefixes) {
			for (const q of [ ...prefixes, ...legacy ]) {
				if (p !== q) expect(p.startsWith(q) || q.startsWith(p)).toBe(false);
			}
		}
	});
});

describe('search records of entities and workspaces whose names share a prefix', () => {
	const articles = entity('articles', {
		id: text().key(),
		title: text().searchable(),
	});
	const shared = defineSchema(article, articles);
	const schemas = {
		article: shared.descMessage('article'),
		articles: shared.descMessage('articles'),
	};
	const reg = new EntityRegistry([ schemas.article, schemas.articles ]);
	const PAIRS = [
		[ 'article', 'ws1' ],
		[ 'article', 'ws10' ],
		[ 'articles', 'ws1' ],
		[ 'articles', 'ws10' ],
	] as const;
	/** Row keys per pair: distinct per pair, so a scan returning another pair's
	 * rows cannot pass. */
	const rowsOf = (e: string, w: string) => [ `${e}-${w}-1`, `${e}-${w}-2` ];

	async function seeded(): Promise<{ kv: KVStore; indexes: IndexCache }> {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(reg);
		for (const [ e, w ] of PAIRS) {
			const schema = schemas[e];
			const changes = rowsOf(e, w).map((id, i) =>
				create(RowChangeSchema, {
					entity: e,
					key: id,
					version: BigInt(i + 1),
					body: {
						case: 'value',
						value: toBinary(schema, create(schema, { id, title: `milk ${id}` })),
					},
				})
			);
			const tx = await kv.write();
			await applyServerChanges(tx, reg, w, changes, new TouchSet(), indexes);
			await tx.commit();
		}
		return { kv, indexes };
	}

	async function rowKeysOf(kv: KVStore, e: string, w: string): Promise<string[]> {
		const prefix = searchIndexWsPrefix(e, w);
		return (await keysUnder(kv, prefix)).map((k) => k.slice(prefix.length));
	}

	it('scan as one prefix per pair, holding that pair alone', async () => {
		const { kv } = await seeded();
		for (const [ e, w ] of PAIRS) expect(await rowKeysOf(kv, e, w)).toEqual(rowsOf(e, w));
		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toHaveLength(PAIRS.length * 2);
	});

	it('leave with a wipe of their workspace and no other', async () => {
		const { kv } = await seeded();
		const tx = await kv.write();
		await wipeConfirmedState(tx, reg, 'ws1', new TouchSet());
		await tx.commit();
		expect(await rowKeysOf(kv, 'article', 'ws1')).toEqual([]);
		expect(await rowKeysOf(kv, 'articles', 'ws1')).toEqual([]);
		expect(await rowKeysOf(kv, 'article', 'ws10')).toEqual(rowsOf('article', 'ws10'));
		expect(await rowKeysOf(kv, 'articles', 'ws10')).toEqual(rowsOf('articles', 'ws10'));
	});

	it('leave with a drop of their entity in their workspace and no other', async () => {
		const { kv, indexes } = await seeded();
		const tx = await kv.write();
		await dropConfirmedEntities(tx, 'ws10', [ 'article' ], new TouchSet(), indexes);
		await tx.commit();
		expect(await rowKeysOf(kv, 'article', 'ws10')).toEqual([]);
		expect(await rowKeysOf(kv, 'article', 'ws1')).toEqual(rowsOf('article', 'ws1'));
		expect(await rowKeysOf(kv, 'articles', 'ws10')).toEqual(rowsOf('articles', 'ws10'));
		expect(await rowKeysOf(kv, 'articles', 'ws1')).toEqual(rowsOf('articles', 'ws1'));
	});

	it('leave one by one with the tombstones of their rows', async () => {
		const { kv, indexes } = await seeded();
		const tx = await kv.write();
		const id = rowsOf('articles', 'ws1')[0]!;
		await applyServerChanges(
			tx,
			reg,
			'ws1',
			[
				create(RowChangeSchema, {
					entity: 'articles',
					key: id,
					version: 9n,
					body: { case: 'tombstone', value: true },
				}),
			],
			new TouchSet(),
			indexes,
		);
		await tx.commit();
		expect(await rowKeysOf(kv, 'articles', 'ws1')).toEqual(rowsOf('articles', 'ws1').slice(1));
		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toHaveLength(PAIRS.length * 2 - 1);
	});
});
