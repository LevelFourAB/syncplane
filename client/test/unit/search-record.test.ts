// The search postings (`s/<scope><tag>…`): per-term heads and chunks of row
// keys, and a term list per row, kept as records of their own so a write that
// leaves the searchable text alone leaves the postings alone.
//
// Two properties are load-bearing and neither is visible in a query result. A
// rewrite that does not change the terms must not touch any posting — the
// whole point of the term list — and every path that removes a row must remove
// its postings, since a posting outliving its row would answer for a row
// nothing can read. Both are asserted against the keys the store is actually
// asked to write.

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
	SEARCH_POSTINGS_ROOT,
	SEARCH_SPEC_TAG_LENGTH,
	searchIndexWsPrefix,
	searchPostingsBase,
	searchPostingsScope,
	searchSpecTag,
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
import {
	POSTINGS_LAST_PAGE,
	postingsPageKey,
	postingsTermsKey,
	readAllKeys,
	readTerm,
	termListOf,
} from '../../src/query/postings.js';
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

/** The search entry of one term, as query/searchindex.ts encodes it. */
const entryOf = (term: string) => `\u0001${term}\u0000\u0000`;
const OVERFLOW = '\u0002';

function baseOf(indexes: IndexCache, entityName = 'article', ws = WS): string {
	return searchPostingsBase(entityName, ws, searchIndexOf(indexes.for(entityName))!.specHash);
}

/** The rows posted under one entry, in key order. */
async function posted(kv: KVStore, indexes: IndexCache, entry: string): Promise<string[]> {
	const base = baseOf(indexes);
	const tx = await kv.read();
	try {
		const head = await readTerm(tx, base, entry);
		return head === undefined ? [] : await readAllKeys(tx, base, [ head ]);
	} finally {
		tx.release();
	}
}

/** The entries one row's term list holds, sorted. */
async function termsOf(kv: KVStore, indexes: IndexCache, id: string): Promise<string[]> {
	const tx = await kv.read();
	try {
		return termListOf(await tx.get(postingsTermsKey(baseOf(indexes), id)));
	} finally {
		tx.release();
	}
}

const isPosting = (k: string) => k.startsWith(SEARCH_POSTINGS_ROOT);

describe('the search postings of a row', () => {
	it('are left alone by a change that does not move the terms', async () => {
		const { kv, puts } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		expect(puts).toContain(postingsTermsKey(baseOf(indexes), 'a1'));
		expect(puts).toContain(postingsPageKey(baseOf(indexes), POSTINGS_LAST_PAGE));

		puts.length = 0;
		await apply(kv, indexes, [ change('a1', 'milk cow', 'live', 2n) ]);
		expect(puts.filter(isPosting)).toEqual([]);
		expect(puts).toContain('c/article/ws1/a1');
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a1' ]);
	});

	it('are left alone when the same terms arrive in another order', async () => {
		const { kv, puts } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);

		puts.length = 0;
		await apply(kv, indexes, [ change('a1', 'cow milk', 'draft', 2n) ]);
		expect(puts.filter(isPosting)).toEqual([]);
	});

	it('follow a change that moves the terms, and touch only the records that hold them', async () => {
		const { kv, puts } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [
			change('a1', 'milk cow', 'draft', 1n),
			change('a2', 'milk', 'draft', 2n),
		]);

		puts.length = 0;
		await apply(kv, indexes, [ change('a1', 'milk goat', 'draft', 3n) ]);
		const base = baseOf(indexes);
		// Three small terms in one page: the page and the row's term list.
		expect(puts.filter(isPosting).sort()).toEqual(
			[ postingsPageKey(base, POSTINGS_LAST_PAGE), postingsTermsKey(base, 'a1') ].sort(),
		);
		expect(await termsOf(kv, indexes, 'a1')).toEqual([ entryOf('goat'), entryOf('milk') ]);
		expect(await posted(kv, indexes, entryOf('cow'))).toEqual([]);
		expect(await posted(kv, indexes, entryOf('goat'))).toEqual([ 'a1' ]);
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a1', 'a2' ]);
	});

	it('follow a row whose searchable text empties out', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		await apply(kv, indexes, [ change('a1', '', 'draft', 2n) ]);
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).toEqual([]);
	});

	it('are deleted by a tombstone', async () => {
		const { kv, dels } = recordingWrites(new MemoryKVStore());
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [
			change('a1', 'milk cow', 'draft', 1n),
			change('a2', 'milk', 'draft', 2n),
		]);

		dels.length = 0;
		await apply(kv, indexes, [ tombstone('a1', 3n) ]);
		expect(dels).toContain(postingsTermsKey(baseOf(indexes), 'a1'));
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a2' ]);
		expect(await posted(kv, indexes, entryOf('cow'))).toEqual([]);

		await apply(kv, indexes, [ tombstone('a2', 4n) ]);
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).toEqual([]);
	});

	it('are deleted by a tombstone in a build that declares no search', async () => {
		const kv = new MemoryKVStore();
		await apply(kv, new IndexCache(registry), [ change('a1', 'milk cow', 'draft', 1n) ]);
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).not.toEqual([]);

		// The same entity without its search declaration, as an older build of
		// the app would register it.
		const plain = defineSchema(entity('article', { id: text().key(), title: text(), state: text() }));
		const plainRegistry = new EntityRegistry([ plain.descMessage('article') ]);
		const tx = await kv.write();
		await applyServerChanges(
			tx,
			plainRegistry,
			WS,
			[ tombstone('a1', 2n) ],
			new TouchSet(),
			new IndexCache(plainRegistry),
		);
		await tx.commit();
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).toEqual([]);
	});

	it('are swept with the confirmed rows of a workspace', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).not.toEqual([]);

		const tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, new TouchSet());
		await tx.commit();
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).toEqual([]);
		expect(await keysUnder(kv, 'c/')).toEqual([]);
	});

	it('are swept with a dropped entity', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);

		const tx = await kv.write();
		await dropConfirmedEntities(tx, WS, [ 'article' ], new TouchSet(), indexes);
		await tx.commit();
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).toEqual([]);
	});

	it('are swept with a purged workspace', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		const client = new SyncClient({
			workspaceId: WS,
			kvStore: kv,
			entities: [ ArticleSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		}) as unknown as SyncClient<never>;
		try {
			await client.removeWorkspace(WS, { purge: true });
		} finally {
			await client.close();
		}
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).toEqual([]);
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

describe('search queries over the postings', () => {
	it('find rows written by a pull apply', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, SEED);
		const { keys, receipt } = await queryServedByIndex(kv, indexes, MILK);
		expect(receipt.index).toBe('__search');
		expect(keys).toEqual([ 'a1', 'a3' ]);
	});

	it('find rows written by a bootstrap', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const tx = await kv.write();
		await applyBootstrapRows(tx, registry, WS, SEED, new TouchSet(), indexes);
		await tx.commit();
		const { keys, receipt } = await queryServedByIndex(kv, indexes, MILK);
		expect(receipt.index).toBe('__search');
		expect(keys).toEqual([ 'a1', 'a3' ]);
	});

	it('find rows a resumed bootstrap rewrote with other text', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		for (const rows of [ SEED, [ change('a1', 'water cow', 'draft', 4n) ] ]) {
			const tx = await kv.write();
			await applyBootstrapRows(tx, registry, WS, rows, new TouchSet(), indexes);
			await tx.commit();
		}
		const { keys } = await queryServedByIndex(kv, indexes, MILK);
		expect(keys).toEqual([ 'a3' ]);
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a3' ]);
	});

	it('find rows the background build walked', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		// Seeded WITHOUT the index, which is the state a staged bootstrap swap
		// leaves: rows with no positions and no postings at all.
		const seed = await kv.write();
		await applyServerChanges(seed, registry, WS, SEED, new TouchSet());
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(metaSnapshotGen(WS), 1);
		await seed.commit();
		expect(await keysUnder(kv, SEARCH_POSTINGS_ROOT)).toEqual([]);

		const mutex = new ChainMutex();
		const built = await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		expect(built.ready).toBe(true);
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a1', 'a3' ]);

		const { keys, receipt } = await queryThrough(kv, MILK);
		expect(receipt.index).toBe('__search');
		expect(keys).toEqual([ 'a1', 'a3' ]);
	});
});

/** A title with more distinct terms than one row stores postings for. */
const oversized = (tail: string) =>
	Array.from({ length: SEARCH_TERMS_PER_ROW + 20 }, (_, i) => `unique${i}`).join(' ') + tail;

describe('search postings', () => {
	it('list every row of one term under one head, in key order', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [
			change('a2', 'cow milk', 'live', 2n),
			change('a1', 'milk cow', 'draft', 1n),
		]);
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a1', 'a2' ]);
		expect(await posted(kv, indexes, entryOf('cow'))).toEqual([ 'a1', 'a2' ]);
		expect(await termsOf(kv, indexes, 'a1')).toEqual([ entryOf('cow'), entryOf('milk') ]);
	});

	it('list every oversized row under the overflow entry alone', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [
			change('b1', oversized(' needle'), 'draft', 1n),
			change('b2', oversized(''), 'draft', 2n),
		]);
		expect(await posted(kv, indexes, OVERFLOW)).toEqual([ 'b1', 'b2' ]);
		expect(await termsOf(kv, indexes, 'b1')).toEqual([ OVERFLOW ]);
		expect(await posted(kv, indexes, entryOf('needle'))).toEqual([]);
	});

	it('occupy no position in the store index', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await apply(kv, indexes, [ change('a1', 'milk cow', 'draft', 1n) ]);
		const tx = await kv.read();
		const owners: string[] = [];
		for await (const k of tx.indexScan()) owners.push(k);
		tx.release();
		expect(owners.filter(isPosting)).toEqual([]);
	});
});

describe('search queries over shared postings', () => {
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

/** The marker the current search spec is built under. */
function searchMarker(indexes: IndexCache): string {
	return metaIndexBuilt(
		WS,
		'article',
		SEARCH_INDEX_NAME,
		searchIndexOf(indexes.for('article'))!.specHash,
	);
}

/** Where position layouts up to 3 kept the search record of `id`. */
const legacyKey = (id: string) => `x/article/${WS}/${id}`;

describe('a store written under an older position layout', () => {
	it('is searched by scan until the build writes its postings', async () => {
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

		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a1', 'a3' ]);
		const after = await queryThrough(kv, MILK);
		expect(after.receipt.index).toBe('__search');
		expect(after.keys).toEqual([ 'a1', 'a3' ]);

		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toEqual([]);
		const r = await kv.read();
		expect(await r.has(oldMarker)).toBe(false);
		expect(await r.has(searchMarker(indexes))).toBe(true);
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
	// Layout 3 composed positions exactly as layout 4 does, with a 3 hashed
	// in, and kept the search record at `x/<entity>/<ws>/<key>`. Spelled out
	// rather than derived, like the layout 2 case above.
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

	it('is searched by scan until the build, and from the postings after it', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const oldMarker = await seedLayout3(kv, indexes);

		const before = await queryThrough(kv, MILK);
		expect(before.receipt.index).toBeUndefined();
		expect(before.keys).toEqual([ 'a1', 'a3' ]);

		const mutex = new ChainMutex();
		const built = await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		expect(built).toEqual({ rows: 3, ready: true });

		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toEqual([]);
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

describe('a store written under position layout 4', () => {
	// Layout 4 kept a row's search postings as index positions on its record
	// at `x/<scope><key>`, under the search spec of encoding version 2. Spelled
	// out rather than derived, like the cases above.
	async function seedLayout4(kv: KVStore, indexes: IndexCache) {
		const decl = searchIndexOf(indexes.for('article'))!.search!;
		const spec = fingerprint(`searchv2|${decl.spec}|cap:${SEARCH_TERMS_PER_ROW}`);
		const prefix = indexPositionPrefix('article', SEARCH_INDEX_NAME, spec, WS);
		const term = (t: string) => `${prefix}\u0001${t}\u0000\u0000`;
		const marker = metaIndexBuilt(WS, 'article', SEARCH_INDEX_NAME, spec);
		const x = (id: string) => `${searchIndexWsPrefix('article', WS)}${id}`;
		const seed = await kv.write();
		await applyServerChanges(seed, registry, WS, SEED, new TouchSet());
		await seed.put(x('a1'), { ix: [ term('milk'), term('cow') ] });
		await seed.put(x('a2'), { ix: [ term('water') ] });
		await seed.put(x('a3'), { ix: [ term('milk'), term('goat') ] });
		await seed.put(x('a9'), { ix: [ term('milk') ] });
		await seed.put(marker, true);
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(metaSnapshotGen(WS), 1);
		await seed.commit();
		return marker;
	}

	it('is searched by scan until the build, and from the postings after it', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const oldMarker = await seedLayout4(kv, indexes);

		const before = await queryThrough(kv, MILK);
		expect(before.receipt.index).toBeUndefined();
		expect(before.keys).toEqual([ 'a1', 'a3' ]);

		// A pull landing before the build posts its rows, and leaves the old
		// records for the build to sweep.
		await apply(kv, indexes, [ change('a2', 'milk water', 'live', 7n) ]);
		const mid = await queryThrough(kv, MILK);
		expect(mid.receipt.index).toBeUndefined();
		expect(mid.keys).toEqual([ 'a1', 'a2', 'a3' ]);

		const mutex = new ChainMutex();
		const built = await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		expect(built).toEqual({ rows: 3, ready: true });

		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toEqual([]);
		const tx = await kv.read();
		const owners: string[] = [];
		for await (const k of tx.indexScan()) owners.push(k);
		expect(owners.every((k) => k.startsWith('c/'))).toBe(true);
		expect(await tx.has(oldMarker)).toBe(false);
		expect(await tx.has(searchMarker(indexes))).toBe(true);
		tx.release();

		const after = await queryThrough(kv, MILK);
		expect(after.receipt.index).toBe('__search');
		expect(after.keys).toEqual([ 'a1', 'a2', 'a3' ]);
	});

	it('loses its old records to a wipe of the workspace', async () => {
		const kv = new MemoryKVStore();
		await seedLayout4(kv, new IndexCache(registry));
		const tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, new TouchSet());
		await tx.commit();
		expect(await keysUnder(kv, SEARCH_INDEX_ROOT)).toEqual([]);
	});
});

describe('a store holding the postings of another search declaration', () => {
	// The same entity searchable over its state as well, as another build of
	// the app could declare it: another spec, so another tag.
	const wider = defineSchema(
		entity('article', { id: text().key(), title: text().searchable(), state: text().searchable() }),
	);
	const widerRegistry = new EntityRegistry([ wider.descMessage('article') ]);
	const widerChange = (id: string, title: string, state: string, version: bigint) => {
		const schema = wider.descMessage('article');
		return create(RowChangeSchema, {
			entity: 'article',
			key: id,
			version,
			body: { case: 'value', value: toBinary(schema, create(schema, { id, title, state })) },
		});
	};

	it('keeps the declarations apart, takes a row out of the other one it rewrites, and sweeps the rest at the build', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		const widerIndexes = new IndexCache(widerRegistry);
		const tx = await kv.write();
		await applyServerChanges(
			tx,
			widerRegistry,
			WS,
			[ widerChange('a1', 'milk', 'draft', 1n), widerChange('a2', 'milk', 'live', 2n) ],
			new TouchSet(),
			widerIndexes,
		);
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.put(metaSnapshotGen(WS), 1);
		await tx.commit();
		const scope = searchPostingsScope('article', WS);
		const widerBase = baseOf(widerIndexes);
		expect(baseOf(indexes)).not.toBe(widerBase);
		expect(widerBase.startsWith(scope)).toBe(true);

		// This build rewrites a1: the other declaration loses it, since this
		// build cannot analyze it the way that one does.
		await apply(kv, indexes, [ change('a1', 'milk', 'draft', 3n) ]);
		const r = await kv.read();
		const head = await readTerm(r, widerBase, entryOf('milk'));
		expect(head === undefined ? [] : await readAllKeys(r, widerBase, [ head ])).toEqual([ 'a2' ]);
		r.release();
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a1' ]);

		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		expect(await keysUnder(kv, widerBase)).toEqual([]);
		expect(await posted(kv, indexes, entryOf('milk'))).toEqual([ 'a1', 'a2' ]);
	});
});

describe('the search postings key', () => {
	it('is the root, an 8-character scope, then a 4-character spec tag', () => {
		const scope = searchPostingsScope('article', WS);
		expect(scope.startsWith(SEARCH_POSTINGS_ROOT)).toBe(true);
		const chars = scope.slice(SEARCH_POSTINGS_ROOT.length);
		expect(chars).toHaveLength(8);
		expect([ ...chars ].every((c) => c.charCodeAt(0) <= 0xff)).toBe(true);
		const base = searchPostingsBase('article', WS, '0123456789abcdef');
		expect(base).toBe(scope + searchSpecTag('0123456789abcdef'));
		expect(base.length - scope.length).toBe(SEARCH_SPEC_TAG_LENGTH);
		// The high bit is what keeps the x/ scope apart from an old
		// `x/<entity>/` key, whose third character is an ASCII entity name.
		// Checked over many pairs, since half of all fingerprints have it set by
		// chance.
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
		const scopes = pairs.map(([ e, w ]) => searchPostingsScope(e, w));
		expect(new Set(scopes).size).toBe(pairs.length);
		for (const p of scopes) {
			for (const q of scopes) {
				if (p !== q) expect(p.startsWith(q) || q.startsWith(p)).toBe(false);
			}
		}
		const legacy = pairs.map(([ e, w ]) => legacySearchIndexWsPrefix(e, w));
		const old = pairs.map(([ e, w ]) => searchIndexWsPrefix(e, w));
		for (const p of old) {
			for (const q of [ ...old, ...legacy ]) {
				if (p !== q) expect(p.startsWith(q) || q.startsWith(p)).toBe(false);
			}
		}
	});
});

describe('search postings of entities and workspaces whose names share a prefix', () => {
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

	/** The rows of one pair posted under `milk`. */
	async function rowKeysOf(
		kv: KVStore,
		indexes: IndexCache,
		e: string,
		w: string,
	): Promise<string[]> {
		const base = baseOf(indexes, e, w);
		const tx = await kv.read();
		try {
			const head = await readTerm(tx, base, entryOf('milk'));
			return head === undefined ? [] : await readAllKeys(tx, base, [ head ]);
		} finally {
			tx.release();
		}
	}

	it('are posted under one scope per pair, holding that pair alone', async () => {
		const { kv, indexes } = await seeded();
		for (const [ e, w ] of PAIRS) expect(await rowKeysOf(kv, indexes, e, w)).toEqual(rowsOf(e, w));
	});

	it('leave with a wipe of their workspace and no other', async () => {
		const { kv, indexes } = await seeded();
		const tx = await kv.write();
		await wipeConfirmedState(tx, reg, 'ws1', new TouchSet());
		await tx.commit();
		expect(await rowKeysOf(kv, indexes, 'article', 'ws1')).toEqual([]);
		expect(await rowKeysOf(kv, indexes, 'articles', 'ws1')).toEqual([]);
		expect(await rowKeysOf(kv, indexes, 'article', 'ws10')).toEqual(rowsOf('article', 'ws10'));
		expect(await rowKeysOf(kv, indexes, 'articles', 'ws10')).toEqual(rowsOf('articles', 'ws10'));
	});

	it('leave with a drop of their entity in their workspace and no other', async () => {
		const { kv, indexes } = await seeded();
		const tx = await kv.write();
		await dropConfirmedEntities(tx, 'ws10', [ 'article' ], new TouchSet(), indexes);
		await tx.commit();
		expect(await rowKeysOf(kv, indexes, 'article', 'ws10')).toEqual([]);
		expect(await rowKeysOf(kv, indexes, 'article', 'ws1')).toEqual(rowsOf('article', 'ws1'));
		expect(await rowKeysOf(kv, indexes, 'articles', 'ws10')).toEqual(rowsOf('articles', 'ws10'));
		expect(await rowKeysOf(kv, indexes, 'articles', 'ws1')).toEqual(rowsOf('articles', 'ws1'));
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
		expect(await rowKeysOf(kv, indexes, 'articles', 'ws1')).toEqual(
			rowsOf('articles', 'ws1').slice(1),
		);
		for (const [ e, w ] of PAIRS) {
			if (e === 'articles' && w === 'ws1') continue;
			expect(await rowKeysOf(kv, indexes, e, w)).toEqual(rowsOf(e, w));
		}
	});
});
