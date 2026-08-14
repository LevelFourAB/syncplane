// Search-index-served queries must return EXACTLY what the scan returns —
// same rows, same order, same versions, same totals. Every case runs twice
// against identical data, once with the index built and once without, and
// compares the two: the scan path is the definition, the term-postings index
// an optimization over it.

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
import { compileQuery, type Query } from '../../src/query/index.js';
import { SEARCH_TERMS_PER_ROW } from '../../src/query/searchindex.js';
import { prepareQuery } from '../../src/query/wire.js';
import { defineSchema, entity, type Infer, text } from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

// Both declaration surfaces: `title` carries the searchable mark (most
// important), `summary` and `author` come from the entity option, in order.
const article = entity('article', {
	id: text().key(),
	title: text().searchable(),
	summary: text(),
	author: text(),
	state: text(),
}, {
	search: { fields: [ 'summary', 'author' ] },
});

type Article = Infer<typeof article>;
const bundle = defineSchema(article);
const ArticleSchema = bundle.descMessage('article');
const registry = new EntityRegistry([ ArticleSchema ]);
const WS = 'ws1';

const row = (partial: Partial<Article> & { id: string }): Article => ({
	title: '',
	summary: '',
	author: '',
	state: '',
	...partial,
});

const SEED: Article[] = [
	row({ id: 'a1', title: 'Milk cow', state: 'draft' }),
	row({ id: 'a2', summary: 'milk', state: 'live' }),
	row({ id: 'a3', title: 'water', state: 'live' }),
	row({ id: 'a4', author: 'milk', state: 'live' }),
	row({ id: 'a5', title: 'Café society', state: 'live' }),
	// Oversized: more distinct terms than one row may store postings for, with
	// the probe target past the cap — only the overflow union can find it.
	row({
		id: 'big',
		summary: Array.from({ length: SEARCH_TERMS_PER_ROW + 20 }, (_, i) => `unique${i}`).join(' ')
			+ ' needle',
	}),
];

/** Plants rows as CONFIRMED state — the only state an index covers. */
async function seedConfirmed(kv: KVStore, indexes: IndexCache): Promise<void> {
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		WS,
		SEED.map((r, i) =>
			create(RowChangeSchema, {
				entity: 'article',
				key: r.id,
				version: BigInt(i + 1),
				body: { case: 'value', value: toBinary(ArticleSchema, create(ArticleSchema, r)) },
			})
		),
		new TouchSet(),
		indexes,
	);
	await tx.commit();
}

async function clientWith(built: boolean, overlay?: Article[], deletes?: string[]) {
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
		entities: [ ArticleSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	}) as unknown as SyncClient<never>;
	for (const r of overlay ?? []) await client.mutate.put(ArticleSchema, r as never);
	for (const key of deletes ?? []) await client.mutate.del(ArticleSchema, key);
	return client;
}

const run = (client: SyncClient<never>, q: Query<Article>): Promise<RowSet<Article>> =>
	client.query(ArticleSchema, q as never) as unknown as Promise<RowSet<Article>>;

async function both(q: Query<Article>, overlay?: Article[], deletes?: string[]) {
	const shape = (rows: RowSet<Article>) => ({
		rows: rows.map((r) => ({ key: r.key, version: r.version, state: r.value.state })),
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
function same(
	name: string,
	q: Query<Article>,
	want: string[],
	overlay?: Article[],
	deletes?: string[],
): void {
	it(name, async () => {
		const { scanned, indexed } = await both(q, overlay, deletes);
		expect(indexed).toEqual(scanned);
		expect(scanned.rows.map((r) => r.key)).toEqual(want);
	});
}

describe('search-index-served match queries', () => {
	same('ranks by relevance across the declared fields', {
		where: (a) => a.match('milk'),
		orderBy: (a) => [ a.relevance() ],
	}, [ 'a1', 'a2', 'a4' ]);

	same('probes the final term as a prefix', {
		where: (a) => a.match('mil'),
		orderBy: (a) => [ a.relevance() ],
	}, [ 'a1', 'a2', 'a4' ]);

	same('folds case and diacritics', {
		where: (a) => a.match('CAFE'),
	}, [ 'a5' ]);

	same('intersects terms conjunctively', {
		where: (a) => a.match('milk cow'),
	}, [ 'a1' ]);

	same('composes with an ordinary filter', {
		where: (a /* and */) => ({
			kind: 'and',
			conds: [ a.match('milk'), a.state.eq('live') ],
		}),
	}, [ 'a2', 'a4' ]);

	same('finds an oversized row through the overflow union', {
		where: (a) => a.match('needle'),
	}, [ 'big' ]);

	same('slices a relevance page and reports the total', {
		where: (a) => a.match('milk'),
		orderBy: (a) => [ a.relevance() ],
		limit: 2,
	}, [ 'a1', 'a2' ]);

	// a7 ties with a1 (both hold the term at title position 0), so the row key
	// breaks it — which is itself worth pinning: overlay rows rank by the same
	// tuple confirmed rows do.
	same(
		'merges a matching overlay row the index cannot know about',
		{
			where: (a) => a.match('milk'),
			orderBy: (a) => [ a.relevance() ],
		},
		[ 'a1', 'a7', 'a2', 'a4' ],
		[ row({ id: 'a7', title: 'milk first' }) ],
	);

	same(
		'a locally edited row answers from the overlay, not its stale postings',
		{
			where: (a) => a.match('milk'),
			orderBy: (a) => [ a.relevance() ],
		},
		[ 'a2', 'a4' ],
		[ row({ id: 'a1', title: 'tea' }) ],
	);

	same(
		'a locally deleted row drops out',
		{
			where: (a) => a.match('milk'),
			orderBy: (a) => [ a.relevance() ],
		},
		[ 'a2', 'a4' ],
		undefined,
		[ 'a1' ],
	);

	same('a non-matching query returns nothing either way', {
		where: (a) => a.match('zebra'),
	}, []);
});

describe('the search index path is actually taken', () => {
	async function served(built: boolean, q: Query<Article>): Promise<string | undefined> {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await seedConfirmed(kv, indexes);
		if (built) {
			const mutex = new ChainMutex();
			await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		}
		const tx = await kv.read();
		try {
			const report = { candidates: 0 } as {
				index?: string;
				candidates: number;
			};
			const out = await queryByIndex(
				tx,
				registry,
				registry.get('article')!,
				ArticleSchema,
				indexes.for('article'),
				prepareQuery(ArticleSchema, compileQuery(q)),
				WS,
				undefined,
				report,
			);
			return out === undefined ? undefined : report.index;
		} finally {
			tx.release();
		}
	}

	it('serves a match from the postings once built', async () => {
		expect(await served(true, { where: (a) => a.match('milk') })).toBe('__search');
	});

	it('declines before the build has marked the index', async () => {
		expect(await served(false, { where: (a) => a.match('milk') })).toBeUndefined();
	});

	it('an empty query narrows nothing and falls to the scan', async () => {
		expect(await served(true, { where: (a) => a.match('') })).toBeUndefined();
	});
});
