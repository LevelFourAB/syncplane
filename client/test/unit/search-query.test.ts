// Search-index-served queries must return EXACTLY what the scan returns —
// same rows, same order, same versions, same totals. Every case runs twice
// against identical data, once with the index built and once without, and
// compares the two: the scan path is the definition, the term-postings index
// an optimization over it.

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { ArticleSchema as TextArticleSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor, overlayTextKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import {
	type IndexReport,
	queryByIndex,
	withIndexCandidateBudget,
} from '../../src/query/execute.js';
import { compileQuery, type Query } from '../../src/query/index.js';
import { SEARCH_TERMS_PER_ROW } from '../../src/query/searchindex.js';
import { prepareQuery } from '../../src/query/wire.js';
import { defineSchema, entity, type Infer, text } from '../../src/schema/builder.js';
import type { QueryEvent } from '../../src/sync/events.js';
import { decodeTextDoc, encodeFragmentFrom, encodeTextDoc, TextDoc } from '../../src/text/index.js';
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
	// A term wide enough that no candidate walk should read its postings, and
	// one row that also carries a rare one. What a query naming both costs is
	// set by the rare term alone, and the two arms have to agree on the answer
	// whichever way the walk decides to get there.
	...Array.from(
		{ length: 100 },
		(_, i) =>
			row({ id: `bulk${String(i).padStart(3, '0')}`, summary: 'common filler', state: 'live' }),
	),
	row({ id: 'hit', summary: 'common zygote', state: 'live' }),
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
	const queries: QueryEvent[] = [];
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ ArticleSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		onSyncEvent: (e) => {
			if (e.type === 'query') queries.push(e);
		},
	}) as unknown as SyncClient<never>;
	for (const r of overlay ?? []) await client.mutate.put(ArticleSchema, r as never);
	for (const key of deletes ?? []) await client.mutate.del(ArticleSchema, key);
	return { client, queries };
}

const run = (client: SyncClient<never>, q: Query<Article>): Promise<RowSet<Article>> =>
	client.query(ArticleSchema, q as never) as unknown as Promise<RowSet<Article>>;

async function both(q: Query<Article>, overlay?: Article[], deletes?: string[]) {
	const shape = (rows: RowSet<Article>) => ({
		rows: rows.map((r) => ({ key: r.key, version: r.version, state: r.value.state })),
		total: rows.total,
		hasMore: rows.hasMore,
	});
	const scanned = await clientWith(false, overlay, deletes);
	const indexed = await clientWith(true, overlay, deletes);
	try {
		return {
			scanned: shape(await run(scanned.client, q)),
			indexed: shape(await run(indexed.client, q)),
			receipt: indexed.queries.at(-1)!,
		};
	} finally {
		await scanned.client.close();
		await indexed.client.close();
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

	// The common term comes FIRST, which is where a walk that seeded from the
	// leading term rather than the most selective one would pick it.
	same('answers a mix of common and rare terms from the rare one', {
		where: (a) => a.match('common zygote '),
	}, [ 'hit' ]);

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

// A single-term match with no ranking is answered from the postings themselves
// rather than from the rows they name — so a term every row carries costs a
// page of reads instead of a posting list of them. The budget is lowered past
// the common term's list, which is exactly where the candidate walk gives up:
// every case here is one only the postings path can answer, and each still has
// to agree with the scan.
const POSTINGS_BUDGET = 8;

/**
 * `same`, run with the candidate budget below the term's posting list, and
 * pinning the total and hasMore the two arms have to agree on. The receipt is
 * asserted too: without it a case would pass on the fallback scan, which is
 * where the budget would otherwise send it.
 */
function fromPostings(
	name: string,
	q: Query<Article>,
	want: { rows: string[]; total: number | undefined; hasMore: boolean },
	overlay?: Article[],
	deletes?: string[],
): void {
	it(name, async () => {
		const restore = withIndexCandidateBudget(POSTINGS_BUDGET);
		try {
			const { scanned, indexed, receipt } = await both(q, overlay, deletes);
			expect(receipt.path).toBe('index');
			expect(receipt.index).toBe('__search');
			expect(indexed).toEqual(scanned);
			expect(scanned.rows.map((r) => r.key)).toEqual(want.rows);
			expect(scanned.total).toBe(want.total);
			expect(scanned.hasMore).toBe(want.hasMore);
		} finally {
			restore();
		}
	});
}

describe('a single-term search answered from the postings', () => {
	fromPostings('cuts a page out of a term every row carries', {
		where: (a) => a.match('common '),
		limit: 3,
	}, { rows: [ 'bulk000', 'bulk001', 'bulk002' ], total: 101, hasMore: true });

	fromPostings('offsets into the postings', {
		where: (a) => a.match('common '),
		offset: 2,
		limit: 3,
	}, { rows: [ 'bulk002', 'bulk003', 'bulk004' ], total: 101, hasMore: true });

	fromPostings('reports hasMore with the total opted out', {
		where: (a) => a.match('common '),
		limit: 3,
		total: false,
	}, { rows: [ 'bulk000', 'bulk001', 'bulk002' ], total: undefined, hasMore: true });

	// One stored term under the prefix, so the range is still ordered by row key.
	fromPostings('answers a prefix reaching one stored term', {
		where: (a) => a.match('commo'),
		limit: 3,
	}, { rows: [ 'bulk000', 'bulk001', 'bulk002' ], total: 101, hasMore: true });

	fromPostings('counts nothing for a term no row carries', {
		where: (a) => a.match('zebra '),
		limit: 3,
	}, { rows: [], total: 0, hasMore: false });

	fromPostings('takes the whole match set when nothing limits it', {
		where: (a) => a.match('zygote '),
	}, { rows: [ 'hit' ], total: 1, hasMore: false });

	fromPostings(
		'drops a row the overlay edited out of the match',
		{ where: (a) => a.match('common '), limit: 3 },
		{ rows: [ 'bulk001', 'bulk002', 'bulk003' ], total: 100, hasMore: true },
		[ row({ id: 'bulk000', summary: 'tea' }) ],
	);

	fromPostings(
		'merges a row the overlay edited into the match',
		{ where: (a) => a.match('common '), limit: 3 },
		{ rows: [ 'a2', 'bulk000', 'bulk001' ], total: 102, hasMore: true },
		[ row({ id: 'a2', summary: 'common' }) ],
	);

	fromPostings(
		'drops a locally deleted row from the page and the count',
		{ where: (a) => a.match('common '), limit: 3 },
		{ rows: [ 'bulk001', 'bulk002', 'bulk003' ], total: 100, hasMore: true },
		undefined,
		[ 'bulk000' ],
	);

	// The oversized row stores the overflow marker instead of its terms, so the
	// range says nothing about it either way.
	fromPostings('finds an oversized row the postings cannot name', {
		where: (a) => a.match('needle '),
		limit: 3,
	}, { rows: [ 'big' ], total: 1, hasMore: false });
});

describe('what a search reads to answer', () => {
	async function receipt(
		q: Query<Article>,
		budget?: number,
	): Promise<IndexReport & { served: boolean }> {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(registry);
		await seedConfirmed(kv, indexes);
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
		const restore = budget === undefined ? () => {} : withIndexCandidateBudget(budget);
		const tx = await kv.read();
		try {
			const report: IndexReport = { candidates: 0 };
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
			return { ...report, served: out !== undefined };
		} finally {
			tx.release();
			restore();
		}
	}

	const common: Query<Article> = { where: (a) => a.match('common '), limit: 3 };

	it('reads a page of postings, not the term list', async () => {
		const r = await receipt(common);
		expect(r.index).toBe('__search');
		expect(r.candidates).toBeLessThanOrEqual(POSTINGS_BUDGET);
	});

	it('answers past the budget the candidate walk gives up at', async () => {
		const r = await receipt(common, POSTINGS_BUDGET);
		expect(r.served).toBe(true);
		expect(r.index).toBe('__search');
		expect(r.declined).toBeUndefined();
	});

	it('a relevance ordering keeps reading every candidate', async () => {
		const ranked: Query<Article> = {
			where: (a) => a.match('common '),
			orderBy: (a) => [ a.relevance() ],
			limit: 3,
		};
		const wide = await receipt(ranked);
		expect(wide.index).toBe('__search');
		expect(wide.candidates).toBeGreaterThan(100);
		expect((await receipt(ranked, POSTINGS_BUDGET)).declined).toBe('budget');
	});

	it('a second required term keeps the seed-and-narrow walk', async () => {
		const two: Query<Article> = { where: (a) => a.match('common zygote '), limit: 3 };
		expect((await receipt(two)).index).toBe('__search');
		expect((await receipt(two, POSTINGS_BUDGET)).declined).toBe('budget');
	});

	it('a filter beside the match keeps the candidate walk', async () => {
		const filtered: Query<Article> = {
			where: (a /* and */) => ({ kind: 'and', conds: [ a.match('common '), a.state.eq('live') ] }),
			limit: 3,
		};
		expect((await receipt(filtered)).candidates).toBeGreaterThan(100);
		expect((await receipt(filtered, POSTINGS_BUDGET)).declined).toBe('budget');
	});

	it('a prefix reaching several stored terms keeps the candidate walk', async () => {
		// "cafe", "common" and "cow" all sit under "c", and their postings
		// interleave by term rather than by row key.
		const r = await receipt({ where: (a) => a.match('c'), limit: 3 });
		expect(r.index).toBe('__search');
		expect(r.candidates).toBeGreaterThan(100);
	});

	same("a prefix over several stored terms still answers the scan's rows", {
		where: (a) => a.match('c'),
		limit: 3,
	}, [ 'a1', 'a5', 'bulk000' ]);
});

// The index covers CONFIRMED state, and an unacked collaborative-text edit
// lands in the `ot/` log beside the row rather than in it — so a posting can
// describe a document the query no longer sees, in either direction.
describe('a search over a collaborative-text field', () => {
	const textRegistry = new EntityRegistry([ TextArticleSchema ]);
	const BODIES: [string, string][] = [
		[ 'r1', 'alpha' ],
		[ 'r2', 'beta gamma' ],
		[ 'r3', 'beta delta' ],
	];

	const docOf = (s: string): Uint8Array => {
		const d = new TextDoc('seed');
		d.insert(0, s);
		return encodeTextDoc(d);
	};

	/** One local edit of a row's body, as the fragment the keystroke path
	 * appends — derived from the same bytes the confirmed row holds. */
	function fragmentFor(body: string, edit: (d: TextDoc) => { peer: string; counter: number }[]) {
		const d = decodeTextDoc(docOf(body), 'local');
		return encodeFragmentFrom(d, edit(d), []);
	}

	const inserting = (body: string, at: number, s: string) =>
		fragmentFor(body, (d) => {
			const range = d.insert(at, s);
			return Array.from(
				{ length: range.count },
				(_, i) => ({ peer: range.peer, counter: range.start + i }),
			);
		});

	const deleting = (body: string, at: number, length: number) =>
		fragmentFor(body, (d) => d.delete(at, length).flatMap((seg) => seg.ids));

	async function textClient(built: boolean, edits: [string, Uint8Array][]) {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(textRegistry);
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await applyServerChanges(
			tx,
			textRegistry,
			WS,
			BODIES.map(([ id, body ], i) =>
				create(RowChangeSchema, {
					entity: 'article',
					key: id,
					version: BigInt(i + 1),
					body: {
						case: 'value',
						value: toBinary(
							TextArticleSchema,
							create(TextArticleSchema, { id, body: docOf(body) }),
						),
					},
				})
			),
			new TouchSet(),
			indexes,
		);
		await tx.commit();
		if (built) {
			const mutex = new ChainMutex();
			await buildIndexes(kv, (fn) => mutex.run(fn), textRegistry, indexes, WS);
		}
		const edit = await kv.write();
		for (const [ key, fragment ] of edits) {
			await edit.put(overlayTextKey('article', WS, key, 'body', 1n, 0), fragment);
		}
		await edit.commit();
		return new SyncClient({
			workspaceId: WS,
			kvStore: kv,
			entities: [ TextArticleSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		}) as unknown as SyncClient<never>;
	}

	async function bothWith(edits: [string, Uint8Array][]) {
		const q = { where: (a: { match(s: string): unknown }) => a.match('beta ') };
		const shape = (rows: RowSet<{ id: string }>) => ({
			rows: rows.map((r) => r.key),
			total: rows.total,
		});
		const scanned = await textClient(false, edits);
		const indexed = await textClient(true, edits);
		try {
			return {
				scanned: shape(
					await scanned.query(TextArticleSchema, q as never) as unknown as RowSet<{ id: string }>,
				),
				indexed: shape(
					await indexed.query(TextArticleSchema, q as never) as unknown as RowSet<{ id: string }>,
				),
			};
		} finally {
			await scanned.close();
			await indexed.close();
		}
	}

	it('answers from the postings when nothing is edited locally', async () => {
		const { scanned, indexed } = await bothWith([]);
		expect(indexed).toEqual(scanned);
		expect(scanned).toEqual({ rows: [ 'r2', 'r3' ], total: 2 });
	});

	it('counts a row a local edit brought into the match', async () => {
		const { scanned, indexed } = await bothWith([ [ 'r1', inserting('alpha', 5, ' beta') ] ]);
		expect(indexed).toEqual(scanned);
		expect(scanned).toEqual({ rows: [ 'r1', 'r2', 'r3' ], total: 3 });
	});

	it('drops a row a local edit took out of the match', async () => {
		const { scanned, indexed } = await bothWith([ [ 'r2', deleting('beta gamma', 0, 4) ] ]);
		expect(indexed).toEqual(scanned);
		expect(scanned).toEqual({ rows: [ 'r3' ], total: 1 });
	});
});
