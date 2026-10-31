// The search postings store (query/postings.ts): the codec, how a term's row
// keys are split across its head and chunks, and — against a model of what
// every term should list — that a run of random writes keeps every structure
// exact, and that queries served from it answer what the scan answers.

import 'fake-indexeddb/auto';

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor, metaSnapshotGen, searchPostingsBase } from '../../src/keys.js';
import { IDBKVStore } from '../../src/kv/idb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead, KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { buildIndexes, markIndexesBuilt } from '../../src/optimistic/index-build.js';
import { IndexCache, searchIndexOf } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { withIndexCandidateBudget } from '../../src/query/execute.js';
import type { Query } from '../../src/query/index.js';
import {
	decodeHead,
	decodeKeyList,
	decodePage,
	encodeHead,
	encodeKeyList,
	encodePage,
	POSTINGS_CHUNK_CAP,
	POSTINGS_INLINE_MAX,
	POSTINGS_LAST_PAGE,
	POSTINGS_PAGE_CAP,
	postingsChunkKey,
	postingsDirectoryKey,
	postingsHeadKey,
	postingsPageKey,
	postingsTermsKey,
	readAllKeys,
	readTerm,
	scanTerms,
	SearchPostingsWriter,
	termListOf,
} from '../../src/query/postings.js';
import { SEARCH_TERMS_PER_ROW } from '../../src/query/searchindex.js';
import { defineSchema, entity, type Infer, text } from '../../src/schema/builder.js';
import type { QueryEvent } from '../../src/sync/events.js';
import { TouchSet } from '../../src/touch.js';

const CAP = POSTINGS_CHUNK_CAP;
const WS = 'ws1';
const SPEC = '0123456789abcdef';
const BASE = searchPostingsBase('doc', WS, SPEC);

/** mulberry32, so a failing seed reproduces. */
function prng(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

const pad = (i: number) => String(i).padStart(5, '0');
const sorted = (xs: Iterable<string>) => [ ...xs ].sort();

const STORES: [string, () => Promise<KVStore>][] = [
	[ 'memory', async () => new MemoryKVStore() ],
	[ 'idb (fake-indexeddb)', () => IDBKVStore.open(`postings-test-${Math.random()}`) ],
];

describe('the postings codec', () => {
	it('round-trips key lists, including keys outside Latin-1 and keys prefixing each other', () => {
		const cases = [
			[],
			[ '' ],
			[ 'a', 'a\u0000', 'a\u0000b', 'ab', 'abc', 'b' ],
			[ '06DQE23ER1W13B6NKVAJVYNXAG', '06DQE23ER5YYX8Q71BQVC3J53C' ],
			[ 'ÿ', 'Ā', '😀', '￿' ],
			[ 'x'.repeat(300), `${'x'.repeat(300)}y` ],
		];
		for (const keys of cases) expect(decodeKeyList(encodeKeyList(keys))).toEqual(keys);
	});

	it('keeps a list of Latin-1 keys at one byte per character', () => {
		const keys = Array.from({ length: 300 }, (_, i) => `row-${pad(i * 7)}`);
		const s = encodeKeyList(keys);
		expect([ ...s ].every((c) => c.charCodeAt(0) <= 0xff)).toBe(true);
		// Front coding: the shared stem is stored once.
		expect(s.length).toBeLessThan(keys.join('').length / 2);
	});

	it('round-trips a page, marking large terms', () => {
		const page = [
			{ entry: '\u0001a\u0000\u0000', keys: [ 'k1', 'k2' ] },
			{ entry: '\u0001ab\u0000\u0000', keys: undefined },
			{ entry: '\u0002', keys: [ 'z' ] },
		];
		expect(decodePage(encodePage(page))).toEqual(page);
		expect(decodePage(encodePage([]))).toEqual([]);
	});

	it('round-trips a head', () => {
		const head = { counts: [ 3, 200, 1 ], bounds: [ 'a', 'm', 'qĀ' ], tail: [ 'r', 's' ] };
		expect(decodeHead(encodeHead(head))).toEqual(head);
		const empty = { counts: [], bounds: [], tail: [] };
		expect(decodeHead(encodeHead(empty))).toEqual(empty);
	});

	it('refuses a truncated value rather than inventing keys', () => {
		const s = encodeKeyList([ 'alpha', 'beta' ]);
		expect(() => decodeKeyList(s.slice(0, -2))).toThrow();
	});
});

/**
 * Every term's row keys under `base`, with every invariant of the layout
 * checked on the way: the directory names exactly the pages there are, pages
 * hold sorted terms inside their bounds and within their cap, a page marks
 * exactly the terms that have a head, and a head's chunks are exactly the
 * chunk records of its term.
 */
async function checkedBase(tx: KVRead, base: string): Promise<Map<string, string[]>> {
	const kinds = new Map<string, Map<string, unknown>>();
	for await (const [ k, v ] of tx.scan(base)) {
		const kind = k.charAt(base.length);
		let m = kinds.get(kind);
		if (m === undefined) kinds.set(kind, m = new Map());
		m.set(k.slice(base.length + 1), v);
	}
	const pages = kinds.get('p') ?? new Map<string, unknown>();
	const heads = kinds.get('h') ?? new Map<string, unknown>();
	const chunks = kinds.get('c') ?? new Map<string, unknown>();
	const out = new Map<string, string[]>();
	const dir = kinds.get('d')?.get('');
	if (pages.size === 0 && heads.size === 0) {
		expect(dir, 'a directory outlived its pages').toBeUndefined();
		expect(chunks.size).toBe(0);
		return out;
	}
	const bounds = decodeKeyList(dir as string);
	expect(bounds.at(-1)).toBe(POSTINGS_LAST_PAGE);
	expect(sorted(pages.keys())).toEqual(bounds);
	expect(bounds).toEqual(sorted(new Set(bounds)));
	const flagged = new Set<string>();
	let below: string | undefined;
	for (const bound of bounds) {
		const raw = pages.get(bound) as string;
		const terms = decodePage(raw);
		expect(raw.length <= POSTINGS_PAGE_CAP || terms.length === 1, 'an oversized page').toBe(true);
		expect(terms.map((t) => t.entry)).toEqual(sorted(new Set(terms.map((t) => t.entry))));
		for (const t of terms) {
			if (below !== undefined) expect(t.entry > below).toBe(true);
			expect(t.entry <= bound).toBe(true);
			if (t.keys === undefined) {
				flagged.add(t.entry);
				continue;
			}
			expect(t.keys.length).toBeGreaterThan(0);
			expect(t.keys.length).toBeLessThanOrEqual(POSTINGS_INLINE_MAX);
			expect(t.keys).toEqual(sorted(new Set(t.keys)));
			out.set(t.entry, t.keys);
		}
		below = bound;
	}
	expect(sorted(heads.keys()), 'heads and page marks disagree').toEqual(sorted(flagged));
	const owned = new Set<string>();
	for (const [ entry, raw ] of heads) {
		const head = decodeHead(raw as string);
		expect(head.tail.length).toBeLessThanOrEqual(CAP);
		const keys: string[] = [];
		let low: string | undefined;
		for (let i = 0; i < head.bounds.length; i++) {
			const bound = head.bounds[i]!;
			if (low !== undefined) expect(bound > low, 'bounds out of order').toBe(true);
			owned.add(entry + bound);
			const chunk = decodeKeyList(chunks.get(entry + bound) as string);
			expect(chunk.length).toBe(head.counts[i]);
			expect(chunk.length).toBeGreaterThan(0);
			expect(chunk.length).toBeLessThanOrEqual(CAP);
			for (const key of chunk) {
				if (low !== undefined) expect(key > low).toBe(true);
				expect(key <= bound).toBe(true);
			}
			keys.push(...chunk);
			low = bound;
		}
		for (const key of head.tail) if (low !== undefined) expect(key > low).toBe(true);
		keys.push(...head.tail);
		expect(keys).toEqual(sorted(new Set(keys)));
		// A head that shrank to the demotion point went back into its page.
		expect(head.bounds.length > 0 || keys.length > POSTINGS_INLINE_MAX / 2).toBe(true);
		out.set(entry, keys);
	}
	expect(sorted(chunks.keys()), 'a chunk no head names').toEqual(sorted(owned));
	return out;
}

/** One term's row keys, checked through checkedBase and through readTerm. */
async function checkedTerm(tx: KVRead, base: string, entry: string): Promise<string[]> {
	const keys = (await checkedBase(tx, base)).get(entry) ?? [];
	const read = await readTerm(tx, base, entry);
	expect(read === undefined ? [] : await readAllKeys(tx, base, [ read ])).toEqual(keys);
	return keys;
}

async function write(kv: KVStore, rows: [string, string[]][]): Promise<void> {
	const tx = await kv.write();
	const w = new SearchPostingsWriter();
	for (const [ key, entries ] of rows) w.set('doc', WS, SPEC, key, entries);
	await w.flush(tx);
	await tx.commit();
}

async function termKeysOf(kv: KVStore, entry: string): Promise<string[]> {
	const tx = await kv.read();
	try {
		return await checkedTerm(tx, BASE, entry);
	} finally {
		tx.release();
	}
}

async function headOf(kv: KVStore, entry: string) {
	const tx = await kv.read();
	try {
		const raw = await tx.get(postingsHeadKey(BASE, entry));
		return raw === undefined ? undefined : decodeHead(raw as string);
	} finally {
		tx.release();
	}
}

const T = '\u0001t\u0000\u0000';

describe.each(STORES)('the postings of one term (%s)', (_, open) => {
	it('stay in the head up to the cap, and move whole chunks out past it', async () => {
		const kv = await open();
		const keys = Array.from({ length: 2 * CAP + 1 }, (_, i) => `k${pad(i)}`);
		await write(kv, keys.slice(0, CAP).map((k) => [ k, [ T ] ]));
		expect(await headOf(kv, T)).toEqual({ counts: [], bounds: [], tail: keys.slice(0, CAP) });

		await write(kv, [ [ keys[CAP]!, [ T ] ] ]);
		expect(await headOf(kv, T)).toEqual({
			counts: [ CAP ],
			bounds: [ keys[CAP - 1] ],
			tail: [ keys[CAP] ],
		});

		await write(kv, keys.slice(CAP + 1).map((k) => [ k, [ T ] ]));
		const head = (await headOf(kv, T))!;
		expect(head.counts).toEqual([ CAP, CAP ]);
		expect(head.tail).toEqual([ keys[2 * CAP] ]);
		expect(await termKeysOf(kv, T)).toEqual(keys);
		await kv.close();
	});

	it('split a full chunk that a row lands inside, keeping its bound on the upper half', async () => {
		const kv = await open();
		const keys = Array.from({ length: CAP + 1 }, (_, i) => `k${pad(i * 2)}`);
		await write(kv, keys.map((k) => [ k, [ T ] ]));
		const bound = keys[CAP - 1]!;
		expect((await headOf(kv, T))!.bounds).toEqual([ bound ]);

		await write(kv, [ [ `k${pad(1)}`, [ T ] ] ]);
		const head = (await headOf(kv, T))!;
		expect(head.counts).toEqual([ (CAP + 1) / 2 + 0.5, (CAP + 1) / 2 - 0.5 ].map(Math.floor));
		expect(head.bounds[1]).toBe(bound);
		expect(await termKeysOf(kv, T)).toEqual(sorted([ ...keys, `k${pad(1)}` ]));
		await kv.close();
	});

	it('delete a chunk that empties out, and the head with the last row', async () => {
		const kv = await open();
		const keys = Array.from({ length: CAP + 3 }, (_, i) => `k${pad(i)}`);
		await write(kv, keys.map((k) => [ k, [ T ] ]));
		const bound = (await headOf(kv, T))!.bounds[0]!;
		await write(kv, keys.slice(0, CAP).map((k) => [ k, [] ]));
		const tx = await kv.read();
		expect(await tx.has(postingsChunkKey(BASE, T, bound))).toBe(false);
		tx.release();
		// Three rows left: the term went back into its page.
		expect(await headOf(kv, T)).toBeUndefined();
		expect(await termKeysOf(kv, T)).toEqual(keys.slice(CAP));

		await write(kv, keys.slice(CAP).map((k) => [ k, [] ]));
		expect(await termKeysOf(kv, T)).toEqual([]);
		const r = await kv.read();
		expect(await r.has(postingsDirectoryKey(BASE))).toBe(false);
		r.release();
		await kv.close();
	});

	it('order rows whose keys sort first and last of any key', async () => {
		const kv = await open();
		const middle = Array.from({ length: CAP * 2 }, (_, i) => `m${pad(i)}`);
		await write(kv, middle.map((k) => [ k, [ T ] ]));
		const edges = [ '\u0000', '\u0000\u0000', '￿', '￿￿', 'm', `m${pad(0)}\u0000` ];
		await write(kv, edges.map((k) => [ k, [ T ] ]));
		expect(await termKeysOf(kv, T)).toEqual(sorted([ ...middle, ...edges ]));
		await write(kv, edges.map((k) => [ k, [] ]));
		expect(await termKeysOf(kv, T)).toEqual(middle);
		await kv.close();
	});

	it('write nothing for a row recorded with the terms it already has', async () => {
		const kv = await open();
		await write(kv, [ [ 'a', [ T ] ], [ 'b', [ T ] ] ]);
		const tx = await kv.write();
		const puts: string[] = [];
		const put = tx.put.bind(tx);
		tx.put = (k, v) => {
			puts.push(k);
			return put(k, v);
		};
		const w = new SearchPostingsWriter();
		w.set('doc', WS, SPEC, 'a', [ T, T ]);
		await w.flush(tx);
		await tx.commit();
		expect(puts).toEqual([]);
		await kv.close();
	});
});

const entryOf = (term: string) => `\u0001${term}\u0000\u0000`;

async function baseOf(kv: KVStore): Promise<Map<string, string[]>> {
	const tx = await kv.read();
	try {
		return await checkedBase(tx, BASE);
	} finally {
		tx.release();
	}
}

async function directoryOf(kv: KVStore): Promise<string[]> {
	const tx = await kv.read();
	try {
		const raw = await tx.get(postingsDirectoryKey(BASE));
		return raw === undefined ? [] : decodeKeyList(raw as string);
	} finally {
		tx.release();
	}
}

describe.each(STORES)('pages of small terms (%s)', (_, open) => {
	/** Terms of three rows each, about 20 characters a term: several pages'
	 * worth whatever the cap. */
	const many = Array.from({ length: Math.ceil(POSTINGS_PAGE_CAP / 5) }, (_, i) => entryOf(`t${i}`));
	const rowsFor = (entries: string[]): [string, string[]][] =>
		[ 'ka', 'kb', 'kc' ].map((k) => [ k, entries ]);

	it('split past the cap, and the directory names every page', async () => {
		const kv = await open();
		await write(kv, rowsFor(many.slice(0, 10)));
		expect(await directoryOf(kv)).toEqual([ POSTINGS_LAST_PAGE ]);
		await write(kv, rowsFor(many));
		const bounds = await directoryOf(kv);
		expect(bounds.length).toBeGreaterThan(2);
		const stored = await baseOf(kv);
		expect(sorted(stored.keys())).toEqual(sorted(many));
		for (const e of many) expect(stored.get(e)).toEqual([ 'ka', 'kb', 'kc' ]);
		// A term whose entry is a page's bound is found in that page.
		const tx = await kv.read();
		const read = await readTerm(tx, BASE, bounds[0]!);
		tx.release();
		expect(read?.head.tail).toEqual([ 'ka', 'kb', 'kc' ]);
		await kv.close();
	});

	it('fold an emptied or small page into the next one', async () => {
		const kv = await open();
		await write(kv, rowsFor(many));
		const before = (await directoryOf(kv)).length;
		const keep = many.filter((_, i) => i % 40 === 0);
		await write(kv, rowsFor(keep));
		const after = await directoryOf(kv);
		expect(after.length).toBeLessThan(before);
		expect(sorted((await baseOf(kv)).keys())).toEqual(sorted(keep));
		await write(kv, rowsFor([]));
		expect(await directoryOf(kv)).toEqual([]);
		expect(await baseOf(kv)).toEqual(new Map());
		await kv.close();
	});

	it('move a term to a head of its own past the inline limit, and back only at half of it', async () => {
		const kv = await open();
		const e = entryOf('grow');
		const keys = Array.from({ length: POSTINGS_INLINE_MAX + 1 }, (_, i) => `r${pad(i)}`);
		await write(kv, keys.slice(0, POSTINGS_INLINE_MAX).map((k) => [ k, [ e ] ]));
		expect(await headOf(kv, e)).toBeUndefined();
		expect((await baseOf(kv)).get(e)).toEqual(keys.slice(0, POSTINGS_INLINE_MAX));

		await write(kv, [ [ keys[POSTINGS_INLINE_MAX]!, [ e ] ] ]);
		expect((await headOf(kv, e))?.tail).toEqual(keys);
		expect((await baseOf(kv)).get(e)).toEqual(keys);

		// Shrinking to just above half keeps the head, so a term at the limit
		// does not move on every write.
		const half = POSTINGS_INLINE_MAX / 2;
		await write(kv, keys.slice(half + 1).map((k) => [ k, [] ]));
		expect((await headOf(kv, e))?.tail).toEqual(keys.slice(0, half + 1));
		await write(kv, [ [ keys[half]!, [] ] ]);
		expect(await headOf(kv, e)).toBeUndefined();
		expect((await baseOf(kv)).get(e)).toEqual(keys.slice(0, half));
		await kv.close();
	});

	it('answer a prefix across page boundaries, small and large terms in entry order', async () => {
		const kv = await open();
		await write(kv, rowsFor(many));
		// One large term inside the prefix range, and one past it.
		const big = Array.from({ length: POSTINGS_INLINE_MAX * 3 }, (_, i) => `b${pad(i)}`);
		await write(kv, big.map((k) => [ k, [ entryOf('t15x'), entryOf('u0') ] ]));
		expect((await directoryOf(kv)).length).toBeGreaterThan(2);
		const stored = await baseOf(kv);
		for (const prefix of [ 't1', 't', 't39', 't15', 'u', 'zz' ]) {
			const lo = `\u0001${prefix}`;
			const hi = lo.slice(0, -1) + String.fromCharCode(lo.charCodeAt(lo.length - 1) + 1);
			const want = sorted([ ...stored.keys() ].filter((e) => e >= lo && e < hi));
			const tx = await kv.read();
			const got: string[] = [];
			const keys: string[][] = [];
			for await (const t of scanTerms(tx, BASE, lo, hi)) {
				got.push(t.entry);
				keys.push(await readAllKeys(tx, BASE, [ t ]));
			}
			tx.release();
			expect(got, prefix).toEqual(want);
			expect(keys).toEqual(want.map((e) => stored.get(e)));
		}
		await kv.close();
	});
});

describe.each(STORES)('postings under random writes (%s)', (_, open) => {
	it('list exactly the rows a model says hold each term', async () => {
		const rand = prng(42);
		const kv = await open();
		// Few terms over many rows, so the common ones span several chunks,
		// and keys spread so writes land inside chunks as well as at the tail.
		// Six common terms and a long tail of rare ones, which share pages.
		const terms = Array.from({ length: 300 }, (_, i) => `\u0001w${i}\u0000\u0000`);
		const model = new Map<string, Set<string>>();
		const rows = new Map<string, string[]>();
		for (let round = 0; round < 25; round++) {
			const batch: [string, string[]][] = [];
			const n = 1 + Math.floor(rand() * 120);
			for (let i = 0; i < n; i++) {
				const key = `r${pad(Math.floor(rand() * 1500))}`;
				const entries = rand() < 0.15
					? []
					: terms.filter((_, t) => rand() < (t < 6 ? 1 / (t + 1) : 0.02));
				batch.push([ key, entries ]);
			}
			await write(kv, batch);
			for (const [ key, entries ] of batch) {
				for (const e of rows.get(key) ?? []) model.get(e)?.delete(key);
				rows.set(key, entries);
				for (const e of entries) {
					let s = model.get(e);
					if (s === undefined) model.set(e, s = new Set());
					s.add(key);
				}
			}
			const stored = await baseOf(kv);
			for (const t of terms) expect(stored.get(t) ?? []).toEqual(sorted(model.get(t) ?? []));
			for (const t of terms.slice(0, 8)) {
				expect(await termKeysOf(kv, t)).toEqual(sorted(model.get(t) ?? []));
			}
		}
		const tx = await kv.read();
		for (const [ key, entries ] of rows) {
			expect(termListOf(await tx.get(postingsTermsKey(BASE, key)))).toEqual(
				sorted(new Set(entries)),
			);
		}
		// The common term outgrew one chunk and the rare ones several pages, or
		// the run tested less than it claims.
		expect((await readTerm(tx, BASE, terms[0]!))!.head.bounds.length).toBeGreaterThan(1);
		tx.release();
		expect((await directoryOf(kv)).length).toBeGreaterThan(2);
		await kv.close();
	});
});

// Queries served from the postings must answer what the scan answers. The
// vocabulary is small enough that the common words span several chunks, and a
// prefix reaches many of them; a few rows overflow the per-row term cap.
const doc = entity('doc', {
	id: text().key(),
	body: text().searchable(),
	state: text(),
});
type Doc = Infer<typeof doc>;
const docBundle = defineSchema(doc);
const DocSchema = docBundle.descMessage('doc');
const docRegistry = new EntityRegistry([ DocSchema ]);

const WORDS = [
	'alpha',
	'alpine',
	'alps',
	'altar',
	'beta',
	'bet',
	'better',
	'gamma',
	'delta',
	'epsilon',
	'zeta',
	'eta',
];

function body(rand: () => number): string {
	if (rand() < 0.01) {
		return Array.from({ length: SEARCH_TERMS_PER_ROW + 5 }, (_, i) => `u${i}`).join(' ')
			+ ' alpha';
	}
	return WORDS.filter((_, i) => rand() < 0.9 / (1 + i / 2)).join(' ');
}

const docChange = (id: string, b: string, version: bigint) =>
	create(RowChangeSchema, {
		entity: 'doc',
		key: id,
		version,
		body: {
			case: 'value',
			value: toBinary(DocSchema, create(DocSchema, { id, body: b, state: 'open' })),
		},
	});

const docTombstone = (id: string, version: bigint) =>
	create(RowChangeSchema, {
		entity: 'doc',
		key: id,
		version,
		body: { case: 'tombstone', value: true },
	});

const QUERIES: Query<Doc>[] = [
	{ where: (d) => d.match('alpha ') },
	{ where: (d) => d.match('alpha '), limit: 10 },
	{ where: (d) => d.match('alpha '), offset: 300, limit: 25 },
	{ where: (d) => d.match('alpha '), offset: 300, limit: 25, total: false },
	{ where: (d) => d.match('beta '), offset: 5, limit: 400 },
	{ where: (d) => d.match('zeta '), limit: 3 },
	{ where: (d) => d.match('nothing '), limit: 3 },
	{ where: (d) => d.match('u7 ') },
	{ where: (d) => d.match('gam'), limit: 20 },
	{ where: (d) => d.match('al'), limit: 20 },
	{ where: (d) => d.match('al') },
	{ where: (d) => d.match('alp'), offset: 10, limit: 20 },
	{ where: (d) => d.match('alpha delta ') },
	{ where: (d) => d.match('zeta alpha '), limit: 7 },
	{ where: (d) => d.match('alpha'), orderBy: (d) => [ d.relevance() ], limit: 5 },
	{
		where: (d /* and */) => ({ kind: 'and', conds: [ d.match('beta '), d.state.eq('open') ] }),
		limit: 9,
	},
];

describe('a build that writes postings across batches', () => {
	it('leaves the postings of rows a pull changed between its batches to that pull', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(docRegistry);
		// Rows without postings, as a staged bootstrap swap leaves them.
		const seed = await kv.write();
		await applyServerChanges(
			seed,
			docRegistry,
			WS,
			Array.from({ length: 700 }, (_, i) => docChange(`d${pad(i)}`, 'alpha beta', BigInt(i + 1))),
			new TouchSet(),
		);
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(metaSnapshotGen(WS), 1);
		await seed.commit();

		// After the first batch, a pull rewrites a row the build has visited,
		// deletes another, and rewrites one it has not reached yet.
		let batches = 0;
		const runLocked = async <T>(fn: () => Promise<T>): Promise<T> => {
			const out = await fn();
			if (++batches === 1) {
				const tx = await kv.write();
				await applyServerChanges(
					tx,
					docRegistry,
					WS,
					[
						docChange('d00005', 'gamma', 1000n),
						docTombstone('d00006', 1001n),
						docChange('d00650', 'gamma alpha', 1002n),
					],
					new TouchSet(),
					indexes,
				);
				await tx.commit();
			}
			return out;
		};
		const built = await buildIndexes(kv, runLocked, docRegistry, indexes, WS);
		expect(built.ready).toBe(true);

		const base = searchPostingsBase('doc', WS, searchIndexOf(indexes.for('doc'))!.specHash);
		const all = Array.from({ length: 700 }, (_, i) => `d${pad(i)}`);
		const tx = await kv.read();
		try {
			expect(await checkedTerm(tx, base, '\u0001alpha\u0000\u0000')).toEqual(
				all.filter((k) => k !== 'd00005' && k !== 'd00006'),
			);
			expect(await checkedTerm(tx, base, '\u0001beta\u0000\u0000')).toEqual(
				all.filter((k) => k !== 'd00005' && k !== 'd00006' && k !== 'd00650'),
			);
			expect(await checkedTerm(tx, base, '\u0001gamma\u0000\u0000')).toEqual([ 'd00005', 'd00650' ]);
			expect(termListOf(await tx.get(postingsTermsKey(base, 'd00006')))).toEqual([]);
		} finally {
			tx.release();
		}
	});
});

describe('queries over postings under random writes', () => {
	it('answer what the scan answers, page for page', async () => {
		const rand = prng(7);
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(docRegistry);
		const seed = await kv.write();
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(metaSnapshotGen(WS), 1);
		await markIndexesBuilt(seed, indexes, WS, [ 'doc' ]);
		await seed.commit();
		const marker = await (async () => {
			const tx = await kv.read();
			const out: string[] = [];
			for await (const [ k ] of tx.scan(`meta/ws/${WS}/index/doc/`)) out.push(k);
			tx.release();
			return out;
		})();

		let version = 1n;
		const live = new Set<string>();
		for (let round = 0; round < 6; round++) {
			const changes = [];
			const n = round === 0 ? 900 : 150;
			for (let i = 0; i < n; i++) {
				const id = `d${pad(Math.floor(rand() * 1200))}`;
				if (live.has(id) && rand() < 0.25) {
					changes.push(docTombstone(id, version++));
					live.delete(id);
				} else {
					changes.push(docChange(id, body(rand), version++));
					live.add(id);
				}
			}
			const tx = await kv.write();
			await applyServerChanges(tx, docRegistry, WS, changes, new TouchSet(), indexes);
			await tx.commit();
			await compareAll(kv, marker, round === 5);
		}
		const base = searchPostingsBase('doc', WS, searchIndexOf(indexes.for('doc'))!.specHash);
		const tx = await kv.read();
		expect((await readTerm(tx, base, '\u0001alpha\u0000\u0000'))!.head.bounds.length)
			.toBeGreaterThan(1);
		tx.release();
	});

	it('answer what the scan answers past a candidate budget the walk declines at', async () => {
		const rand = prng(11);
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(docRegistry);
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await markIndexesBuilt(tx, indexes, WS, [ 'doc' ]);
		await applyServerChanges(
			tx,
			docRegistry,
			WS,
			Array.from({ length: 700 }, (_, i) => docChange(`d${pad(i)}`, body(rand), BigInt(i + 1))),
			new TouchSet(),
			indexes,
		);
		await tx.commit();
		const restore = withIndexCandidateBudget(300);
		try {
			await compareAll(kv, [ `meta/ws/${WS}/index/doc/` ], true);
		} finally {
			restore();
		}
	});
});

interface Answer {
	rows: string[];
	total: number | undefined;
	hasMore: boolean;
	path: string | undefined;
}

/**
 * Every query of QUERIES twice — with the markers in place, then with them
 * removed so the client scans — and the two answers compared. The markers are
 * put back afterwards. MemoryKVStore stays usable after a client closes it.
 */
async function compareAll(kv: KVStore, markers: string[], withOverlay: boolean): Promise<void> {
	const saved: [string, unknown][] = [];
	{
		const tx = await kv.read();
		for (const m of markers) for await (const e of tx.scan(m)) saved.push(e);
		tx.release();
	}
	const answer = async (): Promise<Answer[]> => {
		const events: QueryEvent[] = [];
		const client = new SyncClient({
			workspaceId: WS,
			kvStore: kv,
			entities: [ DocSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
			onSyncEvent: (e) => {
				if (e.type === 'query') events.push(e);
			},
		}) as unknown as SyncClient<never>;
		try {
			// A pending edit and a pending delete, so the overlay merge and the
			// count subtraction run over postings that describe other text.
			if (withOverlay) {
				await client.mutate.put(
					DocSchema,
					{ id: 'd00003', body: 'alpha beta', state: 'open' } as never,
				);
				await client.mutate.put(DocSchema, { id: 'd00004', body: 'tea', state: 'open' } as never);
				await client.mutate.del(DocSchema, 'd00005');
			}
			const out: Answer[] = [];
			for (const q of QUERIES) {
				const rows = await client.query(DocSchema, q as never) as unknown as RowSet<Doc>;
				out.push({
					rows: rows.map((r) => r.key),
					total: rows.total,
					hasMore: rows.hasMore,
					path: events.at(-1)?.index,
				});
			}
			return out;
		} finally {
			await client.close();
		}
	};
	const indexed = await answer();
	const tx = await kv.write();
	for (const [ k ] of saved) await tx.del(k);
	await tx.commit();
	const scanned = await answer();
	const back = await kv.write();
	for (const [ k, v ] of saved) await back.put(k, v);
	await back.commit();
	for (let i = 0; i < QUERIES.length; i++) {
		const { path: scanPath, ...want } = scanned[i]!;
		const { path: _path, ...got } = indexed[i]!;
		expect(got, `query ${i}`).toEqual(want);
		expect(scanPath, `query ${i} scanned`).toBeUndefined();
	}
	expect(indexed.some((r) => r.path === '__search')).toBe(true);
}
