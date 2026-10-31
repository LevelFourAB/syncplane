/**
 * The page half of 08-search-maintenance.spec.ts: what keeping the full-text
 * search index current costs the client, apart from the network and the sync
 * protocol.
 *
 * It drives the client's own write paths — the cold-bootstrap chunk writer,
 * the pull apply and the background index build — against a real IDBKVStore,
 * with row changes made here rather than pulled from a server. The text is
 * shaped like the size benchmark's corpus (see benchapp.SeedSearchSize): an
 * 8-word title and a 300-word body of Zipf-distributed words from a
 * 20,000-word vocabulary, and keys shaped like `newId()` ids.
 *
 * Every scenario runs twice: on `doc`, whose title and body are searchable,
 * and on `plain`, the same fields with no search declaration. The difference
 * between the two is the search maintenance; `plain` alone is the cost of the
 * rows.
 */

import { create, type DescMessage, toBinary } from '@bufbuild/protobuf';

import { SyncClient } from '../../../src/client.js';
import { RowChangeSchema } from '../../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor, metaSnapshotGen } from '../../../src/keys.js';
import { IDBKVStore } from '../../../src/kv/idb.js';
import { EntityRegistry } from '../../../src/mutations.js';
import { buildIndexes, markIndexesBuilt } from '../../../src/optimistic/index-build.js';
import { IndexCache } from '../../../src/optimistic/index-maintain.js';
import { applyBootstrapRows, applyServerChanges } from '../../../src/optimistic/rebase.js';
import { SearchPostingsWriter } from '../../../src/query/postings.js';
import { defineSchema, entity, text } from '../../../src/schema/builder.js';
import { TouchSet } from '../../../src/touch.js';

const docEntity = entity('doc', {
	id: text().key(),
	title: text().searchable(),
	body: text().searchable(),
	state: text(),
});
const plainEntity = entity('plain', {
	id: text().key(),
	title: text(),
	body: text(),
	state: text(),
});
const bundle = defineSchema(docEntity, plainEntity);
const SCHEMAS: Record<Kind, DescMessage> = {
	doc: bundle.descMessage('doc'),
	plain: bundle.descMessage('plain'),
};

export type Kind = 'doc' | 'plain';

const WS = '06DQDPN801W2V44AN45G16BNJG';
const ROWS = 2000;
const VOCAB = 20000;
const BODY_WORDS = 300;
const TITLE_WORDS = 8;
/** Rows per cold-bootstrap chunk, as the server sends them. */
const BOOTSTRAP_CHUNK = 256;

/** mulberry32: a small deterministic PRNG, so every run writes the same text. */
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

const rand = prng(7);

const SYLLABLES = [
	'ka',
	'lo',
	'mi',
	'ne',
	'ru',
	'sa',
	'ti',
	'vo',
	'ze',
	'pra',
	'stel',
	'dor',
	'fin',
	'gal',
	'hus',
	'jen',
	'kor',
	'lin',
	'mor',
	'nes',
	'pol',
	'qui',
	'ras',
	'sor',
	'tan',
	'ul',
	'ver',
	'wen',
	'xi',
	'yor',
];

const vocab: string[] = (() => {
	const seen = new Set<string>();
	const out: string[] = [];
	while (out.length < VOCAB) {
		let w = '';
		for (let s = 2 + Math.floor(rand() * 4); s > 0; s--) {
			w += SYLLABLES[Math.floor(rand() * SYLLABLES.length)];
		}
		if (seen.has(w)) continue;
		seen.add(w);
		out.push(w);
	}
	return out;
})();

/** Zipf (s = 1.07, v = 2) over the vocabulary, as an inverse CDF. */
const zipfCdf: Float64Array = (() => {
	const cdf = new Float64Array(VOCAB);
	let sum = 0;
	for (let k = 0; k < VOCAB; k++) {
		sum += 1 / Math.pow(2 + k, 1.07);
		cdf[k] = sum;
	}
	for (let k = 0; k < VOCAB; k++) cdf[k]! /= sum;
	return cdf;
})();

function zipfWord(): string {
	const u = rand();
	let lo = 0;
	let hi = VOCAB - 1;
	while (lo < hi) {
		const mid = (lo + hi) >>> 1;
		if (zipfCdf[mid]! < u) lo = mid + 1;
		else hi = mid;
	}
	return vocab[lo]!;
}

function words(n: number): string[] {
	const out: string[] = [];
	for (let i = 0; i < n; i++) out.push(zipfWord());
	return out;
}

const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

/** A `newId()`-shaped key: a millisecond timestamp, then 80 random bits. */
function idAt(ms: number): string {
	const b = new Uint8Array(16);
	for (let j = 6; j < 16; j++) b[j] = Math.floor(rand() * 256);
	let t = ms;
	for (let j = 5; j >= 0; j--) {
		b[j] = t % 256;
		t = Math.floor(t / 256);
	}
	b[6] = (b[6]! & 0x0f) | 0x70;
	b[8] = (b[8]! & 0x3f) | 0x80;
	let out = '';
	let value = 0;
	let bits = 0;
	for (const byte of b) {
		value = (value << 8) | byte;
		bits += 8;
		while (bits >= 5) {
			out += CROCKFORD[(value >>> (bits - 5)) & 31];
			bits -= 5;
		}
	}
	if (bits > 0) out += CROCKFORD[(value << (5 - bits)) & 31];
	return out;
}

interface Doc {
	id: string;
	title: string;
	body: string[];
}

const BASE_MS = 1_767_225_600_000;
const seeded: Doc[] = [];
for (let i = 0; i < ROWS; i++) {
	seeded.push({
		id: idAt(BASE_MS + i),
		title: words(TITLE_WORDS).join(' '),
		body: words(BODY_WORDS),
	});
}
/** Rows a pull inserts: keys after every seeded one, as new ids are. */
const inserted: Doc[] = [];
for (let i = 0; i < 500; i++) {
	inserted.push({
		id: idAt(BASE_MS + 10_000_000 + i),
		title: words(TITLE_WORDS).join(' '),
		body: words(BODY_WORDS),
	});
}
/** Every seeded row with three body words replaced. */
const edited: Doc[] = seeded.map((d) => {
	const body = [ ...d.body ];
	for (let n = 0; n < 3; n++) body[Math.floor(rand() * body.length)] = zipfWord();
	return { ...d, body };
});

let version = 1n;

function change(kind: Kind, d: Doc) {
	const schema = SCHEMAS[kind];
	return create(RowChangeSchema, {
		entity: kind,
		key: d.id,
		version: version++,
		body: {
			case: 'value',
			value: toBinary(
				schema,
				create(schema, { id: d.id, title: d.title, body: d.body.join(' '), state: 'open' }),
			),
		},
	});
}

function tombstone(kind: Kind, d: Doc) {
	return create(RowChangeSchema, {
		entity: kind,
		key: d.id,
		version: version++,
		body: { case: 'tombstone', value: true },
	});
}

const registry = new EntityRegistry([ SCHEMAS.doc, SCHEMAS.plain ]);

async function timed(fn: () => Promise<void>): Promise<number> {
	const t0 = performance.now();
	await fn();
	return performance.now() - t0;
}

async function openStore(name: string): Promise<IDBKVStore> {
	await deleteDb(name);
	return IDBKVStore.open(name);
}

function deleteDb(name: string): Promise<void> {
	return new Promise((resolve, reject) => {
		const req = indexedDB.deleteDatabase(name);
		req.onsuccess = () => resolve();
		req.onerror = () => reject(req.error);
		req.onblocked = () => resolve();
	});
}

/** One pull page: `changes` applied and committed in one transaction. */
async function pull(kv: IDBKVStore, indexes: IndexCache, changes: ReturnType<typeof change>[]) {
	const tx = await kv.write();
	await applyServerChanges(tx, registry, WS, changes, new TouchSet(), indexes);
	await tx.commit();
}

/**
 * A cold bootstrap of the seeded rows, one transaction per chunk, with the
 * search postings recorded across chunks and written in a transaction of
 * their own once the rows are in, as Coordinator.doBootstrap does.
 */
async function bootstrap(kv: IDBKVStore, indexes: IndexCache, kind: Kind) {
	const postings = new SearchPostingsWriter();
	for (let at = 0; at < seeded.length; at += BOOTSTRAP_CHUNK) {
		const tx = await kv.write();
		await applyBootstrapRows(
			tx,
			registry,
			WS,
			seeded.slice(at, at + BOOTSTRAP_CHUNK).map((d) => change(kind, d)),
			new TouchSet(),
			indexes,
			undefined,
			postings,
		);
		await tx.commit();
	}
	const flush = await kv.write();
	await postings.flush(flush);
	await flush.commit();
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await tx.put(metaSnapshotGen(WS), 1);
	await markIndexesBuilt(tx, indexes, WS, [ kind ]);
	await tx.commit();
}

export interface MaintainResult {
	kind: Kind;
	run: number;
	bootstrapMs: number;
	insert500Ms: number;
	edit500Ms: number;
	edit20Ms: number;
	delete500Ms: number;
	buildMs: number;
	buildRows: number;
	/** The longest the build held the store's write lock in one batch. */
	buildLockMs: number;
	/** Warm search times on the bootstrapped store, `doc` only. */
	search?: Record<string, { ms: number; rows: number; total: number | undefined }>;
}

/**
 * The search terms by how many rows carry them: the most frequent word, one
 * of rank 100, one of rank 3000, a prefix and a two-word query.
 */
const SEARCHES: Record<string, string> = {
	common: `${vocab[0]} `,
	mid: `${vocab[100]} `,
	rare: `${vocab[3000]} `,
	prefix: vocab[100]!.slice(0, 4),
	'common+mid': `${vocab[0]} ${vocab[100]} `,
};

async function searches(kv: IDBKVStore): Promise<MaintainResult['search']> {
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ SCHEMAS.doc, SCHEMAS.plain ],
		streamEnabled: false,
		pollIntervalMs: 0,
	}) as unknown as SyncClient<never>;
	const out: NonNullable<MaintainResult['search']> = {};
	try {
		for (const [ name, terms ] of Object.entries(SEARCHES)) {
			const q = { where: (d: { match(t: string): unknown }) => d.match(terms), limit: 50 };
			// Cold once, then the median of five warm reads.
			await client.query(SCHEMAS.doc, q as never);
			const times: number[] = [];
			let rows = 0;
			let total: number | undefined;
			for (let i = 0; i < 5; i++) {
				const t0 = performance.now();
				const r = await client.query(SCHEMAS.doc, q as never) as unknown as {
					length: number;
					total?: number;
				};
				times.push(performance.now() - t0);
				rows = r.length;
				total = r.total;
			}
			times.sort((a, b) => a - b);
			out[name] = { ms: times[2]!, rows, total };
		}
	} finally {
		// Closing the client closes the store it was handed.
		await client.close();
	}
	return out;
}

async function run(kind: Kind, n: number): Promise<MaintainResult> {
	const name = `maintain-${kind}-${n}`;
	const indexes = new IndexCache(registry);
	let kv = await openStore(name);
	const bootstrapMs = await timed(() => bootstrap(kv, indexes, kind));
	const insert500Ms = await timed(() => pull(kv, indexes, inserted.map((d) => change(kind, d))));
	const edit500Ms = await timed(() =>
		pull(kv, indexes, edited.slice(0, 500).map((d) => change(kind, d)))
	);
	const edit20Ms = await timed(() =>
		pull(kv, indexes, edited.slice(1000, 1020).map((d) => change(kind, d)))
	);
	const delete500Ms = await timed(() =>
		pull(kv, indexes, seeded.slice(1500, 2000).map((d) => tombstone(kind, d)))
	);
	let search: MaintainResult['search'];
	if (kind === 'doc') search = await searches(kv);
	else await kv.close();
	await deleteDb(name);

	// The build: the same rows written without positions, as a staged
	// bootstrap swap leaves them, then one pass over them.
	kv = await openStore(name);
	for (let at = 0; at < seeded.length; at += BOOTSTRAP_CHUNK) {
		const tx = await kv.write();
		await applyServerChanges(
			tx,
			registry,
			WS,
			seeded.slice(at, at + BOOTSTRAP_CHUNK).map((d) => change(kind, d)),
			new TouchSet(),
		);
		await tx.commit();
	}
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await tx.put(metaSnapshotGen(WS), 1);
	await tx.commit();
	let buildRows = 0;
	let buildLockMs = 0;
	const buildMs = await timed(async () => {
		const built = await buildIndexes(
			kv,
			async (fn) => {
				const t0 = performance.now();
				try {
					return await fn();
				} finally {
					buildLockMs = Math.max(buildLockMs, performance.now() - t0);
				}
			},
			registry,
			indexes,
			WS,
		);
		buildRows = built.rows;
	});
	await kv.close();
	await deleteDb(name);
	return {
		kind,
		run: n,
		bootstrapMs,
		insert500Ms,
		edit500Ms,
		edit20Ms,
		delete500Ms,
		buildMs,
		buildRows,
		buildLockMs,
		search,
	};
}

declare global {
	interface Window {
		maintainBench: { run(kind: Kind, n: number): Promise<MaintainResult> };
	}
}

window.maintainBench = { run };
