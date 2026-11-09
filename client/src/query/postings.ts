// The search index's postings, stored as ordinary KV records: an inverted
// index from each term to the rows that hold it. Nothing here occupies a
// position in the store's secondary index (kv/store.ts INDEX_FIELD).
//
// A posting names its row by a ROW NUMBER, not by the row key. The index gives
// each row it posts a small integer, and a term lists the numbers of its rows
// in ascending order, each as the difference from the one before. A posting
// then costs one or two bytes whatever the length of the row key, and the
// index holds each row key twice in all: once in each of the two maps below.
//
// Every record of one search index lives under one BASE: the postings root,
// the (entity, workspace) scope and the spec tag (see keys.ts
// searchPostingsBase). After the base, one character says what the record is:
//
// 	base 'd'                  the DIRECTORY: the upper bound of every page
// 	base 'p' bound            one PAGE: the small terms in (previous bound,
// 	                          bound], each with its row numbers, and a mark
// 	                          for each large term in that range
// 	base 'h' entry            the HEAD of a large term: its chunk directory
// 	                          and its tail, the row numbers above every chunk
// 	base 'c' entry bound      one CHUNK of a large term: the row numbers in
// 	                          (previous bound, bound]
// 	base 'k'                  the HEAD of the row map: the next row number to
// 	                          give out, and the chunk directory of the map
// 	base 'm' bound            one CHUNK of the row map: the posted row keys
// 	                          in (previous bound, bound], each with its number
// 	base 'l'                  the LAST chunk of the row map: the posted row
// 	                          keys above every bound
// 	base 'n' block            one BLOCK of row keys by number: the keys of the
// 	                          rows numbered block × ROW_BLOCK and up
//
// `entry` is the search entry of query/searchindex.ts — a tag, then the term
// escaped and terminated — so terms sort by entry, a prefix term brackets a
// range of them, and the overflow pseudo-term (whose tag sorts above every
// term) is in no term range.
//
// WHY PAGES. A write costs about one record per distinct term it changes, and
// on IndexedDB a record costs far more than its bytes. Most terms of a
// vocabulary are rare, so a pull of a few hundred text-heavy rows changes
// thousands of them. Pages put the rare terms of one entry range in one
// record, so such a pull rewrites the pages and the heads of the common terms
// instead of one head per term. A term moves to a head of its own past
// POSTINGS_INLINE_MAX rows, and back at half of that, so a term near the limit
// does not move on every write.
//
// The directory is what lets a writer find the page of every term it changes
// with one read; a reader finds the one page it needs by a seek instead.
//
// A large term's row numbers are, in order: chunk 0, chunk 1, …, then the
// tail. The head lists each chunk's bound and count, so a reader knows every
// chunk key and the term's posting count from one record, and a writer finds
// the chunk a row number belongs to — the first whose bound is not below it,
// else the tail — without a search of the keyspace. A new row gets the
// highest number, so its postings append to the tail.
//
// THE ROW MAP. A row gets its number when the index first posts it, keeps it
// while the index posts it, and the number is not given out again. Two maps
// hold the pairs. The row map is in row-key order: a writer finds the number
// of a row it changes there, and a reader that wants the rows of a term in key
// order walks it and asks the term about each number. The blocks are in number
// order: a reader that holds numbers finds their keys there.
//
// A row in the row map is a POSTED row: the index holds the terms of its
// value. The index keeps no list of the terms of a row. A writer gets the
// terms a row held from the row's earlier value, and the row map says whether
// those terms were posted.
//
// Numbers follow the order rows are posted in. A bootstrap and a build post
// rows in key order, so number order is then key order and each map chunk
// names one run of numbers. Rows posted later can have keys anywhere. Nothing
// depends on the two orders agreeing: the answers are the same, and a reader
// in key order then reads more of a term's chunks.
//
// Values are strings of one character per byte where the row keys allow it —
// lengths, counts and numbers are base-128 varints in \u0000-ÿ — so an
// engine keeps them at one byte per character. Lists of keys are front-coded:
// each key is the length it shares with the previous one, the length of the
// rest, and the rest.

import {
	type IndexBuildCursor,
	metaIndexBuildCursor,
	metaIndexBuilt,
	metaIndexEntityPrefix,
	SEARCH_SPEC_TAG_LENGTH,
	searchPostingsScope,
	searchSpecTag,
} from '../keys.js';
import type { KVRead, KVWrite } from '../kv/store.js';
import { SEARCH_INDEX_NAME } from './searchindex.js';

/**
 * Row numbers one chunk, or a large term's tail, holds at most. A chunk that
 * outgrows it splits; a tail that outgrows it moves its lowest numbers into
 * new chunks.
 */
export const POSTINGS_CHUNK_CAP = 1024;

/**
 * Rows a term may post inside a page. A term past it moves to a head of its
 * own; a term with a head and no chunk moves back at half of it.
 */
export const POSTINGS_INLINE_MAX = 512;

/**
 * Characters a page holds before it splits. A page with one term may hold
 * more: a term is never split across pages.
 */
export const POSTINGS_PAGE_CAP = 16384;

/** Rows one chunk of the row map holds at most. A chunk past it splits. */
export const ROW_MAP_CHUNK_CAP = 256;

/** Row numbers one block of row keys answers for. */
export const ROW_BLOCK = 64;

const DIRECTORY = 'd';
const PAGE = 'p';
const HEAD = 'h';
const CHUNK = 'c';
const MAP_HEAD = 'k';
const MAP_CHUNK = 'm';
const MAP_LAST = 'l';
const BLOCK = 'n';

/**
 * The upper bound of the last page. Every entry opens with a tag below it, so
 * every entry is in some page.
 */
export const POSTINGS_LAST_PAGE = '\u0003';

/** The page directory of one search index. */
export const postingsDirectoryKey = (base: string): string => base + DIRECTORY;

/** The page whose upper bound is the entry `bound`. */
export const postingsPageKey = (base: string, bound: string): string => base + PAGE + bound;

/** The head record of the large term with search entry `entry`. */
export const postingsHeadKey = (base: string, entry: string): string => base + HEAD + entry;

/** The chunk of `entry` whose upper bound is the row number `bound`. */
export const postingsChunkKey = (base: string, entry: string, bound: number): string =>
	base + CHUNK + entry + bound.toString(36);

/** The head record of the row map. */
export const rowMapHeadKey = (base: string): string => base + MAP_HEAD;

/**
 * The chunk of the row map whose upper bound is the row key `bound`, or the
 * last chunk for an undefined bound.
 */
export const rowMapChunkKey = (base: string, bound: string | undefined): string =>
	bound === undefined ? base + MAP_LAST : base + MAP_CHUNK + bound;

/** The block that holds the key of the row numbered `row`. */
export const rowBlockKey = (base: string, row: number): string =>
	base + BLOCK + Math.floor(row / ROW_BLOCK).toString(36);

const upperChar = (c: string): string => String.fromCharCode(c.charCodeAt(0) + 1);

// --- codec ---------------------------------------------------------------

class Reader {
	at = 0;
	constructor(readonly s: string) {}

	varint(): number {
		let v = 0;
		let shift = 0;
		for (;;) {
			const c = this.s.charCodeAt(this.at++);
			if (Number.isNaN(c)) throw new Error('postings: truncated value');
			v += (c & 0x7f) * 2 ** shift;
			if (c < 0x80) return v;
			shift += 7;
		}
	}

	/** Steps over one list of numbers without building it. */
	skipNumbers(): void {
		const n = this.varint();
		for (let i = 0; i < n; i++) this.varint();
	}

	numbers(): number[] {
		const n = this.varint();
		const out = new Array<number>(n);
		let prev = -1;
		for (let i = 0; i < n; i++) {
			prev += this.varint();
			out[i] = prev;
		}
		return out;
	}

	list(): string[] {
		const n = this.varint();
		const out = new Array<string>(n);
		let prev = '';
		for (let i = 0; i < n; i++) {
			const shared = this.varint();
			const len = this.varint();
			const end = this.at + len;
			if (end > this.s.length || shared > prev.length) {
				throw new Error('postings: truncated value');
			}
			prev = prev.slice(0, shared) + this.s.slice(this.at, end);
			this.at = end;
			out[i] = prev;
		}
		return out;
	}
}

function varint(v: number): string {
	let out = '';
	while (v >= 0x80) {
		out += String.fromCharCode((v % 0x80) | 0x80);
		v = Math.floor(v / 0x80);
	}
	return out + String.fromCharCode(v);
}

/** `keys` front-coded, in the order given, behind their count. */
export function encodeKeyList(keys: readonly string[]): string {
	let out = varint(keys.length);
	let prev = '';
	for (const k of keys) {
		const max = Math.min(prev.length, k.length);
		let shared = 0;
		while (shared < max && prev.charCodeAt(shared) === k.charCodeAt(shared)) shared++;
		out += varint(shared) + varint(k.length - shared) + k.slice(shared);
		prev = k;
	}
	return out;
}

/** Inverse of encodeKeyList. Throws on a value it did not produce. */
export function decodeKeyList(s: string): string[] {
	return new Reader(s).list();
}

/**
 * `rows` behind their count, each as its distance from the one before. The
 * numbers must ascend without repeats.
 */
export function encodeRows(rows: readonly number[]): string {
	let out = varint(rows.length);
	let prev = -1;
	for (const n of rows) {
		out += varint(n - prev);
		prev = n;
	}
	return out;
}

/** Inverse of encodeRows. Throws on a truncated value. */
export function decodeRows(s: string): number[] {
	return new Reader(s).numbers();
}

/**
 * A large term's head: the chunks in number order, each by its count and its
 * upper bound, and the tail — the row numbers above the last bound, ascending.
 */
export interface PostingsHead {
	counts: number[];
	bounds: number[];
	tail: number[];
}

/** How many rows a head's postings name. */
export function headCount(head: PostingsHead): number {
	let n = head.tail.length;
	for (const c of head.counts) n += c;
	return n;
}

export function encodeHead(head: PostingsHead): string {
	let out = varint(head.counts.length);
	for (const c of head.counts) out += varint(c);
	return out + encodeRows(head.bounds) + encodeRows(head.tail);
}

/** Inverse of encodeHead. Throws on a value it did not produce. */
export function decodeHead(s: string): PostingsHead {
	const r = new Reader(s);
	const n = r.varint();
	const counts: number[] = [];
	for (let i = 0; i < n; i++) counts.push(r.varint());
	const bounds = r.numbers();
	if (bounds.length !== n) throw new Error('postings: head directory mismatch');
	return { counts, bounds, tail: r.numbers() };
}

/**
 * One term of a page: its ascending row numbers, or undefined for a large
 * term, whose postings are under its own head.
 */
export interface PageTerm {
	entry: string;
	rows: number[] | undefined;
}

/**
 * A page: its terms' entries, front-coded, then each term's row numbers. An
 * empty list marks a large term — a small term with no rows is not stored.
 */
export function encodePage(terms: readonly PageTerm[]): string {
	let out = encodeKeyList(terms.map((t) => t.entry));
	for (const t of terms) out += encodeRows(t.rows ?? []);
	return out;
}

/** Inverse of encodePage. Throws on a value it did not produce. */
export function decodePage(s: string): PageTerm[] {
	const r = new Reader(s);
	const entries = r.list();
	return entries.map((entry) => {
		const rows = r.numbers();
		return { entry, rows: rows.length === 0 ? undefined : rows };
	});
}

/** The head stored at a key, or undefined for an absent or foreign value. */
function headOf(value: unknown): PostingsHead | undefined {
	return typeof value === 'string' ? decodeHead(value) : undefined;
}

/** The row numbers stored at a key: none for an absent or foreign value. */
function rowsOf(value: unknown): number[] {
	return typeof value === 'string' ? decodeRows(value) : [];
}

/** First index whose bound is not below `at`, or the bound count. */
function boundFor<T extends string | number>(bounds: readonly T[], at: T): number {
	let lo = 0;
	let hi = bounds.length;
	while (lo < hi) {
		const mid = (lo + hi) >>> 1;
		if (bounds[mid]! < at) lo = mid + 1;
		else hi = mid;
	}
	return lo;
}

// --- the row map ---------------------------------------------------------

/** The head of the row map. */
interface RowMapHead {
	/** The number the next posted row gets. */
	next: number;
	/** Rows in each chunk, the last chunk's count at the end. */
	counts: number[];
	/** The upper bound of each chunk but the last, ascending. */
	bounds: string[];
}

const NO_ROW_MAP: RowMapHead = Object.freeze({
	next: 0,
	counts: [],
	bounds: [],
}) as unknown as RowMapHead;

function encodeRowMapHead(head: RowMapHead): string {
	let out = varint(head.next) + varint(head.counts.length);
	for (const c of head.counts) out += varint(c);
	return out + encodeKeyList(head.bounds);
}

function rowMapHeadOf(value: unknown): RowMapHead {
	if (typeof value !== 'string') return NO_ROW_MAP;
	const r = new Reader(value);
	const next = r.varint();
	const n = r.varint();
	const counts: number[] = [];
	for (let i = 0; i < n; i++) counts.push(r.varint());
	return { next, counts, bounds: r.list() };
}

/** One row of the row map: its key and its number. */
export type PostedRow = [key: string, row: number];

/**
 * A chunk of the row map: its keys, front-coded in key order, then each key's
 * number as its signed distance from the number before, so a run of rows
 * posted in key order costs one character a row.
 */
function encodeRowMapChunk(rows: readonly PostedRow[]): string {
	let out = encodeKeyList(rows.map(([ k ]) => k));
	let prev = 0;
	for (const [ , n ] of rows) {
		const d = n - prev;
		out += varint(d >= 0 ? d * 2 : -d * 2 - 1);
		prev = n;
	}
	return out;
}

function rowMapChunkOf(value: unknown): PostedRow[] {
	if (typeof value !== 'string') return [];
	const r = new Reader(value);
	const keys = r.list();
	let prev = 0;
	return keys.map((k): PostedRow => {
		const z = r.varint();
		prev += z % 2 === 0 ? z / 2 : -(z + 1) / 2;
		return [ k, prev ];
	});
}

/** The keys of one block, by position; an empty key is a number with no row. */
function blockOf(value: unknown): string[] {
	return typeof value === 'string' ? decodeKeyList(value) : [];
}

/** How many rows the index under `base` posts. */
export async function postedRowCount(tx: KVRead, base: string): Promise<number> {
	let n = 0;
	for (const c of rowMapHeadOf(await tx.get(rowMapHeadKey(base))).counts) n += c;
	return n;
}

/**
 * The posted rows of the index under `base` in row-key order, one chunk of
 * the row map at a time, read as the iteration reaches them: the first
 * `first` chunks in one request, and twice as many in each request after it.
 */
export async function* scanPostedRows(
	tx: KVRead,
	base: string,
	first = 1,
): AsyncIterable<PostedRow[]> {
	const head = rowMapHeadOf(await tx.get(rowMapHeadKey(base)));
	if (head.counts.length === 0) return;
	const keys = [
		...head.bounds.map((b) => rowMapChunkKey(base, b)),
		rowMapChunkKey(base, undefined),
	];
	let want = Math.max(1, first);
	for (let i = 0; i < keys.length;) {
		const chunks = await tx.getMany(keys.slice(i, i + want));
		for (const v of chunks) yield rowMapChunkOf(v);
		i += want;
		want *= 2;
	}
}

/**
 * The key of each of `rows` in the index under `base`, by one read of the
 * blocks that hold them. A number the index gives no row has no entry.
 */
export async function rowKeysOf(
	tx: KVRead,
	base: string,
	rows: Iterable<number>,
): Promise<Map<number, string>> {
	const wanted = new Map<string, number[]>();
	for (const n of rows) {
		const k = rowBlockKey(base, n);
		const held = wanted.get(k);
		if (held === undefined) wanted.set(k, [ n ]);
		else held.push(n);
	}
	const out = new Map<number, string>();
	if (wanted.size === 0) return out;
	const blocks = await tx.getMany([ ...wanted.keys() ]);
	let at = 0;
	for (const numbers of wanted.values()) {
		const keys = blockOf(blocks[at++]);
		for (const n of numbers) {
			const key = keys[n % ROW_BLOCK];
			if (key !== undefined && key !== '') out.set(n, key);
		}
	}
	return out;
}

// --- reads ---------------------------------------------------------------

/**
 * One term's postings as a reader holds them. A small term's row numbers are
 * all in `head.tail`, with no chunk.
 */
export interface TermHead {
	entry: string;
	head: PostingsHead;
	count: number;
}

const inlineTerm = (entry: string, rows: number[]): TermHead => ({
	entry,
	head: { counts: [], bounds: [], tail: rows },
	count: rows.length,
});

const largeTerm = (entry: string, head: PostingsHead): TermHead => ({
	entry,
	head,
	count: headCount(head),
});

/**
 * The postings of one term, or undefined when no row holds the term. Its head
 * and its page are looked for at once, so a probe costs two requests to the
 * store whether the term is large or small.
 */
export async function readTerm(
	tx: KVRead,
	base: string,
	entry: string,
): Promise<TermHead | undefined> {
	const [ raw, pageKey ] = await Promise.all([
		tx.get(postingsHeadKey(base, entry)),
		tx.seek(postingsPageKey(base, entry), base + upperChar(PAGE)),
	]);
	const head = headOf(raw);
	if (head !== undefined) return largeTerm(entry, head);
	if (pageKey === undefined) return undefined;
	const page = await tx.get(pageKey);
	if (typeof page !== 'string') return undefined;
	const terms = new Map<string, string>();
	rawPage(page, terms);
	const list = terms.get(entry);
	return list === undefined || list === LARGE ? undefined : inlineTerm(entry, decodeRows(list));
}

/**
 * The postings of every term whose entry is in [lo, hi), in entry order. Lazy:
 * it reads the pages and heads of the range as the iteration reaches them.
 */
export async function* scanTerms(
	tx: KVRead,
	base: string,
	lo: string,
	hi: string | undefined,
): AsyncIterable<TermHead> {
	const heads = scanLarge(tx, base, lo, hi)[Symbol.asyncIterator]();
	let large = await heads.next();
	for await (const t of scanSmall(tx, base, lo, hi)) {
		while (!large.done && large.value.entry < t.entry) {
			yield large.value;
			large = await heads.next();
		}
		yield t;
	}
	while (!large.done) {
		yield large.value;
		large = await heads.next();
	}
}

/** The large terms in [lo, hi), by their heads. */
async function* scanLarge(
	tx: KVRead,
	base: string,
	lo: string,
	hi: string | undefined,
): AsyncIterable<TermHead> {
	const at = base.length + HEAD.length;
	const end = hi === undefined ? base + upperChar(HEAD) : postingsHeadKey(base, hi);
	for await (const [ k, v ] of tx.scanRange(postingsHeadKey(base, lo), end)) {
		const head = headOf(v);
		if (head !== undefined) yield largeTerm(k.slice(at), head);
	}
}

/**
 * The small terms in [lo, hi), by their pages: every page whose bound is in
 * the range, then the one page whose bound is the first above it.
 */
async function* scanSmall(
	tx: KVRead,
	base: string,
	lo: string,
	hi: string | undefined,
): AsyncIterable<TermHead> {
	const pick = function*(value: unknown): Iterable<TermHead> {
		if (typeof value !== 'string') return;
		const terms = new Map<string, string>();
		rawPage(value, terms);
		for (const [ entry, list ] of terms) {
			if (list === LARGE || entry < lo) continue;
			if (hi !== undefined && entry >= hi) return;
			yield inlineTerm(entry, decodeRows(list));
		}
	};
	const end = base + upperChar(PAGE);
	const top = hi === undefined ? end : postingsPageKey(base, hi);
	for await (const [ , v ] of tx.scanRange(postingsPageKey(base, lo), top)) yield* pick(v);
	if (hi === undefined) return;
	const last = await tx.seek(top, end);
	if (last !== undefined) yield* pick(await tx.get(last));
}

/**
 * Every row number the given terms post, term by term and ascending within a
 * term — one getMany for all their chunks.
 */
export async function readAllRows(
	tx: KVRead,
	base: string,
	terms: readonly TermHead[],
): Promise<number[]> {
	const keys: string[] = [];
	for (const t of terms) {
		for (const b of t.head.bounds) keys.push(postingsChunkKey(base, t.entry, b));
	}
	const chunks = keys.length === 0 ? [] : await tx.getMany(keys);
	const out: number[] = [];
	let at = 0;
	for (const t of terms) {
		for (let i = 0; i < t.head.bounds.length; i++) {
			for (const n of rowsOf(chunks[at++])) out.push(n);
		}
		for (const n of t.head.tail) out.push(n);
	}
	return out;
}

/**
 * Answers whether one term posts a row number, reading only the chunks of
 * the term that the numbers asked about fall in.
 */
export class TermMembers {
	private readonly held: Set<number>;
	private readonly loaded = new Set<number>();

	constructor(
		private readonly tx: KVRead,
		private readonly base: string,
		private readonly term: TermHead,
	) {
		this.held = new Set(term.head.tail);
	}

	/** Reads the chunks that `rows` fall in. Call it before `has` on them. */
	async load(rows: Iterable<number>): Promise<void> {
		const { bounds } = this.term.head;
		const wanted: number[] = [];
		for (const n of rows) {
			const at = boundFor(bounds, n);
			if (at === bounds.length || this.loaded.has(at)) continue;
			this.loaded.add(at);
			wanted.push(at);
		}
		if (wanted.length === 0) return;
		const chunks = await this.tx.getMany(
			wanted.map((at) => postingsChunkKey(this.base, this.term.entry, bounds[at]!)),
		);
		for (const v of chunks) for (const n of rowsOf(v)) this.held.add(n);
	}

	/** Whether the term posts `row`. Only valid for a number given to `load`. */
	has(row: number): boolean {
		return this.held.has(row);
	}
}

// --- writes --------------------------------------------------------------

/**
 * The spec tags with records under `scope`, by one seek per tag: the first
 * key at or above the scope names one, and the next seek starts past every
 * key of that tag.
 */
export async function specTagsUnder(tx: KVRead, scope: string): Promise<string[]> {
	const hi = keyAbove(scope);
	const tags: string[] = [];
	let from = scope;
	for (;;) {
		const k = await tx.seek(from, hi);
		if (k === undefined) return tags;
		const tag = k.slice(scope.length, scope.length + SEARCH_SPEC_TAG_LENGTH);
		tags.push(tag);
		from = keyAbove(scope + tag);
	}
}

/** The least string above every string that starts with `prefix`. */
function keyAbove(prefix: string): string {
	for (let i = prefix.length - 1; i >= 0; i--) {
		const c = prefix.charCodeAt(i);
		if (c !== 0xffff) return prefix.slice(0, i) + String.fromCharCode(c + 1);
	}
	// Only an all-U+FFFF prefix has no such string, and no scope is one.
	throw new Error('postings: unbounded prefix');
}

/** One term's pending changes inside one base. */
interface TermOps {
	base: string;
	entry: string;
	adds: number[];
	removes: Set<number>;
}

/** One row as a writer records it. */
interface RowChange {
	/** The entries of the row the store held before, undefined for none. */
	old: (() => readonly string[]) | undefined;
	/** The entries of the row now: none for a row that is gone. */
	entries: readonly string[];
}

/** Rows recorded for one scope, and the tag of the index this build keeps. */
interface ScopeRows {
	entity: string;
	workspaceId: string;
	specHash: string | undefined;
	tag: string | undefined;
	rows: Map<string, RowChange>;
}

/** One page a flush changes: its bound, and its terms as the flush leaves them. */
interface PagePlan {
	bound: string;
	/** Each term's row numbers as encodeRows writes them, LARGE for a large
	 * term; a term the flush does not change is never decoded. */
	terms: Map<string, string>;
	stored: boolean;
	dirty: boolean;
}

/** One large term a flush changes, and the head it starts from. */
interface LargePlan {
	o: TermOps;
	page: PagePlan;
	head: PostingsHead;
	had: boolean;
	touched: Set<number>;
}

/**
 * Collects the search terms of rows as they are written, then writes every
 * posting change in one pass: one read of the row map for the recorded rows,
 * the directories, the pages the changed terms fall in, the heads of the large
 * ones and the chunks their changes land in, and one write per changed
 * record. Reads within a pass are issued together, so a backend that answers
 * a batch in one round trip (KVRead.getMany) does. A pass over a spec with
 * nothing stored yet reads nothing at all.
 *
 * The index holds no list of the terms of a row. A change is the difference
 * between the entries of the row the store held before and the entries of the
 * row now, so each caller gives both. The row map says whether the index
 * holds the terms of the earlier row: a row that is stored but not in the map
 * posts all its entries, and a row in the map posts the difference.
 *
 * The cost of a pass grows with the pages and large terms it touches rather
 * than with its rows, so a writer that records more rows per pass pays less
 * per row; see Coordinator.doBootstrap and index-build.ts for the two that
 * hold one across transactions.
 *
 * Not safe for concurrent use. A flush runs inside one write transaction,
 * which is what keeps the row map, the directory, the pages, the heads and
 * their chunks consistent with each other: the store's write lock admits one
 * writer. Every row recorded must still be what the confirmed state holds
 * when that transaction commits.
 */
export class SearchPostingsWriter {
	private scopes = new Map<string, ScopeRows>();
	private count = 0;

	/** Rows recorded since the last flush. */
	get rows(): number {
		return this.count;
	}

	/** Forgets one recorded row, so a flush leaves its postings as they are. */
	drop(entity: string, workspaceId: string, key: string): void {
		const s = this.scopes.get(searchPostingsScope(entity, workspaceId));
		if (s !== undefined && s.rows.delete(key)) this.count--;
	}

	/**
	 * Records that the row at `key` of `entity` in `workspaceId` now posts
	 * `entries` under the search index with spec hash `specHash` — none for a
	 * row that is gone or holds no terms.
	 *
	 * `old` gives the entries of the row the store held at `key` before this
	 * write, analyzed under the same spec, and is undefined when the store held
	 * none. It is called at most once, in flush. When a row is recorded more
	 * than once before a flush, the first `old` and the last `entries` count.
	 *
	 * A flush deletes the postings of every OTHER spec found for the pair,
	 * whose analysis this build cannot reproduce; an undefined `specHash`
	 * deletes them all.
	 *
	 * `entries` must not change until flush returns.
	 */
	set(
		entity: string,
		workspaceId: string,
		specHash: string | undefined,
		key: string,
		old: (() => readonly string[]) | undefined,
		entries: readonly string[],
	): void {
		const scope = searchPostingsScope(entity, workspaceId);
		let s = this.scopes.get(scope);
		if (s === undefined) {
			s = {
				entity,
				workspaceId,
				specHash,
				tag: specHash === undefined ? undefined : searchSpecTag(specHash),
				rows: new Map(),
			};
			this.scopes.set(scope, s);
		}
		const held = s.rows.get(key);
		if (held === undefined) {
			this.count++;
			s.rows.set(key, { old, entries });
		} else {
			held.entries = entries;
		}
	}

	/** Writes every change recorded since the last flush, in `tx`. */
	async flush(tx: KVWrite): Promise<void> {
		if (this.scopes.size === 0) return;
		const scopes = this.scopes;
		this.scopes = new Map();
		this.count = 0;

		for (const [ scope, s ] of scopes) {
			const tags = await specTagsUnder(tx, scope);
			// The postings of another declaration go whole: this build cannot
			// analyze a row the way that declaration does, so it cannot take one
			// row out of them. The index they made must then read as unbuilt.
			let foreign = false;
			for (const t of tags) {
				if (t === s.tag) continue;
				foreign = true;
				const stale: string[] = [];
				for await (const [ k ] of tx.scan(scope + t)) stale.push(k);
				for (const k of stale) await tx.del(k);
			}
			if (foreign) await dropForeignSearch(tx, s);
			if (s.tag === undefined) continue;
			const base = scope + s.tag;
			// A base with no record at all needs none of the reads below: every
			// page and head under it is absent, and no row is posted.
			const empty = !tags.includes(s.tag);
			const map = new RowMapFlush(tx, base);
			await map.read([ ...s.rows.keys() ], empty);

			const terms = new Map<string, TermOps>();
			const op = (entry: string): TermOps => {
				let o = terms.get(entry);
				if (o === undefined) {
					o = { base, entry, adds: [], removes: new Set() };
					terms.set(entry, o);
				}
				return o;
			};
			for (const [ key, row ] of s.rows) {
				const was = map.numberOf(key);
				const old = was !== undefined && row.old !== undefined ? row.old() : [];
				if (row.entries.length === 0) {
					if (was === undefined) continue;
					for (const e of old) op(e).removes.add(was);
					map.remove(key);
					continue;
				}
				const n = was ?? map.add(key);
				if (old === row.entries) continue;
				const had = new Set(old);
				const has = new Set(row.entries);
				for (const e of has) if (!had.has(e)) op(e).adds.push(n);
				for (const e of had) if (!has.has(e)) op(e).removes.add(n);
			}
			await map.write();
			if (terms.size === 0) continue;
			await new BaseFlush(tx, base, empty).run([ ...terms.values() ]);
		}
	}
}

/**
 * Makes the search index of every declaration but the scope's own read as
 * unbuilt: deletes its built marker, and the cursor of a build walk that
 * builds it.
 */
async function dropForeignSearch(tx: KVWrite, s: ScopeRows): Promise<void> {
	const prefix = metaIndexEntityPrefix(s.workspaceId, s.entity);
	const own = s.specHash === undefined
		? undefined
		: metaIndexBuilt(s.workspaceId, s.entity, SEARCH_INDEX_NAME, s.specHash);
	const name = `${SEARCH_INDEX_NAME}@`;
	const markers: string[] = [];
	for await (const [ k ] of tx.scan(prefix)) {
		if (k !== own && k.startsWith(name, prefix.length)) markers.push(k);
	}
	for (const k of markers) await tx.del(k);
	const cursorKey = metaIndexBuildCursor(s.workspaceId, s.entity);
	const cursor = await tx.get(cursorKey) as IndexBuildCursor | undefined;
	if (cursor === undefined) return;
	// The cursor names the indexes its walk builds as their markers do.
	const built = cursor.m?.split(',').find((m) => m.startsWith(name));
	if (built !== undefined && prefix + built !== own) await tx.del(cursorKey);
}

/** The row map of one base through one flush: the chunks the recorded rows
 * fall in, the rows it gains and loses, and the blocks those change. */
class RowMapFlush {
	private head: RowMapHead = NO_ROW_MAP;
	private next = 0;
	/** The chunks read, by position in the head's directory. */
	private readonly chunks = new Map<number, Map<string, number>>();
	private readonly dirty = new Set<number>();
	/** The key each changed row number now has; empty for a number given up. */
	private readonly renamed = new Map<number, string>();

	constructor(private readonly tx: KVWrite, private readonly base: string) {}

	private chunkKey(at: number): string {
		return rowMapChunkKey(this.base, this.head.bounds[at]);
	}

	/** Reads the chunks `keys` fall in; nothing for a base with no record. */
	async read(keys: readonly string[], empty: boolean): Promise<void> {
		if (!empty) this.head = rowMapHeadOf(await this.tx.get(rowMapHeadKey(this.base)));
		this.next = this.head.next;
		const wanted = new Set<number>();
		for (const k of keys) wanted.add(boundFor(this.head.bounds, k));
		const order = [ ...wanted ];
		const values = this.head.counts.length === 0
			? []
			: await this.tx.getMany(order.map((at) => this.chunkKey(at)));
		for (let i = 0; i < order.length; i++) {
			this.chunks.set(order[i]!, new Map(rowMapChunkOf(values[i])));
		}
	}

	private chunkOf(key: string): number {
		return boundFor(this.head.bounds, key);
	}

	/** The number of a posted row, or undefined for a row the map lacks. */
	numberOf(key: string): number | undefined {
		return this.chunks.get(this.chunkOf(key))?.get(key);
	}

	/** Gives `key` the next number. */
	add(key: string): number {
		const at = this.chunkOf(key);
		const n = this.next++;
		this.chunks.get(at)!.set(key, n);
		this.dirty.add(at);
		this.renamed.set(n, key);
		return n;
	}

	remove(key: string): void {
		const at = this.chunkOf(key);
		const chunk = this.chunks.get(at)!;
		this.renamed.set(chunk.get(key)!, '');
		chunk.delete(key);
		this.dirty.add(at);
	}

	/** Writes the chunks, the blocks and the head that changed. */
	async write(): Promise<void> {
		if (this.dirty.size === 0) return;
		const { tx, base, head } = this;
		const last = head.bounds.length;
		const counts: number[] = [];
		const bounds: string[] = [];
		for (let at = 0; at <= last; at++) {
			const isLast = at === last;
			const bound = head.bounds[at];
			if (!this.dirty.has(at)) {
				if (isLast && head.counts.length === 0) continue;
				counts.push(head.counts[at]!);
				if (!isLast) bounds.push(bound!);
				continue;
			}
			const rows = [ ...this.chunks.get(at)! ].sort(([ a ], [ b ]) => (a < b ? -1 : a > b ? 1 : 0));
			if (rows.length === 0 && !isLast) {
				await tx.del(rowMapChunkKey(base, bound));
				continue;
			}
			// Near-equal pieces of at most the cap. Every piece but the last is
			// bounded by its own highest key; the last keeps the chunk's bound,
			// so the range the next chunk answers for does not move.
			const pieces = Math.max(1, Math.ceil(rows.length / ROW_MAP_CHUNK_CAP));
			const size = Math.ceil(rows.length / pieces);
			for (let p = 0; p < pieces - 1; p++) {
				const piece = rows.slice(p * size, (p + 1) * size);
				const pieceBound = piece[piece.length - 1]![0];
				await tx.put(rowMapChunkKey(base, pieceBound), encodeRowMapChunk(piece));
				counts.push(piece.length);
				bounds.push(pieceBound);
			}
			const rest = rows.slice((pieces - 1) * size);
			if (rest.length === 0) await tx.del(rowMapChunkKey(base, bound));
			else await tx.put(rowMapChunkKey(base, bound), encodeRowMapChunk(rest));
			counts.push(rest.length);
			if (!isLast) bounds.push(bound!);
		}
		await this.writeBlocks();
		// A map that names no row has no head: the next row then starts the
		// numbers again, which no posting can still hold.
		if (bounds.length === 0 && counts[0] === 0) {
			if (head.counts.length > 0) await tx.del(rowMapHeadKey(base));
			return;
		}
		await tx.put(rowMapHeadKey(base), encodeRowMapHead({ next: this.next, counts, bounds }));
	}

	private async writeBlocks(): Promise<void> {
		const { tx, base } = this;
		const byBlock = new Map<number, [number, string][]>();
		for (const [ n, key ] of this.renamed) {
			const block = Math.floor(n / ROW_BLOCK);
			const held = byBlock.get(block);
			if (held === undefined) byBlock.set(block, [ [ n, key ] ]);
			else held.push([ n, key ]);
		}
		// A block above every number given out before this flush holds nothing
		// to read.
		const top = Math.floor((this.head.next - 1) / ROW_BLOCK);
		const stored = [ ...byBlock.keys() ].filter((block) => block <= top);
		const values = stored.length === 0
			? []
			: await tx.getMany(stored.map((block) => rowBlockKey(base, block * ROW_BLOCK)));
		const held = new Map<number, string[]>();
		for (let i = 0; i < stored.length; i++) held.set(stored[i]!, blockOf(values[i]));
		for (const [ block, changes ] of byBlock) {
			const keys = held.get(block) ?? [];
			for (const [ n, key ] of changes) {
				const slot = n % ROW_BLOCK;
				while (keys.length <= slot) keys.push('');
				keys[slot] = key;
			}
			while (keys.length > 0 && keys[keys.length - 1] === '') keys.pop();
			const blockKey = rowBlockKey(base, block * ROW_BLOCK);
			if (keys.length === 0) await tx.del(blockKey);
			else await tx.put(blockKey, encodeKeyList(keys));
		}
	}
}

/** The flush of one base: its directory, pages, heads and chunks. */
class BaseFlush {
	constructor(
		private readonly tx: KVWrite,
		private readonly base: string,
		private readonly empty: boolean,
	) {}

	async run(ops: TermOps[]): Promise<void> {
		const { tx, base } = this;
		const dirKey = postingsDirectoryKey(base);
		const dir = this.empty ? undefined : await tx.get(dirKey);
		const bounds = typeof dir === 'string' ? decodeKeyList(dir) : [ POSTINGS_LAST_PAGE ];

		// The pages the changed terms fall in, read together.
		const pages = new Map<number, PagePlan>();
		const pageOfOp = new Map<TermOps, PagePlan>();
		const wanted: number[] = [];
		for (const o of ops) {
			const at = boundFor(bounds, o.entry);
			let page = pages.get(at);
			if (page === undefined) {
				page = { bound: bounds[at]!, terms: new Map(), stored: dir !== undefined, dirty: false };
				pages.set(at, page);
				wanted.push(at);
			}
			pageOfOp.set(o, page);
		}
		const values = dir === undefined
			? []
			: await tx.getMany(wanted.map((at) => postingsPageKey(base, bounds[at]!)));
		for (let i = 0; i < values.length; i++) {
			const page = pages.get(wanted[i]!)!;
			if (typeof values[i] === 'string') rawPage(values[i] as string, page.terms);
		}

		// Small terms change in their page. A small term that outgrows the page
		// becomes a large one, starting from an empty head with all its rows to
		// add; a large one is changed through its head below.
		const large: LargePlan[] = [];
		const headless: LargePlan[] = [];
		for (const o of ops) {
			const page = pageOfOp.get(o)!;
			const raw = page.terms.get(o.entry);
			if (raw === LARGE) {
				large.push({ o, page, head: EMPTY_HEAD, had: true, touched: new Set() });
				continue;
			}
			const before = raw === undefined ? [] : decodeRows(raw);
			const after = mergeRows(before, o.adds.sort(ascending), o.removes);
			if (after === before) continue;
			page.dirty = true;
			const encoded = encodeRows(after);
			if (after.length > POSTINGS_INLINE_MAX || encoded.length > POSTINGS_PAGE_CAP / 2) {
				const grown = { ...o, adds: after, removes: new Set<number>() };
				headless.push({ o: grown, page, head: EMPTY_HEAD, had: false, touched: new Set() });
			} else if (after.length === 0) {
				page.terms.delete(o.entry);
			} else {
				page.terms.set(o.entry, encoded);
			}
		}

		const heads = large.length === 0
			? []
			: await tx.getMany(large.map((l) => postingsHeadKey(base, l.o.entry)));
		const plans = [ ...large, ...headless ];
		const chunkKeys: string[] = [];
		for (let i = 0; i < large.length; i++) {
			large[i]!.head = headOf(heads[i]) ?? EMPTY_HEAD;
		}
		for (const plan of plans) {
			const route = (row: number) => {
				const at = boundFor(plan.head.bounds, row);
				if (plan.touched.has(at)) return;
				plan.touched.add(at);
				if (at < plan.head.bounds.length) {
					chunkKeys.push(postingsChunkKey(base, plan.o.entry, plan.head.bounds[at]!));
				}
			};
			for (const n of plan.o.adds) route(n);
			for (const n of plan.o.removes) route(n);
		}
		const chunkValues = chunkKeys.length === 0 ? [] : await tx.getMany(chunkKeys);
		const chunks = new Map<string, unknown>();
		for (let i = 0; i < chunkKeys.length; i++) chunks.set(chunkKeys[i]!, chunkValues[i]);

		for (const plan of plans) {
			const next = await applyLarge(tx, base, plan.o, plan.head, plan.touched, chunks);
			if (next === plan.head) continue;
			const headKey = postingsHeadKey(base, plan.o.entry);
			const n = headCount(next);
			if (n === 0) {
				if (plan.had) await tx.del(headKey);
				plan.page.terms.delete(plan.o.entry);
				plan.page.dirty = true;
			} else if (
				next.bounds.length === 0 && n <= POSTINGS_INLINE_MAX / 2
				&& encodeRows(next.tail).length <= POSTINGS_PAGE_CAP / 4
			) {
				// Back into its page. The hysteresis keeps a term near the limit
				// from moving on every write.
				if (plan.had) await tx.del(headKey);
				plan.page.terms.set(plan.o.entry, encodeRows(next.tail));
				plan.page.dirty = true;
			} else {
				await tx.put(headKey, encodeHead(next));
				if (!plan.had) {
					plan.page.terms.set(plan.o.entry, LARGE);
					plan.page.dirty = true;
				}
			}
		}

		await this.writePages(bounds, pages, typeof dir === 'string');
	}

	/**
	 * Writes the pages that changed, splitting one past POSTINGS_PAGE_CAP and
	 * folding one under a quarter of it into the next page when that page was
	 * read too, and the directory when a bound moved.
	 */
	private async writePages(
		bounds: string[],
		pages: Map<number, PagePlan>,
		hadDirectory: boolean,
	): Promise<void> {
		const { tx, base } = this;
		const order = [ ...pages.keys() ].sort((a, b) => a - b);
		const added = new Map<number, string[]>();
		const removed = new Set<number>();
		let wrote = false;
		for (const at of order) {
			const page = pages.get(at)!;
			if (!page.dirty) continue;
			const terms = sortedTerms(page);
			const isLast = at === bounds.length - 1;
			const chars = pageChars(terms);
			const next = pages.get(at + 1);
			if (next !== undefined && chars < POSTINGS_PAGE_CAP / 4) {
				if (chars + pageChars(sortedTerms(next)) <= POSTINGS_PAGE_CAP) {
					// The next page's range takes this one's in: its bound is above
					// every entry here.
					for (const t of terms) next.terms.set(t.entry, t.raw);
					next.dirty = true;
					if (page.stored) await tx.del(postingsPageKey(base, page.bound));
					removed.add(at);
					continue;
				}
			}
			if (terms.length === 0) {
				if (!isLast) {
					if (page.stored) await tx.del(postingsPageKey(base, page.bound));
					removed.add(at);
					continue;
				}
				if (bounds.length - removed.size === 1 && added.size === 0) {
					// The only page left, emptied: the index holds nothing, and
					// neither does its directory.
					if (page.stored) await tx.del(postingsPageKey(base, page.bound));
					if (hadDirectory) await tx.del(postingsDirectoryKey(base));
					return;
				}
			}
			// Past the cap, pieces filled to three quarters of it, so the next
			// few writes to a split page do not split it again. Every piece but
			// the last is bounded by its own highest entry; the last keeps the
			// page's bound, so the range the next page answers for does not move.
			const pieces: RawTerm[][] = [ [] ];
			if (chars > POSTINGS_PAGE_CAP && terms.length > 1) {
				let size = 0;
				for (const t of terms) {
					const c = termChars(t);
					if (size > 0 && size + c > (POSTINGS_PAGE_CAP * 3) / 4) {
						pieces.push([]);
						size = 0;
					}
					pieces[pieces.length - 1]!.push(t);
					size += c;
				}
			} else {
				pieces[0] = terms;
			}
			const extra: string[] = [];
			for (let p = 0; p < pieces.length - 1; p++) {
				const piece = pieces[p]!;
				const bound = piece[piece.length - 1]!.entry;
				await tx.put(postingsPageKey(base, bound), encodeRawPage(piece));
				extra.push(bound);
			}
			await tx.put(postingsPageKey(base, page.bound), encodeRawPage(pieces[pieces.length - 1]!));
			wrote = true;
			if (extra.length > 0) added.set(at, extra);
		}
		if (hadDirectory ? added.size === 0 && removed.size === 0 : !wrote) return;
		const next: string[] = [];
		for (let at = 0; at < bounds.length; at++) {
			for (const b of added.get(at) ?? []) next.push(b);
			if (!removed.has(at)) next.push(bounds[at]!);
		}
		await tx.put(postingsDirectoryKey(base), encodeKeyList(next));
	}
}

const ascending = (a: number, b: number): number => a - b;

const EMPTY_HEAD: PostingsHead = Object.freeze({
	counts: [],
	bounds: [],
	tail: [],
}) as unknown as PostingsHead;

/** One term of a page as a flush holds it: its row numbers still encoded. */
interface RawTerm {
	entry: string;
	raw: string;
}

/** The encoded row numbers a page stores for a large term: none. */
const LARGE = encodeRows([]);

/** Fills `into` with the terms of the page `s`, each list left encoded. */
function rawPage(s: string, into: Map<string, string>): void {
	const r = new Reader(s);
	for (const entry of r.list()) {
		const start = r.at;
		r.skipNumbers();
		into.set(entry, s.slice(start, r.at));
	}
	if (r.at > s.length) throw new Error('postings: truncated value');
}

/** A page from terms whose row numbers are already encoded; see encodePage. */
function encodeRawPage(terms: readonly RawTerm[]): string {
	let out = encodeKeyList(terms.map((t) => t.entry));
	for (const t of terms) out += t.raw;
	return out;
}

/** A page's terms in entry order. */
function sortedTerms(page: PagePlan): RawTerm[] {
	return [ ...page.terms ]
		.sort(([ a ], [ b ]) => (a < b ? -1 : a > b ? 1 : 0))
		.map(([ entry, raw ]) => ({ entry, raw }));
}

/** About the characters one term adds to a page. */
function termChars(t: RawTerm): number {
	return t.entry.length + 2 + t.raw.length;
}

/** About the characters a page encodes to, without encoding it. */
function pageChars(terms: readonly RawTerm[]): number {
	let n = 1;
	for (const t of terms) n += termChars(t);
	return n;
}

/**
 * Merges one large term's changes into its chunks, writes the chunks that
 * change, and returns the head that describes the result — `head` itself
 * when nothing changed. The caller decides where the head goes.
 */
async function applyLarge(
	tx: KVWrite,
	base: string,
	o: TermOps,
	head: PostingsHead,
	touched: ReadonlySet<number>,
	chunks: Map<string, unknown>,
): Promise<PostingsHead> {
	const { entry } = o;
	const n = head.bounds.length;
	const adds = new Map<number, number[]>();
	for (const row of o.adds) {
		const at = boundFor(head.bounds, row);
		let a = adds.get(at);
		if (a === undefined) adds.set(at, a = []);
		a.push(row);
	}
	// Only the lists that change are rewritten: a build revisiting a row it
	// already indexed, or an add the chunk already holds, writes nothing.
	const merged = new Map<number, number[]>();
	for (const at of touched) {
		const before = at < n
			? rowsOf(chunks.get(postingsChunkKey(base, entry, head.bounds[at]!)))
			: head.tail;
		const after = mergeRows(before, (adds.get(at) ?? []).sort(ascending), o.removes);
		if (after !== before) merged.set(at, after);
	}
	if (merged.size === 0) return head;

	const counts: number[] = [];
	const bounds: number[] = [];
	const putChunk = async (rows: number[], bound: number) => {
		await tx.put(postingsChunkKey(base, entry, bound), encodeRows(rows));
		counts.push(rows.length);
		bounds.push(bound);
	};
	for (let at = 0; at < n; at++) {
		const rows = merged.get(at);
		const bound = head.bounds[at]!;
		if (rows === undefined) {
			counts.push(head.counts[at]!);
			bounds.push(bound);
			continue;
		}
		if (rows.length === 0) {
			await tx.del(postingsChunkKey(base, entry, bound));
			continue;
		}
		// Near-equal pieces of at most the cap. Every piece but the last is
		// bounded by its own highest number; the last keeps the chunk's bound,
		// so the range the next chunk answers for does not move.
		const pieces = Math.ceil(rows.length / POSTINGS_CHUNK_CAP);
		const size = Math.ceil(rows.length / pieces);
		for (let p = 0; p < pieces - 1; p++) {
			const piece = rows.slice(p * size, (p + 1) * size);
			await putChunk(piece, piece[piece.length - 1]!);
		}
		await putChunk(rows.slice((pieces - 1) * size), bound);
	}
	let tail = merged.get(n) ?? head.tail;
	// The tail's lowest numbers move into chunks of exactly the cap, which is
	// what a run of appends leaves behind: full chunks and a short tail.
	while (tail.length > POSTINGS_CHUNK_CAP) {
		const piece = tail.slice(0, POSTINGS_CHUNK_CAP);
		await putChunk(piece, piece[piece.length - 1]!);
		tail = tail.slice(POSTINGS_CHUNK_CAP);
	}
	return { counts, bounds, tail };
}

/**
 * `before` with `adds` merged in and `removes` taken out, both lists
 * ascending and free of repeats. Returns `before` itself when nothing changes.
 */
function mergeRows(
	before: readonly number[],
	adds: readonly number[],
	removes: ReadonlySet<number>,
): number[] {
	const out: number[] = [];
	let changed = false;
	let j = 0;
	for (const n of before) {
		while (j < adds.length && adds[j]! < n) {
			out.push(adds[j++]!);
			changed = true;
		}
		if (j < adds.length && adds[j] === n) j++;
		if (removes.has(n)) {
			changed = true;
			continue;
		}
		out.push(n);
	}
	while (j < adds.length) {
		out.push(adds[j++]!);
		changed = true;
	}
	return changed ? out : before as number[];
}
