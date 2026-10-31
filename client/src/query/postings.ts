// The search index's postings, stored as ordinary KV records: an inverted
// index from each term to the sorted keys of the rows that hold it. Nothing
// here occupies a position in the store's secondary index (kv/store.ts
// INDEX_FIELD), so a posting costs the bytes of its row key in a packed list
// rather than an index row per (term, row) pair.
//
// Every record of one search index lives under one BASE: the postings root,
// the (entity, workspace) scope and the spec tag (see keys.ts
// searchPostingsBase). After the base, one character says what the record is:
//
// 	base 'd'                  the DIRECTORY: the upper bound of every page
// 	base 'p' bound            one PAGE: the small terms in (previous bound,
// 	                          bound], each with its row keys, and a mark for
// 	                          each large term in that range
// 	base 'h' entry            the HEAD of a large term: its chunk directory
// 	                          and its tail, the row keys above every chunk
// 	base 'c' entry bound      one CHUNK of a large term: the row keys in
// 	                          (previous bound, bound]
// 	base 'r' key              the TERM LIST of a row: the entries it is
// 	                          posted under
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
// record, so such a pull rewrites a few hundred pages and the heads of the
// common terms instead of one head per term. A term moves to a head of its
// own past POSTINGS_INLINE_MAX rows, and back at half of that, so a term
// near the limit does not move on every write.
//
// The directory is what lets a writer find the page of every term it changes
// with one read; a reader finds the one page it needs by a seek instead.
//
// A large term's row keys are, in order: chunk 0, chunk 1, …, then the tail.
// The head lists each chunk's bound and count, so a reader knows every chunk
// key and the term's posting count from one record, and a writer finds the
// chunk a row key belongs to — the first whose bound is not below it, else the
// tail — without a search of the keyspace. Rows arrive in key order on the
// paths that write the most postings (a bootstrap, a build), so those append
// to the tail.
//
// The term list is what lets a writer remove a row's postings without
// reading the row's previous value: the diff between the stored list and the
// new entries is the whole of the change.
//
// Values are strings of one character per byte where the row keys allow it —
// lengths and counts are base-128 varints in \u0000-ÿ — so an engine
// keeps them at one byte per character. Lists of keys are front-coded: each
// key is the length it shares with the previous one, the length of the rest,
// and the rest.

import { SEARCH_SPEC_TAG_LENGTH, searchPostingsScope, searchSpecTag } from '../keys.js';
import type { KVRead, KVWrite } from '../kv/store.js';

/**
 * Row keys one chunk, or a large term's tail, holds at most. A chunk that
 * outgrows it splits; a tail that outgrows it moves its lowest keys into new
 * chunks.
 */
export const POSTINGS_CHUNK_CAP = 256;

/**
 * Rows a term may post inside a page. A term past it moves to a head of its
 * own; a term with a head and no chunk moves back at half of it.
 */
export const POSTINGS_INLINE_MAX = 128;

/**
 * Characters a page holds before it splits. A page with one term may hold
 * more: a term is never split across pages.
 */
export const POSTINGS_PAGE_CAP = 16384;

const DIRECTORY = 'd';
const PAGE = 'p';
const HEAD = 'h';
const CHUNK = 'c';
const TERMS = 'r';

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

/** The chunk of `entry` whose upper bound is the row key `bound`. */
export const postingsChunkKey = (base: string, entry: string, bound: string): string =>
	base + CHUNK + entry + bound;

/** The term list of the row at `key`. */
export const postingsTermsKey = (base: string, key: string): string => base + TERMS + key;

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

	/** Steps over one list without building its strings. */
	skipList(): void {
		const n = this.varint();
		for (let i = 0; i < n; i++) {
			this.varint();
			const len = this.varint();
			this.at += len;
		}
		if (this.at > this.s.length) throw new Error('postings: truncated value');
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
 * A large term's head: the chunks in key order, each by its count and its
 * upper bound, and the tail — the row keys above the last bound, sorted.
 */
export interface PostingsHead {
	counts: number[];
	bounds: string[];
	tail: string[];
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
	return out + encodeKeyList(head.bounds) + encodeKeyList(head.tail);
}

/** Inverse of encodeHead. Throws on a value it did not produce. */
export function decodeHead(s: string): PostingsHead {
	const r = new Reader(s);
	const n = r.varint();
	const counts: number[] = [];
	for (let i = 0; i < n; i++) counts.push(r.varint());
	const bounds = r.list();
	if (bounds.length !== n) throw new Error('postings: head directory mismatch');
	return { counts, bounds, tail: r.list() };
}

/**
 * One term of a page: its sorted row keys, or undefined for a large term,
 * whose postings are under its own head.
 */
export interface PageTerm {
	entry: string;
	keys: string[] | undefined;
}

/**
 * A page: its terms' entries, front-coded, then each term's row keys. An empty
 * key list marks a large term — a small term with no rows is not stored.
 */
export function encodePage(terms: readonly PageTerm[]): string {
	let out = encodeKeyList(terms.map((t) => t.entry));
	for (const t of terms) out += encodeKeyList(t.keys ?? []);
	return out;
}

/** Inverse of encodePage. Throws on a value it did not produce. */
export function decodePage(s: string): PageTerm[] {
	const r = new Reader(s);
	const entries = r.list();
	return entries.map((entry) => {
		const keys = r.list();
		return { entry, keys: keys.length === 0 ? undefined : keys };
	});
}

/** The head stored at a key, or undefined for an absent or foreign value. */
function headOf(value: unknown): PostingsHead | undefined {
	return typeof value === 'string' ? decodeHead(value) : undefined;
}

/** The term list stored at a key: empty for an absent or foreign value. */
export function termListOf(value: unknown): string[] {
	return typeof value === 'string' ? decodeKeyList(value) : [];
}

/** First index whose bound is not below `key`, or the bound count. */
function boundFor(bounds: readonly string[], key: string): number {
	let lo = 0;
	let hi = bounds.length;
	while (lo < hi) {
		const mid = (lo + hi) >>> 1;
		if (bounds[mid]! < key) lo = mid + 1;
		else hi = mid;
	}
	return lo;
}

// --- reads ---------------------------------------------------------------

/**
 * One term's postings as a reader holds them. A small term's row keys are all
 * in `head.tail`, with no chunk.
 */
export interface TermHead {
	entry: string;
	head: PostingsHead;
	count: number;
}

const inlineTerm = (entry: string, keys: string[]): TermHead => ({
	entry,
	head: { counts: [], bounds: [], tail: keys },
	count: keys.length,
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
	return list === undefined || list === LARGE ? undefined : inlineTerm(entry, decodeKeyList(list));
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
			yield inlineTerm(entry, decodeKeyList(list));
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
 * Every row key the given terms post, term by term and in key order within a
 * term — one getMany for all their chunks.
 */
export async function readAllKeys(
	tx: KVRead,
	base: string,
	terms: readonly TermHead[],
): Promise<string[]> {
	const keys: string[] = [];
	for (const t of terms) {
		for (const b of t.head.bounds) keys.push(postingsChunkKey(base, t.entry, b));
	}
	const chunks = keys.length === 0 ? [] : await tx.getMany(keys);
	const out: string[] = [];
	let at = 0;
	for (const t of terms) {
		for (let i = 0; i < t.head.bounds.length; i++) {
			const v = chunks[at++];
			if (typeof v !== 'string') continue;
			for (const k of decodeKeyList(v)) out.push(k);
		}
		for (const k of t.head.tail) out.push(k);
	}
	return out;
}

/**
 * One term's row keys in key order, reading its chunks as the iteration
 * reaches them: the first `first` chunks in one request, and twice as many in
 * each request after it.
 */
export async function* termKeys(
	tx: KVRead,
	base: string,
	term: TermHead,
	first = 1,
): AsyncIterable<string> {
	const { bounds } = term.head;
	let want = Math.max(1, first);
	for (let i = 0; i < bounds.length;) {
		const n = Math.min(want, bounds.length - i);
		const chunks = await tx.getMany(
			bounds.slice(i, i + n).map((b) => postingsChunkKey(base, term.entry, b)),
		);
		for (const v of chunks) {
			if (typeof v === 'string') yield* decodeKeyList(v);
		}
		i += n;
		want *= 2;
	}
	yield* term.head.tail;
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
	adds: string[];
	removes: Set<string>;
}

/** Rows recorded for one scope, and the tag of the index this build keeps. */
interface ScopeRows {
	tag: string | undefined;
	rows: Map<string, readonly string[]>;
}

/** One page a flush changes: its bound, and its terms as the flush leaves them. */
interface PagePlan {
	bound: string;
	/** Each term's key list as encodeKeyList writes it, LARGE for a large term;
	 * a term the flush does not change is never decoded. */
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
 * posting change in one pass: one read each of the recorded rows' term lists,
 * the directories, the pages the changed terms fall in, the heads of the large
 * ones and the chunks their changes land in, and one write per changed record.
 * Reads within a pass are issued together, so a backend that answers a batch
 * in one round trip (KVRead.getMany) does. A pass over a spec with nothing
 * stored yet reads nothing at all.
 *
 * The cost of a pass grows with the pages and large terms it touches rather
 * than with its rows, so a writer that records more rows per pass pays less
 * per row; see Coordinator.doBootstrap and index-build.ts for the two that
 * hold one across transactions.
 *
 * Not safe for concurrent use. A flush runs inside one write transaction,
 * which is what keeps the directory, the pages, the heads, their chunks and
 * the term lists consistent with each other: the store's write lock admits
 * one writer. Every row recorded must still be what the confirmed state holds
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
	 * row that is gone or holds no terms. The row is also removed from every
	 * OTHER spec's postings found for the pair, whose analysis this build
	 * cannot reproduce; an undefined `specHash` removes it from all of them. A
	 * later call for the same row replaces an earlier one.
	 *
	 * `entries` must not change until flush returns.
	 */
	set(
		entity: string,
		workspaceId: string,
		specHash: string | undefined,
		key: string,
		entries: readonly string[],
	): void {
		const scope = searchPostingsScope(entity, workspaceId);
		let s = this.scopes.get(scope);
		if (s === undefined) {
			s = { tag: specHash === undefined ? undefined : searchSpecTag(specHash), rows: new Map() };
			this.scopes.set(scope, s);
		}
		if (!s.rows.has(key)) this.count++;
		s.rows.set(key, entries);
	}

	/** Writes every change recorded since the last flush, in `tx`. */
	async flush(tx: KVWrite): Promise<void> {
		if (this.scopes.size === 0) return;
		const scopes = this.scopes;
		this.scopes = new Map();
		this.count = 0;

		// Which bases each scope holds, other than the one this build keeps.
		// A base with no record at all needs none of the reads below: every
		// term list, page and head under it is absent.
		const lists: { base: string; key: string; own: readonly string[] | undefined }[] = [];
		const empty = new Set<string>();
		for (const [ scope, s ] of scopes) {
			const tags = await specTagsUnder(tx, scope);
			if (s.tag !== undefined && !tags.includes(s.tag)) empty.add(scope + s.tag);
			const others = tags.filter((t) => t !== s.tag);
			for (const [ key, entries ] of s.rows) {
				if (s.tag !== undefined) lists.push({ base: scope + s.tag, key, own: entries });
				for (const t of others) lists.push({ base: scope + t, key, own: undefined });
			}
		}
		const stored = await getManyUnless(
			tx,
			lists.map((l) => postingsTermsKey(l.base, l.key)),
			lists.map((l) => empty.has(l.base)),
		);

		const byBase = new Map<string, Map<string, TermOps>>();
		const op = (base: string, entry: string): TermOps => {
			let terms = byBase.get(base);
			if (terms === undefined) byBase.set(base, terms = new Map());
			let o = terms.get(entry);
			if (o === undefined) {
				o = { base, entry, adds: [], removes: new Set() };
				terms.set(entry, o);
			}
			return o;
		};
		for (let i = 0; i < lists.length; i++) {
			const { base, key, own } = lists[i]!;
			const old = termListOf(stored[i]);
			const termsKey = postingsTermsKey(base, key);
			if (own === undefined) {
				if (stored[i] === undefined) continue;
				for (const e of old) op(base, e).removes.add(key);
				await tx.del(termsKey);
				continue;
			}
			const next = [ ...new Set(own) ].sort();
			const had = new Set(old);
			const has = new Set(next);
			let changed = old.length !== next.length;
			for (const e of next) {
				if (had.has(e)) continue;
				op(base, e).adds.push(key);
				changed = true;
			}
			for (const e of old) if (!has.has(e)) op(base, e).removes.add(key);
			if (!changed && typeof stored[i] === 'string') continue;
			if (next.length === 0) await tx.del(termsKey);
			else await tx.put(termsKey, encodeKeyList(next));
		}
		for (const [ base, terms ] of byBase) {
			await new BaseFlush(tx, base, empty.has(base)).run([ ...terms.values() ]);
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
			const before = raw === undefined ? [] : decodeKeyList(raw);
			const after = mergeKeys(before, o.adds.sort(), o.removes);
			if (after === before) continue;
			page.dirty = true;
			const encoded = encodeKeyList(after);
			if (after.length > POSTINGS_INLINE_MAX || encoded.length > POSTINGS_PAGE_CAP / 2) {
				const grown = { ...o, adds: after, removes: new Set<string>() };
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
			const route = (key: string) => {
				const at = boundFor(plan.head.bounds, key);
				if (plan.touched.has(at)) return;
				plan.touched.add(at);
				if (at < plan.head.bounds.length) {
					chunkKeys.push(postingsChunkKey(base, plan.o.entry, plan.head.bounds[at]!));
				}
			};
			for (const k of plan.o.adds) route(k);
			for (const k of plan.o.removes) route(k);
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
				&& encodeKeyList(next.tail).length <= POSTINGS_PAGE_CAP / 4
			) {
				// Back into its page. The hysteresis keeps a term near the limit
				// from moving on every write.
				if (plan.had) await tx.del(headKey);
				plan.page.terms.set(plan.o.entry, encodeKeyList(next.tail));
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

const EMPTY_HEAD: PostingsHead = Object.freeze({
	counts: [],
	bounds: [],
	tail: [],
}) as unknown as PostingsHead;

/** One term of a page as a flush holds it: its key list still encoded. */
interface RawTerm {
	entry: string;
	raw: string;
}

/** The encoded key list a page stores for a large term: an empty one. */
const LARGE = encodeKeyList([]);

/** Fills `into` with the terms of the page `s`, each key list left encoded. */
function rawPage(s: string, into: Map<string, string>): void {
	const r = new Reader(s);
	for (const entry of r.list()) {
		const start = r.at;
		r.skipList();
		into.set(entry, s.slice(start, r.at));
	}
}

/** A page from terms whose key lists are already encoded; see encodePage. */
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

/** `keys` read in one getMany, except those `skip` marks, which read as absent. */
async function getManyUnless(
	tx: KVRead,
	keys: readonly string[],
	skip: readonly boolean[],
): Promise<unknown[]> {
	const wanted: string[] = [];
	for (let i = 0; i < keys.length; i++) if (!skip[i]) wanted.push(keys[i]!);
	if (wanted.length === keys.length) return keys.length === 0 ? [] : tx.getMany(keys);
	const got = wanted.length === 0 ? [] : await tx.getMany(wanted);
	const out = new Array<unknown>(keys.length);
	let at = 0;
	for (let i = 0; i < keys.length; i++) out[i] = skip[i] ? undefined : got[at++];
	return out;
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
	const adds = new Map<number, string[]>();
	for (const k of o.adds) {
		const at = boundFor(head.bounds, k);
		let a = adds.get(at);
		if (a === undefined) adds.set(at, a = []);
		a.push(k);
	}
	// Only the lists that change are rewritten: a build revisiting a row it
	// already indexed, or an add the chunk already holds, writes nothing.
	const merged = new Map<number, string[]>();
	for (const at of touched) {
		const before = at < n
			? termListOf(chunks.get(postingsChunkKey(base, entry, head.bounds[at]!)))
			: head.tail;
		const after = mergeKeys(before, (adds.get(at) ?? []).sort(), o.removes);
		if (after !== before) merged.set(at, after);
	}
	if (merged.size === 0) return head;

	const counts: number[] = [];
	const bounds: string[] = [];
	const putChunk = async (keys: string[], bound: string) => {
		await tx.put(postingsChunkKey(base, entry, bound), encodeKeyList(keys));
		counts.push(keys.length);
		bounds.push(bound);
	};
	for (let at = 0; at < n; at++) {
		const keys = merged.get(at);
		const bound = head.bounds[at]!;
		if (keys === undefined) {
			counts.push(head.counts[at]!);
			bounds.push(bound);
			continue;
		}
		if (keys.length === 0) {
			await tx.del(postingsChunkKey(base, entry, bound));
			continue;
		}
		// Near-equal pieces of at most the cap. Every piece but the last is
		// bounded by its own highest key; the last keeps the chunk's bound,
		// so the range the next chunk answers for does not move.
		const pieces = Math.ceil(keys.length / POSTINGS_CHUNK_CAP);
		const size = Math.ceil(keys.length / pieces);
		for (let p = 0; p < pieces - 1; p++) {
			const piece = keys.slice(p * size, (p + 1) * size);
			await putChunk(piece, piece[piece.length - 1]!);
		}
		await putChunk(keys.slice((pieces - 1) * size), bound);
	}
	let tail = merged.get(n) ?? head.tail;
	// The tail's lowest keys move into chunks of exactly the cap, which is
	// what a run of appends leaves behind: full chunks and a short tail.
	while (tail.length > POSTINGS_CHUNK_CAP) {
		const piece = tail.slice(0, POSTINGS_CHUNK_CAP);
		await putChunk(piece, piece[piece.length - 1]!);
		tail = tail.slice(POSTINGS_CHUNK_CAP);
	}
	return { counts, bounds, tail };
}

/**
 * `before` with `adds` merged in and `removes` taken out, both lists sorted
 * and free of repeats. Returns `before` itself when nothing changes.
 */
function mergeKeys(
	before: readonly string[],
	adds: readonly string[],
	removes: ReadonlySet<string>,
): string[] {
	const out: string[] = [];
	let changed = false;
	let j = 0;
	for (const k of before) {
		while (j < adds.length && adds[j]! < k) {
			out.push(adds[j++]!);
			changed = true;
		}
		if (j < adds.length && adds[j] === k) j++;
		if (removes.has(k)) {
			changed = true;
			continue;
		}
		out.push(k);
	}
	while (j < adds.length) {
		out.push(adds[j++]!);
		changed = true;
	}
	return changed ? out : before as string[];
}
