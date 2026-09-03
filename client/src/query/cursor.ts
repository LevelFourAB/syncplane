// Query cursors: a row's position in an ordering, as an opaque token.
//
// A cursor says WHERE a page ended rather than how many rows to discard before
// it, so the next page is unaffected by rows inserted ahead of it. It is the
// index entry (indexkey.ts) the row would occupy in an index whose components
// are exactly the query's order keys, framed with a version byte and a shape
// fingerprint and rendered base64url:
//
// 	cursor := 0x01 shape entry
// 	shape  := 16 ASCII hex digits — cursorShape, over the entity and order keys
//
// Key order over entries equals compareRows order, which is the one property
// the entry format guarantees and the only one a cursor needs. So "after this
// cursor" is `entry > cursorEntry` and "before" is `<`, and the same token
// serves as either bound.
//
// Unlike an index, a cursor may be built over a collaborative-text order key.
// compileIndexComponent rejects one because a keystroke would rewrite the
// stored entry; a cursor is computed and thrown away, so that cost never
// arises, and the resolver already projects text to its plain string for the
// comparator and the encoder alike.
//
// engine/query/order.go is the other half of the mirror: a cursor minted here
// is replayed there and the other way round, which is what makes specHash and
// the entry encoding a shared contract rather than two private ones.

import type { DescMessage } from '@bufbuild/protobuf';

import type { CompiledQuery, OrderKey, PageWindow, SeekBound } from './ast.js';
import { matcherOf } from './evaluate.js';
import {
	fingerprint,
	type IndexComponent,
	IndexEncoder,
	type IndexKind,
	specHash,
} from './indexkey.js';
import { leafKind, requireComparable, resolveLeafRef } from './wire.js';

/** The first byte of every encoded cursor. Bumping it makes every outstanding
 * cursor decode as unreadable rather than as something else. */
const CURSOR_VERSION = 0x01;

/** The width of the shape fingerprint inside a cursor. */
const CURSOR_SHAPE_LEN = 16;

/**
 * An opaque page boundary, tied to the (entity, orderBy) pair it was issued
 * under.
 *
 * It is a base64url STRING rather than bytes because that is what a pager does
 * with one: survive `JSON.stringify`, sit in a URL, live in component state.
 * Do not construct or edit one — read it off a result's `startCursor`
 * or `endCursor` and pass it back as `after` or `before`.
 */
export type QueryCursor = string & { readonly __queryCursor: unique symbol };

/**
 * Thrown when a cursor was minted for a different entity or a different
 * ordering than the query replaying it. Nothing is lost by it: the same query
 * run without the cursor issues one that fits.
 */
export class CursorShapeError extends Error {
	constructor(entity: string) {
		super(`query cursor belongs to a different entity or ordering than this query on "${entity}"`);
		this.name = 'CursorShapeError';
	}
}

/**
 * The cursor codec for one (schema, ordering) pair: it resolves the order keys
 * once and encodes a position per row, which is what a paged query does for
 * every candidate it considers.
 */
export class OrderCursors {
	private readonly encoder: IndexEncoder;
	private readonly spec: string;

	constructor(private readonly schema: DescMessage, orderBy: readonly OrderKey[]) {
		const comps = orderBy.map((key) => orderComponent(schema, key));
		this.encoder = new IndexEncoder(comps);
		this.spec = specHash(comps);
	}

	/**
	 * `row`'s position in this ordering, unframed — the entry rows are compared
	 * against. {@link encode} is the form a caller is handed.
	 *
	 * An order key never fans out (the resolver rejects the repeated and map
	 * leaves that would make it), so a row occupies exactly one position.
	 */
	entryOf(row: unknown, key: string): string {
		return this.encoder.encode(row, key, this.schema)[0]!;
	}

	/** Fingerprints the (entity, ordering) pair a cursor is valid against, as 16
	 * hex digits. */
	shape(entity: string): string {
		return fingerprint(`${this.spec}|${entity}`);
	}

	/** `row`'s position framed as the token a caller pages with. */
	encode(entity: string, row: unknown, key: string): QueryCursor {
		return toBase64Url(
			String.fromCharCode(CURSOR_VERSION) + this.shape(entity) + this.entryOf(row, key),
		) as QueryCursor;
	}

	/**
	 * Unwraps a cursor and returns the entry to compare rows against. Throws
	 * {@link CursorShapeError} for a cursor belonging to another entity or
	 * ordering, and a descriptive Error for anything that is not a cursor.
	 */
	decode(entity: string, cursor: QueryCursor): string {
		const framed = fromBase64Url(cursor);
		if (framed.length < 1 + CURSOR_SHAPE_LEN) {
			throw new Error(`query cursor is ${framed.length} bytes, too short to be one`);
		}
		if (framed.charCodeAt(0) !== CURSOR_VERSION) {
			throw new Error(`query cursor version ${framed.charCodeAt(0)} is not ${CURSOR_VERSION}`);
		}
		if (framed.slice(1, 1 + CURSOR_SHAPE_LEN) !== this.shape(entity)) {
			throw new CursorShapeError(entity);
		}
		const entry = framed.slice(1 + CURSOR_SHAPE_LEN);
		this.encoder.splitEntry(entry);
		return entry;
	}

	/**
	 * The component half of an entry from {@link entryOf} or {@link decode}: the
	 * position without the row-key tiebreak. It is the seek bound for an index
	 * carrying components past the ordering, whose entries the whole entry would
	 * not be a prefix of; see IndexEncoder.splitEntry.
	 */
	prefix(entry: string): string {
		return this.encoder.splitEntry(entry).components;
	}
}

/**
 * Resolves a compiled query's cursor bounds, or undefined when it carries
 * none. Decoding happens once here rather than per row; what the walk then
 * does per candidate is one encode and a string comparison.
 *
 * Throws CursorShapeError for a cursor minted under a different entity or
 * ordering, which is the whole reason the shape is carried.
 */
export function pageWindowOf(
	schema: DescMessage,
	entity: string,
	compiled: CompiledQuery,
): PageWindow | undefined {
	if (compiled.after === undefined && compiled.before === undefined) return undefined;
	const orderBy = compiled.orderBy ?? [];
	const cursors = new OrderCursors(schema, orderBy);
	const after = compiled.after === undefined ? undefined : cursors.decode(entity, compiled.after);
	const before = compiled.before === undefined ? undefined : cursors.decode(entity, compiled.before);
	return {
		backward: after === undefined && before !== undefined,
		seek: {
			...(after === undefined ? {} : { after: seekBound(cursors, after) }),
			...(before === undefined ? {} : { before: seekBound(cursors, before) }),
		},
		within(value, key) {
			// Both bounds are EXCLUSIVE, so the row a cursor was minted from is
			// never returned again.
			const entry = cursors.entryOf(value, key);
			if (after !== undefined && entry <= after) return false;
			if (before !== undefined && entry >= before) return false;
			return true;
		},
	};
}

/** One decoded cursor entry as the pair of prefixes a seek may compose with. */
function seekBound(cursors: OrderCursors, entry: string): SeekBound {
	return { components: cursors.prefix(entry), entry };
}

/**
 * One order key as an index component.
 *
 * Order keys and index components are the same thing — a resolved path and a
 * direction — and building one here rather than through compileIndexComponent
 * is what admits a collaborative-text order key and what resolves a map entry
 * addressed by key (`custom.priority.number`) rather than by the `*` fan an
 * index declares.
 */
function orderComponent(schema: DescMessage, key: OrderKey): IndexComponent {
	if (key.relevance) {
		if (key.match?.kind !== 'match') {
			throw new Error('a relevance order key must be prepared against its query (prepareQuery)');
		}
		return {
			path: [],
			protoPath: [],
			desc: key.dir === 'desc',
			kind: 'number',
			unsigned: false,
			fans: [],
			keyComponent: false,
			rank: matcherOf(key.match, schema),
		};
	}
	const leaf = resolveLeafRef(schema, key.path, true);
	requireComparable(key.path, leaf);
	return {
		path: leaf.path,
		protoPath: leaf.protoPath,
		desc: key.dir === 'desc',
		kind: leafKind(leaf) as IndexKind,
		unsigned: unsignedOf(leaf),
		fans: [],
		keyComponent: false,
	};
}

/**
 * Whether the leaf's values are already in unsigned byte order. It reads the
 * MAP VALUE type for an entry addressed by key, because that is the type the
 * value has — and because the Go encoder reads it off the same descriptor.
 */
function unsignedOf(
	leaf: { field: { fieldKind: string; scalar?: number }; keyed: boolean },
): boolean {
	// ScalarType.UINT64 === 4, ScalarType.FIXED64 === 6 — the proto field-type
	// numbers, which protobuf-es reuses verbatim.
	const is64 = leaf.field.scalar === 4 || leaf.field.scalar === 6;
	if (leaf.field.fieldKind === 'map') return leaf.keyed && is64;
	return leaf.field.fieldKind === 'scalar' && is64;
}

/** `row`'s unframed position under `orderBy`; see OrderCursors.entryOf. */
export function orderCursor(
	schema: DescMessage,
	orderBy: readonly OrderKey[],
	row: unknown,
	key: string,
): string {
	return new OrderCursors(schema, orderBy).entryOf(row, key);
}

/** The shape fingerprint a cursor over this (entity, ordering) carries. */
export function cursorShape(
	schema: DescMessage,
	entity: string,
	orderBy: readonly OrderKey[],
): string {
	return new OrderCursors(schema, orderBy).shape(entity);
}

/**
 * The wire form of a cursor. The RPC field is `bytes`, so the token is decoded
 * at the transport edge and nowhere else.
 */
export function cursorToBytes(cursor: QueryCursor): Uint8Array {
	const raw = fromBase64Url(cursor);
	const out = new Uint8Array(raw.length);
	for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i) & 0xff;
	return out;
}

/** {@link cursorToBytes} inverted, for a cursor arriving in a response. */
export function cursorFromBytes(bytes: Uint8Array): QueryCursor {
	let raw = '';
	for (let i = 0; i < bytes.length; i += 4096) {
		raw += String.fromCharCode(...bytes.subarray(i, i + 4096));
	}
	return toBase64Url(raw) as QueryCursor;
}

const B64URL = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

/**
 * base64url without padding, over a string of one character per byte. Written
 * out rather than delegated to `btoa`, which is absent in some of the runtimes
 * this client ships to and throws on any character above 0xff rather than
 * telling the caller which one.
 */
function toBase64Url(raw: string): string {
	let out = '';
	for (let i = 0; i < raw.length; i += 3) {
		const a = raw.charCodeAt(i) & 0xff;
		const b = i + 1 < raw.length ? raw.charCodeAt(i + 1) & 0xff : 0;
		const c = i + 2 < raw.length ? raw.charCodeAt(i + 2) & 0xff : 0;
		out += B64URL[a >> 2]! + B64URL[((a & 0x03) << 4) | (b >> 4)]!;
		if (i + 1 < raw.length) out += B64URL[((b & 0x0f) << 2) | (c >> 6)]!;
		if (i + 2 < raw.length) out += B64URL[c & 0x3f]!;
	}
	return out;
}

const B64URL_INDEX = new Map([ ...B64URL ].map((ch, i) => [ ch, i ]));

function fromBase64Url(text: string): string {
	let out = '';
	let acc = 0;
	let bits = 0;
	for (const ch of text) {
		const v = B64URL_INDEX.get(ch);
		if (v === undefined) throw new Error(`query cursor is not base64url: "${ch}"`);
		acc = (acc << 6) | v;
		bits += 6;
		if (bits >= 8) {
			bits -= 8;
			out += String.fromCharCode((acc >> bits) & 0xff);
		}
	}
	return out;
}
