// Canonical binary codec for TextDoc — byte-identical to the Go
// engine/text encoder (see encode.go there for the format definition).
// Cross-language vectors compare raw bytes, so every ordering choice here
// (peer table sort, run splitting, mark sort) must match Go exactly.

import {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	AnchorDocEnd,
	AnchorDocStart,
	type AnchorKind,
	comparePeers,
	type Expand,
	ExpandBoth,
	idKey,
	type ItemID,
	type MarkOp,
	type MarkValue,
	requireWellFormed,
	SIDE_L,
	SIDE_R,
	TextDoc,
	type TextNode,
} from './doc.js';
import { textResidency } from './residency.js';

const FORMAT_VERSION = 1;

/** Format version 2 is the delta-state FRAGMENT layout (see fragment.ts):
 * v1's sections with relaxed reference resolution and the source document's
 * full version vector. Stored rows are always version 1. */
export const FRAGMENT_VERSION = 2;

/** Format version 3 is a PLAIN-STRING value: the tag followed by the text's
 * UTF-8 bytes, the form a field carries when it says what it should read as
 * without having been resolved into a document. The Go counterpart and the
 * reasoning are in engine/text/plain.go. */
export const PLAIN_STRING_VERSION = 3;

const utf8Encoder = new TextEncoder();
// ignoreBOM keeps a leading U+FEFF as content. The default removes it, and a
// run, mark value or plain string can start with that code point.
const utf8Decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

/** Decodes UTF-8, restating the platform's decoder error as the message the
 * Go decoder uses for the same bytes (see `text: invalid UTF-8 ...` there). */
export function decodeUtf8(b: Uint8Array, what: string): string {
	try {
		return utf8Decoder.decode(b);
	} catch {
		throw new Error(`text: invalid UTF-8 ${what}`);
	}
}

export class ByteWriter {
	private chunks: number[] = [];

	byte(b: number): void {
		this.chunks.push(b & 0xff);
	}

	/** Unsigned LEB128. Safe integers only — counters stay far below 2^53. */
	uvarint(v: number): void {
		if (!Number.isSafeInteger(v) || v < 0) {
			throw new Error(`text: cannot encode varint ${v}`);
		}
		while (v >= 0x80) {
			this.chunks.push((v % 0x80) | 0x80);
			v = Math.floor(v / 0x80);
		}
		this.chunks.push(v);
	}

	raw(b: Uint8Array): void {
		for (const x of b) this.chunks.push(x);
	}

	bytes(b: Uint8Array): void {
		this.uvarint(b.length);
		this.raw(b);
	}

	str(s: string): void {
		this.bytes(utf8Encoder.encode(s));
	}

	finish(): Uint8Array {
		return Uint8Array.from(this.chunks);
	}
}

export class ByteReader {
	private off = 0;

	constructor(private readonly buf: Uint8Array) {}

	get done(): boolean {
		return this.off === this.buf.length;
	}

	get remaining(): number {
		return this.buf.length - this.off;
	}

	byte(): number {
		if (this.off >= this.buf.length) throw new Error('text: truncated document');
		return this.buf[this.off++]!;
	}

	uvarint(): number {
		let v = 0;
		let shiftMul = 1;
		for (let i = 0; i < 10; i++) {
			const b = this.byte();
			v += (b & 0x7f) * shiftMul;
			if ((b & 0x80) === 0) {
				if (!Number.isSafeInteger(v)) throw new Error('text: varint overflows safe integer');
				return v;
			}
			shiftMul *= 0x80;
		}
		throw new Error('text: malformed varint');
	}

	/**
	 * Reads an element count and rejects it when the bytes left cannot hold
	 * that many elements, given the smallest legal encoding of one (minBytes).
	 * Every count in the format is immediately followed by the elements it
	 * describes, so a larger count is always malformed. Mirrors the Go
	 * decoder's count(), message included, so both languages refuse the same
	 * bytes for the same stated reason.
	 */
	count(what: string, minBytes: number): number {
		const n = this.uvarint();
		if (n > Math.floor(this.remaining / minBytes)) {
			throw new Error(`text: ${what} ${n} exceeds the ${this.remaining} bytes remaining`);
		}
		return n;
	}

	bytes(): Uint8Array {
		const n = this.uvarint();
		if (this.buf.length - this.off < n) throw new Error('text: truncated document');
		const b = this.buf.subarray(this.off, this.off + n);
		this.off += n;
		return b;
	}

	str(): string {
		return decodeUtf8(this.bytes(), 'string');
	}

	float64(): number {
		if (this.buf.length - this.off < 8) throw new Error('text: truncated document');
		const v = new DataView(this.buf.buffer, this.buf.byteOffset + this.off, 8).getFloat64(0, false);
		this.off += 8;
		return v;
	}
}

/** Serializes a document canonically (see the Go encoder for the format). */
export function encodeTextDoc(doc: TextDoc): Uint8Array {
	const peers = [ ...doc.vv.keys() ].sort(comparePeers);
	const peerIdx = new Map<string, number>();
	peers.forEach((p, i) => peerIdx.set(p, i));

	const w = new ByteWriter();
	w.byte(FORMAT_VERSION);
	w.uvarint(peers.length);
	for (const p of peers) w.str(p);
	for (const p of peers) w.uvarint(doc.vv.get(p)!);

	// Canonical maximal runs: per peer (table order), counters ascending,
	// split where the run invariant breaks (see Go encode.go).
	const nodesByPeer = new Map<string, TextNode[]>();
	for (const n of doc.byId.values()) {
		let list = nodesByPeer.get(n.id.peer);
		if (!list) {
			list = [];
			nodesByPeer.set(n.id.peer, list);
		}
		list.push(n);
	}
	interface Run {
		peerIdx: number;
		start: number;
		items: TextNode[];
	}
	const runs: Run[] = [];
	for (const p of peers) {
		const nodes = (nodesByPeer.get(p) ?? []).sort((a, b) => a.id.counter - b.id.counter);
		for (let i = 0; i < nodes.length; i++) {
			const n = nodes[i]!;
			if (i > 0) {
				const prev = nodes[i - 1]!;
				const last = runs[runs.length - 1]!;
				if (
					n.id.counter === prev.id.counter + 1
					&& n.parent === prev && n.side === SIDE_R
					&& n.deleted === prev.deleted
				) {
					last.items.push(n);
					continue;
				}
			}
			runs.push({ peerIdx: peerIdx.get(p)!, start: n.id.counter, items: [ n ] });
		}
	}

	w.uvarint(runs.length);
	for (const r of runs) {
		w.uvarint(r.peerIdx);
		w.uvarint(r.start);
		w.uvarint(r.items.length);
		const head = r.items[0]!;
		let flags = 0;
		if (head.deleted) flags |= 1;
		if (head.parent === doc.root) flags |= 2;
		if (head.side === SIDE_R) flags |= 4;
		w.byte(flags);
		if ((flags & 2) === 0) {
			w.uvarint(peerIdx.get(head.parent!.id.peer)!);
			w.uvarint(head.parent!.id.counter);
		}
		let content = '';
		for (const it of r.items) content += it.content;
		w.bytes(utf8Encoder.encode(content));
	}

	const marks = [ ...doc.marks ].sort((a, b) =>
		peerIdx.get(a.id.peer)! - peerIdx.get(b.id.peer)!
		|| a.id.counter - b.id.counter
	);
	w.uvarint(marks.length);
	for (const m of marks) {
		w.uvarint(peerIdx.get(m.id.peer)!);
		w.uvarint(m.id.counter);
		w.str(m.key);
		const v = m.value;
		if (v === null) {
			w.byte(0);
		} else if (typeof v === 'boolean') {
			w.byte(v ? 2 : 1);
		} else if (typeof v === 'string') {
			w.byte(3);
			w.str(v);
		} else {
			w.byte(4);
			const buf = new Uint8Array(8);
			new DataView(buf.buffer).setFloat64(0, v, false);
			w.raw(buf);
		}
		for (const a of [ m.start, m.end ]) {
			w.byte(a.kind);
			if (a.kind === AnchorBefore || a.kind === AnchorAfter) {
				w.uvarint(peerIdx.get(a.item!.peer)!);
				w.uvarint(a.item!.counter);
			}
		}
		w.byte(m.expand);
	}
	return w.finish();
}

/**
 * Parses an encoded document. peer becomes the writer identity for later
 * edits (pass '' for read-only use). Empty input decodes to an empty
 * document — an absent bytes field is a document nobody wrote yet.
 */
export function decodeTextDoc(data: Uint8Array | undefined, peer: string): TextDoc {
	const doc = new TextDoc(peer);
	if (!data || data.length === 0) return doc;
	const r = new ByteReader(data);
	const version = r.byte();
	if (version === FRAGMENT_VERSION) {
		// Fragments are replication payloads only; a caller decoding one as a
		// document is about to persist partial state as a row, which must fail
		// loudly instead.
		throw new Error('text: fragment where full document expected');
	}
	if (version === PLAIN_STRING_VERSION) {
		// A plain-string value names text without saying who wrote it, and
		// building a document from it would have to mint items under some peer
		// — the choice this package refuses to make for a caller. plainTextOf
		// reads it; a resolver applies it.
		throw new Error(
			'text: plain-string value where full document expected; resolve it under a writer identity first',
		);
	}
	if (version !== FORMAT_VERSION) {
		throw new Error(`text: unsupported format version ${version}`);
	}

	// A peer costs at least two bytes: a zero-length name plus its version
	// vector counter.
	const peerCount = r.count('peer count', 2);
	const peers: string[] = [];
	for (let i = 0; i < peerCount; i++) peers.push(r.str());
	for (let i = 0; i < peerCount; i++) {
		doc.vv.set(peers[i]!, r.uvarint());
	}
	const peerAt = (idx: number): string => {
		const p = peers[idx];
		if (p === undefined) throw new Error(`text: peer index ${idx} out of range`);
		return p;
	};

	/** A run HEAD, whose parent may be any item and so resolves after pass 1. */
	interface PendingAttach {
		n: TextNode;
		parent?: ItemID;
	}
	// A run costs at least six bytes: peerIdx, startCounter, itemCount, flags,
	// contentLen, and — since itemCount must be non-zero — a byte of content.
	const runCount = r.count('run count', 6);
	const attaches: PendingAttach[] = [];
	for (let i = 0; i < runCount; i++) {
		const p = peerAt(r.uvarint());
		const start = r.uvarint();
		if (start === 0) {
			// Counter 0 belongs to the root sentinel. An item holding it would
			// share the root's id, and bumpVV never records a peer whose highest
			// counter is 0, so the encoder would find no peer-table entry for
			// the item. Only a run head can hold it: later items count up from
			// the head.
			throw new Error(`text: item ${p}:0 uses reserved counter 0`);
		}
		const itemCount = r.uvarint();
		if (itemCount === 0) throw new Error('text: empty run');
		const flags = r.byte();
		const deleted = (flags & 1) !== 0;
		const rootParent = (flags & 2) !== 0;
		const side = (flags & 4) !== 0 ? SIDE_R : SIDE_L;
		let parentId: ItemID | undefined;
		if (!rootParent) {
			const pp = peerAt(r.uvarint());
			parentId = { peer: pp, counter: r.uvarint() };
		}
		const content = decodeUtf8(r.bytes(), 'in run content');
		let prev: TextNode | undefined;
		let seen = 0;
		for (const ch of content) {
			if (seen >= itemCount) throw new Error('text: run content longer than item count');
			const id = { peer: p, counter: start + seen };
			const key = idKey(id);
			const n: TextNode = {
				id,
				key,
				content: ch,
				deleted,
				parent: null,
				side: SIDE_R,
				left: [],
				right: [],
			};
			if (doc.byId.has(key)) {
				throw new Error(`text: duplicate item ${n.id.peer}:${n.id.counter}`);
			}
			doc.addNode(n, key);
			if (!prev) {
				n.side = side;
				if (rootParent) n.parent = doc.root;
				attaches.push({ n, parent: rootParent ? undefined : parentId });
			} else {
				// Attached here rather than in the pass below: the parent is the
				// item just decoded and has no children yet, so the sorted insert
				// has nothing to search. Only run HEADS reference an item that may
				// not exist yet.
				n.parent = prev;
				n.side = SIDE_R;
				prev.right = [ n ];
			}
			prev = n;
			seen++;
		}
		if (seen !== itemCount) throw new Error('text: run content shorter than item count');
	}
	// Only run heads are still detached; every other item was attached to its
	// run predecessor above. Sorted inserts make attach order irrelevant to the
	// result, and attaching a head whose parent is another head works either
	// way round, since attaching reads the parent's child list and not its
	// parent pointer.
	for (const a of attaches) {
		if (a.parent) {
			const p = doc.byId.get(idKey(a.parent));
			if (!p) {
				throw new Error(
					`text: item ${a.n.id.peer}:${a.n.id.counter} references unknown parent `
						+ `${a.parent.peer}:${a.parent.counter}`,
				);
			}
			a.n.parent = p;
		}
		doc.attach(a.n);
	}
	doc.invalidateOrder();
	if (doc.order().length !== doc.byId.size) {
		throw new Error(
			`text: ${doc.byId.size - doc.order().length} of ${doc.byId.size} items `
				+ `unreachable from the root (parent cycle)`,
		);
	}

	// A mark costs at least seven bytes: peerIdx, counter, keyLen, the value
	// tag, both anchor kinds, and expand.
	const markCount = r.count('mark count', 7);
	const decodeAnchor = (): Anchor => {
		const kind = r.byte() as AnchorKind;
		switch (kind) {
			case AnchorDocStart:
			case AnchorDocEnd:
				return { kind };
			case AnchorBefore:
			case AnchorAfter: {
				const p = peerAt(r.uvarint());
				const id: ItemID = { peer: p, counter: r.uvarint() };
				if (!doc.byId.has(idKey(id))) {
					throw new Error(`text: anchor references unknown item ${p}:${id.counter}`);
				}
				return { kind, item: id };
			}
			default:
				throw new Error(`text: invalid anchor kind ${kind}`);
		}
	};
	const seenMarks = new Set<string>();
	for (let i = 0; i < markCount; i++) {
		const p = peerAt(r.uvarint());
		const id: ItemID = { peer: p, counter: r.uvarint() };
		// Reserved for the same reason as an item's counter 0 above.
		if (id.counter === 0) throw new Error(`text: mark op ${p}:0 uses reserved counter 0`);
		if (seenMarks.has(idKey(id))) {
			throw new Error(`text: duplicate mark op ${p}:${id.counter}`);
		}
		seenMarks.add(idKey(id));
		const key = r.str();
		const tag = r.byte();
		let value: MarkValue;
		switch (tag) {
			case 0:
				value = null;
				break;
			case 1:
				value = false;
				break;
			case 2:
				value = true;
				break;
			case 3:
				value = r.str();
				break;
			case 4:
				value = r.float64();
				// TextDoc.mark refuses these at creation; refusing them here
				// too keeps the rule at both ends of the pipe. A NaN would
				// otherwise compare unequal to itself and break resolveMarks
				// into one span per character.
				if (!Number.isFinite(value)) throw new Error('text: non-finite mark value');
				break;
			default:
				throw new Error(`text: invalid mark value tag ${tag}`);
		}
		const start = decodeAnchor();
		const end = decodeAnchor();
		const expand = r.byte();
		if (expand > ExpandBoth) throw new Error(`text: invalid expand ${expand}`);
		const op: MarkOp = { id, key, value, start, end, expand: expand as Expand };
		doc.marks.push(op);
		doc.bumpVV(p, id.counter);
	}
	if (!r.done) throw new Error(`text: ${r.remaining} trailing bytes`);
	return doc;
}

/** Serializes an anchor for storage outside its document (mirror of Go
 * EncodeAnchor: kind byte, then peer length + bytes and counter for item
 * anchors). */
export function encodeAnchor(a: Anchor): Uint8Array {
	const w = new ByteWriter();
	w.byte(a.kind);
	if (a.kind === AnchorBefore || a.kind === AnchorAfter) {
		w.str(a.item!.peer);
		w.uvarint(a.item!.counter);
	}
	return w.finish();
}

/** Parses an encodeAnchor payload. */
export function decodeAnchor(data: Uint8Array): Anchor {
	if (data.length === 0) throw new Error('text: empty anchor');
	const r = new ByteReader(data);
	const kind = r.byte() as AnchorKind;
	switch (kind) {
		case AnchorDocStart:
		case AnchorDocEnd:
			if (!r.done) throw new Error('text: trailing bytes in anchor');
			return { kind };
		case AnchorBefore:
		case AnchorAfter: {
			const peer = r.str();
			const counter = r.uvarint();
			if (!r.done) throw new Error('text: trailing bytes in anchor');
			return { kind, item: { peer, counter } };
		}
		default:
			throw new Error(`text: invalid anchor kind ${kind}`);
	}
}

/**
 * Returns the text an encoded value reads as: a document's visible text, or a
 * plain-string value's text. Empty input reads as `''`.
 *
 * The cheap read path for query evaluation, which decodes each row fresh, and
 * the one to take when only the text is wanted — `decodeTextDoc` accepts
 * documents alone, and a field-history record's before-state carries the other
 * form. Rendering wants `textOf`, which memoizes this per byte instance.
 *
 * A resident document is borrowed rather than decoded (residency.ts), which is
 * what keeps a predicate over the row someone is typing into off the decode
 * path. Nothing is registered on a miss: a query scans rows it will not look at
 * again, and filling the budget with them would evict the documents that are
 * being read repeatedly.
 */
export function plainTextOf(data: Uint8Array | undefined): string {
	if (!data || data.length === 0) return '';
	if (data[0] === PLAIN_STRING_VERSION) return plainString(data);
	return (textResidency.lend(data) ?? decodeTextDoc(data, '')).plainText();
}

/**
 * Whether data is a plain-string value. Empty input is not one: an absent
 * bytes field is a document nobody wrote yet.
 */
export function isPlainString(data: Uint8Array | undefined): boolean {
	return data !== undefined && data.length > 0 && data[0] === PLAIN_STRING_VERSION;
}

/**
 * Builds the plain-string value reading as s. Throws for the text the write
 * entry points refuse (an unpaired surrogate), so a value that could never be
 * applied cannot be built.
 *
 * The empty string encodes to a one-byte value rather than to nothing, which
 * distinguishes "reads as empty" from an absent field. Callers that want the
 * two to be indistinguishable should store no value at all.
 */
export function encodePlainString(s: string): Uint8Array {
	requireWellFormed('plain string', s);
	const body = utf8Encoder.encode(s);
	const out = new Uint8Array(body.length + 1);
	out[0] = PLAIN_STRING_VERSION;
	out.set(body, 1);
	return out;
}

/**
 * Returns the text a plain-string value reads as. Throws when data is not one
 * — including when it is a document, which plainTextOf reads instead.
 */
export function plainString(data: Uint8Array): string {
	if (!isPlainString(data)) throw new Error('text: not a plain-string value');
	return decodeUtf8(data.subarray(1), 'in plain-string value');
}
