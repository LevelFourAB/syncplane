// Convergent (CRDT) text with Peritext-style marks and stable anchors — the
// TypeScript mirror of the Go engine/text package. Both sides implement the
// identical model with a byte-identical canonical encoding; cross-language
// conformance vectors live in testdata/text/vectors.json.
//
// Ordering is Fugue (https://arxiv.org/abs/2305.00583, tree formulation), so
// concurrent insertions at the same position never interleave. Replication is
// state-based: every write ships the whole encoded document and merging is a
// pure union (idempotent, commutative, associative).
//
// All indices in this module count Unicode CODE POINTS over the visible text
// — the same unit the Go side and the shared vectors use. The schema-bound
// client API (client.db.<entity>.text(...)) converts JS UTF-16 string indices
// at its boundary; see utf16ToCodePoint/codePointToUtf16.

/** Permanent identity of an item or mark operation. */
export interface ItemID {
	/** Writer identity (the sync client_id or a server-side writer's name). */
	peer: string;
	/**
	 * Unique per peer within a document, allocated Lamport-style (past every
	 * counter seen at allocation time). Never zero.
	 */
	counter: number;
}

export const AnchorDocStart = 0;
export const AnchorDocEnd = 1;
export const AnchorBefore = 2;
export const AnchorAfter = 3;
export type AnchorKind =
	| typeof AnchorDocStart
	| typeof AnchorDocEnd
	| typeof AnchorBefore
	| typeof AnchorAfter;

/**
 * A stable position in a document: immediately before or after a specific
 * item, or a document edge. An anchor held INSIDE a document (a mark boundary)
 * stays resolvable for the life of the document — across remote edits and
 * deletions of the referenced item — because compaction keeps every item a
 * mark anchors to (see compact.ts).
 *
 * An anchor stored OUTSIDE its document (an undo entry, a presence broadcast)
 * has no such protection: compaction can drop the item it names, and
 * resolveAnchor then throws. Callers holding long-lived external anchors must
 * treat that as "the position is gone", not as corruption.
 *
 * An anchor also survives the deletion of the text it was placed on but not
 * that deletion being undone: undo re-inserts the content as NEW items (there
 * is no un-delete on a monotone tombstone), so an anchor into the old items
 * resolves to the collapsed position rather than to the restored text. Marks
 * are exempt — they are re-applied over the restored range, see undo-ops.ts.
 */
export interface Anchor {
	kind: AnchorKind;
	/** Set for AnchorBefore/AnchorAfter. */
	item?: ItemID;
}

export const ExpandNone = 0;
export const ExpandBefore = 1;
export const ExpandAfter = 2;
export const ExpandBoth = 3;
/** Whether text typed at a mark's boundary joins the mark. */
export type Expand =
	| typeof ExpandNone
	| typeof ExpandBefore
	| typeof ExpandAfter
	| typeof ExpandBoth;

/** A mark's value: null is reserved for removals (unmark). */
export type MarkValue = null | boolean | string | number;

/** A resolved mark range over the visible text, [start, end) in code points. */
export interface Span {
	start: number;
	end: number;
	key: string;
	value: Exclude<MarkValue, null>;
}

/** One key/value that applies at a caret position (see TextDoc.marksAt). */
export interface PendingMark {
	key: string;
	value: Exclude<MarkValue, null>;
}

/** A resolved zero-width mark, at the visible index it sits between (see
 * TextDoc.points). */
export interface Point {
	index: number;
	key: string;
	value: Exclude<MarkValue, null>;
}

export const SIDE_L = 0;
export const SIDE_R = 1;

/** One item of the Fugue tree (internal, exposed for the codec). */
export interface TextNode {
	id: ItemID;
	/**
	 * idKey(id), computed once at creation. Every map holding items is keyed by
	 * it, and looking an item up in ANOTHER document — which merge, diff and
	 * compaction all do per item, per write — would otherwise rebuild the string
	 * every time.
	 */
	key: string;
	/** One Unicode code point. */
	content: string;
	deleted: boolean;
	parent: TextNode | null; // null only on the root sentinel
	side: typeof SIDE_L | typeof SIDE_R;
	left: TextNode[];
	right: TextNode[]; // children, each sorted ascending by id
}

/** The id range of one insert's items (consecutive counters, count 0 when
 * nothing was inserted). */
export interface InsertedRange {
	peer: string;
	start: number;
	count: number;
}

/** A mark run over a deleted segment's content, [start, end) in code points
 * RELATIVE TO THE SEGMENT (unlike Span, which counts over the whole visible
 * text). See DeletedSegment.marks. */
export interface SegmentMark {
	start: number;
	end: number;
	key: string;
	value: Exclude<MarkValue, null>;
}

/** One visibly-contiguous run of deleted content plus the anchor to
 * re-insert it at (after the nearest surviving predecessor). */
export interface DeletedSegment {
	content: string;
	at: Anchor;
	/**
	 * The items this segment tombstoned, in visible order. A delete mints no
	 * new id, so the version vector cannot reveal it: this is the only way a
	 * caller can name what changed, which is what encoding the edit as a
	 * fragment needs (see text/fragment.ts encodeFragmentFrom). Has no
	 * counterpart in `engine/text` — the server writes whole documents and has
	 * no local delta storage to feed.
	 */
	ids: ItemID[];
	/**
	 * The marks resolved over this content at delete time, so an undo can put
	 * the formatting back. Tombstones are monotone, so undoing a delete
	 * re-inserts the content as new items and every mark op over the old ones
	 * resolves to nothing; without this the restored text comes back bare and
	 * comment ranges (annotations.ts) are silently orphaned. Empty for an
	 * unmarked document — resolving is only paid for when the document has
	 * mark ops at all.
	 */
	marks: SegmentMark[];
}

/** A resolved prior value of one key over a sub-range, anchored so the range
 * survives concurrent edits (see TextDoc.priorSpans). */
export interface PriorSpan {
	start: Anchor;
	end: Anchor;
	value: Exclude<MarkValue, null>;
}

/** One mark operation (apply, or with a null value, remove). */
export interface MarkOp {
	id: ItemID;
	key: string;
	value: MarkValue;
	start: Anchor;
	end: Anchor;
	expand: Expand;
}

const utf8 = new TextEncoder();

/** Compares strings by UTF-8 bytes — Go's `<` on strings, NOT UTF-16 order. */
export function comparePeers(a: string, b: string): number {
	if (a === b) return 0;
	const ba = utf8.encode(a);
	const bb = utf8.encode(b);
	const n = Math.min(ba.length, bb.length);
	for (let i = 0; i < n; i++) {
		if (ba[i]! !== bb[i]!) return ba[i]! < bb[i]! ? -1 : 1;
	}
	return ba.length - bb.length;
}

/** Total order on ids: (counter, peer). Mirrors Go ID.less. */
export function compareIds(a: ItemID, b: ItemID): number {
	if (a.counter !== b.counter) return a.counter - b.counter;
	return comparePeers(a.peer, b.peer);
}

export function idKey(id: ItemID): string {
	return `${id.counter}\u0000${id.peer}`;
}

/**
 * The id of n's parent. The root sentinel carries the zero id, which no real
 * item can hold (counters are never zero), so children of the root compare
 * equal to each other and to nothing else.
 */
export function parentId(n: TextNode): ItemID {
	return n.parent?.id ?? { peer: '', counter: 0 };
}

/**
 * Whether two nodes — normally from different documents — sit under the same
 * parent. Equivalent to comparing parentId()s, but allocates nothing: merge
 * and fragment diffing ask this of every item in the document on every write,
 * where building two ids and two key strings per item dominated the cost.
 */
export function sameParent(a: TextNode, b: TextNode): boolean {
	const ap = a.parent?.id;
	const bp = b.parent?.id;
	return ap?.counter === bp?.counter && ap?.peer === bp?.peer;
}

function sameAnchor(a: Anchor, b: Anchor): boolean {
	if (a.kind !== b.kind) return false;
	if (a.item === undefined || b.item === undefined) return a.item === b.item;
	return a.item.counter === b.item.counter && a.item.peer === b.item.peer;
}

/**
 * Whether two mark ops carrying the same id really are the same op. Values
 * compare with Object.is so a NaN — which mark() rejects but a hand-built
 * encoding could still carry — equals itself instead of turning a document
 * into a permanent merge failure.
 */
export function sameMarkOp(a: MarkOp, b: MarkOp): boolean {
	return a.key === b.key && Object.is(a.value, b.value) && a.expand === b.expand
		&& sameAnchor(a.start, b.start) && sameAnchor(a.end, b.end);
}

/** Inserts c into the correct side child list of parent, sorted by id. */
function addChild(parent: TextNode, c: TextNode): void {
	const list = c.side === SIDE_L ? parent.left : parent.right;
	let lo = 0;
	let hi = list.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (compareIds(list[mid]!.id, c.id) < 0) lo = mid + 1;
		else hi = mid;
	}
	list.splice(lo, 0, c);
}

/** Converts a UTF-16 index into text to a code-point index. Throws when the
 * index splits a surrogate pair. */
export function utf16ToCodePoint(text: string, index16: number): number {
	if (index16 < 0 || index16 > text.length) {
		throw new RangeError(`index ${index16} out of range [0..${text.length}]`);
	}
	let cp = 0;
	let i = 0;
	while (i < index16) {
		const code = text.codePointAt(i)!;
		i += code > 0xffff ? 2 : 1;
		cp++;
	}
	if (i !== index16) {
		throw new RangeError(`index ${index16} splits a surrogate pair`);
	}
	return cp;
}

/** Converts a code-point index into text to a UTF-16 index. */
export function codePointToUtf16(text: string, indexCp: number): number {
	let cp = 0;
	let i = 0;
	while (cp < indexCp) {
		if (i >= text.length) {
			throw new RangeError(`code point index ${indexCp} out of range`);
		}
		const code = text.codePointAt(i)!;
		i += code > 0xffff ? 2 : 1;
		cp++;
	}
	return i;
}

/**
 * The UTF-16 offset of every code-point index of text, entry i being the
 * offset of code point i and the last entry text.length.
 *
 * codePointToUtf16 counts from the start of the string, so converting a whole
 * set of positions one at a time costs a full scan per position — the shape
 * that made resolved mark spans expensive to hand out, since every span has two
 * boundaries. Callers converting more than a couple of indices into the same
 * string should build this once and index into it.
 */
export function codePointOffsets(text: string): Int32Array {
	// One code point per UTF-16 unit is the upper bound; trimmed at the end.
	const offsets = new Int32Array(text.length + 1);
	let cp = 0;
	let i = 0;
	while (i < text.length) {
		offsets[cp++] = i;
		i += text.codePointAt(i)! > 0xffff ? 2 : 1;
	}
	offsets[cp] = text.length;
	return offsets.subarray(0, cp + 1);
}

/** Sentinel distinguishing "painted removed" from "never painted". */
const REMOVED = Symbol('removed');

/**
 * One mark op's resolved coverage: a half-open range of full-order positions
 * and the value it paints there (REMOVED for an unmark, so "painted as
 * removed" stays distinct from "never painted").
 */
interface OpRange {
	value: MarkValue | typeof REMOVED;
	lo: number;
	hi: number;
}

/** A stretch of the full order that one mark key's ops cover. */
interface Segment {
	lo: number;
	hi: number;
}

/**
 * Reduces one key's op ranges to the disjoint stretches they cover, so
 * resolution can skip everything between them.
 *
 * Two ranges join when they overlap, touch, OR are separated only by
 * tombstones. The last case is the one that is easy to get wrong: the emit walk
 * skips deleted characters, so a gap made entirely of them does not interrupt a
 * run of equal value, and splitting there would report two spans where
 * whole-document painting reports one.
 */
function mergeSegments(ranges: OpRange[], idxs: number[], visBefore: Int32Array): Segment[] {
	const order = [ ...idxs ].sort((a, b) => ranges[a]!.lo - ranges[b]!.lo);
	const segs: Segment[] = [];
	for (const ri of order) {
		const r = ranges[ri]!;
		const last = segs[segs.length - 1];
		if (last && (r.lo <= last.hi || visBefore[r.lo] === visBefore[last.hi])) {
			if (r.hi > last.hi) last.hi = r.hi;
			continue;
		}
		segs.push({ lo: r.lo, hi: r.hi });
	}
	return segs;
}

/** Index of the segment containing full-order position lo. */
function segmentOf(segs: Segment[], lo: number): number {
	let low = 0;
	let high = segs.length;
	while (low < high) {
		const mid = (low + high) >>> 1;
		if (segs[mid]!.hi > lo) high = mid;
		else low = mid + 1;
	}
	return low;
}

/**
 * A mutable text document. Not safe for concurrent use; the sync client keeps
 * one per open handle and rebuilds from encoded state on remote changes.
 */
export class TextDoc {
	/** Local writer identity; '' = read-only. */
	peer: string;
	readonly root: TextNode;
	readonly byId = new Map<string, TextNode>();
	/** Version vector: highest counter seen per peer. */
	readonly vv = new Map<string, number>();
	marks: MarkOp[] = [];

	private order_: TextNode[] | null = null;

	constructor(peer: string) {
		this.peer = peer;
		this.root = {
			id: { peer: '', counter: 0 },
			key: idKey({ peer: '', counter: 0 }),
			content: '',
			deleted: false,
			parent: null,
			side: SIDE_R,
			left: [],
			right: [],
		};
	}

	/** Marks the cached traversal stale (codec + merge use this). */
	invalidateOrder(): void {
		this.order_ = null;
	}

	/**
	 * The in-order traversal, tombstones included. Iterative — a document
	 * typed backwards is a deep chain of left children and recursion would
	 * blow the JS stack.
	 */
	order(): TextNode[] {
		if (this.order_) return this.order_;
		const out: TextNode[] = [];
		interface Frame {
			n: TextNode;
			stage: 0 | 1;
			idx: number;
		}
		const stack: Frame[] = [ { n: this.root, stage: 0, idx: 0 } ];
		while (stack.length > 0) {
			const f = stack[stack.length - 1]!;
			if (f.stage === 0) {
				if (f.idx < f.n.left.length) {
					stack.push({ n: f.n.left[f.idx]!, stage: 0, idx: 0 });
					f.idx++;
					continue;
				}
				if (f.n !== this.root) out.push(f.n);
				f.stage = 1;
				f.idx = 0;
				continue;
			}
			if (f.idx < f.n.right.length) {
				stack.push({ n: f.n.right[f.idx]!, stage: 0, idx: 0 });
				f.idx++;
				continue;
			}
			stack.pop();
		}
		this.order_ = out;
		return out;
	}

	/** Visible length in code points. */
	get length(): number {
		let n = 0;
		for (const it of this.order()) {
			if (!it.deleted) n++;
		}
		return n;
	}

	/** The visible text. */
	plainText(): string {
		let s = '';
		for (const it of this.order()) {
			if (!it.deleted) s += it.content;
		}
		return s;
	}

	versionVector(): Map<string, number> {
		return new Map(this.vv);
	}

	/**
	 * Raises the version vector to cover vv WITHOUT importing any items, so
	 * subsequent edits allocate counters past everything vv describes. Mirror
	 * of Go ObserveVersion — the enabler for three-way writers that diff
	 * against a stale ancestor but merge into a newer state while keeping one
	 * peer (see the Go doc for the full reasoning).
	 */
	observeVersion(vv: Map<string, number>): void {
		for (const [ p, c ] of vv) {
			this.bumpVV(p, c);
		}
	}

	/**
	 * Replaces this document's contents with a deep copy of other's, keeping
	 * this object's identity and writer peer. The one caller is a live handle
	 * that has just seen the server compact the field (see handle.ts): the
	 * incoming state is not a superset of what the handle holds, so merging it
	 * would keep characters the server dropped, and callers hold the handle's
	 * `doc` by reference and must keep seeing the same object.
	 */
	replaceWith(other: TextDoc): void {
		this.byId.clear();
		this.vv.clear();
		this.marks = [];
		// Source node -> its copy, keyed by node identity rather than by item key,
		// so relinking the tree below builds no key strings at all.
		const copies = new Map<TextNode, TextNode>();
		for (const on of other.byId.values()) {
			copies.set(on, {
				id: on.id,
				key: on.key,
				content: on.content,
				deleted: on.deleted,
				parent: null,
				side: on.side,
				left: [],
				right: [],
			});
		}
		// Child lists are COPIED rather than rebuilt by sorted insertion: the
		// source lists are already in id order (addChild is the only thing that
		// ever extends one), so mapping them straight across reproduces the same
		// tree a decode of the same state would build, without a sort or a splice
		// per item.
		const relink = (src: TextNode, dst: TextNode): void => {
			dst.left = src.left.map((c) => copies.get(c)!);
			dst.right = src.right.map((c) => copies.get(c)!);
		};
		for (const [ key, on ] of other.byId) {
			const n = copies.get(on)!;
			n.parent = on.parent === other.root ? this.root : copies.get(on.parent!)!;
			relink(on, n);
			this.byId.set(key, n);
		}
		relink(other.root, this.root);
		for (const m of other.marks) this.marks.push({ ...m });
		for (const [ p, c ] of other.vv) this.vv.set(p, c);
		this.invalidateOrder();
	}

	/**
	 * Registers a node (codec/merge path); does not attach or invalidate.
	 * `key` lets a caller that already computed idKey(n.id) — the decoder, for
	 * its duplicate check — pass it in rather than build the string twice.
	 */
	addNode(n: TextNode, key = idKey(n.id)): void {
		this.byId.set(key, n);
		this.bumpVV(n.id.peer, n.id.counter);
	}

	bumpVV(peer: string, counter: number): void {
		const cur = this.vv.get(peer) ?? 0;
		if (counter > cur) this.vv.set(peer, counter);
	}

	attach(n: TextNode): void {
		addChild(n.parent!, n);
	}

	private nextCounter(): number {
		let max = 0;
		for (const c of this.vv.values()) {
			if (c > max) max = c;
		}
		return max + 1;
	}

	private visibleAt(index: number): TextNode | undefined {
		let i = 0;
		for (const it of this.order()) {
			if (it.deleted) continue;
			if (i === index) return it;
			i++;
		}
		return undefined;
	}

	/** The node immediately after a in the full order (undefined when last). */
	successor(a: TextNode): TextNode | undefined {
		if (a.right.length > 0) {
			let n = a.right[0]!;
			while (n.left.length > 0) n = n.left[0]!;
			return n;
		}
		const order = this.order();
		const i = order.indexOf(a);
		return i >= 0 && i + 1 < order.length ? order[i + 1] : undefined;
	}

	/** The node immediately before n in the full order (undefined when first). */
	predecessor(n: TextNode): TextNode | undefined {
		const order = this.order();
		const i = order.indexOf(n);
		return i > 0 ? order[i - 1] : undefined;
	}

	private requirePeer(): void {
		if (this.peer === '') {
			throw new Error('text: document has no peer; set one before editing');
		}
	}

	/**
	 * Inserts s at a visible code-point index (0..length). Returns the id
	 * range of the created items (counters are consecutive) so callers can
	 * record an exact op-level inverse ("delete these ids").
	 *
	 * s must contain no unpaired surrogate (see requireWellFormed).
	 */
	insert(index: number, s: string): InsertedRange {
		this.requirePeer();
		if (s === '') return { peer: this.peer, start: 0, count: 0 };
		requireWellFormed('insert', s);
		const len = this.length;
		if (index < 0 || index > len) {
			throw new RangeError(`text: insert index ${index} out of range [0..${len}]`);
		}

		// Fugue placement (see Go Insert): a = visible predecessor (root at
		// index 0); if a has no right children the first new item becomes a's
		// right child, otherwise the left child of a's full-order successor.
		let a: TextNode;
		if (index === 0) {
			a = this.root;
		} else {
			const at = this.visibleAt(index - 1);
			if (!at) throw new Error(`text: internal: no node at index ${index - 1}`);
			a = at;
		}

		let counter = this.nextCounter();
		const range: InsertedRange = { peer: this.peer, start: counter, count: 0 };
		let prev = a;
		let first = true;
		for (const ch of s) {
			const id = { peer: this.peer, counter };
			const n: TextNode = {
				id,
				key: idKey(id),
				content: ch,
				deleted: false,
				parent: null,
				side: SIDE_R,
				left: [],
				right: [],
			};
			if (first) {
				if (prev.right.length === 0) {
					n.parent = prev;
					n.side = SIDE_R;
				} else {
					n.parent = this.successor(prev)!;
					n.side = SIDE_L;
				}
				first = false;
			} else {
				n.parent = prev;
				n.side = SIDE_R;
			}
			addChild(n.parent, n);
			this.byId.set(n.key, n);
			this.bumpVV(this.peer, counter);
			counter++;
			range.count++;
			prev = n;
		}
		this.order_ = null;
		return range;
	}

	/**
	 * Tombstones length visible code points starting at index. Returns the
	 * deleted segments (content plus a surviving-neighbour anchor) so callers
	 * can record an op-level inverse ("re-insert this content here").
	 */
	delete(index: number, length: number): DeletedSegment[] {
		this.requirePeer();
		if (length === 0) return [];
		const len = this.length;
		if (index < 0 || length < 0 || index + length > len) {
			throw new RangeError(
				`text: delete range [${index},${index + length}) out of range [0..${len}]`,
			);
		}
		const targets: TextNode[] = [];
		let i = 0;
		for (const it of this.order()) {
			if (it.deleted) continue;
			if (i >= index) {
				targets.push(it);
				if (targets.length === length) break;
			}
			i++;
		}
		return this.deleteNodes(targets);
	}

	/**
	 * Tombstones the live items with the given consecutive-counter ids
	 * (missing or already-deleted ids are skipped) — the materialisation of
	 * an insert's inverse. Returns the deleted segments like delete().
	 */
	deleteItems(peer: string, start: number, count: number): DeletedSegment[] {
		this.requirePeer();
		const wanted = new Set<string>();
		for (let c = start; c < start + count; c++) {
			wanted.add(idKey({ peer, counter: c }));
		}
		const targets: TextNode[] = [];
		for (const it of this.order()) {
			if (!it.deleted && wanted.has(it.key)) targets.push(it);
		}
		return this.deleteNodes(targets);
	}

	/**
	 * Tombstones the given live nodes (already in visible order), grouping
	 * visibly-contiguous runs into segments. Each segment's anchor sits after
	 * the nearest surviving predecessor, so re-inserting at the anchor
	 * restores the content between the same neighbours even after remote
	 * edits move or delete them; each segment also carries the marks resolved
	 * over its content, so the re-insert can restore the formatting too.
	 */
	private deleteNodes(targets: TextNode[]): DeletedSegment[] {
		if (targets.length === 0) return [];
		const targetSet = new Set(targets);
		const segments: DeletedSegment[] = [];
		// Each segment's visible start index, for intersecting resolved mark
		// spans below. Kept beside the segments rather than on them: it is a
		// position in a document the caller is about to stop seeing.
		const starts: number[] = [];
		let current: DeletedSegment | undefined;
		let lastSurvivor: TextNode | undefined;
		let visible = 0;
		for (const it of this.order()) {
			if (it.deleted) continue;
			if (targetSet.has(it)) {
				if (!current) {
					current = {
						content: '',
						at: lastSurvivor
							? { kind: AnchorAfter, item: lastSurvivor.id }
							: { kind: AnchorDocStart },
						ids: [],
						marks: [],
					};
					segments.push(current);
					starts.push(visible);
				}
				current.content += it.content;
				current.ids.push(it.id);
			} else {
				lastSurvivor = it;
				current = undefined;
			}
			visible++;
		}
		// Resolve BEFORE tombstoning — the spans have to cover the content
		// while it is still visible.
		if (this.marks.length > 0) this.captureSegmentMarks(segments, starts);
		for (const it of targets) it.deleted = true;
		return segments;
	}

	/** Fills each segment's marks from the currently resolved spans, clipped
	 * to the segment and re-based onto its content. */
	private captureSegmentMarks(segments: DeletedSegment[], starts: number[]): void {
		const spans = this.resolveMarks();
		if (spans.length === 0) return;
		for (let i = 0; i < segments.length; i++) {
			const seg = segments[i]!;
			const lo = starts[i]!;
			const hi = lo + seg.ids.length;
			for (const span of spans) {
				if (span.start >= hi) break; // spans are sorted by start
				const s = Math.max(span.start, lo);
				const e = Math.min(span.end, hi);
				if (s >= e) continue;
				seg.marks.push({ start: s - lo, end: e - lo, key: span.key, value: span.value });
			}
		}
	}

	/**
	 * Applies key=value over the visible range [start, end). expand decides
	 * whether text later typed at a boundary joins the mark. Returns the
	 * operation id (higher ids win where same-key ranges overlap).
	 *
	 * start === end is a COLLAPSED mark: it covers no character and
	 * resolveMarks never reports it, but marksAt does, and with ExpandBoth the
	 * first character typed at the position falls inside it and it becomes an
	 * ordinary span. That is the pending-format state an editor needs for
	 * "press Cmd-B, then type", and with ExpandNone the zero-width annotation
	 * that attaches a note to a position rather than a span. See
	 * collapsedAnchors.
	 */
	mark(
		start: number,
		end: number,
		key: string,
		value: Exclude<MarkValue, null>,
		expand: Expand,
	): ItemID {
		if (value === null) throw new Error('text: null mark value is reserved for unmark');
		return this.markOpAt(start, end, key, value, expand);
	}

	/**
	 * Removes key over [start, end): records a null-valued mark op with a
	 * higher id than everything seen. Pass the expand policy the mark was
	 * created with so boundary inserts don't resurrect it.
	 *
	 * It is also how a collapsed mark is cancelled: an unmark at the same
	 * caret with the same expand policy resolves to the same anchors, so it
	 * outranks the mark both while they cover nothing and after text typed at
	 * the position makes them cover it. Nothing is physically removed —
	 * compaction reclaims the pair once neither can affect resolution again
	 * (see compact.ts).
	 */
	unmark(start: number, end: number, key: string, expand: Expand): ItemID {
		return this.markOpAt(start, end, key, null, expand);
	}

	private markOpAt(
		start: number,
		end: number,
		key: string,
		value: MarkValue,
		expand: Expand,
	): ItemID {
		this.requirePeer();
		if (key === '') throw new Error('text: empty mark key');
		const len = this.length;
		if (start < 0 || end > len || start > end) {
			throw new RangeError(`text: mark range [${start},${end}) invalid for length ${len}`);
		}
		// NaN has no useful equality (it would compare unequal to itself and
		// split resolveMarks into one span per character) and the infinities
		// have no counterpart in JSON. The decoders enforce the same rule, so
		// a document can never carry one.
		if (typeof value === 'number' && !Number.isFinite(value)) {
			throw new Error('text: non-finite mark value');
		}

		let startAnchor: Anchor;
		let endAnchor: Anchor;
		if (start === end) {
			[ startAnchor, endAnchor ] = this.collapsedAnchors(start, expand);
		} else {
			const first = this.visibleAt(start)!;
			const last = this.visibleAt(end - 1)!;
			// Anchor sides encode the expand policy: a non-expanding boundary
			// anchors to the covered item, an expanding one to the adjacent
			// item in the full order (an item inserted between them falls
			// inside).
			startAnchor = { kind: AnchorBefore, item: first.id };
			if (expand === ExpandBefore || expand === ExpandBoth) {
				const p = this.predecessor(first);
				startAnchor = p ? { kind: AnchorAfter, item: p.id } : { kind: AnchorDocStart };
			}
			endAnchor = { kind: AnchorAfter, item: last.id };
			if (expand === ExpandAfter || expand === ExpandBoth) {
				const s = this.successor(last);
				endAnchor = s ? { kind: AnchorBefore, item: s.id } : { kind: AnchorDocEnd };
			}
		}

		const id: ItemID = { peer: this.peer, counter: this.nextCounter() };
		this.marks.push({ id, key, value, start: startAnchor, end: endAnchor, expand });
		this.bumpVV(id.peer, id.counter);
		return id;
	}

	/**
	 * The two items an insert at `index` would land between: the one it
	 * attaches after (undefined at the start of the document) and the one
	 * immediately following that in the FULL, tombstone-inclusive order
	 * (undefined at the end).
	 *
	 * insert places a new item strictly between the two, which is what makes
	 * an anchor pair built from them straddle whatever is typed at the
	 * position. The full-order neighbour is the right one to use, not the next
	 * VISIBLE item: anchoring past a stretch of tombstones would give a
	 * "collapsed" mark that spans them, which resolves to the same empty span
	 * but is no longer a point, and compaction would keep every one of those
	 * tombstones alive to hold an anchor that does not need them.
	 */
	private caretNeighbours(index: number): [TextNode | undefined, TextNode | undefined] {
		const before = index > 0 ? this.visibleAt(index - 1) : undefined;
		if (!before) {
			const order = this.order();
			return [ undefined, order.length > 0 ? order[0] : undefined ];
		}
		return [ before, this.successor(before) ];
	}

	/**
	 * The anchor pair for a zero-width mark at caret position index. Only two
	 * policies mean anything without a covered character, and they are the two
	 * states an editor actually has:
	 *
	 *   - ExpandBoth straddles the caret — after the item before it, before
	 *     the item after it — so the first character typed there falls inside
	 *     and the mark becomes an ordinary span. This is a pending format.
	 *   - ExpandNone pins both boundaries to the same edge, so the mark stays
	 *     zero-width for the life of the document while drifting with the text
	 *     around it. This is a note attached to a position.
	 *
	 * ExpandBefore and ExpandAfter name one side of a range that has no sides,
	 * so they are refused rather than silently resolved to one of the two
	 * above.
	 */
	private collapsedAnchors(index: number, expand: Expand): [Anchor, Anchor] {
		if (expand !== ExpandNone && expand !== ExpandBoth) {
			throw new RangeError(
				`text: collapsed mark at ${index} needs ExpandNone or ExpandBoth, got expand ${expand}`,
			);
		}
		const [ before, after ] = this.caretNeighbours(index);
		const start: Anchor = before
			? { kind: AnchorAfter, item: before.id }
			: { kind: AnchorDocStart };
		if (expand === ExpandNone) return [ start, start ];
		const end: Anchor = after ? { kind: AnchorBefore, item: after.id } : { kind: AnchorDocEnd };
		return [ start, end ];
	}

	/**
	 * What text inserted at the visible index (0..length) would be marked
	 * with: the active formatting for a caret, and what the next character
	 * typed there comes out as.
	 *
	 * This is a different question from resolveMarks, which reports what is
	 * painted on characters that already exist, and neither answers the other.
	 * A collapsed mark covers nothing and never appears in resolveMarks, but
	 * reporting it is the whole point of this call. An ordinary mark appears in
	 * both exactly when its expand policy takes in text typed at the position —
	 * so the two agree about a caret in the middle of a bold run, and disagree
	 * at its edges, which is correct in both directions.
	 *
	 * Winners are decided the same way as in resolveMarks (highest id per key),
	 * and a null-valued winner reports no entry. Sorted by key.
	 */
	marksAt(index: number): PendingMark[] {
		const len = this.length;
		if (index < 0 || index > len) {
			throw new RangeError(`text: index ${index} out of range [0..${len}]`);
		}
		if (this.marks.length === 0) return [];
		const order = this.order();
		const fullIndex = new Map<string, number>();
		for (let i = 0; i < order.length; i++) fullIndex.set(order[i]!.key, i);

		// Where a new item would land in the full order, and what every anchor
		// resolves to once it has: everything at or past that position shifts
		// by one, so an op covers the new item exactly when its shifted range
		// contains it. Computing the shift instead of doing a trial insert is
		// what keeps this a read.
		const [ before ] = this.caretNeighbours(index);
		const at = before ? fullIndex.get(idKey(before.id))! + 1 : 0;
		const shifted = (a: Anchor): number => {
			switch (a.kind) {
				case AnchorDocStart:
					return 0;
				case AnchorDocEnd:
					return order.length + 1;
			}
			let i = fullIndex.get(idKey(a.item!));
			if (i === undefined) {
				throw new Error(`text: anchor references unknown item ${a.item!.peer}:${a.item!.counter}`);
			}
			if (i >= at) i++;
			if (a.kind === AnchorAfter) i++;
			return i;
		};

		const ops = [ ...this.marks ].sort((a, b) => compareIds(a.id, b.id));
		const winner = new Map<string, MarkValue>();
		for (const op of ops) {
			const s = shifted(op.start);
			const e = shifted(op.end);
			if (s <= at && at < e) winner.set(op.key, op.value);
		}
		const out: PendingMark[] = [];
		for (const [ key, value ] of winner) {
			// An unmark wins here: the key is off, not pending.
			if (value !== null) out.push({ key, value });
		}
		out.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
		return out;
	}

	/**
	 * The zero-width marks over the current text — the marks resolveMarks
	 * cannot report, because a span covering no characters is not a span.
	 *
	 * Both kinds of collapsed mark appear here (see collapsedAnchors): a note
	 * attached to a position, which stays zero-width for good, and a pending
	 * format that has not been typed into yet, which stops being a point the
	 * moment it covers a character. The document does not distinguish them
	 * beyond their anchors, so neither does this; callers that need to tell
	 * them apart use distinct keys, which is what the annotation pattern
	 * already does (one key per comment, see annotations.ts).
	 *
	 * Winners are decided per key AND position rather than per key alone: two
	 * notes on the same key at different places in the document are both real,
	 * whereas two ops on the same key at the same position are the same
	 * collision resolveMarks resolves — highest id wins, null-valued winner
	 * reports nothing, which is what lets an unmark at the caret cancel a
	 * point. Sorted by (index, key).
	 */
	points(): Point[] {
		if (this.marks.length === 0) return [];
		const order = this.order();
		const fullIndex = new Map<string, number>();
		for (let i = 0; i < order.length; i++) fullIndex.set(order[i]!.key, i);
		const anchorPos = (a: Anchor): number => {
			switch (a.kind) {
				case AnchorDocStart:
					return 0;
				case AnchorDocEnd:
					return order.length;
			}
			const i = fullIndex.get(idKey(a.item!));
			if (i === undefined) {
				throw new Error(`text: anchor references unknown item ${a.item!.peer}:${a.item!.counter}`);
			}
			return a.kind === AnchorBefore ? i : i + 1;
		};
		const visBefore = new Int32Array(order.length + 1);
		let visible = 0;
		for (let i = 0; i < order.length; i++) {
			visBefore[i] = visible;
			if (!order[i]!.deleted) visible++;
		}
		visBefore[order.length] = visible;

		const ops = [ ...this.marks ].sort((a, b) => compareIds(a.id, b.id));
		const winner = new Map<string, { at: number; key: string; value: MarkValue }>();
		for (const op of ops) {
			const s = anchorPos(op.start);
			// Covers something (or is inverted); resolveMarks' business.
			if (s !== anchorPos(op.end)) continue;
			winner.set(`${s}\u0000${op.key}`, { at: s, key: op.key, value: op.value });
		}
		const out: Point[] = [];
		for (const w of winner.values()) {
			if (w.value !== null) out.push({ index: visBefore[w.at]!, key: w.key, value: w.value });
		}
		out.sort((a, b) => a.index - b.index || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
		return out;
	}

	/**
	 * Resolves every mark key over the current text: per character and key
	 * the covering op with the highest id wins; null winners (removals)
	 * produce no span. Spans are maximal runs of equal value, sorted by
	 * (start, end, key).
	 *
	 * A mark whose anchor references an item this document does not hold
	 * throws rather than guessing: the two implementations would otherwise
	 * paint different ranges for the same input. Decoding validates anchors
	 * and mark() builds them from live items, so this only fires on a
	 * document assembled in memory from foreign parts — mergeInto with an
	 * unrelated replica, say.
	 *
	 * Cost is O(document + covered), not O(keys × document): the two
	 * full-order passes below are shared by every key, and each key then only
	 * touches the stretches its own ops cover. That distinction is what makes
	 * the annotation pattern (one mark key per comment — see annotations.ts)
	 * usable, since a document with C comments has C keys that each cover a
	 * few words. Bindings re-resolve on every text change, so this runs on the
	 * render path.
	 */
	resolveMarks(): Span[] {
		if (this.marks.length === 0) return [];
		const order = this.order();

		const fullIndex = new Map<string, number>();
		for (let i = 0; i < order.length; i++) {
			fullIndex.set(order[i]!.key, i);
		}
		const anchorPos = (a: Anchor): number => {
			switch (a.kind) {
				case AnchorDocStart:
					return 0;
				case AnchorDocEnd:
					return order.length;
			}
			const i = fullIndex.get(idKey(a.item!));
			if (i === undefined) {
				throw new Error(`text: anchor references unknown item ${a.item!.peer}:${a.item!.counter}`);
			}
			return a.kind === AnchorBefore ? i : i + 1;
		};

		// visBefore[f] is how many visible characters precede full-order
		// position f, so a full-order position converts to a visible index in
		// O(1) instead of by counting from the start of the document once per
		// key.
		const visBefore = new Int32Array(order.length + 1);
		let visible = 0;
		for (let i = 0; i < order.length; i++) {
			visBefore[i] = visible;
			if (!order[i]!.deleted) visible++;
		}
		visBefore[order.length] = visible;

		// Resolve every op's range up front, in ascending id order, so an
		// unresolvable anchor is reported for the same op it always was.
		const ops = [ ...this.marks ].sort((a, b) => compareIds(a.id, b.id));
		const ranges: OpRange[] = [];
		const byKey = new Map<string, number[]>(); // key -> indices into ranges, id order
		for (const op of ops) {
			const s = anchorPos(op.start);
			const e = Math.min(anchorPos(op.end), order.length);
			if (s >= e) continue; // covers nothing; painting it would be a no-op
			let idxs = byKey.get(op.key);
			if (!idxs) {
				idxs = [];
				byKey.set(op.key, idxs);
			}
			idxs.push(ranges.length);
			ranges.push({ value: op.value === null ? REMOVED : op.value, lo: s, hi: e });
		}

		const spans: Span[] = [];
		for (const [ key, idxs ] of byKey) {
			const segs = mergeSegments(ranges, idxs, visBefore);
			// Bucket each op into the one segment that contains it, keeping id
			// order within a bucket so the last painter still wins.
			const buckets: number[][] = segs.map(() => []);
			for (const ri of idxs) {
				buckets[segmentOf(segs, ranges[ri]!.lo)]!.push(ri);
			}

			for (let si = 0; si < segs.length; si++) {
				const seg = segs[si]!;
				const paint: (MarkValue | typeof REMOVED | undefined)[] = new Array(seg.hi - seg.lo)
					.fill(undefined);
				// Paint in DESCENDING id order, never overwriting. The highest-id
				// op covering a character wins, so filling from the top and
				// keeping the first value written gives the same result as
				// filling from the bottom and letting the last writer win —
				// except each character is written once instead of once per op
				// covering it, and a segment that fills up stops the walk early.
				//
				// That is what bounds a long formatting history: toggling one
				// mark over a whole document N times costs one pass, not N. The
				// bucket is in ascending id order, so this runs it backwards.
				const bucket = buckets[si]!;
				let unpainted = paint.length;
				for (let i = bucket.length - 1; i >= 0 && unpainted > 0; i--) {
					const r = ranges[bucket[i]!]!;
					for (let f = r.lo - seg.lo, end = r.hi - seg.lo; f < end; f++) {
						if (paint[f] === undefined) {
							paint[f] = r.value;
							unpainted--;
						}
					}
				}

				// Walk the segment's visible characters, emitting maximal runs
				// of equal value.
				let visIdx = visBefore[seg.lo]!;
				let runStart = -1;
				let runValue: MarkValue = null;
				const flush = () => {
					if (runStart >= 0 && runValue !== null) {
						spans.push({ start: runStart, end: visIdx, key, value: runValue });
					}
					runStart = -1;
					runValue = null;
				};
				for (let f = seg.lo; f < seg.hi; f++) {
					if (order[f]!.deleted) continue;
					let v = paint[f - seg.lo];
					if (v === REMOVED || v === undefined) v = null;
					if (runStart >= 0 && v !== runValue) flush();
					if (runStart < 0 && v !== null) {
						runStart = visIdx;
						runValue = v;
					}
					visIdx++;
				}
				flush();
			}
		}
		spans.sort((a, b) =>
			a.start - b.start
			|| a.end - b.end
			|| (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)
		);
		return spans;
	}

	/**
	 * The currently-resolved non-null values of one key inside the visible
	 * code-point range [start, end), as anchored sub-ranges. Recorded BEFORE
	 * a mark/unmark so an undo can restore exactly what the op overwrote:
	 * unmark the op's range, then re-apply these.
	 */
	priorSpans(start: number, end: number, key: string): PriorSpan[] {
		const out: PriorSpan[] = [];
		for (const span of this.resolveMarks()) {
			if (span.key !== key) continue;
			const s = Math.max(span.start, start);
			const e = Math.min(span.end, end);
			if (s >= e) continue;
			out.push({
				start: this.anchorAt(s, AnchorBefore),
				end: this.anchorAt(e, AnchorAfter),
				value: span.value,
			});
		}
		return out;
	}

	/**
	 * An anchor for caret position index (0..length). AnchorBefore attaches
	 * to the item at index, AnchorAfter to the item at index-1 (the anchor
	 * then follows text inserted at the position). Document edges fall back
	 * to AnchorDocStart/AnchorDocEnd.
	 */
	anchorAt(index: number, kind: typeof AnchorBefore | typeof AnchorAfter): Anchor {
		const len = this.length;
		if (index < 0 || index > len) {
			throw new RangeError(`text: anchor index ${index} out of range [0..${len}]`);
		}
		if (kind === AnchorBefore) {
			if (index === len) return { kind: AnchorDocEnd };
			return { kind: AnchorBefore, item: this.visibleAt(index)!.id };
		}
		if (index === 0) return { kind: AnchorDocStart };
		return { kind: AnchorAfter, item: this.visibleAt(index - 1)!.id };
	}

	/**
	 * Maps an anchor back to a visible index (0..length). A live item
	 * resolves exactly; a deleted one to the nearest live neighbour on the
	 * anchor's side; an unknown item throws (reachable only for an anchor held
	 * outside the document, whose item a compaction has since purged).
	 */
	resolveAnchor(a: Anchor): number {
		switch (a.kind) {
			case AnchorDocStart:
				return 0;
			case AnchorDocEnd:
				return this.length;
		}
		const n = this.byId.get(idKey(a.item!));
		if (!n) {
			throw new Error(`text: anchor references unknown item ${a.item!.peer}:${a.item!.counter}`);
		}
		const order = this.order();
		const full = order.indexOf(n);
		let idx = 0;
		const upto = a.kind === AnchorBefore ? full : full + 1;
		for (let i = 0; i < upto; i++) {
			if (!order[i]!.deleted) idx++;
		}
		return idx;
	}

	/**
	 * Rewrites the visible text to s. The longest common prefix and suffix are
	 * kept, and the middle is patched with a minimal multi-hunk diff (Myers,
	 * over code points), so items — and the marks and anchors attached to
	 * them — survive everywhere the text did not actually change. This is the
	 * write path for whole-value writers, so an external edit merges with
	 * concurrent edits instead of clobbering the document. Strings further
	 * apart than maxDiffCost fall back to a single replacement hunk.
	 *
	 * Returns what the edit did, in the same op-level shapes insert/delete
	 * return — one entry per hunk, in application order (right to left).
	 *
	 * s must contain no unpaired surrogate. Checking up front rather than
	 * leaving it to insert keeps the rejection atomic: hunks apply one at a
	 * time, so a throw from the middle of the loop would leave the document
	 * half-rewritten. (The Go mirror needs the check here for a second reason
	 * — converting to runes there substitutes U+FFFD before Insert sees the
	 * bytes at all.)
	 */
	setString(s: string): { deleted: DeletedSegment[]; inserted: InsertedRange[] } {
		requireWellFormed('setString', s);
		const old = [ ...this.plainText() ];
		const next = [ ...s ];
		let prefix = 0;
		while (prefix < old.length && prefix < next.length && old[prefix] === next[prefix]) {
			prefix++;
		}
		let suffix = 0;
		while (
			suffix < old.length - prefix && suffix < next.length - prefix
			&& old[old.length - 1 - suffix] === next[next.length - 1 - suffix]
		) {
			suffix++;
		}
		const oldMid = old.slice(prefix, old.length - suffix);
		const newMid = next.slice(prefix, next.length - suffix);
		if (oldMid.length === 0 && newMid.length === 0) {
			return { deleted: [], inserted: [] };
		}
		let hunks = myersHunks(oldMid, newMid);
		if (!hunks) {
			hunks = [ { pos: 0, del: oldMid.length, ins: newMid } ];
		}
		const deleted: DeletedSegment[] = [];
		const inserted: InsertedRange[] = [];
		// Right to left, so earlier hunks' indices stay valid as later ones edit.
		for (let i = hunks.length - 1; i >= 0; i--) {
			const h = hunks[i]!;
			if (h.del > 0) {
				deleted.push(...this.delete(prefix + h.pos, h.del));
			}
			if (h.ins.length > 0) {
				inserted.push(this.insert(prefix + h.pos, h.ins.join('')));
			}
		}
		return { deleted, inserted };
	}
}

/**
 * Rejects text the document cannot store faithfully. Items hold code points
 * and the encoder writes them as UTF-8, which an unpaired surrogate has no
 * representation in: TextEncoder substitutes U+FFFD for it, silently, and
 * every check further down the write path sees a well-formed document. The
 * writer would only find out by reading its own field back and finding text it
 * never wrote. This is the JS-side half of a contract the Go mirror states as
 * "valid UTF-8" (see requireWellFormed there); an unpaired surrogate is the
 * only way a JS string can fail it.
 *
 * op names the operation for the message; the index counts UTF-16 code units,
 * the unit of the string the caller passed.
 */
export function requireWellFormed(op: string, s: string): void {
	for (let i = 0; i < s.length; i++) {
		const c = s.charCodeAt(i);
		if (c < 0xd800 || c > 0xdfff) continue;
		// A high surrogate followed by a low one is a well-formed pair; skip
		// both. Anything else — a lone high, a bare low — cannot encode.
		if (c < 0xdc00 && i + 1 < s.length) {
			const next = s.charCodeAt(i + 1);
			if (next >= 0xdc00 && next <= 0xdfff) {
				i++;
				continue;
			}
		}
		throw new Error(`text: ${op}: unpaired surrogate at index ${i}`);
	}
}

/**
 * Bounds the Myers edit-distance search setString runs: when the old and new
 * strings are further apart than this, the multi-hunk diff falls back to one
 * replacement hunk. Part of the cross-language contract — Go uses the
 * identical constant and the shared vectors pin behavior on both sides of the
 * boundary — because the fallback changes which items survive and therefore
 * the canonical encoding.
 */
export const maxDiffCost = 1024;

/** One contiguous change: at code-point index pos into the old text, delete
 * del points and insert ins. */
interface EditHunk {
	pos: number;
	del: number;
	ins: string[];
}

/**
 * Computes a minimal edit script from old to next with the greedy O(ND) Myers
 * algorithm, coalesced into hunks. Returns undefined when the edit distance
 * exceeds maxDiffCost. Deterministic — including its down/right tie-break —
 * since both language implementations must emit identical scripts (identical
 * scripts mean identical item ids, which the vectors pin).
 */
function myersHunks(old: string[], next: string[]): EditHunk[] | undefined {
	const n = old.length;
	const m = next.length;
	const maxD = Math.min(n + m, maxDiffCost);
	const v = new Array<number>(2 * maxD + 1).fill(0);
	const offset = maxD;
	// trace[d] holds V for diagonals -d..d after round d (index k+d).
	const trace: number[][] = [];
	let found = -1;
	search:
	for (let d = 0; d <= maxD; d++) {
		for (let k = -d; k <= d; k += 2) {
			let x: number;
			if (k === -d || (k !== d && v[offset + k - 1]! < v[offset + k + 1]!)) {
				x = v[offset + k + 1]!;
			} else {
				x = v[offset + k - 1]! + 1;
			}
			let y = x - k;
			while (x < n && y < m && old[x] === next[y]) {
				x++;
				y++;
			}
			v[offset + k] = x;
			if (x >= n && y >= m) {
				trace.push(v.slice(offset - d, offset + d + 1));
				found = d;
				break search;
			}
		}
		trace.push(v.slice(offset - d, offset + d + 1));
	}
	if (found < 0) return undefined;

	// Backtrack from (n, m), collecting single-point ops in reverse order.
	interface EditOp {
		pos: number; // old-text index the op applies at
		del: boolean;
		insert: string;
	}
	const ops: EditOp[] = [];
	let x = n;
	let y = m;
	for (let d = found; d > 0; d--) {
		const vp = trace[d - 1]!;
		const at = (k: number): number => vp[d - 1 + k]!;
		const k = x - y;
		const down = k === -d || (k !== d && at(k - 1) < at(k + 1));
		const prevK = down ? k + 1 : k - 1;
		const prevX = at(prevK);
		const prevY = prevX - prevK;
		if (down) {
			ops.push({ pos: prevX, del: false, insert: next[prevY]! });
		} else {
			ops.push({ pos: prevX, del: true, insert: '' });
		}
		x = prevX;
		y = prevY;
	}

	// Reverse into forward order and coalesce adjacent ops into hunks: an op
	// extends the current hunk when it sits exactly past the points the hunk
	// already deletes (retained points in between close it).
	const hunks: EditHunk[] = [];
	for (let i = ops.length - 1; i >= 0; i--) {
		const op = ops[i]!;
		const h = hunks[hunks.length - 1];
		if (h && op.pos === h.pos + h.del) {
			if (op.del) h.del++;
			else h.ins.push(op.insert);
			continue;
		}
		hunks.push({
			pos: op.pos,
			del: op.del ? 1 : 0,
			ins: op.del ? [] : [ op.insert ],
		});
	}
	return hunks;
}
