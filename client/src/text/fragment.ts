// Delta-state fragments: format-version 2 of the text encoding — the
// line-by-line TypeScript mirror of Go engine/text/fragment.go. See that file
// for the format definition and the apply≡merge contract; the shared vectors
// in testdata/text/vectors.json pin both implementations byte-for-byte.
//
// A fragment is a partial document — the items, tombstone flips and mark
// operations one state carries beyond some base state, plus the source
// document's FULL version vector. Applying a fragment to any receiver that
// already holds the base (or more) produces exactly the same bytes as merging
// the full source state. Rows and IndexedDB always hold full v1 documents;
// only replication payloads may carry fragments, and full state remains the
// universal fallback when a fragment cannot apply.

import {
	ByteReader,
	ByteWriter,
	checkRunCeiling,
	decodeTextDoc,
	decodeUtf8,
	encodeTextDoc,
	FRAGMENT_VERSION,
} from './codec.js';
import {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	AnchorDocEnd,
	AnchorDocStart,
	type AnchorKind,
	compareIds,
	comparePeers,
	type Expand,
	ExpandBoth,
	idKey,
	type ItemID,
	type MarkOp,
	type MarkValue,
	parentId,
	sameMarkOp,
	sameParent,
	SIDE_L,
	SIDE_R,
	TextDoc,
	type TextNode,
} from './doc.js';

const FORMAT_VERSION = 1;

const ROOT_ID: ItemID = { peer: '', counter: 0 };

/** One item carried by a fragment, detached from any tree: the parent is a
 * plain id (the zero id = child of the root sentinel), resolved against the
 * receiver at apply time. */
interface FragItem {
	id: ItemID;
	content: string;
	deleted: boolean;
	parent: ItemID;
	side: typeof SIDE_L | typeof SIDE_R;
}

/**
 * A decoded delta-state payload. Inert data: nothing can be read from or
 * edited in a fragment, it can only be applied to a full document
 * (applyFragment) or unioned with another fragment (mergeFragments).
 */
export class TextFragment {
	readonly vv = new Map<string, number>();
	items: FragItem[] = [];
	marks: MarkOp[] = [];

	versionVector(): Map<string, number> {
		return new Map(this.vv);
	}

	/** Serializes the fragment canonically (same sort rules as the doc codec). */
	encode(): Uint8Array {
		const peers = [ ...this.vv.keys() ].sort(comparePeers);
		const peerIdx = new Map<string, number>();
		peers.forEach((p, i) => peerIdx.set(p, i));

		const w = new ByteWriter();
		w.byte(FRAGMENT_VERSION);
		w.uvarint(peers.length);
		for (const p of peers) w.str(p);
		for (const p of peers) w.uvarint(this.vv.get(p)!);

		// Canonical maximal runs over the fragment's items: per peer in table
		// order, counters ascending, split where the run invariant breaks. The
		// invariant compares parent ids rather than node pointers — ids are
		// unique, so this is the same rule the doc encoder applies.
		const itemsByPeer = new Map<string, FragItem[]>();
		for (const it of this.items) {
			let list = itemsByPeer.get(it.id.peer);
			if (!list) {
				list = [];
				itemsByPeer.set(it.id.peer, list);
			}
			list.push(it);
		}
		interface Run {
			peerIdx: number;
			start: number;
			items: FragItem[];
		}
		const runs: Run[] = [];
		for (const p of peers) {
			const items = (itemsByPeer.get(p) ?? []).sort((a, b) => a.id.counter - b.id.counter);
			for (let i = 0; i < items.length; i++) {
				const it = items[i]!;
				if (i > 0) {
					const prev = items[i - 1]!;
					const last = runs[runs.length - 1]!;
					if (
						it.id.counter === prev.id.counter + 1
						&& idKey(it.parent) === idKey(prev.id) && it.side === SIDE_R
						&& it.deleted === prev.deleted
					) {
						last.items.push(it);
						continue;
					}
				}
				runs.push({ peerIdx: peerIdx.get(p)!, start: it.id.counter, items: [ it ] });
			}
		}

		const utf8 = new TextEncoder();
		w.uvarint(runs.length);
		for (const r of runs) {
			w.uvarint(r.peerIdx);
			w.uvarint(r.start);
			w.uvarint(r.items.length);
			const head = r.items[0]!;
			let flags = 0;
			if (head.deleted) flags |= 1;
			if (idKey(head.parent) === idKey(ROOT_ID)) flags |= 2;
			if (head.side === SIDE_R) flags |= 4;
			w.byte(flags);
			if ((flags & 2) === 0) {
				w.uvarint(peerIdx.get(head.parent.peer)!);
				w.uvarint(head.parent.counter);
			}
			let content = '';
			for (const it of r.items) content += it.content;
			w.bytes(utf8.encode(content));
		}

		const marks = [ ...this.marks ].sort((a, b) =>
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
}

/** Whether b is fragment-encoded (leading byte 0x02). Empty input is not a
 * fragment — it decodes as an empty full document. */
export function isFragment(b: Uint8Array | undefined): boolean {
	return b !== undefined && b.length > 0 && b[0] === FRAGMENT_VERSION;
}

/**
 * Encodes the state doc carries beyond base: items base lacks, tombstone
 * flips (live in base, deleted in doc), mark ops base lacks, and doc's full
 * version vector. A null base yields a self-contained fragment carrying the
 * whole document.
 *
 * base must be a causal ancestor of doc — every item, tombstone and mark op
 * in base present unchanged in doc. Anything else means the two states
 * diverged and no fragment of doc can stand in for a merge of it.
 */
export function encodeFragment(doc: TextDoc, base: TextDoc | null): Uint8Array {
	const f = new TextFragment();
	for (const [ p, c ] of doc.vv) f.vv.set(p, c);

	if (base) checkFragmentBase(doc, base);

	// Iterated as entries so the base lookup can reuse the key the map already
	// holds: rebuilding it per item made diffing a large document the most
	// expensive operation on the push path.
	const entries = [ ...doc.byId ].sort((a, b) => compareIds(a[1].id, b[1].id));
	for (const [ key, n ] of entries) {
		const bn = base?.byId.get(key);
		if (bn && (bn.deleted || !n.deleted)) {
			continue; // base already carries this item in this (or a later) state
		}
		f.items.push({
			id: n.id,
			content: n.content,
			deleted: n.deleted,
			parent: parentId(n),
			side: n.side,
		});
	}

	const baseMarks = new Set<string>();
	if (base) {
		for (const m of base.marks) baseMarks.add(idKey(m.id));
	}
	for (const m of doc.marks) {
		if (!baseMarks.has(idKey(m.id))) f.marks.push(m);
	}
	return f.encode();
}

/** Verifies base is a causal ancestor of doc: everything base holds, doc
 * holds identically (tombstones may only have advanced). */
function checkFragmentBase(doc: TextDoc, base: TextDoc): void {
	const fail = () => new Error('text: fragment: base is not an ancestor of doc');
	for (const [ p, c ] of base.vv) {
		if ((doc.vv.get(p) ?? 0) < c) throw fail();
	}
	// Entries, not values: the map key is the item key, so the lookup below
	// costs nothing to build. Parents compare field-wise for the same reason —
	// via idKey this ran four string allocations per item over the whole
	// document on every push. The root sentinel carries the zero id, so nodes
	// parented to it compare equal without a special case.
	for (const [ key, bn ] of base.byId) {
		const dn = doc.byId.get(key);
		if (!dn || dn.content !== bn.content || dn.side !== bn.side) throw fail();
		if (!sameParent(dn, bn)) throw fail();
		if (bn.deleted && !dn.deleted) throw fail();
	}
	const mine = new Map(doc.marks.map((m) => [ idKey(m.id), m ]));
	for (const bm of base.marks) {
		const dm = mine.get(idKey(bm.id));
		if (!dm || !sameMarkOp(dm, bm)) throw fail();
	}
}

/**
 * Encodes a fragment from a caller-recorded set of changes rather than by
 * diffing against a base: `itemIds` are the items an edit minted or
 * tombstoned, `markIds` the mark ops it added. Everything else about the
 * result is identical to `encodeFragment` — same items, same full version
 * vector, same canonical encoding — so the two are interchangeable on the
 * wire and in storage.
 *
 * It exists because `encodeFragment` walks the whole document (it has to: only
 * a comparison against the base can tell what is new), which makes encoding a
 * one-character edit O(document). A writer that already knows what it changed
 * pays O(edit) here instead, which is what lets a keystroke's pending-queue
 * entry be the size of the keystroke.
 *
 * The caller owns the correctness condition `encodeFragment` checks for it:
 * every recorded id must still be in `doc`, and the receiver must hold a state
 * the recording started from. Ids absent from `doc` are skipped — they cannot
 * be described — rather than silently producing a fragment that claims less
 * than it should; a caller that has lost track of an id must fall back to full
 * state. Marks anchored to items the receiver lacks are the caller's
 * responsibility in the same way: record the items too, or send full state.
 */
export function encodeFragmentFrom(
	doc: TextDoc,
	itemIds: Iterable<ItemID>,
	markIds: Iterable<ItemID>,
): Uint8Array {
	const f = new TextFragment();
	for (const [ p, c ] of doc.vv) f.vv.set(p, c);

	// Deduplicated and sorted like encodeFragment's output: an edit can touch
	// the same item twice (insert then delete inside one burst), and the
	// encoder's run detection assumes ascending counters per peer.
	const seen = new Set<string>();
	for (const id of itemIds) {
		const key = idKey(id);
		if (seen.has(key)) continue;
		seen.add(key);
		const n = doc.byId.get(key);
		if (!n) continue;
		f.items.push({
			id: n.id,
			content: n.content,
			deleted: n.deleted,
			parent: parentId(n),
			side: n.side,
		});
	}
	f.items.sort((a, b) => compareIds(a.id, b.id));

	const wantedMarks = new Set<string>();
	for (const id of markIds) wantedMarks.add(idKey(id));
	if (wantedMarks.size > 0) {
		for (const m of doc.marks) {
			if (wantedMarks.has(idKey(m.id))) f.marks.push(m);
		}
	}
	return f.encode();
}

/**
 * Parses a fragment payload. Structural validity is enforced exactly as for
 * full documents; only reference resolution (parents, anchors) is deferred to
 * apply time. The version vector must cover every item and mark op the
 * fragment carries — it claims to be the writer's full knowledge.
 */
export function decodeFragment(data: Uint8Array): TextFragment {
	const r = new ByteReader(data);
	const version = r.byte();
	if (version === FORMAT_VERSION) {
		throw new Error('text: full document where fragment expected');
	}
	if (version !== FRAGMENT_VERSION) {
		throw new Error(`text: unsupported format version ${version}`);
	}

	const f = new TextFragment();

	// A peer costs at least two bytes: a zero-length name plus its version
	// vector counter.
	const peerCount = r.count('peer count', 2);
	const peers: string[] = [];
	for (let i = 0; i < peerCount; i++) peers.push(r.str());
	for (let i = 0; i < peerCount; i++) {
		// Dropped for the reason decodeTextDoc drops it.
		const c = r.uvarint();
		if (c > 0) f.vv.set(peers[i]!, c);
	}
	const peerAt = (idx: number): string => {
		const p = peers[idx];
		if (p === undefined) throw new Error(`text: peer index ${idx} out of range`);
		return p;
	};

	// A run costs at least six bytes (see the doc decoder).
	const runCount = r.count('run count', 6);
	const seenItems = new Set<string>();
	for (let i = 0; i < runCount; i++) {
		const p = peerAt(r.uvarint());
		const start = r.uvarint();
		// Reserved for the root sentinel (see decodeTextDoc). Under peer '' it
		// is also ROOT_ID, which here means "child of the root".
		if (start === 0) throw new Error(`text: item ${p}:0 uses reserved counter 0`);
		const itemCount = r.uvarint();
		if (itemCount === 0) throw new Error('text: empty run');
		checkRunCeiling(p, start, itemCount);
		const flags = r.byte();
		const deleted = (flags & 1) !== 0;
		const rootParent = (flags & 2) !== 0;
		const side = (flags & 4) !== 0 ? SIDE_R : SIDE_L;
		let parent = ROOT_ID;
		if (!rootParent) {
			const pp = peerAt(r.uvarint());
			parent = { peer: pp, counter: r.uvarint() };
		}
		const content = decodeUtf8(r.bytes(), 'in run content');
		let seen = 0;
		let prevParent = parent;
		let prevSide: typeof SIDE_L | typeof SIDE_R = side;
		for (const ch of content) {
			if (seen >= itemCount) throw new Error('text: run content longer than item count');
			const id: ItemID = { peer: p, counter: start + seen };
			if (seenItems.has(idKey(id))) {
				throw new Error(`text: duplicate item ${id.peer}:${id.counter}`);
			}
			seenItems.add(idKey(id));
			if ((f.vv.get(p) ?? 0) < id.counter) {
				throw new Error(`text: fragment vv does not cover item ${id.peer}:${id.counter}`);
			}
			f.items.push({ id, content: ch, deleted, parent: prevParent, side: prevSide });
			prevParent = id;
			prevSide = SIDE_R;
			seen++;
		}
		if (seen !== itemCount) throw new Error('text: run content shorter than item count');
	}

	// A mark costs at least seven bytes (see the doc decoder).
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
				// The item may be absent from the fragment; resolved at apply.
				return { kind, item: { peer: p, counter: r.uvarint() } };
			}
			default:
				throw new Error(`text: invalid anchor kind ${kind}`);
		}
	};
	const seenMarks = new Set<string>();
	for (let i = 0; i < markCount; i++) {
		const p = peerAt(r.uvarint());
		const id: ItemID = { peer: p, counter: r.uvarint() };
		if (id.counter === 0) throw new Error(`text: mark op ${p}:0 uses reserved counter 0`);
		if (seenMarks.has(idKey(id))) {
			throw new Error(`text: duplicate mark op ${p}:${id.counter}`);
		}
		seenMarks.add(idKey(id));
		if ((f.vv.get(p) ?? 0) < id.counter) {
			throw new Error(`text: fragment vv does not cover mark op ${p}:${id.counter}`);
		}
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
				if (!Number.isFinite(value)) throw new Error('text: non-finite mark value');
				break;
			default:
				throw new Error(`text: invalid mark value tag ${tag}`);
		}
		const start = decodeAnchor();
		const end = decodeAnchor();
		const expand = r.byte();
		if (expand > ExpandBoth) throw new Error(`text: invalid expand ${expand}`);
		f.marks.push({ id, key, value, start, end, expand: expand as Expand });
	}
	if (!r.done) throw new Error(`text: ${r.remaining} trailing bytes`);
	return f;
}

/**
 * Merges frag into doc: the same pure union mergeInto performs, with the
 * fragment's dangling references resolved against doc. Validation is complete
 * before the first mutation — a rejected fragment leaves doc untouched.
 */
export function applyFragment(doc: TextDoc, frag: TextFragment): void {
	// Classify items and run the same conflict checks as a full merge.
	const newByID = new Map<string, FragItem>();
	for (const fi of frag.items) {
		const mine = doc.byId.get(idKey(fi.id));
		if (mine) {
			if (mine.content !== fi.content) {
				throw new Error(
					`text: merge conflict: item ${fi.id.peer}:${fi.id.counter} has different content on each side`,
				);
			}
			if (idKey(parentId(mine)) !== idKey(fi.parent) || mine.side !== fi.side) {
				throw new Error(
					`text: merge conflict: item ${fi.id.peer}:${fi.id.counter} has different placement on each side`,
				);
			}
			continue;
		}
		newByID.set(idKey(fi.id), fi);
	}

	// Every new item must reach the root through parents that exist — in the
	// receiver or in the fragment itself. This is decode's reachability check
	// relocated to apply time.
	const UNRESOLVED = 0;
	const RESOLVING = 1;
	const REACHABLE = 2;
	const state = new Map<string, number>();
	let unreachableCount = 0;
	for (const fi of frag.items) {
		if (!newByID.has(idKey(fi.id)) || (state.get(idKey(fi.id)) ?? UNRESOLVED) !== UNRESOLVED) {
			continue;
		}
		const path: string[] = [];
		let cur = fi;
		let reachable = true;
		for (;;) {
			const s = state.get(idKey(cur.id)) ?? UNRESOLVED;
			if (s === REACHABLE) break;
			if (s === RESOLVING) {
				reachable = false; // cycle: everything on the path is unreachable
				break;
			}
			state.set(idKey(cur.id), RESOLVING);
			path.push(idKey(cur.id));
			const p = cur.parent;
			if (idKey(p) === idKey(ROOT_ID)) break;
			if (doc.byId.has(idKey(p))) break;
			const np = newByID.get(idKey(p));
			if (!np) {
				throw new Error(
					`text: item ${cur.id.peer}:${cur.id.counter} references unknown parent ${p.peer}:${p.counter}`,
				);
			}
			cur = np;
		}
		for (const k of path) {
			if (reachable) state.set(k, REACHABLE);
			else unreachableCount++;
		}
	}
	if (unreachableCount > 0) {
		throw new Error(
			`text: ${unreachableCount} of ${frag.items.length} items unreachable from the root (parent cycle)`,
		);
	}

	// Marks: same-id ops must agree (a full merge's check); new ops' anchors
	// must resolve against receiver ∪ fragment.
	const mine = new Map(doc.marks.map((m) => [ idKey(m.id), m ]));
	const newMarks: MarkOp[] = [];
	for (const m of frag.marks) {
		const dm = mine.get(idKey(m.id));
		if (dm) {
			if (!sameMarkOp(dm, m)) {
				throw new Error(
					`text: merge conflict: mark op ${m.id.peer}:${m.id.counter} differs on each side`,
				);
			}
			continue;
		}
		for (const a of [ m.start, m.end ]) {
			if (a.kind !== AnchorBefore && a.kind !== AnchorAfter) continue;
			if (doc.byId.has(idKey(a.item!)) || newByID.has(idKey(a.item!))) continue;
			throw new Error(
				`text: anchor references unknown item ${a.item!.peer}:${a.item!.counter}`,
			);
		}
		newMarks.push(m);
	}

	// Commit — everything below is infallible.
	const created: TextNode[] = [];
	for (const [ key, fi ] of newByID) {
		const n: TextNode = {
			id: fi.id,
			key,
			content: fi.content,
			deleted: fi.deleted,
			parent: null,
			side: fi.side,
			left: [],
			right: [],
		};
		doc.byId.set(key, n);
		created.push(n);
	}
	for (const fi of frag.items) {
		if (newByID.has(idKey(fi.id))) continue;
		if (fi.deleted) doc.byId.get(idKey(fi.id))!.deleted = true;
	}
	created.sort((a, b) => compareIds(a.id, b.id));
	for (const n of created) {
		const p = newByID.get(n.key)!.parent;
		n.parent = p.counter === 0 && p.peer === '' ? doc.root : doc.byId.get(idKey(p))!;
		doc.attach(n);
	}
	for (const m of newMarks) {
		doc.marks.push({ ...m });
	}
	for (const [ p, c ] of frag.vv) {
		doc.bumpVV(p, c);
	}
	if (created.length > 0) doc.invalidateOrder();
}

/**
 * Applies an encoded fragment to an encoded full document and re-encodes
 * canonically. When the fragment was cut against a base the receiver already
 * covers, the result is byte-identical to a full-state merge with the
 * fragment's source document.
 */
export function applyFragmentBytes(full: Uint8Array | undefined, frag: Uint8Array): Uint8Array {
	const doc = decodeTextDoc(full, '');
	const f = decodeFragment(frag);
	applyFragment(doc, f);
	return encodeTextDoc(doc);
}

/**
 * Unions two encoded fragments into one: union of items with tombstones
 * OR-ed, union of mark ops, pointwise-max version vectors. Associative and
 * commutative like the full-state merge, so a span of per-write fragments can
 * be folded into a single one in any order. Same-id disagreements are the
 * same shared-peer corruption the merge rejects.
 */
export function mergeFragments(a: Uint8Array, b: Uint8Array): Uint8Array {
	const fa = decodeFragment(a);
	const fb = decodeFragment(b);
	const byId = new Map<string, number>();
	fa.items.forEach((it, i) => byId.set(idKey(it.id), i));
	const extraItems: FragItem[] = [];
	for (const it of fb.items) {
		const i = byId.get(idKey(it.id));
		if (i === undefined) {
			extraItems.push(it);
			continue;
		}
		const mine = fa.items[i]!;
		if (mine.content !== it.content) {
			throw new Error(
				`text: merge conflict: item ${it.id.peer}:${it.id.counter} has different content on each side`,
			);
		}
		if (idKey(mine.parent) !== idKey(it.parent) || mine.side !== it.side) {
			throw new Error(
				`text: merge conflict: item ${it.id.peer}:${it.id.counter} has different placement on each side`,
			);
		}
		if (it.deleted) mine.deleted = true;
	}
	fa.items.push(...extraItems);
	const seenMarks = new Map<string, number>();
	fa.marks.forEach((m, i) => seenMarks.set(idKey(m.id), i));
	const extraMarks: MarkOp[] = [];
	for (const m of fb.marks) {
		const i = seenMarks.get(idKey(m.id));
		if (i === undefined) {
			extraMarks.push(m);
			continue;
		}
		if (!sameMarkOp(fa.marks[i]!, m)) {
			throw new Error(
				`text: merge conflict: mark op ${m.id.peer}:${m.id.counter} differs on each side`,
			);
		}
	}
	fa.marks.push(...extraMarks);
	for (const [ p, c ] of fb.vv) {
		if ((fa.vv.get(p) ?? 0) < c) fa.vv.set(p, c);
	}
	return fa.encode();
}
