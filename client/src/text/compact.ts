// Compaction — the TS mirror of Go engine/text/compact.go.
//
// Compaction itself runs on the server (engine/server/compact.go): it drops the
// tombstones a document can no longer need and records the document's version
// vector as the field's compaction floor, which is what stops a replica holding
// older state from merging them back in. The client never compacts a document
// of its own, and mirrors the rules for two reasons:
//
//   - `compactDoc` is pinned by testdata/text/vectors.json, so both languages
//     agree byte-for-byte on what a compacted document is.
//   - `purgedByRemote` is how a live handle notices that the state it just
//     received is compacted, which is the moment it has to rebuild instead of
//     merging (see handle.ts).

import { ByteReader, ByteWriter } from './codec.js';
import {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	AnchorDocEnd,
	AnchorDocStart,
	compareIds,
	comparePeers,
	idKey,
	type MarkOp,
	SIDE_L,
	type TextDoc,
	type TextNode,
} from './doc.js';

/** What one compaction reclaimed. */
export interface CompactResult {
	items: number;
	marks: number;
}

/**
 * The mark operations that can no longer affect resolution — in this document
 * or in any document reachable from it by further edits — and which compaction
 * may therefore drop. Nothing else may be dropped: a mark op is state, and
 * merging is a union, so an op removed while it still matters comes straight
 * back from the next replica that holds it.
 *
 * Two rules, both of which have to hold for EVERY FUTURE document and not just
 * this one. That distinction is the whole difficulty: "shadowed today" is not
 * sufficient, because ops with different expand policies diverge at their
 * boundaries. Op A covering [3,7) with ExpandBoth is entirely shadowed by a
 * higher-id B covering [3,7) with ExpandNone — until one character is typed at
 * edge 3, which A covers and B does not, and A is revealed.
 *
 *  1. A is dead when a higher-id op on the same key has IDENTICAL anchors.
 *     Identical anchors mean identical coverage under every future insert, and
 *     the higher id wins every character they both cover, so A can never be the
 *     winner anywhere. This is the toggle-the-same-range case — the bulk of
 *     what a long editing session accumulates, and exactly what a collapsed
 *     mark plus its cancelling unmark look like.
 *
 *  2. A group of surviving ops on one key that overlap or TOUCH each other, and
 *     nothing else on that key, is dead when every op in it is null-valued.
 *     Removal ops exist only to shadow marks beneath them; with none left they
 *     paint "removed" over a stretch where nothing paints anything, which is
 *     what the absence of the ops paints too. Touching counts as overlapping
 *     because an insert at the shared edge can fall inside both, so a group has
 *     to be separated from the rest of the key by a real gap to be judged
 *     alone.
 *
 * Ops whose anchors do not resolve are left alone, and so is every other op on
 * their key: compaction cannot fail, and a document assembled from foreign
 * parts is not one to start reclaiming state from.
 *
 * What this deliberately does NOT use is "covers nothing". A collapsed mark
 * covers nothing permanently and by design (doc.ts), so emptiness stops being a
 * proxy for deadness the moment they exist — a rule written that way would
 * reclaim every zero-width annotation in the document.
 */
function deadMarks(doc: TextDoc): Set<string> {
	const dead = new Set<string>();
	if (doc.marks.length === 0) return dead;
	const order = doc.order();
	const fullIndex = new Map<string, number>();
	for (let i = 0; i < order.length; i++) fullIndex.set(order[i]!.key, i);
	const anchorPos = (a: Anchor): number | undefined => {
		if (a.kind === AnchorDocStart) return 0;
		if (a.kind === AnchorDocEnd) return order.length;
		const i = fullIndex.get(idKey(a.item!));
		if (i === undefined) return undefined;
		return a.kind === AnchorBefore ? i : i + 1;
	};
	// (start anchor, end anchor, key) as one string, so "same shape" is a map
	// lookup. Anchors hold no other state, so this is exactly the structural
	// equality the Go mirror gets from a struct key. The mark key goes LAST
	// because it is the one part an application chooses freely: anything it
	// contains lands past every separator and cannot shift where the fields
	// split.
	const shapeOf = (m: MarkOp): string => {
		const side = (a: Anchor): string => (a.item ? `${a.kind}:${idKey(a.item)}` : `${a.kind}`);
		return `${side(m.start)}\u0000${side(m.end)}\u0000${m.key}`;
	};

	// Rule 1: per (key, start anchor, end anchor), everything but the highest
	// id goes.
	const top = new Map<string, MarkOp>();
	for (const m of doc.marks) {
		const s = shapeOf(m);
		const cur = top.get(s);
		if (!cur || compareIds(cur.id, m.id) < 0) top.set(s, m);
	}
	for (const m of doc.marks) {
		if (compareIds(top.get(shapeOf(m))!.id, m.id) !== 0) dead.add(idKey(m.id));
	}

	// Rule 2: group the survivors per key into runs of touching-or-overlapping
	// CLOSED edge intervals, and drop any run that is all removals.
	interface Interval {
		lo: number;
		hi: number;
		op: MarkOp;
	}
	const perKey = new Map<string, Interval[]>();
	const opaque = new Set<string>(); // keys holding an op whose anchors don't resolve
	for (const m of doc.marks) {
		if (dead.has(idKey(m.id))) continue;
		const lo = anchorPos(m.start);
		let hi = anchorPos(m.end);
		if (lo === undefined || hi === undefined) {
			opaque.add(m.key);
			continue;
		}
		// Inverted anchors cover nothing; judge it as the point it is.
		if (hi < lo) hi = lo;
		const ivs = perKey.get(m.key);
		if (ivs) ivs.push({ lo, hi, op: m });
		else perKey.set(m.key, [ { lo, hi, op: m } ]);
	}
	for (const [ key, ivs ] of perKey) {
		if (opaque.has(key)) continue;
		// Total order, so both languages sweep the same sequence.
		ivs.sort((a, b) => a.lo - b.lo || a.hi - b.hi || compareIds(a.op.id, b.op.id));
		for (let i = 0; i < ivs.length;) {
			let j = i;
			let reach = ivs[i]!.hi;
			let allNull = ivs[i]!.op.value === null;
			while (j + 1 < ivs.length && ivs[j + 1]!.lo <= reach) {
				j++;
				if (ivs[j]!.hi > reach) reach = ivs[j]!.hi;
				if (ivs[j]!.op.value !== null) allNull = false;
			}
			if (allNull) {
				for (let k = i; k <= j; k++) dead.add(idKey(ivs[k]!.op.id));
			}
			i = j + 1;
		}
	}
	return dead;
}

/**
 * Drops the mark operations that can no longer affect resolution and every
 * tombstone the document can then no longer need, returning how many of each
 * went. The version vector is left alone: it records what the document has
 * seen, and dropping entries would make purged ids look unseen again.
 *
 * Mark ops go FIRST, and the set of anchored items is computed after they do:
 * an op is the only thing keeping its boundary tombstones alive, so reclaiming
 * one is what makes them reclaimable in the same pass.
 *
 * An item is purgeable when it is deleted, has no children, and no surviving
 * mark operation anchors to it — children keep their parent because the tree is
 * what places them, and an anchored item keeps its mark resolvable. Purging
 * cascades, so a run that was typed and then deleted collapses entirely; the
 * result does not depend on visitation order, and the cost is linear in the
 * document however deep the chain runs.
 *
 * What it cannot reclaim: a tombstone with a live descendant. Deleting the
 * middle of a typed run leaves the characters between it and the run's
 * surviving tail, because those are the positions the tail is defined by.
 */
export function compactDoc(doc: TextDoc): CompactResult {
	let marks = 0;
	const dead = deadMarks(doc);
	if (dead.size > 0) {
		const kept = doc.marks.filter((m) => !dead.has(idKey(m.id)));
		marks = doc.marks.length - kept.length;
		doc.marks = kept;
	}

	const anchored = new Set<string>();
	for (const m of doc.marks) {
		for (const a of [ m.start, m.end ]) {
			if ((a.kind === AnchorBefore || a.kind === AnchorAfter) && a.item) {
				anchored.add(idKey(a.item));
			}
		}
	}
	const purgeable = (n: TextNode) =>
		n.deleted && !anchored.has(n.key) && n.left.length === 0 && n.right.length === 0;
	// A worklist seeded with the leaves, re-testing only the parent each removal
	// exposes. Re-testing the whole item table once per round instead costs a
	// round per level, and linear typing builds a chain of right children as deep
	// as the run is long — quadratic on exactly the shape compaction exists for.
	const queue: TextNode[] = [];
	for (const n of doc.byId.values()) {
		if (purgeable(n)) queue.push(n);
	}
	let items = 0;
	for (let n = queue.pop(); n !== undefined; n = queue.pop()) {
		const parent = n.parent!;
		const list = n.side === SIDE_L ? parent.left : parent.right;
		const i = list.indexOf(n);
		if (i >= 0) list.splice(i, 1);
		doc.byId.delete(n.key);
		items++;
		// The parent reaches the queue at most once: it qualifies only on the
		// removal that empties its last child list, and nothing puts children
		// back. The root sentinel is never deleted, so it never qualifies.
		if (purgeable(parent)) queue.push(parent);
	}
	if (items > 0) doc.invalidateOrder();
	return { items, marks };
}

/**
 * Whether `incoming` is state a live document must rebuild onto rather than
 * merge with: it lacks items OR MARK OPERATIONS `local` carries at ids its own
 * version vector covers. Merging is a union, so folding such a state in would
 * put back precisely what it no longer holds.
 *
 * Two things produce it. A server compaction is one: it purges the tombstones
 * and mark ops a document can no longer need. A dropped local mutation is the
 * other — a fragment carries its source document's FULL version vector, so a
 * state that has absorbed a later fragment covers the ids of an earlier one that
 * was rejected while carrying none of its items.
 *
 * Both halves matter. A compaction that reclaimed only mark ops leaves every
 * item in place, so an item-only check reads it as ordinary state and the
 * handle merges instead of rebuilding — which puts the reclaimed ops straight
 * back, locally and in every write that follows, while the server keeps
 * skipping them. The two sides then never agree on the document's marks.
 */
export function purgedByRemote(local: TextDoc, incoming: TextDoc): boolean {
	for (const n of local.byId.values()) {
		if (incoming.byId.has(n.key)) continue;
		if ((incoming.vv.get(n.id.peer) ?? 0) >= n.id.counter) return true;
	}
	if (local.marks.length === 0) return false;
	const theirs = new Set<string>();
	for (const m of incoming.marks) theirs.add(idKey(m.id));
	for (const m of local.marks) {
		if (theirs.has(idKey(m.id))) continue;
		if ((incoming.vv.get(m.id.peer) ?? 0) >= m.id.counter) return true;
	}
	return false;
}

/**
 * Re-applies onto `base` the edits `local` holds that `base` has not seen, and
 * reports whether there were any. `base` is the state to rebuild onto — one
 * {@link purgedByRemote} says cannot be merged with — and must carry a writer
 * peer; `local` is not modified. `seen` is the version vector of the state
 * `base` was built from, which differs from `base`'s own after
 * {@link TextDoc.replaceWith} keeps the writer's counter.
 *
 * Only the difference travels, so whatever `base` holds that `local` lacks —
 * text a peer typed, text a peer deleted — stays as `base` has it:
 *
 *  - A deleted item of `local` that `base` shows is deleted again.
 *  - A visible item of `local` that `base` neither holds nor covers with its
 *    version vector is inserted again, as fresh text right after the nearest
 *    item before it that `base` holds. An item `base` covers but does not hold
 *    was purged, which compaction does only to tombstones, so it stays gone.
 *
 * Re-inserted text gets new ids, and mark operations `base` has not seen are
 * not carried over.
 */
export function reapplyUnseen(
	base: TextDoc,
	local: TextDoc,
	seen: ReadonlyMap<string, number> = base.vv,
): boolean {
	let changed = false;
	for (const n of local.byId.values()) {
		if (!n.deleted) continue;
		const held = base.byId.get(n.key);
		if (held !== undefined && !held.deleted) {
			// The same union a merge applies: a tombstone carries no counter.
			held.deleted = true;
			changed = true;
		}
	}
	// Runs of unseen text, each with the last item before it that base holds
	// (undefined for the start of the document).
	const runs: { after: string | undefined; text: string }[] = [];
	let after: string | undefined;
	let text = '';
	for (const n of local.order()) {
		if (base.byId.has(n.key)) {
			if (text !== '') runs.push({ after, text });
			after = n.key;
			text = '';
			continue;
		}
		if (n.deleted || (seen.get(n.id.peer) ?? 0) >= n.id.counter) continue;
		text += n.content;
	}
	if (text !== '') runs.push({ after, text });
	// Left to right, each index taken after the runs before it went in.
	for (const run of runs) {
		let index = 0;
		if (run.after !== undefined) {
			for (const n of base.order()) {
				if (!n.deleted) index++;
				if (n.key === run.after) break;
			}
		}
		base.insert(index, run.text);
		changed = true;
	}
	return changed;
}

/**
 * Serializes a version vector for storage outside a document — the engine keeps
 * one per compacted field. Canonical: peers sorted by UTF-8 bytes.
 */
export function encodeVV(vv: Map<string, number>): Uint8Array {
	const peers = [ ...vv.keys() ].sort(comparePeers);
	const w = new ByteWriter();
	w.uvarint(peers.length);
	for (const p of peers) {
		w.str(p);
		w.uvarint(vv.get(p)!);
	}
	return w.finish();
}

/** Parses an encodeVV payload. Empty input is the empty vector — no floor. */
export function decodeVV(data: Uint8Array | undefined): Map<string, number> {
	const vv = new Map<string, number>();
	if (data === undefined || data.length === 0) return vv;
	const r = new ByteReader(data);
	// A peer costs at least two bytes: a zero-length name plus its counter.
	const n = r.count('peer count', 2);
	for (let i = 0; i < n; i++) {
		const p = r.str();
		if (vv.has(p)) throw new Error(`text: duplicate peer "${p}" in version vector`);
		vv.set(p, r.uvarint());
	}
	if (!r.done) throw new Error(`text: ${r.remaining} trailing bytes`);
	return vv;
}
