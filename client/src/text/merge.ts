// State merge and item-graph diff for TextDoc — mirrors Go engine/text
// merge.go and diff.go.

import { decodeTextDoc, encodeTextDoc, isPlainString } from './codec.js';
import {
	type Anchor,
	compareIds,
	idKey,
	type ItemID,
	type MarkOp,
	parentId,
	sameMarkOp,
	sameParent,
	type TextDoc,
	type TextNode,
} from './doc.js';
import { applyFragmentBytes, isFragment } from './fragment.js';

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a === b) return true;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/**
 * Merges two encoded document states: a pure union of items (content
 * immutable, tombstones OR-ed), mark operations and version vectors,
 * re-encoded canonically. Idempotent, commutative, associative — the client
 * mirror of the merger the server runs for text fields, so rebase reaches
 * the same bytes the server does.
 *
 * Ids are never reused, so a same-id item or mark op that disagrees on
 * anything is corrupt input (two writers shared a peer) and throws rather than
 * silently picking a side.
 *
 * The base is always a full document — stored rows never hold delta state. The
 * incoming side may instead be a delta-state fragment (fragment.ts), which
 * applies as the same union and yields byte-identical output whenever the base
 * covers the fragment's own base.
 *
 * Neither side may be a plain-string value (codec.ts): applying one means
 * minting the difference under a writer identity, which this function does not
 * have. Resolving happens before the write — see applyTextStringsToCrud.
 *
 * Folding two queued fragments together is a client-only operation on values
 * that have not been stored yet, and is not this function: see
 * optimistic/compact.ts.
 */
export function mergeText(base: Uint8Array, incoming: Uint8Array): Uint8Array {
	if (isFragment(base)) {
		throw new Error('text: merge base is a fragment; stored state is always a full document');
	}
	if (isPlainString(base)) {
		throw new Error(
			'text: merge base is a plain-string value; stored state is always a full document',
		);
	}
	if (isPlainString(incoming)) {
		throw new Error(
			'text: incoming plain-string value must be resolved under a writer identity before a write',
		);
	}
	if (isFragment(incoming)) return applyFragmentBytes(base, incoming);
	if (bytesEqual(base, incoming)) return base;
	const a = decodeTextDoc(base, '');
	const b = decodeTextDoc(incoming, '');
	mergeInto(a, b);
	return encodeTextDoc(a);
}

/** Merges other into doc in place (doc grows; other is not modified). */
export function mergeInto(doc: TextDoc, other: TextDoc): void {
	// Iterated as entries throughout: the map key is the item key, so every
	// lookup below reuses a string the map already holds instead of rebuilding
	// it. This runs over every item of the incoming state on every merge.
	const newNodes: [string, TextNode][] = [];
	for (const [ key, on ] of other.byId) {
		const mine = doc.byId.get(key);
		if (mine) {
			if (mine.content !== on.content) {
				throw new Error(
					`text: merge conflict: item ${on.id.peer}:${on.id.counter} has different content on each side`,
				);
			}
			// Placement is decided once, by the writer that minted the id, and
			// travels with the item — so a disagreement means two writers shared a
			// peer. Checking it is what makes "ids are never reused" an enforced
			// invariant rather than an assumed one: unchecked, this case merges
			// silently with each side keeping its own parent, which makes the merge
			// non-commutative (the same two states merged in opposite orders
			// produce different text).
			if (!sameParent(mine, on) || mine.side !== on.side) {
				throw new Error(
					`text: merge conflict: item ${on.id.peer}:${on.id.counter} has different placement on each side`,
				);
			}
			if (on.deleted) mine.deleted = true;
			continue;
		}
		const n: TextNode = {
			id: on.id,
			key,
			content: on.content,
			deleted: on.deleted,
			parent: null,
			side: on.side,
			left: [],
			right: [],
		};
		doc.addNode(n, key);
		newNodes.push([ key, n ]);
	}
	newNodes.sort((x, y) => compareIds(x[1].id, y[1].id));
	for (const [ key, n ] of newNodes) {
		const on = other.byId.get(key)!;
		if (on.parent === other.root) {
			n.parent = doc.root;
		} else {
			const p = doc.byId.get(on.parent!.key);
			if (!p) {
				throw new Error(
					`text: merge: item ${n.id.peer}:${n.id.counter} references unknown parent `
						+ `${on.parent!.id.peer}:${on.parent!.id.counter}`,
				);
			}
			n.parent = p;
		}
		doc.attach(n);
	}

	// Marks union by op id. A same-id op is the same op, so first-seen wins; any
	// disagreement is the same shared-peer corruption the item check catches,
	// and mark ops allocate out of the same counter space.
	const seen = new Map(doc.marks.map((m) => [ idKey(m.id), m ]));
	for (const m of other.marks) {
		const mine = seen.get(idKey(m.id));
		if (mine) {
			if (!sameMarkOp(mine, m)) {
				throw new Error(
					`text: merge conflict: mark op ${m.id.peer}:${m.id.counter} differs on each side`,
				);
			}
			continue;
		}
		const copy = { ...m };
		doc.marks.push(copy);
		seen.set(idKey(m.id), copy);
	}

	for (const [ p, c ] of other.vv) {
		doc.bumpVV(p, c);
	}
	if (newNodes.length > 0) doc.invalidateOrder();
}

/** One step of a text delta, in visible code-point coordinates. */
export type DeltaOp =
	| { retain: number }
	| { insert: string }
	| { delete: number };

/**
 * How to transform prev's visible text into next's, plus whether the mark
 * spans an editor resolved against prev may have moved.
 */
export interface TextDelta {
	ops: DeltaOp[];
	/**
	 * Whether a binding must re-read resolveMarks() — set exactly when the
	 * formatting it renders could be stale.
	 *
	 * Spans are resolved from anchors against the current item order, not
	 * stored, so they move whenever the text does: an insert inside a bold run
	 * extends it, a delete can empty it. So this is set when the mark-op sets
	 * differ OR when the delta changes text at all and the document carries
	 * marks. That over-reports (an edit far from every mark re-resolves
	 * needlessly) but is never wrong in the direction that matters, and a
	 * document with no marks — the common case — never pays.
	 */
	marksChanged: boolean;
}

/**
 * Computes the delta from prev to next by comparing item graphs, not
 * strings: an item visible in both is a retain even if equal text moved
 * around it, so an editor applying the delta in place keeps its selection.
 * prev must be a causal ancestor of next (always true for a document
 * evolving by local edits and merges).
 */
export function diffText(prev: TextDoc, next: TextDoc): TextDelta {
	const ops: DeltaOp[] = [];
	let prevSeen = 0;
	const emit = (op: DeltaOp) => {
		const last = ops[ops.length - 1];
		if (last) {
			if ('retain' in op && 'retain' in last) {
				last.retain += op.retain;
				return;
			}
			if ('insert' in op && 'insert' in last) {
				last.insert += op.insert;
				return;
			}
			if ('delete' in op && 'delete' in last) {
				last.delete += op.delete;
				return;
			}
		}
		ops.push(op);
	};
	for (const n of next.order()) {
		const p = prev.byId.get(n.key);
		const visPrev = p !== undefined && !p.deleted;
		const visNext = !n.deleted;
		if (visPrev) prevSeen++;
		if (visPrev && visNext) emit({ retain: 1 });
		else if (visPrev && !visNext) emit({ delete: 1 });
		else if (!visPrev && visNext) emit({ insert: n.content });
	}
	if (prevSeen !== prev.length) {
		throw new Error(
			`text: diff: prev is not an ancestor of next (${prevSeen} of ${prev.length} visible items found)`,
		);
	}
	// Canonical deltas drop a trailing retain (it carries no information).
	const last = ops[ops.length - 1];
	if (last && 'retain' in last) ops.pop();
	const marksChanged = markOpsDiffer(prev, next)
		|| (next.marks.length > 0 && ops.some((op) => !('retain' in op)));
	return { ops, marksChanged };
}

/**
 * Whether the two documents hold different sets of mark operations. Comparing
 * sets rather than counts matters because prev is only checked for item
 * ancestry: a caller diffing two states that swapped one mark op for another
 * would otherwise be told nothing changed.
 */
function markOpsDiffer(prev: TextDoc, next: TextDoc): boolean {
	if (prev.marks.length !== next.marks.length) return true;
	const prevIds = new Set(prev.marks.map((m) => idKey(m.id)));
	return next.marks.some((m) => !prevIds.has(idKey(m.id)));
}
