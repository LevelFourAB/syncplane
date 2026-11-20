// Op-level undo for text fields. A per-field-LWW inverse (restore the prior
// snapshot) is a silent no-op for a state CRDT — merging an old state into
// the current one is a union — so text edits record SYMBOLIC inverses at edit
// time and materialise them against the then-current document at undo time:
//
//   - inverse of an insert deletes exactly the inserted item ids (a no-op
//     for ids a peer already deleted);
//   - inverse of a delete re-inserts the content as NEW items anchored
//     between the surviving neighbours, and re-applies the marks that covered
//     it at delete time;
//   - inverse of a mark/unmark re-applies the previously-resolved values,
//     but only over the characters that still hold the value the op set. A
//     character a later op changed keeps that op's value, so a peer's
//     formatting on top of the local mark survives the undo.
//
// Because every recorded op names only items this client authored (or
// content this client removed), undo touches local edits alone and leaves
// concurrent peers' work intact.
//
// What a re-insert restores is the TEXT and its FORMATTING, never the item
// identities: tombstones are monotone, so there is no un-delete to apply and
// the content necessarily comes back under new ids. A reinsert therefore
// records the ids it restores (`ids`), and applyTextInverse reports each old
// id with the new item that now holds its content (RestoredIds). The replay
// then rewrites the other records of every live undo stack of the workspace
// with remapTextInverseOps, so a later step that names the old items (the
// inverse of the insert that created them, a mark range over them, a
// reinsert anchored after them) finds the restored text. Without this, undo
// after undo misses the text the first one put back: "Hi" + " abc", delete
// "c", undo, undo leaves "Hic".
//
// The rewrite reaches only stacks that are live in this client when the
// replay commits. A saved scope that is closed at that time, or a scope of
// another tab, keeps the old ids: its later step skips the restored text.
//
// Other consequences of the new ids worth knowing before building on this:
//
//   - Anchors held outside the document and outside the undo stacks (a saved
//     cursor, a presence position) that pointed into the deleted range stay
//     pointed at the collapsed position; they do not follow the restored
//     text.
//   - Restoring a mark mints a NEW mark op, so an unmark a peer issued
//     concurrently with the delete can be undone along with it.
//   - The original expand policy is not recorded; marks come back as
//     ExpandNone, so text later typed at a restored boundary does not join
//     the mark even if it would have before.
//
// A marks inverse matches what the op set by VALUE, not by op id. An undo or
// redo writes its values through new ops, so the op a later record names may
// no longer be the one that wins; the value it set is still there to compare.
// The cost: a later op that set the same value is reverted along with it.

import {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	ExpandNone,
	idKey,
	type ItemID,
	type MarkValue,
	type PriorSpan,
	type SegmentMark,
	type TextDoc,
} from './doc.js';

/** A run of items of one peer with consecutive counters. */
export interface ItemRun {
	peer: string;
	start: number;
	count: number;
}

/**
 * The items a replay restored, by the idKey of the deleted item whose content
 * each one holds now. Fill it with applyTextInverse and give it to
 * remapTextInverseOps. Keep one map for each text field: item ids are unique
 * only in one document.
 */
export type RestoredIds = Map<string, ItemID>;

/** A value one mark step wrote over an anchored range; null for a removal. */
export interface SetSpan {
	start: Anchor;
	end: Anchor;
	value: MarkValue;
}

/** One symbolic step of a text inverse (see the module comment). */
export type TextInverseOp =
	| { type: 'deleteItems'; peer: string; start: number; count: number }
	| {
		type: 'reinsert';
		content: string;
		at: Anchor;
		marks?: SegmentMark[];
		/** The deleted items, one for each code point of content, as runs.
		 * Absent in records made before ids were recorded: such a reinsert
		 * restores its text, but no other record follows it. */
		ids?: ItemRun[];
	}
	| {
		type: 'marks';
		key: string;
		start: Anchor;
		end: Anchor;
		/** The values to restore. */
		prior: PriorSpan[];
		/** What the step being inverted wrote. Only a character that still
		 * resolves to the value written there is restored. */
		set: SetSpan[];
	};

/** Builds the reinsert inverse of one deleted segment, carrying its marks
 * only when there are any (an unmarked document records no extra state), and
 * the ids of the deleted items (see RestoredIds). */
export function reinsertOp(seg: {
	content: string;
	at: Anchor;
	marks: SegmentMark[];
	ids?: ItemID[];
}): TextInverseOp {
	const op: TextInverseOp = { type: 'reinsert', content: seg.content, at: seg.at };
	if (seg.marks.length > 0) op.marks = seg.marks;
	if (seg.ids !== undefined && seg.ids.length > 0) op.ids = toRuns(seg.ids);
	return op;
}

/** Packs ids, in order, into runs of consecutive counters of one peer. */
function toRuns(ids: readonly ItemID[]): ItemRun[] {
	const runs: ItemRun[] = [];
	for (const id of ids) {
		const last = runs[runs.length - 1];
		if (last !== undefined && last.peer === id.peer && last.start + last.count === id.counter) {
			last.count++;
		} else {
			runs.push({ peer: id.peer, start: id.counter, count: 1 });
		}
	}
	return runs;
}

/** The ids of runs, in order. */
function fromRuns(runs: readonly ItemRun[]): ItemID[] {
	const ids: ItemID[] = [];
	for (const run of runs) {
		for (let c = run.start; c < run.start + run.count; c++) ids.push({ peer: run.peer, counter: c });
	}
	return ids;
}

/**
 * The item that holds the content of id now: id itself when no replay restored
 * it. A restored item can be deleted and restored again, so the lookup follows
 * the chain to its end.
 */
function currentId(id: ItemID, restored: RestoredIds): ItemID {
	let cur = id;
	// Each step goes to an item that a later insert made, so the chain cannot
	// loop; the bound only protects against a corrupt map.
	for (let i = 0; i <= restored.size; i++) {
		const next = restored.get(idKey(cur));
		if (next === undefined) return cur;
		cur = next;
	}
	return cur;
}

function remapAnchor(a: Anchor, restored: RestoredIds): Anchor {
	if (a.item === undefined) return a;
	const item = currentId(a.item, restored);
	return item === a.item ? a : { kind: a.kind, item };
}

function remapSpans<T extends { start: Anchor; end: Anchor }>(
	spans: T[],
	restored: RestoredIds,
): T[] {
	let changed = false;
	const out = spans.map((span) => {
		const start = remapAnchor(span.start, restored);
		const end = remapAnchor(span.end, restored);
		if (start === span.start && end === span.end) return span;
		changed = true;
		return { ...span, start, end };
	});
	return changed ? out : spans;
}

/**
 * Rewrites ops so that every item they name, by id or by anchor, is the item
 * that holds its content now (see RestoredIds). Returns ops itself when no op
 * names a restored item. The ids a reinsert records stay as they are: they name
 * the items that its own delete removed.
 */
export function remapTextInverseOps(ops: TextInverseOp[], restored: RestoredIds): TextInverseOp[] {
	if (restored.size === 0) return ops;
	let changed = false;
	const out: TextInverseOp[] = [];
	for (const op of ops) {
		switch (op.type) {
			case 'deleteItems': {
				let hit = false;
				for (let c = op.start; c < op.start + op.count && !hit; c++) {
					hit = restored.has(idKey({ peer: op.peer, counter: c }));
				}
				if (!hit) {
					out.push(op);
					break;
				}
				changed = true;
				const now = fromRuns([ op ]).map((id) => currentId(id, restored));
				for (const run of toRuns(now)) out.push({ type: 'deleteItems', ...run });
				break;
			}
			case 'reinsert': {
				const at = remapAnchor(op.at, restored);
				if (at === op.at) {
					out.push(op);
					break;
				}
				changed = true;
				out.push({ ...op, at });
				break;
			}
			case 'marks': {
				const start = remapAnchor(op.start, restored);
				const end = remapAnchor(op.end, restored);
				const prior = remapSpans(op.prior, restored);
				const set = remapSpans(op.set, restored);
				if (start === op.start && end === op.end && prior === op.prior && set === op.set) {
					out.push(op);
					break;
				}
				changed = true;
				out.push({ ...op, start, end, prior, set });
				break;
			}
		}
	}
	return changed ? out : ops;
}

/**
 * Applies a recorded inverse against the current document and returns ITS
 * inverse (what redo will apply), captured from the same document just
 * before each step mutated it.
 *
 * With `restored`, each reinsert that records ids adds the new item of each
 * deleted one to it, and each op is first rewritten with what the map holds,
 * so an op later in the same replay finds text an earlier op restored.
 */
export function applyTextInverse(
	doc: TextDoc,
	ops: TextInverseOp[],
	restored?: RestoredIds,
): TextInverseOp[] {
	const redo: TextInverseOp[] = [];
	const current = (op: TextInverseOp): TextInverseOp[] =>
		restored ? remapTextInverseOps([ op ], restored) : [ op ];
	let i = 0;
	while (i < ops.length) {
		if (ops[i]!.type !== 'deleteItems') {
			for (const op of current(ops[i]!)) {
				if (op.type !== 'deleteItems') applyOne(doc, op, redo, restored);
			}
			i++;
			continue;
		}
		// Consecutive deletes go in one pass (see TextDoc.deleteItemRuns). A
		// remapped delete can be several runs, and the restored text in it
		// must come back as one segment with the text around it.
		const runs: ItemRun[] = [];
		for (; i < ops.length && ops[i]!.type === 'deleteItems'; i++) {
			for (const op of current(ops[i]!)) {
				if (op.type === 'deleteItems') runs.push({ peer: op.peer, start: op.start, count: op.count });
			}
		}
		for (const seg of doc.deleteItemRuns(runs)) redo.push(reinsertOp(seg));
	}
	return redo;
}

/** Applies one reinsert or marks op of applyTextInverse and pushes its inverse to redo. */
function applyOne(
	doc: TextDoc,
	op: Exclude<TextInverseOp, { type: 'deleteItems' }>,
	redo: TextInverseOp[],
	restored: RestoredIds | undefined,
): void {
	switch (op.type) {
		case 'reinsert': {
			const index = doc.resolveAnchor(op.at);
			const range = doc.insert(index, op.content);
			if (range.count > 0) {
				redo.push({
					type: 'deleteItems',
					peer: range.peer,
					start: range.start,
					count: range.count,
				});
			}
			restoreMarks(doc, index, range.count, op.marks);
			if (restored !== undefined && op.ids !== undefined) {
				const old = fromRuns(op.ids);
				// Equal unless the record is corrupt: one id for each code point.
				const n = Math.min(old.length, range.count);
				for (let i = 0; i < n; i++) {
					restored.set(idKey(old[i]!), { peer: range.peer, counter: range.start + i });
				}
			}
			break;
		}
		case 'marks': {
			const start = doc.resolveAnchor(op.start);
			const end = doc.resolveAnchor(op.end);
			if (start >= end) break; // the whole range was deleted meanwhile
			const runs = stillSet(doc, start, end, op.key, op.set);
			if (runs.length === 0) break; // later ops wrote over all of it
			const prior = doc.priorSpans(start, end, op.key);
			// Clear each run, then restore what the original op overwrote
			// there. Outside the runs the later ops' values stay.
			for (const [ s, e ] of runs) {
				doc.unmark(s, e, op.key, ExpandNone);
				for (const span of op.prior) {
					const ps = Math.max(doc.resolveAnchor(span.start), s);
					const pe = Math.min(doc.resolveAnchor(span.end), e);
					if (ps < pe) doc.mark(ps, pe, op.key, span.value, ExpandNone);
				}
			}
			redo.push({
				type: 'marks',
				key: op.key,
				start: op.start,
				end: op.end,
				prior,
				set: written(doc, runs, op.key),
			});
			break;
		}
	}
}

/**
 * The resolved value of key at each visible code point in [start, end), null
 * where the key is absent.
 */
function valuesOf(doc: TextDoc, start: number, end: number, key: string): MarkValue[] {
	const out = new Array<MarkValue>(end - start).fill(null);
	for (const span of doc.resolveMarks()) {
		if (span.key !== key) continue;
		const s = Math.max(span.start, start);
		const e = Math.min(span.end, end);
		for (let i = s; i < e; i++) out[i - start] = span.value;
	}
	return out;
}

/** The maximal runs inside [start, end) whose resolved value of key is still
 * the value set wrote there. */
function stillSet(
	doc: TextDoc,
	start: number,
	end: number,
	key: string,
	set: SetSpan[],
): [number, number][] {
	const want = new Array<MarkValue | undefined>(end - start).fill(undefined);
	for (const span of set) {
		const s = Math.max(doc.resolveAnchor(span.start), start);
		const e = Math.min(doc.resolveAnchor(span.end), end);
		for (let i = s; i < e; i++) want[i - start] = span.value;
	}
	const have = valuesOf(doc, start, end, key);
	const runs: [number, number][] = [];
	for (let i = 0; i < want.length; i++) {
		if (want[i] === undefined || want[i] !== have[i]) continue;
		const last = runs[runs.length - 1];
		if (last !== undefined && last[1] === start + i) last[1]++;
		else runs.push([ start + i, start + i + 1 ]);
	}
	return runs;
}

/** What the runs resolve to now, as the set of the step that wrote them. */
function written(doc: TextDoc, runs: [number, number][], key: string): SetSpan[] {
	const out: SetSpan[] = [];
	for (const [ s, e ] of runs) {
		const values = valuesOf(doc, s, e, key);
		let runStart = s;
		for (let i = s + 1; i <= e; i++) {
			if (i < e && values[i - s] === values[runStart - s]) continue;
			out.push({
				start: doc.anchorAt(runStart, AnchorBefore),
				end: doc.anchorAt(i, AnchorAfter),
				value: values[runStart - s]!,
			});
			runStart = i;
		}
	}
	return out;
}

/**
 * Puts the formatting of a re-inserted segment back to what it was at delete
 * time: the content now occupies [index, index+count).
 *
 * This is a replace, not an addition, because the re-insert does not land on
 * neutral ground. A mark whose range STRADDLED the deleted text has both its
 * anchors alive, so the new items fall inside them and inherit it — a bold run
 * ending mid-deletion would come back covering the whole restored segment. So
 * every key resolving over the segment is cleared first and only the recorded
 * spans are re-applied; the restored text then carries exactly the marks it
 * carried when it was deleted, and text outside the segment is untouched.
 */
function restoreMarks(
	doc: TextDoc,
	index: number,
	count: number,
	marks: SegmentMark[] | undefined,
): void {
	if (count === 0) return;
	// Nothing to clear and nothing to restore: the common unmarked document
	// pays neither the resolve nor the ops.
	if (doc.marks.length === 0) return;
	const end = index + count;
	const keys = new Set<string>();
	for (const m of marks ?? []) keys.add(m.key);
	for (const span of doc.resolveMarks()) {
		if (span.start < end && span.end > index) keys.add(span.key);
	}
	for (const key of keys) doc.unmark(index, end, key, ExpandNone);
	// Clipped to the range that actually arrived: undo records outlive the
	// document they were taken from (they persist across reloads), so a span is
	// only trusted as far as the content it came back with.
	for (const m of marks ?? []) {
		const s = index + m.start;
		const e = Math.min(index + m.end, end);
		if (s < e) doc.mark(s, e, m.key, m.value, ExpandNone);
	}
}

/** Builds the marks-inverse record for a mark (or, with a null value, an
 * unmark) over [start, end) code points: capture the prior values, anchor the
 * range. Call BEFORE the op. */
export function recordMarkInverse(
	doc: TextDoc,
	start: number,
	end: number,
	key: string,
	value: MarkValue,
): TextInverseOp {
	const startAnchor = doc.anchorAt(start, AnchorBefore);
	const endAnchor = doc.anchorAt(end, AnchorAfter);
	return {
		type: 'marks',
		key,
		start: startAnchor,
		end: endAnchor,
		prior: doc.priorSpans(start, end, key),
		set: [ { start: startAnchor, end: endAnchor, value } ],
	};
}
