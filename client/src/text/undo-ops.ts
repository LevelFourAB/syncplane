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
//   - inverse of a mark/unmark removes the key over the op's range and
//     re-applies the previously-resolved values.
//
// Because every recorded op names only items this client authored (or
// content this client removed), undo touches local edits alone and leaves
// concurrent peers' work intact.
//
// What a re-insert restores is the TEXT and its FORMATTING, never the item
// identities: tombstones are monotone, so there is no un-delete to apply and
// the content necessarily comes back under new ids. Three consequences worth
// knowing before building on this:
//
//   - Anchors held outside the document (a saved cursor, a presence position)
//     that pointed into the deleted range stay pointed at the collapsed
//     position; they do not follow the restored text.
//   - Restoring a mark mints a NEW mark op, so an unmark a peer issued
//     concurrently with the delete can be undone along with it.
//   - The original expand policy is not recorded; marks come back as
//     ExpandNone, so text later typed at a restored boundary does not join
//     the mark even if it would have before.

import {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	ExpandNone,
	type PriorSpan,
	type SegmentMark,
	type TextDoc,
} from './doc.js';

/** One symbolic step of a text inverse (see the module comment). */
export type TextInverseOp =
	| { type: 'deleteItems'; peer: string; start: number; count: number }
	| { type: 'reinsert'; content: string; at: Anchor; marks?: SegmentMark[] }
	| { type: 'marks'; key: string; start: Anchor; end: Anchor; prior: PriorSpan[] };

/** Builds the reinsert inverse of one deleted segment, carrying its marks
 * only when there are any (an unmarked document records no extra state). */
export function reinsertOp(seg: {
	content: string;
	at: Anchor;
	marks: SegmentMark[];
}): TextInverseOp {
	return seg.marks.length > 0
		? { type: 'reinsert', content: seg.content, at: seg.at, marks: seg.marks }
		: { type: 'reinsert', content: seg.content, at: seg.at };
}

/**
 * Applies a recorded inverse against the current document and returns ITS
 * inverse (what redo will apply), captured from the same document just
 * before each step mutated it.
 */
export function applyTextInverse(doc: TextDoc, ops: TextInverseOp[]): TextInverseOp[] {
	const redo: TextInverseOp[] = [];
	for (const op of ops) {
		switch (op.type) {
			case 'deleteItems': {
				const segments = doc.deleteItems(op.peer, op.start, op.count);
				for (const seg of segments) redo.push(reinsertOp(seg));
				break;
			}
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
				break;
			}
			case 'marks': {
				const start = doc.resolveAnchor(op.start);
				const end = doc.resolveAnchor(op.end);
				if (start >= end) break; // the whole range was deleted meanwhile
				redo.push({
					type: 'marks',
					key: op.key,
					start: op.start,
					end: op.end,
					prior: doc.priorSpans(start, end, op.key),
				});
				// Clear the range, then restore what the original op overwrote.
				doc.unmark(start, end, op.key, ExpandNone);
				for (const span of op.prior) {
					const s = doc.resolveAnchor(span.start);
					const e = doc.resolveAnchor(span.end);
					if (s < e) doc.mark(s, e, op.key, span.value, ExpandNone);
				}
				break;
			}
		}
	}
	return redo;
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

/** Builds the marks-inverse record for a mark/unmark over [start, end) code
 * points: capture the prior values, anchor the range. Call BEFORE the op. */
export function recordMarkInverse(
	doc: TextDoc,
	start: number,
	end: number,
	key: string,
): TextInverseOp {
	return {
		type: 'marks',
		key,
		start: doc.anchorAt(start, AnchorBefore),
		end: doc.anchorAt(end, AnchorAfter),
		prior: doc.priorSpans(start, end, key),
	};
}
