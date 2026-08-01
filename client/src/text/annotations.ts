// The annotation pattern: a comment (or any range-scoped metadata) is split
// between two homes so each part gets the right merge semantics.
//
//   - The RANGE lives inside the text document as a mark keyed
//     "comment:<annotationId>" with value `true` and expand NONE. It moves
//     with the text automatically, survives concurrent edits, and — because
//     resolution is per-key-per-character — overlapping comments coexist
//     instead of clobbering each other.
//   - The CONTENT (body, author, resolved state, …) lives in an ordinary
//     entity row keyed by that same id, inheriting per-field LWW merge,
//     permissions, queries, subscriptions and CRUD undo.
//
// Apply the text PATCH and the annotation row write in one transact() batch
// so they land atomically and undo as one group.
//
// Deleting commented text orphans the annotation — its mark covers nothing, so
// joinAnnotations lists it with no ranges. Undoing that delete brings the range
// back: the re-inserted content is new items (tombstones are monotone), but the
// undo re-applies the marks that covered the deleted text, comment keys
// included. What does NOT come back is any anchor stored outside the document
// pointing into the deleted range — a UI that caches a comment's position must
// re-read it from the spans rather than keep its own anchors.
//
// Nothing here is engine machinery — just the naming convention and the join
// between resolved mark spans and annotation rows.

import type { TextSpan } from './handle.js';

/** Mark-key prefix for annotation ranges. */
export const COMMENT_PREFIX = 'comment:';

/** The mark key carrying an annotation's range. */
export function commentKey(annotationId: string): string {
	return COMMENT_PREFIX + annotationId;
}

/** Extracts the annotation id from a comment mark key, or undefined. */
export function commentIdOf(markKey: string): string | undefined {
	return markKey.startsWith(COMMENT_PREFIX) ? markKey.slice(COMMENT_PREFIX.length) : undefined;
}

/** One annotation's resolved position in the current text. */
export interface CommentRange {
	id: string;
	/** [start, end) in the same units as the spans passed in. */
	start: number;
	end: number;
}

/**
 * Extracts every annotation range from resolved mark spans, ordered by
 * (start, end, id). An annotation whose mark was unmarked (or whose covered
 * text was fully deleted) yields no range. A mark split by an unmark in its
 * middle yields one range per surviving piece.
 */
export function commentRanges(spans: readonly TextSpan[]): CommentRange[] {
	const out: CommentRange[] = [];
	for (const span of spans) {
		const id = commentIdOf(span.key);
		if (id !== undefined && span.value === true) {
			out.push({ id, start: span.start, end: span.end });
		}
	}
	return out;
}

/** An annotation row joined with its resolved ranges. */
export interface ResolvedAnnotation<T> {
	id: string;
	row: T;
	/** Where the annotation sits in the current text; empty when its marked
	 * text was deleted (an "orphaned" comment the UI may still list). */
	ranges: { start: number; end: number }[];
}

/**
 * Joins annotation rows to their resolved mark ranges: for each row, the
 * ranges of the "comment:<id>" mark in the given spans. Rows are returned in
 * text order (first range position; orphans last, in input order) — ready to
 * render as a comment list beside the document.
 */
export function joinAnnotations<T>(
	rows: readonly T[],
	idOf: (row: T) => string,
	spans: readonly TextSpan[],
): ResolvedAnnotation<T>[] {
	const byId = new Map<string, { start: number; end: number }[]>();
	for (const r of commentRanges(spans)) {
		let list = byId.get(r.id);
		if (!list) {
			list = [];
			byId.set(r.id, list);
		}
		list.push({ start: r.start, end: r.end });
	}
	const out = rows.map((row) => {
		const id = idOf(row);
		return { id, row, ranges: byId.get(id) ?? [] };
	});
	out.sort((a, b) => {
		const pa = a.ranges[0]?.start ?? Number.POSITIVE_INFINITY;
		const pb = b.ranges[0]?.start ?? Number.POSITIVE_INFINITY;
		return pa - pb;
	});
	return out;
}
