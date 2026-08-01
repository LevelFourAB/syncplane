// Read-side projections of a collaborative text field: the visible string and
// its resolved mark spans, for code that renders a row it read rather than
// editing one through a TextHandle.
//
// Decoding a document is O(size), and a rendered row is typically projected
// once per watcher per version, so results are memoized on the byte instance.
// That is sound because the read pipeline hands out the SAME Uint8Array for as
// long as the field's value is unchanged (rows are decoded once per version and
// shared across watchers) and never mutates it in place — the same reason rows
// are exposed readonly.
//
// The memo only covers a byte instance that has been projected before, which a
// changing field by definition has not, and a version is decoded once per scan
// scope rather than once. Both of those are answered by residency.ts: whoever
// last materialised this document — the optimistic fold, an arriving pull page,
// or the first reader to get here — left it decoded, addressed by these exact
// bytes. Borrowing it is read-only and does not outlive this call; what the memo
// keeps is the immutable projection, not the document.

import { decodeTextDoc } from './codec.js';
import { codePointOffsets } from './doc.js';
import type { TextSpan } from './handle.js';
import { textResidency } from './residency.js';

interface Projection {
	text: string;
	marks: readonly TextSpan[];
}

const NO_MARKS: readonly TextSpan[] = Object.freeze([]);

// Keyed by the byte instance: a new value decodes fresh, an unchanged one is
// free. Weak so a dropped row takes its projection with it.
const cache = new WeakMap<Uint8Array, Projection>();

function project(data: Uint8Array): Projection {
	const hit = cache.get(data);
	if (hit !== undefined) return hit;
	const resident = textResidency.lend(data);
	const doc = resident ?? decodeTextDoc(data, '');
	const text = doc.plainText();
	// resolveMarks() walks the item order; skip it for the common unmarked
	// document, where it can only return nothing anyway. One offset table
	// converts every span boundary, rather than rescanning the text per span.
	const marks: readonly TextSpan[] = doc.marks.length === 0 ? NO_MARKS : Object.freeze(
		(() => {
			const offsets = codePointOffsets(text);
			return doc.resolveMarks().map((s) => ({
				start: offsets[s.start]!,
				end: offsets[s.end]!,
				key: s.key,
				value: s.value,
			}));
		})(),
	);
	const projection: Projection = { text, marks };
	cache.set(data, projection);
	// A document decoded here is discarded by this function, so handing it over
	// costs nothing and saves the next reader holding a different instance of
	// the same bytes — a second scan scope, a query watcher — the same decode.
	// A borrowed one is already registered.
	//
	// Registered against a COPY: `data` is a view onto the row it was decoded
	// from, so keeping it would keep every other field of that row alive too,
	// however large, against a budget that only counts this one.
	if (resident === undefined) textResidency.put(data.slice(), doc);
	return projection;
}

/**
 * The visible text of an encoded text field — what `TextHandle.value` would
 * report, without opening a handle (no peer lease, no watch, no writes).
 * Absent or empty bytes read as the empty string; corrupt bytes throw, as
 * everywhere else that decodes a document.
 *
 * The Go equivalent is `text.PlainText`.
 */
export function textOf(data: Uint8Array | undefined): string {
	if (data === undefined || data.length === 0) return '';
	return project(data).text;
}

/**
 * The resolved mark spans of an encoded text field, in UTF-16 string indices
 * over `textOf(data)` — the read-only counterpart of `TextHandle.marks()`.
 *
 * The returned array is shared with every other caller holding these bytes and
 * is frozen; copy it before sorting or splicing.
 */
export function textMarksOf(data: Uint8Array | undefined): readonly TextSpan[] {
	if (data === undefined || data.length === 0) return NO_MARKS;
	return project(data).marks;
}
