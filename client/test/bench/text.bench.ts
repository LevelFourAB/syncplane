/**
 * Per-write cost of a text document, broken down by stage.
 *
 * The Go mirror is engine/text/bench_test.go and measures the same stages at
 * the same sizes, so the two languages can be compared directly — the client
 * and the server run the same algorithm over the same encoding, and which side
 * a cost lands on is often the only thing that differs.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench
 */

import { bench, describe } from 'vitest';
import { decodeTextDoc, encodeTextDoc } from '../../src/text/codec.js';
import { ExpandNone, TextDoc } from '../../src/text/doc.js';
import { encodeFragment } from '../../src/text/fragment.js';
import { mergeText } from '../../src/text/merge.js';
import { textMarksOf } from '../../src/text/project.js';

const SIZES = [ 5000, 20000, 50000 ];

/** A document of n code points, typed linearly by one peer. */
function benchDoc(peer: string, n: number): TextDoc {
	const doc = new TextDoc(peer);
	const chunk = 500;
	for (let written = 0; written < n; written += chunk) {
		doc.insert(written, 'a'.repeat(Math.min(chunk, n - written)));
	}
	return doc;
}

for (const n of SIZES) {
	describe(`${n}cp`, () => {
		const doc = benchDoc('w', n);
		const state = encodeTextDoc(doc);

		bench('encode', () => {
			encodeTextDoc(doc);
		});

		bench('decode', () => {
			decodeTextDoc(state, '');
		});

		bench('plainText', () => {
			decodeTextDoc(state, '').plainText();
		});

		// The local edit alone: bounds check, position lookup, tree splice. The
		// decode is charged to every case below too, so they stay comparable.
		bench('insertOne', () => {
			const d = decodeTextDoc(state, 'w');
			d.insert(n / 2, 'x');
		});

		// What a keystroke actually costs today: edit, then a full encode for
		// the PATCH value the pending queue and overlay both store.
		bench('keystroke (edit + encode)', () => {
			const d = decodeTextDoc(state, 'w');
			d.insert(n / 2, 'x');
			encodeTextDoc(d);
		});

		// The wire-side diff the coordinator runs before pushing.
		bench('encodeFragment', () => {
			const base = decodeTextDoc(state, '');
			const next = decodeTextDoc(state, 'c');
			next.insert(n / 2, 'x');
			encodeFragment(next, base);
		});

		// Applying a one-character fragment onto stored state — the shape the
		// server's write closure runs, measured here for cross-language parity.
		const next = decodeTextDoc(state, 'c');
		next.insert(n / 2, 'x');
		const frag = encodeFragment(next, decodeTextDoc(state, ''));
		bench('mergeFragment', () => {
			mergeText(state, frag);
		});
	});
}

/**
 * Mark resolution under the annotation pattern: one mark key per comment, each
 * covering a few words (see src/text/annotations.ts). Resolution is
 * O(document + covered), so the curve across comment counts should stay close
 * to flat — a resolver that allocates or walks per key instead grows with
 * keys × document, which is the shape these cases exist to catch.
 *
 * `marks()` is the public API on top: it converts every span boundary to a
 * UTF-16 index, which is why it is measured separately from resolveMarks().
 * Both run on the render path — a marked document re-resolves whenever its
 * text changes.
 */
describe('mark resolution (20000cp)', () => {
	const N = 20000;
	for (const comments of [ 0, 20, 100, 400 ]) {
		const doc = benchDoc('w', N);
		for (let i = 0; i < comments; i++) {
			const start = (i * 37) % (N - 60);
			doc.mark(start, start + 50, `comment:${i}`, true, ExpandNone);
		}
		const state = encodeTextDoc(doc);

		bench(`resolveMarks, ${comments} comments`, () => {
			doc.resolveMarks();
		});

		// The read-side projection memoizes per byte instance, so this measures
		// a first read of a version — what a watcher pays when the row changes.
		bench(`textMarksOf, ${comments} comments`, () => {
			textMarksOf(state.slice());
		});
	}

	// The other extreme: one key whose ops each span the whole document, so the
	// covered stretch IS the document and nothing can be skipped. Superseded
	// ops accumulate permanently — compaction purges tombstones, not mark ops.
	for (const ops of [ 1, 100, 1000 ]) {
		const doc = benchDoc('w', N);
		for (let i = 0; i < ops; i++) doc.mark(0, N, 'bold', true, ExpandNone);
		bench(`resolveMarks, ${ops} whole-document ops`, () => {
			doc.resolveMarks();
		});
	}
});
