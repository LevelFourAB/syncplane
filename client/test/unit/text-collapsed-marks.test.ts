// Collapsed marks are the pending-format and point-annotation states: a mark
// operation whose range covers no character. They ride in the ordinary mark
// section of the encoding — no new anchor kind, no new expand value, no format
// version — which is why the round-trip check matters as much as the
// resolution ones. The Go mirror is engine/text/text_test.go ("Collapsed
// marks"); testdata/text/vectors.json pins the bytes both sides produce.

import { describe, expect, it } from 'vitest';

import {
	decodeTextDoc,
	encodeTextDoc,
	ExpandAfter,
	ExpandBefore,
	ExpandBoth,
	ExpandNone,
	mergeText,
	TextDoc,
} from '../../src/text/index.js';

function pending(): TextDoc {
	const d = new TextDoc('a');
	d.insert(0, 'abcdef');
	d.mark(3, 3, 'bold', true, ExpandBoth);
	return d;
}

function noted(): TextDoc {
	const d = new TextDoc('a');
	d.insert(0, 'abcdef');
	d.mark(2, 2, 'note:n1', 'look here', ExpandNone);
	return d;
}

describe('a collapsed mark as a pending format', () => {
	it('covers nothing until text is typed at the caret', () => {
		const d = pending();
		expect(d.resolveMarks()).toEqual([]);
		expect(d.marksAt(3)).toEqual([ { key: 'bold', value: true } ]);
		expect(d.marksAt(2)).toEqual([]);
		expect(d.marksAt(4)).toEqual([]);
	});

	it('becomes an ordinary span on the first character typed there', () => {
		const d = pending();
		d.insert(3, 'X');
		expect(d.resolveMarks()).toEqual([ { start: 3, end: 4, key: 'bold', value: true } ]);
		// And keeps taking in what follows, which is what makes it usable as a
		// pending format rather than a one-character one.
		d.insert(4, 'YZ');
		expect(d.resolveMarks()).toEqual([ { start: 3, end: 6, key: 'bold', value: true } ]);
		expect(d.plainText()).toBe('abcXYZdef');
	});

	it('is cancelled by an unmark at the same caret', () => {
		const d = pending();
		d.unmark(3, 3, 'bold', ExpandBoth);
		expect(d.marksAt(3)).toEqual([]);
		d.insert(3, 'X');
		expect(d.resolveMarks()).toEqual([]);
	});

	it('survives a round-trip through the version-1 encoding', () => {
		const d = pending();
		const enc = encodeTextDoc(d);
		const round = decodeTextDoc(enc, 'a');
		expect(encodeTextDoc(round)).toEqual(enc);
		expect(round.resolveMarks()).toEqual([]);
		expect(round.marksAt(3)).toEqual([ { key: 'bold', value: true } ]);
		round.insert(3, 'X');
		expect(round.resolveMarks()).toEqual([ { start: 3, end: 4, key: 'bold', value: true } ]);
	});

	it('expands over text a concurrent replica typed at the caret', () => {
		const base = new TextDoc('seed');
		base.insert(0, 'abcdef');
		const a = decodeTextDoc(encodeTextDoc(base), 'a');
		const b = decodeTextDoc(encodeTextDoc(base), 'b');
		a.mark(3, 3, 'bold', true, ExpandBoth);
		b.insert(3, 'XY');
		const merged = decodeTextDoc(mergeText(encodeTextDoc(a), encodeTextDoc(b)), '');
		expect(merged.plainText()).toBe('abcXYdef');
		expect(merged.resolveMarks()).toEqual([ { start: 3, end: 5, key: 'bold', value: true } ]);
	});
});

describe('a collapsed mark as a point annotation', () => {
	it('reports as a point rather than a span', () => {
		const d = noted();
		expect(d.resolveMarks()).toEqual([]);
		expect(d.points()).toEqual([ { index: 2, key: 'note:n1', value: 'look here' } ]);
		// A note is not pending formatting: text typed at the position does not
		// take it on, which is the whole difference from ExpandBoth.
		expect(d.marksAt(2)).toEqual([]);
	});

	it('stays zero-width when text is typed at its position', () => {
		const d = noted();
		d.insert(2, 'XY');
		expect(d.plainText()).toBe('abXYcdef');
		expect(d.resolveMarks()).toEqual([]);
		// Anchored to the trailing edge of "ab", so the typed text lands after
		// the note and the note keeps its index.
		expect(d.points()).toEqual([ { index: 2, key: 'note:n1', value: 'look here' } ]);
	});

	it('drifts with the text in front of it', () => {
		const d = noted();
		d.insert(0, '>> ');
		expect(d.points()).toEqual([ { index: 5, key: 'note:n1', value: 'look here' } ]);
	});

	it('is removed by an unmark at the same position', () => {
		const d = noted();
		d.unmark(2, 2, 'note:n1', ExpandNone);
		expect(d.points()).toEqual([]);
	});

	it('keeps two notes on the same key at different positions', () => {
		const d = noted();
		d.mark(5, 5, 'note:n1', 'and here', ExpandNone);
		expect(d.points()).toEqual([
			{ index: 2, key: 'note:n1', value: 'look here' },
			{ index: 5, key: 'note:n1', value: 'and here' },
		]);
	});
});

describe('marksAt on ordinary marks', () => {
	it('follows the expand policy at each boundary', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abcde');
		d.mark(1, 3, 'm', true, ExpandAfter);
		const active = [ { key: 'm', value: true } ];
		expect(d.marksAt(1)).toEqual([]); // start boundary does not expand
		expect(d.marksAt(2)).toEqual(active);
		expect(d.marksAt(3)).toEqual(active); // end boundary does
		expect(d.marksAt(4)).toEqual([]);
	});

	it('reports a mark running to the end of the document', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abc');
		d.mark(0, 3, 'm', true, ExpandBoth);
		expect(d.marksAt(3)).toEqual([ { key: 'm', value: true } ]);
	});

	it('rejects an out-of-range index', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abc');
		expect(() => d.marksAt(4)).toThrow('text: index 4 out of range [0..3]');
	});
});

describe('collapsed mark validation', () => {
	it('rejects an inverted range', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abc');
		expect(() => d.mark(2, 1, 'bold', true, ExpandNone))
			.toThrow('text: mark range [2,1) invalid for length 3');
	});

	it.each([ [ 'before', ExpandBefore ], [ 'after', ExpandAfter ] ] as const)(
		'rejects a one-sided expand policy (%s)',
		(_name, expand) => {
			const d = new TextDoc('a');
			d.insert(0, 'abc');
			expect(() => d.mark(1, 1, 'bold', true, expand))
				.toThrow(/needs ExpandNone or ExpandBoth/);
		},
	);
});
