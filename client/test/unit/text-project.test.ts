// Read-side projections of a text field: textOf / textMarksOf turn stored
// bytes into what a renderer needs without opening a handle, and memoize on
// the byte instance the read pipeline shares between watchers.

import { describe, expect, it } from 'vitest';

import {
	encodeTextDoc,
	ExpandNone,
	TextDoc,
	textMarksOf,
	textOf,
} from '../../src/text/index.js';

function doc(peer: string, s: string): TextDoc {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return d;
}

describe('textOf / textMarksOf', () => {
	it('projects the visible text and resolved spans', () => {
		const d = doc('a', 'hello world');
		d.delete(5, 6); // tombstones stay in the state but not in the projection
		d.mark(0, 5, 'bold', true, ExpandNone);
		const bytes = encodeTextDoc(d);

		expect(textOf(bytes)).toBe('hello');
		expect(textMarksOf(bytes)).toEqual([ { start: 0, end: 5, key: 'bold', value: true } ]);
	});

	it('reports spans in UTF-16 units, like TextHandle.marks()', () => {
		const d = doc('a', 'a🎉b');
		d.mark(1, 3, 'comment:c1', true, ExpandNone);
		const bytes = encodeTextDoc(d);

		// Code points 1..3 are UTF-16 units 1..4 — the surrogate pair counts twice.
		expect(textMarksOf(bytes)).toEqual([ { start: 1, end: 4, key: 'comment:c1', value: true } ]);
	});

	it('reads absent and empty bytes as an empty document', () => {
		expect(textOf(undefined)).toBe('');
		expect(textOf(new Uint8Array())).toBe('');
		expect(textMarksOf(undefined)).toEqual([]);
		expect(textMarksOf(new Uint8Array())).toEqual([]);
	});

	it('memoizes per byte instance and shares a frozen mark array', () => {
		const d = doc('a', 'abc');
		d.mark(0, 2, 'bold', true, ExpandNone);
		const bytes = encodeTextDoc(d);

		expect(textMarksOf(bytes)).toBe(textMarksOf(bytes));
		expect(() => (textMarksOf(bytes) as unknown[]).push({})).toThrow();

		// A different instance of the same bytes decodes again — equal, not shared.
		const copy = Uint8Array.from(bytes);
		expect(textOf(copy)).toBe(textOf(bytes));
		expect(textMarksOf(copy)).not.toBe(textMarksOf(bytes));
	});

	it('throws on bytes that are not a document', () => {
		expect(() => textOf(new Uint8Array([ 0x7f, 0x00 ]))).toThrow(/text:/);
	});
});
