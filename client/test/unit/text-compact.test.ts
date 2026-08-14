import { describe, expect, it } from 'vitest';

import {
	compactDoc,
	decodeTextDoc,
	decodeVV,
	encodeTextDoc,
	encodeVV,
	ExpandAfter,
	ExpandBefore,
	ExpandBoth,
	ExpandNone,
	mergeText,
	purgedByRemote,
	TextDoc,
} from '../../src/text/index.js';

function forked(doc: TextDoc, peer: string): TextDoc {
	return decodeTextDoc(encodeTextDoc(doc), peer);
}

describe('compactDoc', () => {
	it('drops a run that was typed and then deleted', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello world');
		d.delete(5, 6);
		const before = encodeTextDoc(d).length;

		expect(compactDoc(d).items).toBe(6);
		expect(d.plainText()).toBe('hello');
		expect(encodeTextDoc(d).length).toBeLessThan(before);
		// Still a well-formed, canonically encoded document.
		const round = decodeTextDoc(encodeTextDoc(d), '');
		expect(encodeTextDoc(round)).toEqual(encodeTextDoc(d));
	});

	it('keeps the version vector so purged ids stay accounted for', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'abc');
		d.delete(0, 3);
		expect(compactDoc(d).items).toBe(3);
		expect([ ...d.versionVector() ]).toEqual([ [ 'alice', 3 ] ]);
		d.insert(0, 'x');
		expect([ ...d.versionVector() ]).toEqual([ [ 'alice', 4 ] ]);
	});

	it('keeps deleted items that still position live text', () => {
		const a = new TextDoc('alice');
		a.insert(0, 'hello world');
		const b = forked(a, 'bob');
		b.insert(7, 'X');
		a.delete(5, 6);
		const both = decodeTextDoc(mergeText(encodeTextDoc(a), encodeTextDoc(b)), 'alice');
		expect(both.plainText()).toBe('helloX');
		expect(compactDoc(both).items).toBe(3);
		expect(both.plainText()).toBe('helloX');
		expect(() => decodeTextDoc(encodeTextDoc(both), '')).not.toThrow();
	});

	it('keeps a deleted item a mark anchors to', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello world');
		// ExpandBefore anchors the start to the preceding item, which the delete
		// below tombstones.
		d.mark(6, 11, 'bold', true, ExpandBefore);
		d.delete(5, 1);
		expect(compactDoc(d).items).toBe(0);
		expect(d.resolveMarks()).toEqual([ { start: 5, end: 10, key: 'bold', value: true } ]);
	});

	it('is idempotent', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'abc');
		d.delete(1, 2);
		expect(compactDoc(d).items).toBe(2);
		const once = encodeTextDoc(d);
		expect(compactDoc(d).items).toBe(0);
		expect(encodeTextDoc(d)).toEqual(once);
	});

	// A run typed left to right is a chain of right children as deep as the run
	// is long, and only its tail is ever purgeable until the tail goes. So a purge
	// that re-tests every item per removal round costs a round per character: at
	// this size that is minutes, against milliseconds for a cascade, which is what
	// the timeout separates.
	it('collapses a long deleted run in one cascade', () => {
		const n = 100000;
		const d = new TextDoc('alice');
		d.insert(0, 'a'.repeat(n));
		d.delete(0, n);

		expect(compactDoc(d)).toEqual({ items: n, marks: 0 });
		expect(d.plainText()).toBe('');
	}, 15000);

	it('converges with a replica that compacted independently', () => {
		const a = new TextDoc('alice');
		a.insert(0, 'hello world');
		a.delete(5, 6);
		const b = forked(a, 'bob');
		expect(compactDoc(a).items).toBe(6);
		expect(compactDoc(b).items).toBe(6);
		expect(encodeTextDoc(a)).toEqual(encodeTextDoc(b));
	});
});

// Mark operations are state like any other, so compaction may only drop the
// ones that can never win again — in this document or in any document reachable
// from it. See deadMarks in src/text/compact.ts for the two rules; these pin the
// line between them and what has to stay, and mirror the Go suite.
describe('compactDoc of mark operations', () => {
	it('reclaims a mark and the unmark that turned it off', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello');
		d.mark(0, 5, 'bold', true, ExpandNone);
		d.unmark(0, 5, 'bold', ExpandNone);
		expect(d.resolveMarks()).toEqual([]);

		expect(compactDoc(d)).toEqual({ items: 0, marks: 2 });
		expect(d.resolveMarks()).toEqual([]);
		expect(d.plainText()).toBe('hello');
	});

	it('reclaims a cancelled pending format and the item it pinned', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello world');
		d.delete(5, 6);
		// A pending format dropped at the end of the surviving text anchors to
		// the first tombstone of the deleted run. The rest of the run collapses
		// behind it as usual; that one item is held back by the anchor alone.
		d.mark(5, 5, 'bold', true, ExpandBoth);
		expect(compactDoc(d)).toEqual({ items: 5, marks: 0 });

		d.unmark(5, 5, 'bold', ExpandBoth);
		expect(compactDoc(d)).toEqual({ items: 1, marks: 2 });
		expect(d.plainText()).toBe('hello');
		expect(compactDoc(d)).toEqual({ items: 0, marks: 0 });
	});

	it('keeps a mark that still paints', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello');
		d.mark(0, 5, 'bold', true, ExpandNone);
		expect(compactDoc(d)).toEqual({ items: 0, marks: 0 });
		expect(d.resolveMarks()).toEqual([ { start: 0, end: 5, key: 'bold', value: true } ]);
	});

	it('keeps a point annotation, which covers nothing by design', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello');
		d.mark(2, 2, 'note:n1', true, ExpandNone);
		expect(compactDoc(d).marks).toBe(0);
		expect(d.points()).toEqual([ { index: 2, key: 'note:n1', value: true } ]);
	});

	it('keeps an unmark that still shadows a mark beneath it', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello');
		d.mark(0, 5, 'bold', true, ExpandNone);
		d.unmark(1, 4, 'bold', ExpandNone);
		const before = d.resolveMarks();

		expect(compactDoc(d).marks).toBe(0);
		expect(d.resolveMarks()).toEqual(before);
	});

	// The rule is anchor identity, not "shadowed right now". These two cover the
	// same characters and the higher id wins every one of them, but their
	// boundaries expand differently, so one character typed at the start
	// boundary reveals the lower op. Dropping it would silently change the
	// document's future.
	it('keeps a shadowed mark whose boundaries expand differently', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'abcdef');
		d.mark(2, 4, 'bold', true, ExpandBoth);
		d.mark(2, 4, 'bold', false, ExpandNone);
		expect(d.resolveMarks()).toEqual([ { start: 2, end: 4, key: 'bold', value: false } ]);

		expect(compactDoc(d).marks).toBe(0);

		d.insert(2, 'X');
		expect(d.resolveMarks()).toEqual([
			{ start: 2, end: 3, key: 'bold', value: true },
			{ start: 3, end: 5, key: 'bold', value: false },
		]);
	});

	it('keeps a removal that touches a mark it does not overlap yet', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'abcdef');
		d.mark(0, 3, 'bold', true, ExpandNone);
		// [3,6) starts exactly where the mark ends: no overlap today, but a
		// character typed at the shared edge can fall inside both.
		d.unmark(3, 6, 'bold', ExpandBoth);
		expect(compactDoc(d).marks).toBe(0);
	});

	it('leaves a document whose marks resolve identically', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello world');
		for (let i = 0; i < 5; i++) d.mark(0, 5, 'bold', true, ExpandNone);
		d.mark(6, 11, 'italic', true, ExpandAfter);
		const before = d.resolveMarks();

		expect(compactDoc(d).marks, 'only the newest of the identical bold ops survives').toBe(4);
		expect(d.resolveMarks()).toEqual(before);

		const enc = encodeTextDoc(d);
		expect(encodeTextDoc(decodeTextDoc(enc, ''))).toEqual(enc);
	});
});

describe('purgedByRemote', () => {
	it('is false for ordinary remote state', () => {
		const local = new TextDoc('alice');
		local.insert(0, 'hello');
		const remote = forked(local, 'bob');
		remote.insert(5, ' world');
		expect(purgedByRemote(local, decodeTextDoc(encodeTextDoc(remote), ''))).toBe(false);
	});

	it('is false for local edits the remote has never seen', () => {
		const remote = new TextDoc('alice');
		remote.insert(0, 'hello');
		const local = forked(remote, 'bob');
		local.insert(5, '!!');
		expect(purgedByRemote(local, decodeTextDoc(encodeTextDoc(remote), ''))).toBe(false);
	});

	it('is true when the remote dropped items it has seen', () => {
		const local = new TextDoc('alice');
		local.insert(0, 'hello world');
		local.delete(5, 6);
		const remote = forked(local, '');
		compactDoc(remote);
		expect(purgedByRemote(local, remote)).toBe(true);
	});

	// A compaction that reclaimed only mark ops leaves every item in place, so
	// the item scan alone reads it as ordinary state — and the handle would
	// merge the ops back in instead of rebuilding onto the compacted document.
	it('is true when the remote dropped only mark ops', () => {
		const local = new TextDoc('alice');
		local.insert(0, 'hello');
		local.mark(0, 5, 'bold', true, ExpandNone);
		local.unmark(0, 5, 'bold', ExpandNone);
		const remote = forked(local, '');
		expect(compactDoc(remote)).toEqual({ items: 0, marks: 2 });
		expect(purgedByRemote(local, remote)).toBe(true);
	});

	it('is false for a mark op the remote has never seen', () => {
		const remote = new TextDoc('alice');
		remote.insert(0, 'hello');
		const local = forked(remote, 'bob');
		local.mark(0, 5, 'bold', true, ExpandNone);
		expect(purgedByRemote(local, decodeTextDoc(encodeTextDoc(remote), ''))).toBe(false);
	});
});

describe('version vector codec', () => {
	it('round-trips canonically', () => {
		const vv = new Map([ [ 'bob', 3 ], [ 'alice', 9 ], [ '', 1 ] ]);
		const enc = encodeVV(vv);
		const back = decodeVV(enc);
		expect(back).toEqual(vv);
		expect(encodeVV(back)).toEqual(enc);
	});

	it('decodes empty input as no floor', () => {
		expect(decodeVV(undefined).size).toBe(0);
		expect(decodeVV(new Uint8Array()).size).toBe(0);
	});

	it('rejects trailing bytes', () => {
		const enc = encodeVV(new Map([ [ 'a', 1 ] ]));
		const withTrailer = new Uint8Array([ ...enc, 0 ]);
		expect(() => decodeVV(withTrailer)).toThrow('text: 1 trailing bytes');
	});
});
