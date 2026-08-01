import { describe, expect, it } from 'vitest';

import {
	AnchorAfter,
	AnchorBefore,
	AnchorDocEnd,
	AnchorDocStart,
	applyFragmentBytes,
	codePointToUtf16,
	decodeAnchor,
	decodeFragment,
	decodeTextDoc,
	diffText,
	encodeAnchor,
	encodeFragment,
	encodeFragmentFrom,
	encodeTextDoc,
	ExpandAfter,
	ExpandNone,
	isFragment,
	mergeText,
	plainTextOf,
	TextDoc,
	utf16ToCodePoint,
} from '../../src/text/index.js';

/** Deterministic PRNG (mulberry32) so failures reproduce. */
function prng(seed: number): () => number {
	let a = seed;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

function fork(d: TextDoc, peer: string): TextDoc {
	return decodeTextDoc(encodeTextDoc(d), peer);
}

/** Merges both ways, asserts commutativity, returns the merged doc. */
function converge(a: TextDoc, b: TextDoc): TextDoc {
	const ab = mergeText(encodeTextDoc(a), encodeTextDoc(b));
	const ba = mergeText(encodeTextDoc(b), encodeTextDoc(a));
	expect(ab).toEqual(ba);
	return decodeTextDoc(ab, '');
}

describe('TextDoc', () => {
	it('inserts and reads plain text', () => {
		const d = new TextDoc('a');
		d.insert(0, 'hello');
		d.insert(5, ' world');
		d.insert(5, ',');
		expect(d.plainText()).toBe('hello, world');
		expect(d.length).toBe(12);
	});

	it('deletes ranges', () => {
		const d = new TextDoc('a');
		d.insert(0, 'hello world');
		d.delete(5, 6);
		expect(d.plainText()).toBe('hello');
	});

	// Undoing a delete re-inserts the content as new items, so nothing the old
	// mark ops covered carries over: the segments have to bring the formatting
	// with them (see undo-ops.ts).
	it('reports the marks over deleted content, offset within each segment', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abcdefgh');
		d.mark(1, 4, 'bold', true, ExpandNone);
		d.mark(6, 8, 'link', 'https://x', ExpandNone);

		const tail = d.delete(5, 3); // 'fgh'; the link starts one code point in
		expect(tail).toHaveLength(1);
		expect(tail[0]!.content).toBe('fgh');
		expect(tail[0]!.marks).toEqual([ { start: 1, end: 3, key: 'link', value: 'https://x' } ]);

		const head = d.delete(0, 2); // 'ab'; bold covers only the 'b'
		expect(head[0]!.marks).toEqual([ { start: 1, end: 2, key: 'bold', value: true } ]);
	});

	// The redo of an undone delete deletes the re-inserted ids, which a peer's
	// concurrent insert can have split into several visible runs. Each run's
	// offsets are its own, not the document's.
	it('offsets segment marks per segment when a delete splits into runs', () => {
		const base = new TextDoc('alice');
		base.insert(0, 'abcd');
		const range = base.insert(4, 'efgh');
		base.mark(2, 7, 'bold', true, ExpandNone);

		const bob = fork(base, 'bob');
		bob.insert(6, 'XY');
		const d = decodeTextDoc(mergeText(encodeTextDoc(base), encodeTextDoc(bob)), 'alice');
		expect(d.plainText()).toBe('abcdefXYgh');

		// Alice's 'efgh' is now two visible runs around bob's insert.
		const segs = d.deleteItems(range.peer, range.start, range.count);
		expect(segs.map((s) => s.content)).toEqual([ 'ef', 'gh' ]);
		expect(segs[0]!.marks).toEqual([ { start: 0, end: 2, key: 'bold', value: true } ]);
		expect(segs[1]!.marks).toEqual([ { start: 0, end: 1, key: 'bold', value: true } ]);
	});

	it('reports no segment marks for an unmarked document', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abc');
		expect(d.delete(0, 3)[0]!.marks).toEqual([]);
	});

	it('counts code points, not UTF-16 units', () => {
		const d = new TextDoc('a');
		d.insert(0, 'héllo');
		d.insert(5, ' 🎉');
		expect(d.plainText()).toBe('héllo 🎉');
		expect(d.length).toBe(7);
		d.delete(6, 1);
		expect(d.plainText()).toBe('héllo ');
	});

	it('rejects out-of-range edits and edits without a peer', () => {
		const d = new TextDoc('a');
		expect(() => d.insert(1, 'x')).toThrow();
		d.insert(0, 'ab');
		expect(() => d.delete(1, 2)).toThrow();
		const ro = new TextDoc('');
		expect(() => ro.insert(0, 'x')).toThrow(/no peer/);
	});

	// An unpaired surrogate has no UTF-8 encoding: TextEncoder would put
	// U+FFFD in its place, silently, and the document would then differ from
	// what the caller wrote with nothing downstream able to tell. This is the
	// JS-side half of the Go mirror's invalid-UTF-8 rejection.
	it('rejects unpaired surrogates rather than substituting U+FFFD', () => {
		const d = new TextDoc('a');
		expect(() => d.insert(0, 'caf\ud83c')).toThrow(
			'text: insert: unpaired surrogate at index 3',
		);
		expect(d.plainText()).toBe('');

		// A well-formed pair (the same high surrogate, now followed by its low
		// half) is ordinary astral-plane text and must still go through.
		d.insert(0, 'caf🎉');
		expect(d.plainText()).toBe('caf🎉');
		expect(d.length).toBe(4);

		// A bare low surrogate, and one at the very end of the string.
		expect(() => d.insert(0, '\udf89x')).toThrow(
			'text: insert: unpaired surrogate at index 0',
		);
		expect(() => d.insert(0, 'ab\ud83c')).toThrow(
			'text: insert: unpaired surrogate at index 2',
		);
		expect(d.plainText()).toBe('caf🎉');
	});

	it('rejects an unpaired surrogate in setString without touching the document', () => {
		const d = new TextDoc('a');
		d.insert(0, 'café latte');
		const before = encodeTextDoc(d);
		expect(() => d.setString('café \ud83clatte')).toThrow(
			'text: setString: unpaired surrogate at index 5',
		);
		expect(d.plainText()).toBe('café latte');
		// Hunks apply one at a time, so the rejection has to happen before the
		// first of them: a half-rewritten document would be unrecoverable.
		expect(encodeTextDoc(d)).toEqual(before);
	});

	it('round-trips through encode/decode canonically', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello world');
		d.delete(2, 3);
		d.mark(0, 4, 'bold', true, ExpandAfter);
		const enc = encodeTextDoc(d);
		const d2 = decodeTextDoc(enc, 'bob');
		expect(d2.plainText()).toBe(d.plainText());
		expect(d2.resolveMarks()).toEqual(d.resolveMarks());
		expect(encodeTextDoc(d2)).toEqual(enc);
	});

	it('decodes empty bytes to an empty document', () => {
		expect(decodeTextDoc(undefined, 'a').plainText()).toBe('');
		expect(plainTextOf(new Uint8Array())).toBe('');
	});
});

describe('mergeText', () => {
	it('merges concurrent inserts at different positions', () => {
		const a = new TextDoc('a');
		a.insert(0, 'hello world');
		const b = fork(a, 'b');
		a.insert(0, '>> ');
		b.insert(11, ' <<');
		expect(converge(a, b).plainText()).toBe('>> hello world <<');
	});

	it('does not interleave concurrent forward typing at the same position', () => {
		const base = new TextDoc('seed');
		base.insert(0, '[]');
		const a = fork(base, 'alice');
		const b = fork(base, 'bob');
		a.insert(1, 'alice');
		b.insert(1, 'bob');
		const got = converge(a, b).plainText();
		expect([ '[alicebob]', '[bobalice]' ]).toContain(got);
	});

	it('does not interleave concurrent backward typing', () => {
		const a = new TextDoc('alice');
		const b = new TextDoc('bob');
		for (const ch of [ 'e', 'c', 'i', 'l', 'a' ]) a.insert(0, ch);
		for (const ch of [ 'b', 'o', 'b' ]) b.insert(0, ch);
		expect(a.plainText()).toBe('alice');
		const got = converge(a, b).plainText();
		expect([ 'alicebob', 'bobalice' ]).toContain(got);
	});

	it('is idempotent', () => {
		const a = new TextDoc('a');
		a.insert(0, 'abc');
		const enc = encodeTextDoc(a);
		expect(mergeText(enc, enc)).toEqual(enc);
	});

	it('merges concurrent delete and insert', () => {
		const base = new TextDoc('seed');
		base.insert(0, 'hello world');
		const a = fork(base, 'a');
		const b = fork(base, 'b');
		a.delete(0, 6);
		b.insert(11, '!');
		expect(converge(a, b).plainText()).toBe('world!');
	});

	// Two replicas sharing a peer is the one input the merge cannot converge,
	// and nothing in the encoding reveals it — so it has to be caught here,
	// loudly. Mirrors engine/text merge_test's shared-peer specs.
	describe('two replicas that shared a peer', () => {
		it('rejects the same id carrying different content', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'AB');
			const a = fork(base, 'dup');
			const b = fork(base, 'dup');
			a.insert(0, 'x');
			b.insert(0, 'y');
			expect(() => mergeText(encodeTextDoc(a), encodeTextDoc(b)))
				.toThrow(/different content/);
		});

		it('rejects the same id placed differently', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'AB');
			const a = fork(base, 'dup');
			const b = fork(base, 'dup');
			// Same character, different position: identical ids, different
			// parents. Unchecked this merges silently and non-commutatively —
			// 'zAB' one way round, 'ABz' the other.
			a.insert(0, 'z');
			b.insert(2, 'z');
			expect(() => mergeText(encodeTextDoc(a), encodeTextDoc(b)))
				.toThrow(/different placement/);
			expect(() => mergeText(encodeTextDoc(b), encodeTextDoc(a)))
				.toThrow(/different placement/);
		});

		it('rejects the same mark op id covering a different range', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'hello');
			const a = fork(base, 'dup');
			const b = fork(base, 'dup');
			a.mark(0, 5, 'bold', true, ExpandNone);
			b.mark(0, 3, 'bold', true, ExpandNone);
			expect(() => mergeText(encodeTextDoc(a), encodeTextDoc(b)))
				.toThrow(/differs on each side/);
		});

		it('still accepts a mark op both sides genuinely share', () => {
			// The ordinary case the check must not break: one op, inherited by
			// two replicas that then diverge elsewhere.
			const base = new TextDoc('seed');
			base.insert(0, 'hello');
			base.mark(0, 5, 'bold', true, ExpandNone);
			const a = fork(base, 'a');
			const b = fork(base, 'b');
			a.insert(5, 'X');
			b.insert(0, 'Y');
			const merged = converge(a, b);
			expect(merged.plainText()).toBe('YhelloX');
			expect(merged.marks).toHaveLength(1);
		});
	});
});

describe('marks', () => {
	it('moves marks with the text and resolves overlaps', () => {
		const d = new TextDoc('a');
		d.insert(0, 'hello world');
		d.mark(6, 11, 'comment:c1', true, ExpandNone);
		d.insert(0, '>> ');
		expect(d.resolveMarks()).toEqual([
			{ start: 9, end: 14, key: 'comment:c1', value: true },
		]);
	});

	it('lets overlapping comment keys coexist', () => {
		const d = new TextDoc('a');
		d.insert(0, 'hello world');
		d.mark(0, 8, 'comment:c1', true, ExpandNone);
		d.mark(3, 11, 'comment:c2', true, ExpandNone);
		expect(d.resolveMarks()).toEqual([
			{ start: 0, end: 8, key: 'comment:c1', value: true },
			{ start: 3, end: 11, key: 'comment:c2', value: true },
		]);
	});

	it('resolves same-key overlaps by highest op id and honors unmark', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abcdef');
		d.mark(0, 6, 'color', 'red', ExpandNone);
		d.mark(2, 4, 'color', 'blue', ExpandNone);
		expect(d.resolveMarks()).toEqual([
			{ start: 0, end: 2, key: 'color', value: 'red' },
			{ start: 2, end: 4, key: 'color', value: 'blue' },
			{ start: 4, end: 6, key: 'color', value: 'red' },
		]);
		d.unmark(0, 6, 'color', ExpandNone);
		expect(d.resolveMarks()).toEqual([]);
	});

	it('expands AFTER marks at the end boundary only', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abc');
		d.mark(1, 2, 'm', true, ExpandAfter);
		d.insert(2, 'xy');
		expect(d.resolveMarks()).toEqual([ { start: 1, end: 4, key: 'm', value: true } ]);
	});

	// Both implementations refuse these at creation and at decode, so a
	// document can never carry one; see the rejection vectors for the decode
	// half.
	it.each([ NaN, Infinity, -Infinity ])('rejects the non-finite mark value %p', (v) => {
		const d = new TextDoc('a');
		d.insert(0, 'abc');
		expect(() => d.mark(0, 3, 'size', v, ExpandNone)).toThrow('text: non-finite mark value');
	});

	// Unreachable through decode (anchors are validated there) or mark (built
	// from live items), but mergeInto takes in-memory documents. Go painted
	// the whole prefix here and TS painted nothing; both now refuse.
	it('throws when a mark anchors to an item the document does not hold', () => {
		const d = new TextDoc('alice');
		d.insert(0, 'hello');
		d.mark(0, 5, 'bold', true, ExpandNone);
		d.marks[0]!.start = { kind: AnchorBefore, item: { peer: 'ghost', counter: 1 } };
		expect(() => d.resolveMarks()).toThrow('text: anchor references unknown item ghost:1');
	});
});

describe('anchors', () => {
	it('round-trips through encodeAnchor/decodeAnchor', () => {
		const d = new TextDoc('a');
		d.insert(0, 'hello');
		for (const kind of [ AnchorBefore, AnchorAfter ] as const) {
			const a = d.anchorAt(3, kind);
			expect(decodeAnchor(encodeAnchor(a))).toEqual(a);
		}
	});

	it('survives remote inserts and deletions', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abcdef');
		const before = d.anchorAt(3, AnchorBefore);
		const after = d.anchorAt(4, AnchorAfter);
		d.delete(2, 2); // "cd"
		expect(d.plainText()).toBe('abef');
		expect(d.resolveAnchor(before)).toBe(2);
		expect(d.resolveAnchor(after)).toBe(2);
	});

	it('resolves document edges', () => {
		const d = new TextDoc('a');
		d.insert(0, 'abc');
		expect(d.anchorAt(0, AnchorAfter).kind).toBe(AnchorDocStart);
		expect(d.anchorAt(3, AnchorBefore).kind).toBe(AnchorDocEnd);
		expect(d.resolveAnchor({ kind: AnchorDocStart })).toBe(0);
		expect(d.resolveAnchor({ kind: AnchorDocEnd })).toBe(3);
	});

	it('errors on unknown items', () => {
		const d = new TextDoc('a');
		expect(() => d.resolveAnchor({ kind: AnchorBefore, item: { peer: 'ghost', counter: 1 } }))
			.toThrow(/unknown item/);
	});
});

describe('diffText', () => {
	it('reports remote edits against retained items', () => {
		const prev = new TextDoc('a');
		prev.insert(0, 'hello world');
		const next = fork(prev, 'b');
		next.insert(5, ',');
		next.delete(7, 5);
		next.insert(7, 'there');
		expect(next.plainText()).toBe('hello, there');
		const delta = diffText(prev, next);
		expect(delta.ops).toEqual([
			{ retain: 5 },
			{ insert: ',' },
			{ retain: 1 },
			{ insert: 'there' },
			{ delete: 5 },
		]);
		expect(delta.marksChanged).toBe(false);
	});

	it('flags mark changes', () => {
		const prev = new TextDoc('a');
		prev.insert(0, 'abc');
		const next = fork(prev, 'b');
		next.mark(0, 2, 'm', true, ExpandNone);
		const delta = diffText(prev, next);
		expect(delta.ops).toEqual([]);
		expect(delta.marksChanged).toBe(true);
	});
});

describe('setString', () => {
	it('merges an external whole-value write with a concurrent edit', () => {
		const base = new TextDoc('seed');
		base.insert(0, 'one two three');
		const a = fork(base, 'connector');
		const b = fork(base, 'client');
		a.setString('one TWO three');
		b.insert(13, ' four');
		expect(converge(a, b).plainText()).toBe('one TWO three four');
	});

	it('merges a two-ended save with a concurrent middle edit', () => {
		const base = new TextDoc('seed');
		base.insert(0, 'Groceries: milk, eggs, bread');
		const a = fork(base, 'connector');
		const b = fork(base, 'client');
		a.setString('* Groceries: milk, eggs, bread.');
		b.insert(17, 'cheese, ');
		expect(converge(a, b).plainText()).toBe('* Groceries: milk, cheese, eggs, bread.');
	});

	it('reports one inverse entry per hunk', () => {
		const d = new TextDoc('a');
		d.insert(0, 'one two three four five');
		const { deleted, inserted } = d.setString('one TWO three FOUR five');
		expect(deleted.length).toBe(2);
		expect(inserted.length).toBe(2);
		expect(d.plainText()).toBe('one TWO three FOUR five');
	});

	it('inserts exactly len(new)-LCS items over random string pairs', () => {
		// The Myers script is minimal, so the number of freshly inserted items
		// is len(new) − LCS(old, new): everything common survives in place.
		// Observable through the version vector — the only counters spent are
		// the inserted items'.
		const alphabet = [ ...'abc é🎉' ];
		const lcs = (a: string[], b: string[]): number => {
			const dp = new Array<number>(b.length + 1).fill(0);
			for (let i = 1; i <= a.length; i++) {
				let prev = 0;
				for (let j = 1; j <= b.length; j++) {
					const cur = dp[j]!;
					if (a[i - 1] === b[j - 1]) dp[j] = prev + 1;
					else if (dp[j - 1]! > dp[j]!) dp[j] = dp[j - 1]!;
					prev = cur;
				}
			}
			return dp[b.length]!;
		};
		const rnd = prng(42);
		const randString = (): string[] => {
			const out: string[] = [];
			for (let n = Math.floor(rnd() * 40); n > 0; n--) {
				out.push(alphabet[Math.floor(rnd() * alphabet.length)]!);
			}
			return out;
		};
		for (let trial = 0; trial < 200; trial++) {
			const old = randString();
			const next = randString();
			const d = new TextDoc('alice');
			d.insert(0, old.join(''));
			d.setString(next.join(''));
			expect(d.plainText()).toBe(next.join(''));
			const inserted = (d.versionVector().get('alice') ?? 0) - old.length;
			expect(inserted).toBe(next.length - lcs(old, next));
		}
	});
});

describe('index conversion', () => {
	it('maps UTF-16 indices to code points and back', () => {
		const s = 'a🎉b';
		expect(utf16ToCodePoint(s, 0)).toBe(0);
		expect(utf16ToCodePoint(s, 1)).toBe(1);
		expect(utf16ToCodePoint(s, 3)).toBe(2);
		expect(utf16ToCodePoint(s, 4)).toBe(3);
		expect(() => utf16ToCodePoint(s, 2)).toThrow(/surrogate/);
		expect(codePointToUtf16(s, 2)).toBe(3);
		expect(codePointToUtf16(s, 3)).toBe(4);
	});
});

describe('fragments', () => {
	it('identifies fragment bytes', () => {
		const d = new TextDoc('a');
		d.insert(0, 'hi');
		const frag = encodeFragment(d, null);
		expect(isFragment(frag)).toBe(true);
		expect(isFragment(encodeTextDoc(d))).toBe(false);
		expect(isFragment(undefined)).toBe(false);
	});

	it('a self-contained fragment applied to an empty doc equals the source', () => {
		const d = new TextDoc('a');
		d.insert(0, 'hello');
		d.mark(0, 2, 'b', true, ExpandNone);
		const frag = encodeFragment(d, null);
		expect(applyFragmentBytes(undefined, frag)).toEqual(encodeTextDoc(d));
	});

	it('rejects a base that is not an ancestor', () => {
		const a = new TextDoc('alice');
		a.insert(0, 'one');
		const b = new TextDoc('bob');
		b.insert(0, 'two');
		expect(() => encodeFragment(a, b)).toThrow(
			'text: fragment: base is not an ancestor of doc',
		);
	});

	it('a rejected apply leaves the receiver untouched', () => {
		const base = new TextDoc('seed');
		base.insert(0, 'abc');
		const l = fork(base, 'alice');
		l.insert(3, 'd');
		const frag = encodeFragment(l, base);
		const s = new TextDoc('bob');
		s.insert(0, 'z');
		const before = encodeTextDoc(s);
		expect(() => applyFragmentBytes(before, frag)).toThrow(
			'text: item alice:4 references unknown parent seed:3',
		);
		expect(encodeTextDoc(s)).toEqual(before);
	});

	// encodeFragmentFrom is what a live text handle uses instead of diffing:
	// it names the items an edit touched, which costs the size of the edit
	// rather than the size of the document. The two must be interchangeable.
	describe('encoded from a recorded edit', () => {
		it('matches the diff for an insert', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'abc');
			const l = fork(base, 'alice');
			const range = l.insert(3, 'defg');
			const ids = [];
			for (let c = range.start; c < range.start + range.count; c++) {
				ids.push({ peer: range.peer, counter: c });
			}
			expect(encodeFragmentFrom(l, ids, [])).toEqual(encodeFragment(l, base));
		});

		it('matches the diff for a delete — the tombstones no version vector shows', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'abcdef');
			const l = fork(base, 'alice');
			const segments = l.delete(1, 3);
			const ids = segments.flatMap((s) => s.ids);
			expect(ids).toHaveLength(3);
			expect(encodeFragmentFrom(l, ids, [])).toEqual(encodeFragment(l, base));
		});

		it('matches the diff for a mark', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'abcdef');
			const l = fork(base, 'alice');
			const id = l.mark(1, 4, 'bold', true, ExpandNone);
			expect(encodeFragmentFrom(l, [], [ id ])).toEqual(encodeFragment(l, base));
		});

		it('matches the diff over a burst of edits recorded together', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'hello world');
			const l = fork(base, 'alice');
			const ids = [];
			const marks = [];
			for (const [ i, ch ] of [ ...'!?.' ].entries()) {
				const r = l.insert(5 + i, ch);
				ids.push({ peer: r.peer, counter: r.start });
			}
			// An item inserted and then deleted inside the burst is recorded twice;
			// the encoder must not emit it twice.
			const dup = l.insert(0, 'x');
			ids.push({ peer: dup.peer, counter: dup.start });
			ids.push(...l.delete(0, 1).flatMap((s) => s.ids));
			marks.push(l.mark(0, 3, 'em', true, ExpandNone));
			expect(encodeFragmentFrom(l, ids, marks)).toEqual(encodeFragment(l, base));
		});

		it('applies onto the base the recording started from', () => {
			const base = new TextDoc('seed');
			base.insert(0, 'abc');
			const l = fork(base, 'alice');
			const range = l.insert(3, 'd');
			const frag = encodeFragmentFrom(l, [ { peer: range.peer, counter: range.start } ], []);
			expect(applyFragmentBytes(encodeTextDoc(base), frag)).toEqual(encodeTextDoc(l));
		});
	});

	it('observeVersion allocates counters past the observed state', () => {
		const d = new TextDoc('writer');
		d.observeVersion(new Map([ [ 'other', 10 ] ]));
		const range = d.insert(0, 'x');
		expect(range.start).toBe(11);
	});

	it('pairwise fragment sync equals full-state merge over random histories', () => {
		const alphabet = [ ...'abcdefghijklmnopqrstuvwxyzåäö🎉' ];
		for (let seed = 100; seed < 110; seed++) {
			const rnd = prng(seed);
			const docs = [ new TextDoc('A'), new TextDoc('B'), new TextDoc('C'), new TextDoc('D') ];
			// snapshots[j][i] is the last state of j that i received — the base
			// fragments from j to i are cut against.
			const snapshots: (Uint8Array | undefined)[][] = docs.map(() => docs.map(() => undefined));
			for (let round = 0; round < 30; round++) {
				for (let di = 0; di < docs.length; di++) {
					const d = docs[di]!;
					const op = Math.floor(rnd() * 10);
					if (op < 5) {
						let s = '';
						for (let n = 1 + Math.floor(rnd() * 4); n > 0; n--) {
							s += alphabet[Math.floor(rnd() * alphabet.length)]!;
						}
						d.insert(Math.floor(rnd() * (d.length + 1)), s);
					} else if (op < 7) {
						if (d.length > 0) {
							const idx = Math.floor(rnd() * d.length);
							d.delete(idx, 1 + Math.floor(rnd() * Math.min(3, d.length - idx)));
						}
					} else if (op < 8) {
						if (d.length > 1) {
							const s = Math.floor(rnd() * (d.length - 1));
							const e = s + 1 + Math.floor(rnd() * (d.length - s - 1));
							const key = `k${Math.floor(rnd() * 3)}`;
							const expand = Math.floor(rnd() * 4) as 0 | 1 | 2 | 3;
							if (rnd() < 1 / 3) d.unmark(s, e, key, expand);
							else d.mark(s, e, key, rnd() < 0.5, expand);
						}
					} else {
						const i = Math.floor(rnd() * docs.length);
						const j = Math.floor(rnd() * docs.length);
						if (i === j) continue;
						const base = decodeTextDoc(snapshots[j]![i], '');
						const frag = encodeFragment(docs[j]!, base);
						const viaFragment = applyFragmentBytes(encodeTextDoc(docs[i]!), frag);
						const viaMerge = mergeText(encodeTextDoc(docs[i]!), encodeTextDoc(docs[j]!));
						expect(viaFragment).toEqual(viaMerge);
						// Fragment round-trip is stable.
						expect(decodeFragment(frag).encode()).toEqual(frag);
						docs[i] = decodeTextDoc(viaMerge, docs[i]!.peer);
						snapshots[i]![j] = encodeTextDoc(docs[j]!);
						snapshots[j]![i] = encodeTextDoc(docs[j]!);
					}
				}
			}
		}
	});
});

describe('convergence property', () => {
	it('converges replicas over random ops merged in random orders', () => {
		const alphabet = [ ...'abcdefghijklmnopqrstuvwxyzåäö🎉' ];
		for (let seed = 1; seed <= 10; seed++) {
			const rnd = prng(seed);
			const docs = [ new TextDoc('A'), new TextDoc('B'), new TextDoc('C'), new TextDoc('D') ];
			for (let round = 0; round < 30; round++) {
				for (let di = 0; di < docs.length; di++) {
					const d = docs[di]!;
					const op = Math.floor(rnd() * 10);
					if (op < 5) {
						let s = '';
						for (let n = 1 + Math.floor(rnd() * 4); n > 0; n--) {
							s += alphabet[Math.floor(rnd() * alphabet.length)]!;
						}
						d.insert(Math.floor(rnd() * (d.length + 1)), s);
					} else if (op < 7) {
						if (d.length > 0) {
							const idx = Math.floor(rnd() * d.length);
							d.delete(idx, 1 + Math.floor(rnd() * Math.min(3, d.length - idx)));
						}
					} else if (op < 9) {
						if (d.length > 1) {
							const s = Math.floor(rnd() * (d.length - 1));
							const e = s + 1 + Math.floor(rnd() * (d.length - s - 1));
							const key = `k${Math.floor(rnd() * 3)}`;
							const expand = Math.floor(rnd() * 4) as 0 | 1 | 2 | 3;
							// Removal is the nil-valued op whose whole purpose is to win
							// resolution over the marks it covers, so it is the part of
							// the mark model most exposed to an order-dependent bug — it
							// belongs in the random mix, not only in fixed examples.
							if (rnd() < 1 / 3) d.unmark(s, e, key, expand);
							else d.mark(s, e, key, rnd() < 0.5, expand);
						}
					} else {
						const i = Math.floor(rnd() * docs.length);
						const j = Math.floor(rnd() * docs.length);
						if (i !== j) {
							docs[i] = decodeTextDoc(
								mergeText(encodeTextDoc(docs[i]!), encodeTextDoc(docs[j]!)),
								docs[i]!.peer,
							);
						}
					}
				}
			}
			const shuffle = () => {
				const order = [ 0, 1, 2, 3 ];
				for (let i = order.length - 1; i > 0; i--) {
					const j = Math.floor(rnd() * (i + 1));
					[ order[i], order[j] ] = [ order[j]!, order[i]! ];
				}
				return order;
			};
			const mergeAll = (order: number[]) => {
				let acc = encodeTextDoc(docs[order[0]!]!);
				for (const i of order.slice(1)) {
					acc = mergeText(acc, encodeTextDoc(docs[i]!));
				}
				return acc;
			};
			const m1 = mergeAll(shuffle());
			const m2 = mergeAll(shuffle());
			expect(m1).toEqual(m2);
			// Canonical: decode → re-encode is stable.
			expect(encodeTextDoc(decodeTextDoc(m1, ''))).toEqual(m1);
		}
	});
});
