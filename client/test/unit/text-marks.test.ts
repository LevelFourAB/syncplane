import { describe, expect, it } from 'vitest';

import {
	AnchorBefore,
	AnchorDocEnd,
	AnchorDocStart,
	type Expand,
	ExpandAfter,
	ExpandBefore,
	ExpandBoth,
	ExpandNone,
	type Span,
	TextDoc,
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

interface ItemIdLike {
	peer: string;
	counter: number;
}
interface NodeLike {
	id: ItemIdLike;
	deleted: boolean;
}
interface AnchorLike {
	kind: number;
	item?: ItemIdLike;
}
interface MarkOpLike {
	id: ItemIdLike;
	key: string;
	value: unknown;
	start: AnchorLike;
	end: AnchorLike;
}
/** The document internals this reference reads; not part of the public API. */
interface DocInternals {
	marks: MarkOpLike[];
	order(): NodeLike[];
}

const REMOVED = Symbol('removed');

function refKey(id: ItemIdLike): string {
	return `${id.peer}/${id.counter}`;
}

/**
 * The definition resolveMarks implements, written the obvious way: one paint
 * array per key spanning the entire item order, painted in ascending id order,
 * then read off in a single walk of the whole document.
 *
 * resolveMarks itself does not do this — it confines each key to the stretches
 * its own ops cover, which is what keeps a document with many keys (the
 * annotation pattern) from costing keys × document. The two must agree exactly,
 * so this stays as the reference the property below differentiates against.
 */
function marksByWholeDocumentPainting(doc: TextDoc): Span[] {
	const d = doc as unknown as DocInternals;
	if (d.marks.length === 0) return [];
	const order = d.order();

	const fullIndex = new Map<string, number>();
	for (let i = 0; i < order.length; i++) fullIndex.set(refKey(order[i]!.id), i);
	const anchorPos = (a: AnchorLike): number => {
		if (a.kind === AnchorDocStart) return 0;
		if (a.kind === AnchorDocEnd) return order.length;
		const i = fullIndex.get(refKey(a.item!));
		if (i === undefined) {
			throw new Error(`text: anchor references unknown item ${a.item!.peer}:${a.item!.counter}`);
		}
		return a.kind === AnchorBefore ? i : i + 1;
	};

	const ops = [ ...d.marks ].sort((x, y) =>
		x.id.counter - y.id.counter
		|| (x.id.peer < y.id.peer ? -1 : x.id.peer > y.id.peer ? 1 : 0)
	);
	const byKey = new Map<string, unknown[]>();
	for (const op of ops) {
		let paint = byKey.get(op.key);
		if (!paint) {
			paint = new Array(order.length).fill(undefined);
			byKey.set(op.key, paint);
		}
		const v = op.value === null ? REMOVED : op.value;
		const s = anchorPos(op.start);
		const e = anchorPos(op.end);
		for (let f = s; f < e && f < order.length; f++) paint[f] = v;
	}

	const spans: Span[] = [];
	for (const [ key, paint ] of byKey) {
		let visIdx = 0;
		let runStart = -1;
		let runValue: unknown = null;
		const flush = () => {
			if (runStart >= 0 && runValue !== null) {
				spans.push({ start: runStart, end: visIdx, key, value: runValue as Span['value'] });
			}
			runStart = -1;
			runValue = null;
		};
		for (let f = 0; f < order.length; f++) {
			if (order[f]!.deleted) continue;
			let v = paint[f];
			if (v === REMOVED || v === undefined) v = null;
			if (runStart >= 0 && v !== runValue) flush();
			if (runStart < 0 && v !== null) {
				runStart = visIdx;
				runValue = v;
			}
			visIdx++;
		}
		flush();
	}
	spans.sort((a, b) =>
		a.start - b.start
		|| a.end - b.end
		|| (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)
	);
	return spans;
}

describe('resolveMarks segmentation', () => {
	it('matches whole-document painting over random marks, unmarks and deletes', () => {
		const expands: Expand[] = [ ExpandNone, ExpandBefore, ExpandAfter, ExpandBoth ];
		// Few enough keys and values that ops genuinely overlap and supersede
		// each other rather than each getting its own stretch.
		const keys = [ 'bold', 'italic', 'comment:1', 'comment:2' ];
		const values = [ true, 'red', 2 ];

		for (let seed = 0; seed < 40; seed++) {
			const rnd = prng(seed + 1);
			const int = (n: number) => Math.floor(rnd() * n);
			const d = new TextDoc('alice');
			d.insert(0, 'abcdefghijklmnopqrstuvwxyz');

			for (let round = 0; round < 40; round++) {
				const l = d.length;
				const op = int(10);
				if (op < 3 && l > 0) {
					// delete, which leaves tombstones between marks
					const start = int(l);
					d.delete(start, 1 + int(Math.min(3, l - start)));
				} else if (op < 5) {
					// insert, so marks meet text that arrived after them
					d.insert(int(l + 1), String.fromCharCode(65 + int(26)));
				} else if (l > 0) {
					const start = int(l);
					const end = start + 1 + int(l - start);
					const key = keys[int(keys.length)]!;
					const expand = expands[int(expands.length)]!;
					if (int(4) === 0) d.unmark(start, end, key, expand);
					else d.mark(start, end, key, values[int(values.length)]!, expand);
				}

				expect(d.resolveMarks(), `seed ${seed} round ${round}: text ${d.plainText()}`)
					.toEqual(marksByWholeDocumentPainting(d));
			}
		}
	});
});
