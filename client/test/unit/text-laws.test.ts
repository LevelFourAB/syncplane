import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import {
	applyFragmentBytes,
	compactDoc,
	decodeTextDoc,
	encodeFragment,
	encodeTextDoc,
	type Expand,
	mergeText,
	TextDoc,
} from '../../src/text/index.js';

// The text-CRDT laws over random histories, and agreement with Go on
// histories and damaged documents the Go suite generates
// (engine/text/laws_test.go and engine/text/decode_test.go). The hand-written
// cases live in text-vectors.test.ts.

const testdata = (name: string) =>
	fileURLToPath(new URL(`../../../testdata/text/${name}`, import.meta.url));

interface LawOp {
	kind: 'insert' | 'delete' | 'mark' | 'unmark' | 'setString' | 'sync' | 'syncFrag';
	r: number;
	j?: number;
	index?: number;
	length?: number;
	text?: string;
	key?: string;
	bool?: boolean;
	str?: string;
	num?: number;
	expand?: number;
}

interface LawCase {
	seed: number;
	peers: string[];
	ops: LawOp[];
	replicas: string[];
	merged: string;
	compacted: string;
	text: string;
	spans: { start: number; end: number; key: string; value: unknown }[];
}

const hex = (b: Uint8Array): string => Buffer.from(b).toString('hex');

function replay(c: LawCase): { replicas: Uint8Array[]; docs: TextDoc[] } {
	const n = c.peers.length;
	let docs = c.peers.map((p) => new TextDoc(p));
	const snaps: (Uint8Array | undefined)[][] = c.peers.map(() => new Array(n).fill(undefined));
	for (const op of c.ops) {
		const d = docs[op.r]!;
		switch (op.kind) {
			case 'insert':
				d.insert(op.index ?? 0, op.text ?? '');
				break;
			case 'delete':
				d.delete(op.index ?? 0, op.length ?? 0);
				break;
			case 'mark': {
				const value = op.bool ?? op.str ?? op.num!;
				const s = op.index ?? 0;
				d.mark(s, s + (op.length ?? 0), op.key!, value, (op.expand ?? 0) as Expand);
				break;
			}
			case 'unmark': {
				const s = op.index ?? 0;
				d.unmark(s, s + (op.length ?? 0), op.key!, (op.expand ?? 0) as Expand);
				break;
			}
			case 'setString':
				d.setString(op.text ?? '');
				break;
			case 'sync': {
				const j = op.j ?? 0;
				const merged = mergeText(encodeTextDoc(d), encodeTextDoc(docs[j]!));
				docs = docs.map((x, i) => (i === op.r ? decodeTextDoc(merged, c.peers[op.r]!) : x));
				snaps[op.r]![j] = encodeTextDoc(docs[j]!);
				snaps[j]![op.r] = encodeTextDoc(docs[j]!);
				break;
			}
			case 'syncFrag': {
				const j = op.j ?? 0;
				const snap = snaps[op.r]![j];
				const base = snap === undefined ? null : decodeTextDoc(snap, '');
				const frag = encodeFragment(docs[j]!, base);
				const via = applyFragmentBytes(encodeTextDoc(d), frag);
				docs = docs.map((x, i) => (i === op.r ? decodeTextDoc(via, c.peers[op.r]!) : x));
				snaps[op.r]![j] = encodeTextDoc(docs[j]!);
				snaps[j]![op.r] = encodeTextDoc(docs[j]!);
				break;
			}
		}
	}
	return { replicas: docs.map((d) => encodeTextDoc(d)), docs };
}

describe('text histories shared with Go', () => {
	it('replays every history to the bytes, text and marks Go produced', () => {
		const { histories } = JSON.parse(readFileSync(testdata('histories.json'), 'utf8')) as {
			histories: LawCase[];
		};
		expect(histories.length).toBeGreaterThan(0);
		for (const c of histories) {
			const { replicas } = replay(c);
			expect(replicas.map(hex), `seed ${c.seed}: replicas`).toEqual(c.replicas);
			let merged = replicas[0]!;
			for (const r of replicas.slice(1)) merged = mergeText(merged, r);
			expect(hex(merged), `seed ${c.seed}: merged`).toBe(c.merged);
			const md = decodeTextDoc(merged, '');
			expect(md.plainText(), `seed ${c.seed}: text`).toBe(c.text);
			expect(md.resolveMarks(), `seed ${c.seed}: marks`).toEqual(c.spans);
			compactDoc(md);
			expect(hex(encodeTextDoc(md)), `seed ${c.seed}: compacted`).toBe(c.compacted);
		}
	});
});

/** A small seeded PRNG (mulberry32), so a failing seed reproduces. */
function rng(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

const alphabet = [
	'a',
	'b',
	' ',
	'\r',
	'\n',
	'\u0000',
	'\ufeff',
	'\u0301',
	'\u200d',
	'\u{1f468}',
	'\u{1f469}',
	'\u{10000}',
	'\uffff',
	'\u00e9',
];

describe('text CRDT laws over random unicode histories', () => {
	it('keeps merge commutative, associative and idempotent byte-for-byte', () => {
		for (let seed = 1; seed <= 40; seed++) {
			const next = rng(seed);
			const pick = (n: number) => Math.floor(next() * n);
			const peers = [ 'A', '\uffff', '\u{10000}' ];
			const docs = peers.map((p) => new TextDoc(p));
			for (let round = 0; round < 80; round++) {
				const r = pick(docs.length);
				const d = docs[r]!;
				const k = pick(10);
				if (k < 5) {
					let s = '';
					for (let n = 1 + pick(4); n > 0; n--) s += alphabet[pick(alphabet.length)];
					d.insert(pick(d.length + 1), s);
				} else if (k < 7 && d.length > 0) {
					const at = pick(d.length);
					d.delete(at, 1 + pick(Math.min(3, d.length - at)));
				} else if (k < 8 && d.length > 1) {
					const s = pick(d.length - 1);
					d.mark(s, s + 1 + pick(d.length - s - 1), 'k', next() < 0.5, pick(4) as Expand);
				} else {
					const j = pick(docs.length);
					if (j === r) continue;
					const ab = mergeText(encodeTextDoc(d), encodeTextDoc(docs[j]!));
					const ba = mergeText(encodeTextDoc(docs[j]!), encodeTextDoc(d));
					expect(hex(ab), `seed ${seed} round ${round}: commutative`).toBe(hex(ba));
					expect(hex(mergeText(ab, ab)), `seed ${seed}: idempotent`).toBe(hex(ab));
					docs[r] = decodeTextDoc(ab, peers[r]!);
				}
			}
			const [ a, b, c ] = docs.map((d) => encodeTextDoc(d)) as [Uint8Array, Uint8Array, Uint8Array];
			const left = mergeText(mergeText(a, b), c);
			const right = mergeText(a, mergeText(b, c));
			expect(hex(left), `seed ${seed}: associative`).toBe(hex(right));
			expect(hex(mergeText(mergeText(c, a), b)), `seed ${seed}: order-free`).toBe(hex(left));
		}
	});
});

interface DecodeCase {
	input: string;
	doc?: string;
	error?: string;
}

describe('damaged text documents shared with Go', () => {
	it('accepts and refuses exactly the inputs Go does, with the same document', () => {
		const { cases } = JSON.parse(readFileSync(testdata('damaged.json'), 'utf8')) as {
			cases: DecodeCase[];
		};
		expect(cases.length).toBeGreaterThan(0);
		const disagreements: string[] = [];
		for (const c of cases) {
			let doc: string | undefined;
			let error: string | undefined;
			try {
				doc = hex(encodeTextDoc(decodeTextDoc(Buffer.from(c.input, 'hex'), '')));
			} catch (err) {
				error = (err as Error).message;
			}
			// The messages may differ for input with several faults: each decoder
			// reports the first fault it checks, and the order differs inside a run
			// (TS validates the run's UTF-8 before its item count).
			if (doc !== c.doc) {
				disagreements.push(`${c.input}: go=${c.doc ?? c.error} ts=${doc ?? error}`);
			}
		}
		expect(disagreements).toEqual([]);
	});
});
