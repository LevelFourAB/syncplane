// Runs the shared cross-language text-CRDT conformance vectors. The Go
// harness (engine/text/vectors_test.go) executes the identical scripts; the
// encodedHex checks prove both languages produce byte-identical canonical
// encodings.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

import {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	applyFragmentBytes,
	compactDoc,
	decodeAnchor,
	decodeFragment,
	decodeTextDoc,
	diffText,
	encodeAnchor,
	encodeFragment,
	encodeTextDoc,
	type Expand,
	ExpandAfter,
	ExpandBefore,
	ExpandBoth,
	ExpandNone,
	mergeFragments,
	mergeText,
	TextDoc,
} from '../../src/text/index.js';

const vectorsPath = fileURLToPath(
	new URL('../../../testdata/text/vectors.json', import.meta.url),
);

interface VectorStep {
	op: string;
	doc: string;
	from?: string;
	peer?: string;
	index?: number;
	len?: number;
	text?: string;
	start?: number;
	end?: number;
	key?: string;
	value?: boolean | string | number;
	expand?: string;
	name?: string;
	kind?: string;
	of?: string;
	base?: string;
	as?: string;
	frag?: string;
	a?: string;
	b?: string;
	purged?: number;
	purgedMarks?: number;
}

interface VectorCheck {
	check: string;
	doc?: string;
	other?: string;
	anchor?: string;
	frag?: string;
	prev?: string;
	next?: string;
	value?: unknown;
	ops?: unknown;
	marksChanged?: boolean;
	/** Caret position for a "marksAt" check. */
	at?: number;
}

interface VectorCase {
	name: string;
	steps: VectorStep[];
	checks: VectorCheck[];
}

/**
 * One payload both decoders must refuse, with the exact message each must
 * produce. Documents the two languages disagree about are the whole failure
 * mode this section exists to prevent: a document the engine stores and no
 * browser can read looks like a client bug.
 */
interface VectorReject {
	name: string;
	/**
	 * 'doc' (default) runs the document decoder, 'anchor' decodeAnchor,
	 * 'fragment' decodeFragment, 'applyFragment' applies hex (a fragment) to
	 * docHex (a full document), 'mergeFragments' merges hex with otherHex,
	 * 'merge' runs the merger seam with docHex as the stored base and hex as
	 * the incoming side.
	 */
	kind?: string;
	hex: string;
	docHex?: string;
	otherHex?: string;
	error: string;
}

const file = JSON.parse(readFileSync(vectorsPath, 'utf8')) as {
	cases: VectorCase[];
	rejects: VectorReject[];
};

function expandOf(s: string | undefined): Expand {
	switch (s ?? 'none') {
		case 'none':
			return ExpandNone;
		case 'before':
			return ExpandBefore;
		case 'after':
			return ExpandAfter;
		case 'both':
			return ExpandBoth;
		default:
			throw new Error(`unknown expand ${s}`);
	}
}

function toHex(b: Uint8Array): string {
	return [ ...b ].map((x) => x.toString(16).padStart(2, '0')).join('');
}

function fromHex(s: string): Uint8Array {
	const b = new Uint8Array(s.length / 2);
	for (let i = 0; i < b.length; i++) b[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16);
	return b;
}

describe('shared text vectors', () => {
	for (const tc of file.cases) {
		it(tc.name, () => {
			const docs = new Map<string, TextDoc>();
			const anchors = new Map<string, Anchor>();
			const anchorDocs = new Map<string, TextDoc>();
			const frags = new Map<string, Uint8Array>();
			const frag = (name: string): Uint8Array => {
				const f = frags.get(name);
				if (!f) throw new Error(`unknown fragment ${name}`);
				return f;
			};
			const doc = (name: string): TextDoc => {
				const d = docs.get(name);
				if (!d) throw new Error(`unknown doc ${name}`);
				return d;
			};

			for (const step of tc.steps) {
				switch (step.op) {
					case 'new':
						docs.set(step.doc, new TextDoc(step.peer!));
						break;
					case 'fork':
						docs.set(step.doc, decodeTextDoc(encodeTextDoc(doc(step.from!)), step.peer ?? ''));
						break;
					case 'insert':
						doc(step.doc).insert(step.index!, step.text!);
						break;
					case 'delete':
						doc(step.doc).delete(step.index!, step.len!);
						break;
					case 'mark':
						doc(step.doc).mark(
							step.start!,
							step.end!,
							step.key!,
							step.value as boolean | string | number,
							expandOf(step.expand),
						);
						break;
					case 'unmark':
						doc(step.doc).unmark(step.start!, step.end!, step.key!, expandOf(step.expand));
						break;
					case 'merge': {
						const target = doc(step.doc);
						const other = doc(step.from!);
						const ab = mergeText(encodeTextDoc(target), encodeTextDoc(other));
						const ba = mergeText(encodeTextDoc(other), encodeTextDoc(target));
						expect(toHex(ab), 'merge must be commutative').toBe(toHex(ba));
						docs.set(step.doc, decodeTextDoc(ab, target.peer));
						break;
					}
					case 'anchor': {
						const kind = step.kind === 'after' ? AnchorAfter : AnchorBefore;
						anchors.set(step.name!, doc(step.doc).anchorAt(step.index!, kind));
						anchorDocs.set(step.name!, doc(step.doc));
						break;
					}
					case 'setString':
						doc(step.doc).setString(step.text!);
						break;
					case 'compact': {
						// "purged" and "purgedMarks" pin how much compaction reclaimed
						// of each, which is the rule both languages have to agree on
						// before the encoding can match.
						const reclaimed = compactDoc(doc(step.doc));
						expect(reclaimed.items, 'items reclaimed').toBe(step.purged);
						expect(reclaimed.marks, 'mark ops reclaimed').toBe(step.purgedMarks ?? 0);
						break;
					}
					case 'fragment': {
						const base = step.base ? doc(step.base) : null;
						const f = encodeFragment(doc(step.of!), base);
						// Fragment decode → re-encode must be stable (canonical form).
						expect(toHex(decodeFragment(f).encode()), 'fragment re-encode not stable')
							.toBe(toHex(f));
						frags.set(step.as!, f);
						break;
					}
					case 'applyFragment': {
						const target = doc(step.doc);
						const out = applyFragmentBytes(encodeTextDoc(target), frag(step.frag!));
						docs.set(step.doc, decodeTextDoc(out, target.peer));
						break;
					}
					case 'mergeFragment': {
						// Same union as applyFragment, but reached through the public
						// merger seam — this is the entry point the registry installs
						// for text fields, so it has to route delta state itself.
						const target = doc(step.doc);
						const out = mergeText(encodeTextDoc(target), frag(step.frag!));
						docs.set(step.doc, decodeTextDoc(out, target.peer));
						break;
					}
					case 'mergeFragments': {
						const ab = mergeFragments(frag(step.a!), frag(step.b!));
						const ba = mergeFragments(frag(step.b!), frag(step.a!));
						expect(toHex(ab), 'mergeFragments must be commutative').toBe(toHex(ba));
						frags.set(step.as!, ab);
						break;
					}
					default:
						throw new Error(`unknown step op ${step.op}`);
				}
			}

			for (const check of tc.checks) {
				switch (check.check) {
					case 'text':
						expect(doc(check.doc!).plainText()).toBe(check.value);
						break;
					case 'len':
						expect(doc(check.doc!).length).toBe(check.value);
						break;
					case 'vv':
						expect(Object.fromEntries(doc(check.doc!).versionVector())).toEqual(check.value);
						break;
					case 'marks':
						expect(doc(check.doc!).resolveMarks()).toEqual(check.value);
						break;
					case 'marksAt':
						expect(doc(check.doc!).marksAt(check.at!)).toEqual(check.value);
						break;
					case 'points':
						expect(doc(check.doc!).points()).toEqual(check.value);
						break;
					case 'encodedHex':
						expect(toHex(encodeTextDoc(doc(check.doc!)))).toBe(check.value);
						break;
					case 'sameEncoding':
						expect(toHex(encodeTextDoc(doc(check.doc!)))).toBe(
							toHex(encodeTextDoc(doc(check.other!))),
						);
						break;
					case 'resolve': {
						const a = anchors.get(check.anchor!)!;
						expect(anchorDocs.get(check.anchor!)!.resolveAnchor(a)).toBe(check.value);
						break;
					}
					case 'fragmentHex':
						expect(toHex(frag(check.frag!)), 'canonical fragment encoding mismatch')
							.toBe(check.value);
						break;
					case 'anchorHex': {
						const a = anchors.get(check.anchor!)!;
						const enc = encodeAnchor(a);
						expect(toHex(enc)).toBe(check.value);
						expect(decodeAnchor(enc)).toEqual(a);
						break;
					}
					case 'diff': {
						const delta = diffText(doc(check.prev!), doc(check.next!));
						expect(delta.ops).toEqual(check.ops);
						expect(delta.marksChanged).toBe(check.marksChanged);
						break;
					}
					default:
						throw new Error(`unknown check ${check.check}`);
				}
			}

			// Every doc must re-encode stably (canonical form) at the end.
			for (const [ name, d ] of docs) {
				const enc = encodeTextDoc(d);
				const rd = decodeTextDoc(enc, '');
				expect(toHex(encodeTextDoc(rd)), `doc ${name} re-encode not stable`).toBe(toHex(enc));
			}
		});
	}

	for (const rj of file.rejects) {
		it(`rejects ${rj.name}`, () => {
			const data = fromHex(rj.hex);
			let message: string | undefined;
			try {
				if (rj.kind === 'anchor') decodeAnchor(data);
				else if (rj.kind === 'fragment') decodeFragment(data);
				else if (rj.kind === 'applyFragment') {
					applyFragmentBytes(fromHex(rj.docHex ?? ''), data);
				} else if (rj.kind === 'mergeFragments') {
					mergeFragments(data, fromHex(rj.otherHex ?? ''));
				} else if (rj.kind === 'merge') {
					mergeText(fromHex(rj.docHex ?? ''), data);
				} else decodeTextDoc(data, '');
			} catch (e) {
				message = (e as Error).message;
			}
			// Exact, not a substring match: the point of the section is that
			// both decoders refuse the same bytes for the same stated reason.
			expect(message, `${rj.name} was accepted`).toBe(rj.error);
		});
	}
});
