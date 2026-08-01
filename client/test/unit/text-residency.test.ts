// Decoded documents kept between the path that materialised them and the path
// that reads them (src/text/residency.ts).
//
// The cache is addressed by CONTENT and confirmed by comparing bytes, so what
// has to be pinned is that the comparison is the only thing a caller gets to
// rely on: a document is handed back for the bytes it encodes to and for
// nothing else, and every path that uses one produces exactly what it produces
// without one. Each integration case below therefore runs twice — once with the
// cache warm, once with it cleared — and demands the same answer.

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { confirmedKey, type ConfirmedRecord, overlayTextKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { readMergedRow } from '../../src/read.js';
import {
	decodeTextDoc,
	encodeFragment,
	encodeFragmentFrom,
	encodeTextDoc,
	ExpandAfter,
	ExpandNone,
	plainTextOf,
	TextDoc,
	textMarksOf,
	textOf,
} from '../../src/text/index.js';
import { DocResidency, textResidency } from '../../src/text/residency.js';
import { TouchSet } from '../../src/touch.js';

const WS = 'ws';
const registry = new EntityRegistry([ WidgetSchema ]);

/** Long enough to be worth keeping resident — see MIN_RESIDENT_BYTES. */
const LONG = 'the quick brown fox jumps over the lazy dog. '.repeat(60);

function docOf(text: string, peer = 'p0'): TextDoc {
	const doc = new TextDoc(peer);
	doc.insert(0, text);
	return doc;
}

const bytesOf = (text: string, peer = 'p0'): Uint8Array => encodeTextDoc(docOf(text, peer));

/** A distinct instance with the same content, as a structured clone produces. */
const copy = (b: Uint8Array): Uint8Array => new Uint8Array(b);

beforeEach(() => {
	textResidency.clear();
});

describe('DocResidency', () => {
	it('answers for equal bytes and for nothing else', () => {
		const cache = new DocResidency(1 << 20);
		const doc = docOf(LONG);
		const bytes = encodeTextDoc(doc);
		cache.put(bytes, doc);

		// Equal but distinct bytes hit: identity is never what is being asked.
		expect(cache.lend(copy(bytes))).toBe(doc);
		// Lending leaves the entry in place, so a second reader gets it too.
		expect(cache.lend(copy(bytes))).toBe(doc);

		// A different document of the SAME encoded length: whether or not its
		// fingerprint collides, the comparison is what decides.
		const other = docOf(LONG.replace('quick', 'slick'));
		const otherBytes = encodeTextDoc(other);
		expect(otherBytes.length).toBe(bytes.length);
		expect(cache.lend(otherBytes)).toBeUndefined();
	});

	it('hands over ownership on take, so nothing can alias a document being merged', () => {
		const cache = new DocResidency(1 << 20);
		const doc = docOf(LONG);
		const bytes = encodeTextDoc(doc);
		cache.put(bytes, doc);

		expect(cache.take(copy(bytes))).toBe(doc);
		// Gone: the taker is free to mutate it, so no reader may still find it
		// registered under the bytes it no longer encodes to.
		expect(cache.take(copy(bytes))).toBeUndefined();
		expect(cache.lend(copy(bytes))).toBeUndefined();

		// What it became goes back under its new address.
		doc.insert(0, 'more ');
		const next = encodeTextDoc(doc);
		cache.put(next, doc);
		expect(cache.lend(copy(next))).toBe(doc);
		expect(cache.lend(copy(bytes))).toBeUndefined();
	});

	it('keeps nothing for documents too small to be worth a decode', () => {
		const cache = new DocResidency(1 << 20);
		const doc = docOf('short');
		const bytes = encodeTextDoc(doc);
		cache.put(bytes, doc);
		expect(cache.count).toBe(0);
		expect(cache.lend(bytes)).toBeUndefined();
	});

	it('evicts least recently used entries to stay inside its budget', () => {
		const texts = [ 'a', 'b', 'c' ].map((c) => LONG + c);
		const encoded = texts.map((t) => bytesOf(t));
		// A budget with room for two of the three.
		const cache = new DocResidency(encoded[0]!.length * 2 + 10);
		for (const [ i, t ] of texts.entries()) cache.put(encoded[i]!, docOf(t));

		expect(cache.lend(encoded[0]!)).toBeUndefined();
		expect(cache.lend(encoded[1]!)).toBeDefined();
		expect(cache.lend(encoded[2]!)).toBeDefined();

		// Reading the second one refreshes it, so a fourth entry evicts the third.
		cache.lend(encoded[1]!);
		const fourth = LONG + 'd';
		cache.put(bytesOf(fourth), docOf(fourth));
		expect(cache.lend(encoded[1]!)).toBeDefined();
		expect(cache.lend(encoded[2]!)).toBeUndefined();
	});

	it('declines a document larger than the whole budget rather than thrashing', () => {
		const cache = new DocResidency(2048);
		const big = docOf(LONG.repeat(4));
		const bytes = encodeTextDoc(big);
		expect(bytes.length).toBeGreaterThan(2048);
		cache.put(bytes, big);
		expect(cache.count).toBe(0);
	});
});

describe('projections over a resident document', () => {
	it('reads the same text and marks whether or not one is resident', () => {
		const doc = docOf(LONG);
		doc.mark(10, 40, 'bold', true, ExpandAfter);
		doc.mark(100, 140, 'comment:c1', 'a note', ExpandNone);
		const bytes = encodeTextDoc(doc);

		const cold = { text: textOf(copy(bytes)), marks: textMarksOf(copy(bytes)) };

		textResidency.put(bytes, decodeTextDoc(bytes, ''));
		const warm = { text: textOf(copy(bytes)), marks: textMarksOf(copy(bytes)) };

		expect(warm.text).toBe(cold.text);
		expect(warm.marks).toEqual(cold.marks);
		expect(warm.marks.length).toBeGreaterThan(0);
		expect(plainTextOf(copy(bytes))).toBe(cold.text);
	});

	it('registers what it decoded, so the next holder of the same bytes reuses it', () => {
		const bytes = bytesOf(LONG);
		expect(textResidency.count).toBe(0);
		textOf(copy(bytes));
		// A second scan scope, a query watcher, a react binding: a different
		// instance of the same value, which the byte-instance memo cannot serve.
		const before = textResidency.stats.hits;
		expect(textOf(copy(bytes))).toBe(LONG);
		expect(textResidency.stats.hits).toBe(before + 1);
	});

	it('never serves a document for bytes it does not encode to', () => {
		const bytes = bytesOf(LONG);
		const other = bytesOf(LONG.replace('quick', 'slick'));
		textResidency.put(bytes, decodeTextDoc(bytes, ''));
		expect(textOf(other)).toBe(LONG.replace('quick', 'slick'));
	});
});

/** One edit's resulting state and the fragment standing for it. */
function typeInto(state: Uint8Array, peer: string, index: number, s: string) {
	const d = decodeTextDoc(state, peer);
	const range = d.insert(index, s);
	const ids = Array.from(
		{ length: range.count },
		(_, i) => ({ peer: range.peer, counter: range.start + i }),
	);
	return { state: encodeTextDoc(d), fragment: encodeFragmentFrom(d, ids, []) };
}

const widgetRow = (body: Uint8Array | undefined, label = ''): Uint8Array =>
	toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', label, body }));

const logKey = (id: number) => overlayTextKey('widget', WS, 'w1', 'body', BigInt(id), 0);

describe('the optimistic fold over a resident document', () => {
	/** Types `count` keystrokes into a row's log and returns the merged row. */
	async function typedRow(count: number, reg: EntityRegistry): Promise<Uint8Array> {
		const kv = new MemoryKVStore();
		let state = bytesOf(LONG);
		const tx = await kv.write();
		await tx.put(
			confirmedKey('widget', WS, 'w1'),
			{ v: widgetRow(state), ver: '1' } satisfies ConfirmedRecord,
		);
		await tx.commit();

		let merged: Uint8Array | undefined;
		for (let i = 1; i <= count; i++) {
			const edit = typeInto(state, 'p1', i - 1, '!');
			state = edit.state;
			const wtx = await kv.write();
			await wtx.put(logKey(i), edit.fragment);
			await wtx.commit();
			// Read between keystrokes, which is what the row watch does and what
			// leaves a document resident for the next fold to extend.
			const rtx = await kv.read();
			merged = (await readMergedRow(rtx, reg, 'widget', WS, 'w1'))!;
			rtx.release();
		}
		return merged!;
	}

	it('folds a burst to the same row it folds with nothing resident', async () => {
		const warm = await typedRow(8, registry);
		textResidency.clear();
		const cold = await typedRow(8, new EntityRegistry([ WidgetSchema ]));
		expect(warm).toEqual(cold);
		expect(textOf(fromBinary(WidgetSchema, warm).body)).toBe('!!!!!!!!' + LONG);
	});

	it('extends the resident document instead of decoding the row again', async () => {
		await typedRow(4, registry);
		// Every fold after the first took the document the one before it left.
		expect(textResidency.stats.hits).toBeGreaterThanOrEqual(3);
	});
});

describe('arriving deltas over a resident document', () => {
	function deltaChange(fragment: Uint8Array, version: bigint): RowChange {
		return create(RowChangeSchema, {
			entity: 'widget',
			key: 'w1',
			version,
			body: { case: 'value', value: widgetRow(undefined) },
			textDeltas: [ { field: 'body', fragment } ],
		});
	}

	/** A peer typing `count` keystrokes at this client, one page each. */
	async function received(count: number): Promise<Uint8Array> {
		const kv = new MemoryKVStore();
		const theirs = docOf(LONG, 'remote');
		const tx = await kv.write();
		await tx.put(
			confirmedKey('widget', WS, 'w1'),
			{ v: widgetRow(encodeTextDoc(theirs)), ver: '1' } satisfies ConfirmedRecord,
		);
		await tx.commit();

		for (let i = 1; i <= count; i++) {
			const before = encodeTextDoc(theirs);
			theirs.insert(0, '>');
			const fragment = encodeFragment(theirs, decodeTextDoc(before, ''));
			const wtx = await kv.write();
			await applyServerChanges(
				wtx,
				registry,
				WS,
				[ deltaChange(fragment, BigInt(i + 1)) ],
				new TouchSet(),
			);
			await wtx.commit();
		}

		const rtx = await kv.read();
		const record = (await rtx.get(confirmedKey('widget', WS, 'w1'))) as ConfirmedRecord;
		rtx.release();
		return fromBinary(WidgetSchema, record.v).body!;
	}

	it('reaches the same confirmed bytes with nothing resident', async () => {
		const warm = await received(5);
		textResidency.clear();
		const cold = await received(5);
		expect(warm).toEqual(cold);
		expect(textOf(warm)).toBe('>>>>>' + LONG);
	});

	it('picks the confirmed document back up rather than decoding it per page', async () => {
		await received(4);
		expect(textResidency.stats.hits).toBeGreaterThanOrEqual(3);
	});
});
