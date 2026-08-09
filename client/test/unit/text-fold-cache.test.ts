import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { confirmedKey, type ConfirmedRecord, overlayTextKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead, KVWrite } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { readMergedRow, scanMergedRows } from '../../src/read.js';
import { decodeTextDoc, encodeFragmentFrom, encodeTextDoc, TextDoc } from '../../src/text/index.js';
import { TextFoldCache } from '../../src/textfold.js';

/**
 * The read path's fold cache (src/textfold.ts).
 *
 * A fold is a pure function of the row bytes it starts from and the fragments
 * it applies, and the cache exploits exactly that: it never asks whether
 * anything was invalidated, it compares. So what has to be pinned is that the
 * comparison is the ONLY thing standing between a reader and a stale document —
 * every case below changes something under a warm entry and demands the answer
 * a cold read would have given.
 *
 * Every case therefore reads twice: once through `registry`, whose cache is
 * shared across this file and warm by the time it matters, and once through a
 * throwaway registry, which is how a cold fold is reached (the cache is keyed
 * by registry, weakly, so a fresh one has none).
 */

const WS = 'ws';
const registry = new EntityRegistry([ WidgetSchema ]);

/** A read view handing back fresh copies, as a structured clone from IndexedDB
 * does — so an identity check can never stand in for the byte comparison. */
function cloningRead(tx: KVRead): KVRead {
	const copy = (v: unknown): unknown => {
		if (v instanceof Uint8Array) return new Uint8Array(v);
		if (v !== null && typeof v === 'object') {
			return Object.fromEntries(Object.entries(v).map(([ k, x ]) => [ k, copy(x) ]));
		}
		return v;
	};
	return {
		get: async (key) => copy(await tx.get(key)),
		getMany: async (keys) => (await tx.getMany(keys)).map(copy),
		has: (key) => tx.has(key),
		async *scan(prefix) {
			for await (const [ k, v ] of tx.scan(prefix)) yield [ k, copy(v) ];
		},
		indexScan: (lo, hi) => tx.indexScan(lo, hi),
		indexCount: (lo, hi) => tx.indexCount(lo, hi),
		async *scanRange(lo, hi) {
			for await (const [ k, v ] of tx.scanRange(lo, hi)) yield [ k, copy(v) ];
		},
		release: () => tx.release(),
	};
}

function docOf(s: string): Uint8Array {
	const d = new TextDoc('p0');
	d.insert(0, s);
	return encodeTextDoc(d);
}

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

async function putRow(tx: KVWrite, body: Uint8Array, label = ''): Promise<void> {
	const rec: ConfirmedRecord = {
		v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', label, body })),
		ver: '1',
	};
	await tx.put(confirmedKey('widget', WS, 'w1'), rec);
}

const logKey = (id: number) => overlayTextKey('widget', WS, 'w1', 'body', BigInt(id), 0);

/** The merged row, read through a registry whose cache may be warm. */
async function read(kv: MemoryKVStore, reg = registry) {
	const tx = cloningRead(await kv.read());
	const bytes = await readMergedRow(tx, reg, 'widget', WS, 'w1');
	tx.release();
	if (bytes === undefined) throw new Error('row is gone');
	const row = fromBinary(WidgetSchema, bytes);
	return { bytes, label: row.label, text: decodeTextDoc(row.body, '').plainText() };
}

/** The same row, folded with no cache to help — the answer everything else must match. */
const readCold = (kv: MemoryKVStore) => read(kv, new EntityRegistry([ WidgetSchema ]));

describe('text fold cache', () => {
	it('extends a warm fold with an appended fragment', async () => {
		const kv = new MemoryKVStore();
		const base = docOf('hello');
		let state = base;
		const tx = await kv.write();
		await putRow(tx, base);
		// Nine keystrokes, each naming items the one before it minted — so the
		// tenth can only be applied on top of a document that already holds them.
		for (let i = 1; i <= 9; i++) {
			const edit = typeInto(state, 'p1', i + 4, String(i));
			state = edit.state;
			await tx.put(logKey(i), edit.fragment);
		}
		await tx.commit();

		expect((await read(kv)).text).toBe('hello123456789');

		// The tenth lands on a log the cache has already folded: the entry's
		// fragments are a prefix of what is now there, so only the tail is applied.
		const wtx = await kv.write();
		await wtx.put(logKey(10), typeInto(state, 'p1', 14, '!').fragment);
		await wtx.commit();

		expect((await read(kv)).text).toBe('hello123456789!');
		expect((await read(kv)).bytes).toEqual((await readCold(kv)).bytes);
	});

	it('does not serve a fold whose row moved under it', async () => {
		const kv = new MemoryKVStore();
		const base = docOf('hi');
		const tx = await kv.write();
		await putRow(tx, base);
		await tx.put(logKey(1), typeInto(base, 'p1', 2, '!').fragment);
		await tx.commit();

		expect(await read(kv)).toMatchObject({ label: '', text: 'hi!' });

		// A PATCH to a scalar field of the same row leaves the log alone and
		// rewrites the row — the one case where the fragments still compare equal
		// and the answer has changed anyway.
		const wtx = await kv.write();
		await putRow(wtx, base, 'renamed');
		await wtx.commit();

		expect(await read(kv)).toMatchObject({ label: 'renamed', text: 'hi!' });
		expect((await read(kv)).bytes).toEqual((await readCold(kv)).bytes);
	});

	it('rebuilds when the log lost a fragment rather than gained one', async () => {
		const kv = new MemoryKVStore();
		const base = docOf('x');
		const tx = await kv.write();
		await putRow(tx, base);
		// Two independent edits — each cut against the same base by a different
		// peer — so either survives the other being dropped.
		await tx.put(logKey(1), typeInto(base, 'p1', 0, 'a').fragment);
		await tx.put(logKey(2), typeInto(base, 'p2', 0, 'b').fragment);
		await tx.commit();

		const both = (await read(kv)).text;
		expect(both).toHaveLength(3);

		// A rejected mutation is dropped from the queue, and replay rebuilds the
		// log without it. The warm entry's fragments are then no longer a prefix
		// of what is there, which is the case a naive append would get wrong.
		const wtx = await kv.write();
		await wtx.del(logKey(1));
		await wtx.commit();

		expect((await read(kv)).text).toBe('bx');
		expect((await read(kv)).bytes).toEqual((await readCold(kv)).bytes);
	});

	it('keeps a fragment it cannot apply from poisoning later folds', async () => {
		const kv = new MemoryKVStore();
		const base = docOf('x');
		const good = typeInto(base, 'p1', 0, 'a');
		// Cut against `good`'s state but logged without it: its items name parents
		// the base does not have, which is what a mutation rejected out from under
		// its successors leaves behind.
		const orphan = typeInto(good.state, 'p1', 0, 'b');
		const tx = await kv.write();
		await putRow(tx, base);
		await tx.put(logKey(1), orphan.fragment);
		await tx.commit();

		const warn = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			expect((await read(kv)).text).toBe('x');

			// The next edit still has to land, on the document the orphan did not change.
			const wtx = await kv.write();
			await wtx.put(logKey(2), good.fragment);
			await wtx.commit();

			expect((await read(kv)).text).toBe('ax');
			expect((await read(kv)).bytes).toEqual((await readCold(kv)).bytes);
		} finally {
			warn.mockRestore();
		}
	});

	it('resolves a scan the same way as a point read', async () => {
		const kv = new MemoryKVStore();
		const base = docOf('hi');
		const tx = await kv.write();
		await putRow(tx, base);
		await tx.put(logKey(1), typeInto(base, 'p1', 2, '!').fragment);
		await tx.commit();

		// Both paths fold the same row, so they share a cache entry; a scan must
		// neither be served the wrong one nor leave one a point read then trusts.
		const point = await read(kv);
		const stx = cloningRead(await kv.read());
		const rows = await scanMergedRows(stx, registry, 'widget', WS);
		stx.release();
		expect(rows).toHaveLength(1);
		expect(rows[0]!.v).toEqual(point.bytes);
		expect((await read(kv)).bytes).toEqual(point.bytes);
	});
});

describe('TextFoldCache', () => {
	const entry = (base: Uint8Array, result: Uint8Array) => ({
		base,
		result,
		frags: new Map<string, readonly Uint8Array[]>(),
		fields: new Map<string, Uint8Array>(),
	});

	it('returns an entry only for the bytes it was folded from', () => {
		const cache = new TextFoldCache(1024);
		cache.put('row', entry(new Uint8Array([ 1, 2, 3 ]), new Uint8Array([ 9 ])));

		// Equal but distinct bytes are a hit — a structured clone is all that
		// stands between the stored row and this one.
		expect(cache.take('row', new Uint8Array([ 1, 2, 3 ]))?.result).toEqual(new Uint8Array([ 9 ]));
		// And taking it removed it, so nothing can alias a document being extended.
		expect(cache.take('row', new Uint8Array([ 1, 2, 3 ]))).toBeUndefined();

		cache.put('row', entry(new Uint8Array([ 1, 2, 3 ]), new Uint8Array([ 9 ])));
		expect(cache.take('row', new Uint8Array([ 1, 2, 4 ]))).toBeUndefined();
		// A mismatch drops the entry too: that row moved on without coming through here.
		expect(cache.take('row', new Uint8Array([ 1, 2, 3 ]))).toBeUndefined();
	});

	it('evicts least recently used entries to stay inside its budget', () => {
		const cache = new TextFoldCache(40);
		const bytes = (fill: number) => new Uint8Array(10).fill(fill);
		for (const [ i, name ] of [ 'a', 'b', 'c' ].entries()) {
			cache.put(name, entry(bytes(i), bytes(i)));
		}
		// 3 × 20 bytes over a 40-byte budget: the oldest is gone, the rest are not.
		expect(cache.take('a', bytes(0))).toBeUndefined();
		expect(cache.take('b', bytes(1))).toBeDefined();
		expect(cache.take('c', bytes(2))).toBeDefined();
	});

	it('declines a row larger than the whole budget rather than thrashing', () => {
		const cache = new TextFoldCache(8);
		const small = new Uint8Array(2);
		cache.put('small', entry(small, small));
		const big = new Uint8Array(64);
		cache.put('big', entry(big, big));
		expect(cache.take('big', big)).toBeUndefined();
		expect(cache.take('small', small)).toBeDefined();
	});
});
