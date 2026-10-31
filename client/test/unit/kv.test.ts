import 'fake-indexeddb/auto';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { IDBKVStore } from '../../src/kv/idb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import {
	type GenerationTracking,
	type IndexEntry,
	type KVStore,
	tracksGenerations,
} from '../../src/kv/store.js';

let dbCounter = 0;

const implementations: [string, () => Promise<KVStore>][] = [
	[ 'memory', async () => new MemoryKVStore() ],
	[ 'idb (fake-indexeddb)', () => IDBKVStore.open(`kv-test-${dbCounter++}`) ],
];

describe.each(implementations)('KVStore: %s', (_name, open) => {
	let store: KVStore | undefined;

	afterEach(async () => {
		await store?.close();
		store = undefined;
	});

	it('round-trips values', async () => {
		store = await open();
		const w = await store.write();
		await w.put('a', 1);
		await w.put('b', { nested: true });
		await w.commit();

		const r = await store.read();
		expect(await r.get('a')).toBe(1);
		expect(await r.get('b')).toEqual({ nested: true });
		expect(await r.get('missing')).toBeUndefined();
		expect(await r.has('a')).toBe(true);
		expect(await r.has('missing')).toBe(false);
		r.release();
	});

	it('reads a batch of keys, parallel to the request', async () => {
		store = await open();
		const w = await store.write();
		await w.put('a', 1);
		await w.put('b', 2);
		await w.commit();

		const r = await store.read();
		// Order and arity follow the request, not the store: missing keys hold
		// their slot and a duplicate is answered twice.
		expect(await r.getMany([ 'b', 'missing', 'a', 'b' ])).toEqual([ 2, undefined, 1, 2 ]);
		expect(await r.getMany([])).toEqual([]);
		r.release();
	});

	it('a write transaction’s getMany sees its own buffered writes', async () => {
		store = await open();
		const setup = await store.write();
		await setup.put('k', 'old');
		await setup.put('gone', 'x');
		await setup.commit();

		const w = await store.write();
		await w.put('k', 'new');
		await w.put('fresh', 'v');
		await w.del('gone');
		expect(await w.getMany([ 'k', 'gone', 'fresh', 'missing' ]))
			.toEqual([ 'new', undefined, 'v', undefined ]);
		w.release();
	});

	it('stores binary values', async () => {
		store = await open();
		const w = await store.write();
		await w.put('bin', new Uint8Array([ 1, 2, 3 ]));
		await w.commit();

		const r = await store.read();
		expect(new Uint8Array((await r.get('bin')) as Uint8Array)).toEqual(new Uint8Array([ 1, 2, 3 ]));
		r.release();
	});

	it('scans by prefix in key order', async () => {
		store = await open();
		const w = await store.write();
		await w.put('p/2', 'two');
		await w.put('p/1', 'one');
		await w.put('q/1', 'other');
		await w.commit();

		const r = await store.read();
		const seen: [string, unknown][] = [];
		for await (const entry of r.scan('p/')) seen.push(entry);
		expect(seen).toEqual([
			[ 'p/1', 'one' ],
			[ 'p/2', 'two' ],
		]);
		r.release();
	});

	// Row keys carry unvalidated user data, so a scan has to bound the prefix
	// the same way `startsWith` does — including for keys around U+FFFF, where
	// an inclusive `prefix + '\uffff'` upper bound loses rows in IDB only.
	// Index probes are the caller: they bracket an encoded range and read what
	// falls inside it, so the two backends have to agree on the bounds down to
	// the degenerate cases. IDB is the one that can get this wrong on its own —
	// it has no way to express an EMPTY key range, and its "no range" value is
	// the one that reads the whole store.
	it('scans a half-open range, both bounds optional', async () => {
		store = await open();
		const w = await store.write();
		for (const k of [ 'a', 'b', 'c', 'd' ]) await w.put(k, k);
		await w.commit();

		const r = await store.read();
		const keysIn = async (lo?: string, hi?: string) => {
			const out: string[] = [];
			for await (const [ key ] of r.scanRange(lo, hi)) out.push(key);
			return out;
		};
		expect(await keysIn('b', 'd')).toEqual([ 'b', 'c' ]);
		expect(await keysIn('b')).toEqual([ 'b', 'c', 'd' ]);
		expect(await keysIn(undefined, 'c')).toEqual([ 'a', 'b' ]);
		expect(await keysIn()).toEqual([ 'a', 'b', 'c', 'd' ]);
		// A collapsed range selects NOTHING. It reaches here as ordinary input —
		// a probe whose bounds met — and answering it with the whole store is
		// the failure mode worth pinning.
		expect(await keysIn('c', 'c')).toEqual([]);
		expect(await keysIn('d', 'b')).toEqual([]);
		r.release();
	});

	it('seeks the least key of a half-open range, both bounds optional', async () => {
		store = await open();
		const w = await store.write();
		for (const k of [ 'a', 'b', 'c', 'd' ]) await w.put(k, k);
		await w.commit();

		const r = await store.read();
		expect(await r.seek('b', 'd')).toBe('b');
		expect(await r.seek('bb', 'd')).toBe('c');
		expect(await r.seek('b')).toBe('b');
		expect(await r.seek(undefined, 'c')).toBe('a');
		expect(await r.seek()).toBe('a');
		expect(await r.seek('e')).toBeUndefined();
		expect(await r.seek('c', 'c')).toBeUndefined();
		expect(await r.seek('d', 'b')).toBeUndefined();
		r.release();
	});

	it('a write transaction’s seek sees its own buffered writes', async () => {
		store = await open();
		const setup = await store.write();
		for (const k of [ 'b', 'c', 'd' ]) await setup.put(k, k);
		await setup.commit();

		const w = await store.write();
		// A buffered put below every stored key, and a run of buffered deletes
		// the stored answer has to step past.
		await w.put('a', 'a');
		expect(await w.seek()).toBe('a');
		expect(await w.seek('b')).toBe('b');
		await w.del('b');
		await w.del('c');
		expect(await w.seek('b')).toBe('d');
		await w.del('d');
		expect(await w.seek('b')).toBeUndefined();
		await w.put('bb', 'bb');
		expect(await w.seek('b')).toBe('bb');
		w.release();
	});

	it('a write transaction’s seek steps past a deleted key onto keys it prefixes', async () => {
		store = await open();
		const setup = await store.write();
		for (const k of [ 'a/b', 'a/b0', 'a/b\u0000', 'a/c' ]) await setup.put(k, k);
		await setup.commit();

		const w = await store.write();
		// The keys after a deleted one include its own extensions, which a
		// resume past the deleted PREFIX would skip.
		await w.del('a/b');
		expect(await w.seek('a/')).toBe('a/b\u0000');
		await w.del('a/b\u0000');
		expect(await w.seek('a/')).toBe('a/b0');
		await w.del('a/b0');
		expect(await w.seek('a/')).toBe('a/c');
		w.release();
	});

	it('scans keys containing U+FFFF', async () => {
		store = await open();
		const w = await store.write();
		await w.put('p/\uffff', 'sentinel');
		await w.put('p/\uffffx', 'past the sentinel');
		await w.put('p/a', 'plain');
		await w.put('q/1', 'other');
		await w.commit();

		const r = await store.read();
		const seen: string[] = [];
		for await (const [ key ] of r.scan('p/')) seen.push(key);
		expect(seen).toEqual([ 'p/a', 'p/\uffff', 'p/\uffffx' ]);

		// A prefix that itself ends in U+FFFF has to carry into the next key.
		const nested: string[] = [];
		for await (const [ key ] of r.scan('p/\uffff')) nested.push(key);
		expect(nested).toEqual([ 'p/\uffff', 'p/\uffffx' ]);
		r.release();
	});

	// All-U+FFFF prefixes have no successor to bound the scan from above.
	it('scans a prefix of only U+FFFF', async () => {
		store = await open();
		const w = await store.write();
		await w.put('\uffff\uffff', 'sentinels');
		await w.put('\uffff\uffffz', 'past them');
		await w.put('\uffff', 'shorter');
		await w.put('a', 'plain');
		await w.commit();

		const r = await store.read();
		const seen: string[] = [];
		for await (const [ key ] of r.scan('\uffff\uffff')) seen.push(key);
		expect(seen).toEqual([ '\uffff\uffff', '\uffff\uffffz' ]);
		r.release();
	});

	// Scans continue past one request in the IDB backend (see SCAN_CHUNK), and
	// the continuation — an exclusive lower bound at the last key yielded — is
	// the one place it could drop or repeat an entry.
	it('scans a range longer than one request', async () => {
		store = await open();
		const w = await store.write();
		const total = 600;
		for (let i = 0; i < total; i++) {
			await w.put(`p/${String(i).padStart(4, '0')}`, i);
		}
		await w.put('q/after', 'other');
		await w.commit();

		const r = await store.read();
		const seen: string[] = [];
		for await (const [ key ] of r.scan('p/')) seen.push(key);
		expect(seen).toHaveLength(total);
		expect(new Set(seen).size).toBe(total);
		expect(seen[0]).toBe('p/0000');
		expect(seen[total - 1]).toBe(`p/${String(total - 1).padStart(4, '0')}`);
		expect(seen).toEqual([ ...seen ].sort());
		r.release();
	});

	// The same continuation, read through a write transaction whose buffered
	// writes land before, inside, on top of, and past the committed range — the
	// shapes a streaming merge with the committed scan could misorder or lose.
	it('merges buffered writes into a scan longer than one request', async () => {
		store = await open();
		const setup = await store.write();
		const total = 600;
		for (let i = 0; i < total; i++) {
			await setup.put(`p/${String(2 * i + 1).padStart(4, '0')}`, 'committed');
		}
		await setup.commit();

		const w = await store.write();
		await w.put('p/0000', 'before the first committed key');
		await w.put('p/0600', 'between committed keys');
		await w.put('p/0333', 'overwrites a committed key');
		await w.put('p/9999', 'past the last committed key');
		await w.del('p/0335');
		await w.del('p/absent');
		const seen = new Map<string, unknown>();
		for await (const [ key, value ] of w.scan('p/')) {
			expect(seen.has(key)).toBe(false);
			seen.set(key, value);
		}
		w.release();

		const keys = [ ...seen.keys() ];
		expect(keys).toEqual([ ...keys ].sort());
		expect(seen.size).toBe(total + 3 - 1);
		expect(seen.get('p/0000')).toBe('before the first committed key');
		expect(seen.get('p/0600')).toBe('between committed keys');
		expect(seen.get('p/0333')).toBe('overwrites a committed key');
		expect(seen.get('p/9999')).toBe('past the last committed key');
		expect(seen.has('p/0335')).toBe(false);
		expect(seen.get('p/0001')).toBe('committed');
	});

	it('write transactions see their own buffered writes', async () => {
		store = await open();
		const setup = await store.write();
		await setup.put('k', 'old');
		await setup.put('gone', 'x');
		await setup.commit();

		const w = await store.write();
		await w.put('k', 'new');
		await w.del('gone');
		expect(await w.get('k')).toBe('new');
		expect(await w.has('gone')).toBe(false);
		const scanned: string[] = [];
		for await (const [ key ] of w.scan('')) scanned.push(key);
		expect(scanned).toEqual([ 'k' ]);
		await w.commit();
	});

	it('discards writes on release', async () => {
		store = await open();
		const w = await store.write();
		await w.put('k', 'v');
		w.release();

		const r = await store.read();
		expect(await r.get('k')).toBeUndefined();
		r.release();
	});

	// The store's one secondary index. Both backends have to agree on it down to
	// the ordering, because it is what every index-served query reads and only
	// one of them is exercised by the unit suite — see kv/store.ts INDEX_FIELD.
	describe('the secondary index', () => {
		/** Rows carrying positions, plus records that must contribute none. */
		async function seedPositions(store: KVStore): Promise<void> {
			const w = await store.write();
			await w.put('c/x', { v: 'x', ix: [ 'i/a/1', 'i/a/3' ] });
			await w.put('c/y', { v: 'y', ix: [ 'i/a/2' ] });
			await w.put('c/z', { v: 'z', ix: [ 'i/b/1' ] });
			// No field, an empty list, and values that are not objects at all:
			// everything a store holds that is not an indexed row, which is most of
			// it. None of them may reach an index read.
			await w.put('c/none', { v: 'none' });
			await w.put('c/empty', { v: 'empty', ix: [] });
			await w.put('meta/thing', { some: 'record' });
			await w.put('meta/count', 7);
			await w.put('p/1', new Uint8Array([ 1 ]));
			await w.commit();
		}

		const positions = async (
			store: KVStore,
			lo?: string,
			hi?: string,
			direction?: 'asc' | 'desc',
		): Promise<string[]> => {
			const r = await store.read();
			try {
				const out: string[] = [];
				for await (const key of r.indexScan(lo, hi, direction)) out.push(key);
				return out;
			} finally {
				r.release();
			}
		};

		it('yields record keys in position order, one per position', async () => {
			store = await open();
			await seedPositions(store);
			// x holds two positions in this range and is yielded at each, between
			// them y at its own — so the order is the POSITIONS' and not the rows'.
			expect(await positions(store, 'i/a/', 'i/a0')).toEqual([ 'c/x', 'c/y', 'c/x' ]);
			expect(await positions(store, 'i/b/', 'i/b0')).toEqual([ 'c/z' ]);
		});

		it('bounds the range half-open, both ends optional', async () => {
			store = await open();
			await seedPositions(store);
			expect(await positions(store, 'i/a/2', 'i/a/3')).toEqual([ 'c/y' ]);
			expect(await positions(store, 'i/a/2')).toEqual([ 'c/y', 'c/x', 'c/z' ]);
			expect(await positions(store, undefined, 'i/a/2')).toEqual([ 'c/x' ]);
			expect(await positions(store)).toEqual([ 'c/x', 'c/y', 'c/x', 'c/z' ]);
			// A collapsed range selects nothing, exactly as scanRange's does.
			expect(await positions(store, 'i/a/2', 'i/a/2')).toEqual([]);
			expect(await positions(store, 'i/b', 'i/a')).toEqual([]);
		});

		it('yields the exact reverse under desc', async () => {
			store = await open();
			await seedPositions(store);
			expect(await positions(store, 'i/a/2', 'i/a/3', 'desc')).toEqual([ 'c/y' ]);
			expect(await positions(store, 'i/a/2', undefined, 'desc')).toEqual([ 'c/z', 'c/x', 'c/y' ]);
			expect(await positions(store, undefined, 'i/a/2', 'desc')).toEqual([ 'c/x' ]);
			expect(await positions(store, undefined, undefined, 'desc')).toEqual([
				'c/z',
				'c/x',
				'c/y',
				'c/x',
			]);
			expect(await positions(store, 'i/a/2', 'i/a/2', 'desc')).toEqual([]);
			expect(await positions(store, 'i/b', 'i/a', 'desc')).toEqual([]);
		});

		it('drops a record’s positions when the record is rewritten or deleted', async () => {
			store = await open();
			await seedPositions(store);
			const w = await store.write();
			// The layout's central claim: positions are part of the record, so
			// rewriting one without them unindexes the row and deleting one takes
			// its positions along. Neither costs a write of its own.
			await w.put('c/x', { v: 'x' });
			await w.del('c/y');
			await w.commit();
			expect(await positions(store, 'i/', 'i0')).toEqual([ 'c/z' ]);
		});

		it('sees a write transaction’s own buffered positions', async () => {
			store = await open();
			await seedPositions(store);
			const w = await store.write();
			await w.put('c/w', { v: 'w', ix: [ 'i/a/0' ] });
			await w.put('c/x', { v: 'x', ix: [ 'i/a/9' ] });
			await w.del('c/y');
			const seen: string[] = [];
			for await (const key of w.indexScan('i/a/', 'i/a0')) seen.push(key);
			// c/w is new, c/x moved to the end of the range and left both its old
			// positions, c/y is gone. A backend reading only committed state would
			// answer the seed's [x, y, x] instead.
			expect(seen).toEqual([ 'c/w', 'c/x' ]);
			const backward: string[] = [];
			for await (const key of w.indexScan('i/a/', 'i/a0', 'desc')) backward.push(key);
			expect(backward).toEqual([ 'c/x', 'c/w' ]);
			await w.commit();
		});

		it('counts positions the way indexScan yields them', async () => {
			store = await open();
			await seedPositions(store);
			const r = await store.read();
			try {
				// Per POSITION, not per record: x holds two in the first range.
				expect(await r.indexCount('i/a/', 'i/a0')).toBe(3);
				expect(await r.indexCount('i/a/2', 'i/a/3')).toBe(1);
				expect(await r.indexCount('i/a/2')).toBe(3);
				expect(await r.indexCount(undefined, 'i/a/2')).toBe(1);
				expect(await r.indexCount()).toBe(4);
				expect(await r.indexCount('i/a/2', 'i/a/2')).toBe(0);
				expect(await r.indexCount('i/b', 'i/a')).toBe(0);
			} finally {
				r.release();
			}
		});

		// A repeated element of an indexed field encodes to the same position
		// twice. IDB's multiEntry index keeps one entry per distinct element, so
		// the other backend has to collapse the repeat too — for the scan and for
		// the count, which reads the range's size as its row count when nothing
		// fans out past the equality prefix.
		it('a position listed twice is one position', async () => {
			store = await open();
			const w = await store.write();
			await w.put('c/dup', { v: 'dup', ix: [ 'i/a/1', 'i/a/1', 'i/a/2' ] });
			await w.commit();
			expect(await positions(store, 'i/a/', 'i/a0')).toEqual([ 'c/dup', 'c/dup' ]);
			const r = await store.read();
			try {
				expect(await r.indexCount('i/a/', 'i/a0')).toBe(2);
			} finally {
				r.release();
			}
		});

		it('a write transaction’s indexCount sees its own buffered positions', async () => {
			store = await open();
			await seedPositions(store);
			const w = await store.write();
			await w.put('c/w', { v: 'w', ix: [ 'i/a/0' ] });
			await w.put('c/x', { v: 'x', ix: [ 'i/a/9' ] });
			await w.del('c/y');
			// The seed's range held [x, y, x]; the buffer added w, collapsed x to
			// one position and removed y — a count of committed state says 3.
			expect(await w.indexCount('i/a/', 'i/a0')).toBe(2);
			w.release();
		});

		// The read path pulls a page out of a range far wider than it, and stops.
		// A backend that materialized the whole range first would pass every
		// assertion above and still be the thing issues/117 set out to remove.
		it('stops when the caller does', async () => {
			store = await open();
			const w = await store.write();
			for (let i = 0; i < 500; i++) {
				await w.put(`c/${String(i).padStart(4, '0')}`, {
					ix: [ `i/a/${String(i).padStart(4, '0')}` ],
				});
			}
			await w.commit();

			const r = await store.read();
			const seen: string[] = [];
			for await (const key of r.indexScan('i/a/')) {
				seen.push(key);
				if (seen.length === 3) break;
			}
			r.release();
			expect(seen).toEqual([ 'c/0000', 'c/0001', 'c/0002' ]);
		});

		indexScanRowsCases(open);

		// Past INDEX_SCAN_CHUNK the IDB backend re-requests a larger prefix and
		// skips what it already yielded, which is the one place it could double
		// up or drop a position.
		it('yields every position of a range longer than one request', async () => {
			store = await open();
			const w = await store.write();
			const total = 300;
			for (let i = 0; i < total; i++) {
				await w.put(`c/${String(i).padStart(4, '0')}`, {
					ix: [ `i/a/${String(i).padStart(4, '0')}` ],
				});
			}
			await w.commit();

			const seen = await positions(store, 'i/a/');
			expect(seen).toHaveLength(total);
			expect(new Set(seen).size).toBe(total);
			expect(seen[0]).toBe('c/0000');
			expect(seen[total - 1]).toBe(`c/${String(total - 1).padStart(4, '0')}`);
		});
	});

	it('commits atomically', async () => {
		store = await open();
		const w = await store.write();
		await w.put('a', 1);
		await w.put('b', 2);
		await w.commit();

		const r = await store.read();
		expect(await r.get('a')).toBe(1);
		expect(await r.get('b')).toBe(2);
		r.release();
	});

	describe('generation tracking', () => {
		// Two keyspaces sharing one generation, a third with its own, and a
		// fourth outside every one — the shape keys.overlayGenerationFor gives
		// the client, written here as the store sees it.
		const generationOf = (key: string): string | undefined =>
			key.startsWith('o/a/') || key.startsWith('ot/a/')
				? 'gen/a'
				: key.startsWith('o/b/')
				? 'gen/b'
				: undefined;

		const tracked = async (): Promise<KVStore> => {
			const opened = await open();
			expect(tracksGenerations(opened)).toBe(true);
			(opened as KVStore & GenerationTracking).trackGenerations(generationOf);
			return opened;
		};

		const generation = async (at: string): Promise<unknown> => {
			const r = await store!.read();
			const token = await r.get(at);
			r.release();
			return token;
		};

		it('stamps a fresh token for the keyspace a commit wrote', async () => {
			store = await tracked();
			expect(await generation('gen/a')).toBeUndefined();

			const w = await store.write();
			await w.put('o/a/row', 1);
			await w.commit();
			const first = await generation('gen/a');
			expect(first).toBeTypeOf('string');

			const w2 = await store.write();
			await w2.put('o/a/row', 2);
			await w2.commit();
			expect(await generation('gen/a')).not.toBe(first);
		});

		it('stamps for a delete and for a text fragment, not only for a row put', async () => {
			store = await tracked();
			const w = await store.write();
			await w.put('o/a/row', 1);
			await w.commit();
			const afterPut = await generation('gen/a');

			const w2 = await store.write();
			await w2.put('ot/a/ws/row/field/0001', new Uint8Array([ 1 ]));
			await w2.commit();
			const afterFragment = await generation('gen/a');
			expect(afterFragment).not.toBe(afterPut);

			const w3 = await store.write();
			await w3.del('o/a/row');
			await w3.commit();
			expect(await generation('gen/a')).not.toBe(afterFragment);
		});

		it('leaves the generations a commit did not write alone', async () => {
			store = await tracked();
			const w = await store.write();
			await w.put('o/a/row', 1);
			await w.put('o/b/row', 1);
			await w.commit();
			const a = await generation('gen/a');
			const b = await generation('gen/b');

			const w2 = await store.write();
			await w2.put('c/a/row', 1);
			await w2.put('o/b/row', 2);
			await w2.commit();

			expect(await generation('gen/a')).toBe(a);
			expect(await generation('gen/b')).not.toBe(b);
		});

		it('stamps each generation that the mapping names for one key', async () => {
			store = await open();
			(store as KVStore & GenerationTracking).trackGenerations((key) =>
				key.startsWith('o/a/')
					? [ 'gen/a', 'gen/all' ]
					: key.startsWith('o/b/')
					? 'gen/all'
					: undefined
			);
			const w = await store.write();
			await w.put('o/a/row', 1);
			await w.commit();
			const a = await generation('gen/a');
			const all = await generation('gen/all');
			expect(a).toBeTypeOf('string');
			expect(all).toBeTypeOf('string');

			const w2 = await store.write();
			await w2.put('o/b/row', 1);
			await w2.commit();
			expect(await generation('gen/a')).toBe(a);
			expect(await generation('gen/all')).not.toBe(all);
		});

		it('stamps nothing for a transaction that was released', async () => {
			store = await tracked();
			const w = await store.write();
			await w.put('o/a/row', 1);
			await w.commit();
			const before = await generation('gen/a');

			const w2 = await store.write();
			await w2.put('o/a/other', 1);
			w2.release();

			expect(await generation('gen/a')).toBe(before);
		});

		it('keeps a generation its own transaction deleted deleted', async () => {
			store = await tracked();
			const w = await store.write();
			await w.put('o/a/row', 1);
			await w.commit();
			expect(await generation('gen/a')).toBeTypeOf('string');

			// What purging a workspace does: the rows and the generation
			// describing them go together, and "absent" reads as unknown.
			const w2 = await store.write();
			await w2.del('o/a/row');
			await w2.del('gen/a');
			await w2.commit();

			expect(await generation('gen/a')).toBeUndefined();
		});

		it('stamps nothing at all while no mapping is installed', async () => {
			store = await open();
			const w = await store.write();
			await w.put('o/a/row', 1);
			await w.commit();

			const r = await store.read();
			const keys: string[] = [];
			for await (const [ k ] of r.scanRange()) keys.push(k);
			r.release();
			expect(keys).toEqual([ 'o/a/row' ]);
		});
	});
});

/**
 * The correctness tests above cannot pin LAZINESS: a backend that materialized
 * a whole range up front would pass every one of them. This pins it for the
 * IDB backend by counting what it asks the store for — a scan abandoned after
 * a few entries must cost one bounded request, not the range. It is what keeps
 * the index build's batch loop (read BUILD_BATCH rows, stop) from re-reading
 * the remainder of the entity every batch, the same property indexScan already
 * pins behaviorally in "stops when the caller does".
 */
describe('IDBKVStore scan laziness', () => {
	it('asks for one bounded chunk when the caller stops early', async () => {
		const store = await IDBKVStore.open(`kv-lazy-${dbCounter++}`);
		const w = await store.write();
		for (let i = 0; i < 600; i++) {
			await w.put(`p/${String(i).padStart(4, '0')}`, i);
		}
		await w.commit();

		const counts: (number | undefined)[] = [];
		const original = IDBObjectStore.prototype.getAll;
		IDBObjectStore.prototype.getAll = function(
			this: IDBObjectStore,
			query?: IDBValidKey | IDBKeyRange | null,
			count?: number,
		) {
			counts.push(count);
			return original.call(this, query, count);
		};
		try {
			const r = await store.read();
			const seen: string[] = [];
			for await (const [ key ] of r.scan('p/')) {
				seen.push(key);
				if (seen.length === 3) break;
			}
			r.release();
			expect(seen).toEqual([ 'p/0000', 'p/0001', 'p/0002' ]);
		} finally {
			IDBObjectStore.prototype.getAll = original;
		}
		expect(counts).toHaveLength(1);
		expect(counts[0]).toBeTypeOf('number');
		await store.close();
	});
});

/**
 * An open connection that does not yield on versionchange blocks the other
 * context forever, so these assert the yield from the other side: the upgrade
 * or delete completes without ever firing onblocked.
 *
 * open()'s own onblocked branch has no counterpart here — reaching it needs a
 * connection at a lower version than the one we ask for, which cannot happen
 * while the version is pinned at 1.
 */
describe('IDBKVStore connection lifecycle', () => {
	it('yields the connection to a version upgrade', async () => {
		const name = `kv-lifecycle-${dbCounter++}`;
		const store = await IDBKVStore.open(name);

		const upgraded = await new Promise<IDBDatabase>((resolve, reject) => {
			const req = indexedDB.open(name, 2);
			req.onsuccess = () => resolve(req.result);
			req.onerror = () => reject(req.error);
			req.onblocked = () => reject(new Error('upgrade blocked: the store did not yield'));
		});
		upgraded.close();

		await expect(store.read()).rejects.toThrow(/upgrading or deleting/);
		await expect(store.write()).rejects.toThrow(/upgrading or deleting/);
	});

	it('yields the connection to deleteDatabase', async () => {
		const name = `kv-lifecycle-${dbCounter++}`;
		const store = await IDBKVStore.open(name);
		const w = await store.write();
		await w.put('k', 'v');
		await w.commit();

		await new Promise<void>((resolve, reject) => {
			const req = indexedDB.deleteDatabase(name);
			req.onsuccess = () => resolve();
			req.onerror = () => reject(req.error);
			req.onblocked = () => reject(new Error('delete blocked: the store did not yield'));
		});

		await expect(store.read()).rejects.toThrow(/upgrading or deleting/);
	});

	it('names the reason a closed store is unusable', async () => {
		const store = await IDBKVStore.open(`kv-lifecycle-${dbCounter++}`);
		await store.close();

		await expect(store.read()).rejects.toThrow(/store was closed/);
		await expect(store.write()).rejects.toThrow(/store was closed/);
	});
});

/**
 * The index-ordered ROW read, as cases every backend runs — and, below, as
 * cases IDBKVStore runs a second time with getAllRecords available.
 *
 * Registered from a function rather than written inline because the two
 * requests an IDB store can serve the read with are a real fork in the
 * backend, and only running the same assertions over both says they agree.
 */
function indexScanRowsCases(open: () => Promise<KVStore>): void {
	let store: KVStore | undefined;

	afterEach(async () => {
		await store?.close();
		store = undefined;
	});

	/** Every entry of a range, as (position, key) pairs — values checked apart. */
	const walk = async (
		lo?: string,
		hi?: string,
		direction?: 'asc' | 'desc',
	): Promise<IndexEntry[]> => {
		const r = await store!.read();
		try {
			const out: IndexEntry[] = [];
			for await (const entry of r.indexScanRows(lo, hi, direction)) out.push(entry);
			return out;
		} finally {
			r.release();
		}
	};

	const pairs = async (
		lo?: string,
		hi?: string,
		direction?: 'asc' | 'desc',
	): Promise<[string, string][]> =>
		(await walk(lo, hi, direction)).map((e) => [ e.position, e.key ]);

	/** indexScan's keys over a range, backward. */
	const keysBackward = async (lo?: string, hi?: string): Promise<string[]> => {
		const r = await store!.read();
		try {
			const out: string[] = [];
			for await (const key of r.indexScan(lo, hi, 'desc')) out.push(key);
			return out;
		} finally {
			r.release();
		}
	};

	/** `count` records under `keyPrefix`, each holding the positions it is given. */
	const seedRows = async (
		s: KVStore,
		count: number,
		keyPrefix: string,
		positions: (i: string) => string[],
	): Promise<void> => {
		const w = await s.write();
		for (let i = 0; i < count; i++) {
			const at = String(i).padStart(4, '0');
			await w.put(`${keyPrefix}${at}`, { v: at, ix: positions(at) });
		}
		await w.commit();
	};

	it('yields each position with the record that holds it, in position order', async () => {
		store = await open();
		const w = await store.write();
		await w.put('c/x', { v: 'x', ix: [ 'i/a/1', 'i/a/3' ] });
		await w.put('c/y', { v: 'y', ix: [ 'i/a/2' ] });
		await w.put('c/z', { v: 'z', ix: [ 'i/b/1' ] });
		await w.put('c/none', { v: 'none' });
		await w.commit();

		// x holds two positions of this range and comes back at each, carrying
		// the same value both times — the order is the POSITIONS' and not the
		// rows', exactly as indexScan's is.
		expect(await walk('i/a/', 'i/a0')).toEqual([
			{ position: 'i/a/1', key: 'c/x', value: { v: 'x', ix: [ 'i/a/1', 'i/a/3' ] } },
			{ position: 'i/a/2', key: 'c/y', value: { v: 'y', ix: [ 'i/a/2' ] } },
			{ position: 'i/a/3', key: 'c/x', value: { v: 'x', ix: [ 'i/a/1', 'i/a/3' ] } },
		]);
	});

	it('bounds the range half-open, both ends optional', async () => {
		store = await open();
		const w = await store.write();
		await w.put('c/x', { v: 'x', ix: [ 'i/a/1', 'i/a/3' ] });
		await w.put('c/y', { v: 'y', ix: [ 'i/a/2' ] });
		await w.put('c/z', { v: 'z', ix: [ 'i/b/1' ] });
		await w.commit();

		expect(await pairs('i/a/2', 'i/a/3')).toEqual([ [ 'i/a/2', 'c/y' ] ]);
		expect(await pairs('i/a/2')).toEqual([
			[ 'i/a/2', 'c/y' ],
			[ 'i/a/3', 'c/x' ],
			[ 'i/b/1', 'c/z' ],
		]);
		expect(await pairs(undefined, 'i/a/2')).toEqual([ [ 'i/a/1', 'c/x' ] ]);
		expect(await pairs()).toEqual([
			[ 'i/a/1', 'c/x' ],
			[ 'i/a/2', 'c/y' ],
			[ 'i/a/3', 'c/x' ],
			[ 'i/b/1', 'c/z' ],
		]);
		expect(await pairs('i/a/2', 'i/a/2')).toEqual([]);
		expect(await pairs('i/b', 'i/a')).toEqual([]);
	});

	it('yields the exact reverse under desc, values included', async () => {
		store = await open();
		const w = await store.write();
		await w.put('c/x', { v: 'x', ix: [ 'i/a/1', 'i/a/3' ] });
		await w.put('c/y', { v: 'y', ix: [ 'i/a/2' ] });
		await w.put('c/z', { v: 'z', ix: [ 'i/b/1' ] });
		await w.commit();

		for (
			const [ lo, hi ] of [ [ 'i/a/', 'i/a0' ], [ 'i/a/2', undefined ], [ undefined, 'i/a/2' ], [] ]
		) {
			expect(await walk(lo, hi, 'desc')).toEqual((await walk(lo, hi)).reverse());
		}
		expect(await pairs('i/a/2', 'i/a/2', 'desc')).toEqual([]);
		expect(await pairs('i/b', 'i/a', 'desc')).toEqual([]);
	});

	it('reads a range longer than one request backward', async () => {
		store = await open();
		await seedRows(store, 600, 'c/', (at) => [ `i/a/${at}`, `i/b/${at}` ]);
		const forward = await pairs('i/');
		expect(await pairs('i/', undefined, 'desc')).toEqual([ ...forward ].reverse());
		expect(await keysBackward('i/')).toEqual(forward.map(([ , k ]) => k).reverse());
	});

	// Backward, a continuation resumes at the position of the last entry
	// yielded with only the records below it on the record key still to come.
	it('yields a tie group that straddles a chunk boundary exactly once, backward', async () => {
		store = await open();
		await seedRows(store, 600, 'c/', () => [ 'i/tie' ]);
		const w = await store.write();
		await w.put('c/before', { v: 'before', ix: [ 'i/aaa' ] });
		await w.put('c/after', { v: 'after', ix: [ 'i/zzz' ] });
		await w.commit();

		const forward = await pairs('i/');
		const backward = await pairs('i/', undefined, 'desc');
		expect(backward).toHaveLength(602);
		expect(backward).toEqual([ ...forward ].reverse());
		expect(await keysBackward('i/')).toEqual(forward.map(([ , k ]) => k).reverse());
	});

	it('stops when the caller does, backward', async () => {
		store = await open();
		await seedRows(store, 600, 'c/', (at) => [ `i/a/${at}` ]);
		const r = await store.read();
		const seen: string[] = [];
		for await (const entry of r.indexScanRows('i/a/', undefined, 'desc')) {
			seen.push(entry.key);
			if (seen.length === 3) break;
		}
		r.release();
		expect(seen).toEqual([ 'c/0599', 'c/0598', 'c/0597' ]);
	});

	it('names the same records indexScan does, in the same order', async () => {
		store = await open();
		await seedRows(store, 400, 'c/', (at) => [ `i/a/${at}`, `i/b/${at}` ]);
		const r = await store.read();
		try {
			const scanned: string[] = [];
			for await (const key of r.indexScan('i/')) scanned.push(key);
			const rows: string[] = [];
			for await (const entry of r.indexScanRows('i/')) rows.push(entry.key);
			expect(rows).toEqual(scanned);
			expect(rows).toHaveLength(800);
		} finally {
			r.release();
		}
	});

	it('yields every entry of a range longer than one request', async () => {
		store = await open();
		await seedRows(store, 600, 'c/', (at) => [ `i/a/${at}` ]);
		const seen = await pairs('i/a/');
		expect(seen).toHaveLength(600);
		expect(seen[0]).toEqual([ 'i/a/0000', 'c/0000' ]);
		expect(seen[599]).toEqual([ 'i/a/0599', 'c/0599' ]);
		expect(new Set(seen.map(([ , k ]) => k)).size).toBe(600);
	});

	// The one shape the chunked read can double up or drop: a run of records
	// sharing ONE position, split by a chunk boundary. Continuing means asking
	// again from that position, so every record of the run is read twice and
	// the ones already yielded have to fall out on the record key alone.
	it('yields a tie group that straddles a chunk boundary exactly once', async () => {
		store = await open();
		await seedRows(store, 600, 'c/', () => [ 'i/tie' ]);
		const w = await store.write();
		await w.put('c/before', { v: 'before', ix: [ 'i/aaa' ] });
		await w.put('c/after', { v: 'after', ix: [ 'i/zzz' ] });
		await w.commit();

		const seen = await pairs('i/');
		expect(seen).toHaveLength(602);
		expect(seen[0]).toEqual([ 'i/aaa', 'c/before' ]);
		expect(seen[601]).toEqual([ 'i/zzz', 'c/after' ]);
		const tied = seen.slice(1, 601);
		expect(tied.every(([ p ]) => p === 'i/tie')).toBe(true);
		expect(tied.map(([ , k ]) => k)).toEqual([ ...tied.map(([ , k ]) => k) ].sort());
		expect(new Set(tied.map(([ , k ]) => k)).size).toBe(600);
	});

	// A record on several positions of the range, read across a boundary: the
	// second chunk starts inside the record's own run of positions, so a
	// backend recovering which position an entry stands for has to resume in
	// the middle of that list rather than at its front.
	it('yields every position of a fanned-out record across a chunk boundary', async () => {
		store = await open();
		await seedRows(store, 400, 'c/', (at) => [ `i/a/${at}`, `i/b/${at}`, `i/c/${at}` ]);
		const seen = await pairs('i/');
		expect(seen).toHaveLength(1200);
		expect(seen.map(([ p ]) => p)).toEqual([ ...seen.map(([ p ]) => p) ].sort());
		for (const [ position, key ] of seen) {
			expect(position.endsWith(key.slice('c/'.length))).toBe(true);
		}
	});

	it('stops when the caller does', async () => {
		store = await open();
		await seedRows(store, 600, 'c/', (at) => [ `i/a/${at}` ]);
		const r = await store.read();
		const seen: string[] = [];
		for await (const entry of r.indexScanRows('i/a/')) {
			seen.push(entry.key);
			if (seen.length === 3) break;
		}
		r.release();
		expect(seen).toEqual([ 'c/0000', 'c/0001', 'c/0002' ]);
	});

	it('sees a write transaction’s own buffered positions', async () => {
		store = await open();
		const setup = await store.write();
		await setup.put('c/x', { v: 'x', ix: [ 'i/a/1', 'i/a/3' ] });
		await setup.put('c/y', { v: 'y', ix: [ 'i/a/2' ] });
		await setup.commit();

		const w = await store.write();
		await w.put('c/w', { v: 'w', ix: [ 'i/a/0' ] });
		await w.put('c/x', { v: 'x2', ix: [ 'i/a/9' ] });
		await w.del('c/y');
		const seen: IndexEntry[] = [];
		for await (const entry of w.indexScanRows('i/a/', 'i/a0')) seen.push(entry);
		expect(seen).toEqual([
			{ position: 'i/a/0', key: 'c/w', value: { v: 'w', ix: [ 'i/a/0' ] } },
			{ position: 'i/a/9', key: 'c/x', value: { v: 'x2', ix: [ 'i/a/9' ] } },
		]);
		const backward: IndexEntry[] = [];
		for await (const entry of w.indexScanRows('i/a/', 'i/a0', 'desc')) backward.push(entry);
		expect(backward).toEqual([ ...seen ].reverse());
		w.release();
	});
}

/**
 * The same cases over an index that answers getAllRecords — the request
 * IDBKVStore prefers, and the one fake-indexeddb does not implement. Emulated
 * here with a cursor so the backend takes that branch at all; what is under
 * test is the branch, not the emulation.
 */
describe('KVStore: idb with getAllRecords', () => {
	let restore: (() => void) | undefined;

	beforeAll(() => {
		const proto = IDBIndex.prototype as unknown as Record<string, unknown>;
		if ('getAllRecords' in proto) return;
		proto.getAllRecords = function(
			this: IDBIndex,
			options?: { query?: IDBKeyRange | null; count?: number },
		): unknown {
			const out: { key: unknown; primaryKey: unknown; value: unknown }[] = [];
			const request: {
				result: unknown;
				error: unknown;
				onsuccess: (() => void) | null;
				onerror: (() => void) | null;
			} = { result: undefined, error: null, onsuccess: null, onerror: null };
			const cursorReq = this.openCursor(options?.query ?? null);
			cursorReq.onsuccess = () => {
				const cursor = cursorReq.result;
				if (cursor === null || (options?.count !== undefined && out.length >= options.count)) {
					request.result = out;
					request.onsuccess?.();
					return;
				}
				out.push({ key: cursor.key, primaryKey: cursor.primaryKey, value: cursor.value });
				cursor.continue();
			};
			cursorReq.onerror = () => {
				request.error = cursorReq.error;
				request.onerror?.();
			};
			return request;
		};
		restore = () => {
			delete proto.getAllRecords;
		};
	});

	afterAll(() => restore?.());

	indexScanRowsCases(() => IDBKVStore.open(`kv-records-${dbCounter++}`));
});

/**
 * How a scan takes its range, which only the IndexedDB backend has: the memory
 * store walks its own map and issues no requests at all. The chunk sizes are
 * idb.ts's SCAN_CHUNK and its doubling.
 */
describe('IDBKVStore: chunked scans', () => {
	const FIRST_CHUNK = 256;
	const ROWS = FIRST_CHUNK * 4;
	const key = (i: number): string => `k${String(i).padStart(5, '0')}`;

	let store: KVStore | undefined;
	let requests = 0;
	let restore: (() => void) | undefined;

	beforeAll(() => {
		const proto = IDBObjectStore.prototype;
		const original = proto.getAll;
		proto.getAll = function(this: IDBObjectStore, ...args: unknown[]): IDBRequest {
			requests++;
			return (original as (...a: unknown[]) => IDBRequest).apply(this, args);
		} as typeof proto.getAll;
		restore = () => {
			proto.getAll = original;
		};
	});

	afterAll(() => restore?.());

	afterEach(async () => {
		await store?.close();
		store = undefined;
	});

	async function seeded(): Promise<KVStore> {
		const opened = await IDBKVStore.open(`kv-chunks-${dbCounter++}`);
		const w = await opened.write();
		for (let i = 0; i < ROWS; i++) await w.put(key(i), i);
		await w.commit();
		requests = 0;
		return opened;
	}

	it('yields every key in order across the chunk boundaries', async () => {
		store = await seeded();
		const r = await store.read();
		const seen: string[] = [];
		for await (const [ k ] of r.scan('k')) seen.push(k);
		r.release();

		expect(seen).toEqual(Array.from({ length: ROWS }, (_, i) => key(i)));
		// A chunk is asked for only after the one before it was consumed whole,
		// so the growth is the whole cost of a full scan.
		expect(requests).toBeGreaterThan(1);
		expect(requests).toBeLessThanOrEqual(6);
	});

	it('reads nothing ahead for a consumer that stops inside the first chunk', async () => {
		store = await seeded();
		for (const take of [ 1, FIRST_CHUNK ]) {
			requests = 0;
			const r = await store.read();
			let n = 0;
			for await (const [] of r.scan('k')) {
				if (++n === take) break;
			}
			r.release();
			expect(requests).toBe(1);
		}
	});

	it('reads one chunk ahead once a consumer has drained one', async () => {
		store = await seeded();
		const r = await store.read();
		let n = 0;
		for await (const [] of r.scan('k')) {
			// One past the first chunk: the second is being consumed, and the third
			// has been asked for already rather than after it runs out.
			if (++n === FIRST_CHUNK + 1) break;
		}
		r.release();

		expect(requests).toBe(3);
	});
});
