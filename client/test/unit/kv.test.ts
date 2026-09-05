import 'fake-indexeddb/auto';
import { afterEach, describe, expect, it } from 'vitest';

import { IDBKVStore } from '../../src/kv/idb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';

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

		const positions = async (store: KVStore, lo?: string, hi?: string): Promise<string[]> => {
			const r = await store.read();
			try {
				const out: string[] = [];
				for await (const key of r.indexScan(lo, hi)) out.push(key);
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
