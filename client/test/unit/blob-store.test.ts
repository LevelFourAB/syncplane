// The blob store contract, against both backends: who owns staged bytes,
// how a re-queue and a cached blob merge, and what eviction may delete.

import 'fake-indexeddb/auto';

import { afterEach, describe, expect, it } from 'vitest';

import { IDBBlobStore } from '../../src/blob/idb.js';
import { type BlobStore, MemoryBlobStore, type UploadRecord } from '../../src/blob/store.js';

let dbCounter = 0;

const implementations: [string, () => Promise<BlobStore>][] = [
	[ 'memory', async () => new MemoryBlobStore() ],
	[ 'idb (fake-indexeddb)', () => IDBBlobStore.open(`blob-store-test-${dbCounter++}`) ],
];

function digest(n: number): string {
	return 'sp1:' + n.toString(16).padStart(64, '0');
}

async function staged(store: BlobStore, chunks: Uint8Array[]): Promise<string> {
	const id = await store.stage();
	for (let i = 0; i < chunks.length; i++) {
		await store.putChunk(id, i, new Blob([ chunks[i]! as Uint8Array<ArrayBuffer> ]));
	}
	return id;
}

/** The bytes of one stored chunk, or undefined when it is gone. */
async function chunkBytes(
	store: BlobStore,
	bytesId: string,
	index: number,
): Promise<Uint8Array | undefined> {
	const chunk = await store.readChunk(bytesId, index);
	return chunk === undefined ? undefined : new Uint8Array(await chunk.arrayBuffer());
}

function record(bytesId: string, d: string, size: number, at = 1): UploadRecord {
	return {
		workspaceId: 'ws1',
		digest: d,
		size,
		mediaType: 'application/octet-stream',
		chunkHashes: [ new Uint8Array(32) ],
		bytesId,
		createdAt: at,
		state: 'queued',
	};
}

describe.each(implementations)('BlobStore: %s', (_name, open) => {
	let store: BlobStore | undefined;

	afterEach(async () => {
		await store?.close();
		store = undefined;
	});

	it('queues staged bytes and reads them back', async () => {
		store = await open();
		const id = await staged(store, [ new Uint8Array([ 1, 2, 3 ]) ]);
		const rec = await store.queue(record(id, digest(1), 3));
		expect(rec.bytesId).toBe(id);
		expect(await chunkBytes(store, id, 0)).toEqual(new Uint8Array([ 1, 2, 3 ]));
		expect((await store.uploads()).map((r) => r.digest)).toEqual([ digest(1) ]);
	});

	it('stores chunks as Blob values', async () => {
		store = await open();
		const id = await staged(store, [ new Uint8Array([ 7, 8 ]) ]);
		const chunk = await store.readChunk(id, 0);
		expect(chunk).toBeInstanceOf(Blob);
		expect(chunk?.size).toBe(2);
	});

	it('keeps the first bytes when the same blob is queued twice', async () => {
		store = await open();
		const first = await staged(store, [ new Uint8Array([ 1 ]) ]);
		await store.queue(record(first, digest(1), 1));
		const second = await staged(store, [ new Uint8Array([ 1 ]) ]);
		const rec = await store.queue(record(second, digest(1), 1));
		expect(rec.bytesId).toBe(first);
		expect(await chunkBytes(store, second, 0)).toBeUndefined();
	});

	it('sets a failed upload back to queued when it is queued again', async () => {
		store = await open();
		const id = await staged(store, [ new Uint8Array([ 1 ]) ]);
		const rec = await store.queue(record(id, digest(1), 1));
		await store.updateUpload({
			...rec,
			state: 'failed',
			error: { code: 'blob_refused', message: 'no' },
		});
		const again = await store.queue(
			record(await staged(store, [ new Uint8Array([ 1 ]) ]), digest(1), 1),
		);
		expect(again.state).toBe('queued');
		expect(again.error).toBeUndefined();
	});

	it('moves completed uploads to the cache, and cached blobs back to the queue', async () => {
		store = await open();
		const id = await staged(store, [ new Uint8Array([ 9 ]) ]);
		await store.queue(record(id, digest(1), 1));
		await store.completeUpload('ws1', digest(1), 5);
		expect(await store.upload('ws1', digest(1))).toBeUndefined();
		expect((await store.cached('ws1', digest(1)))?.bytesId).toBe(id);

		const rec = await store.queue(
			record(await staged(store, [ new Uint8Array([ 9 ]) ]), digest(1), 1),
		);
		expect(rec.bytesId).toBe(id);
		expect(await store.cached('ws1', digest(1))).toBeUndefined();
		expect(await chunkBytes(store, id, 0)).toEqual(new Uint8Array([ 9 ]));
	});

	it('evicts the least recently used cache entries and never a queued upload', async () => {
		store = await open();
		const queued = await staged(store, [ new Uint8Array(100) ]);
		await store.queue(record(queued, digest(9), 100));
		for (const [ n, at ] of [ [ 1, 30 ], [ 2, 10 ], [ 3, 20 ] ] as const) {
			const id = await staged(store, [ new Uint8Array(10) ]);
			await store.addCached({
				workspaceId: 'ws1',
				digest: digest(n),
				size: 10,
				mediaType: '',
				bytesId: id,
				usedAt: at,
			});
		}
		await store.touch('ws1', digest(2), 40);
		await store.evict(15);
		expect(await store.cached('ws1', digest(3))).toBeUndefined();
		expect(await store.cached('ws1', digest(1))).toBeUndefined();
		expect(await store.cached('ws1', digest(2))).toBeDefined();

		await store.evict(0);
		expect(await store.cached('ws1', digest(2))).toBeUndefined();
		expect(await store.upload('ws1', digest(9))).toBeDefined();
		expect(await chunkBytes(store, queued, 0)).toEqual(new Uint8Array(100));
	});

	it('sweeps only staged bytes older than the cut-off', async () => {
		store = await open();
		const old = await staged(store, [ new Uint8Array([ 1 ]) ]);
		await store.sweep(Date.now() + 1);
		expect(await chunkBytes(store, old, 0)).toBeUndefined();
		const young = await staged(store, [ new Uint8Array([ 1 ]) ]);
		await store.sweep(Date.now() - 60_000);
		expect(await chunkBytes(store, young, 0)).toEqual(new Uint8Array([ 1 ]));
	});

	it('clears one workspace without touching another', async () => {
		store = await open();
		const a = await staged(store, [ new Uint8Array([ 1 ]) ]);
		await store.queue(record(a, digest(1), 1));
		const b = await staged(store, [ new Uint8Array([ 2 ]) ]);
		await store.queue({ ...record(b, digest(2), 1), workspaceId: 'ws2' });
		await store.clear('ws1');
		expect(await store.upload('ws1', digest(1))).toBeUndefined();
		expect(await chunkBytes(store, a, 0)).toBeUndefined();
		expect(await store.upload('ws2', digest(2))).toBeDefined();
		await store.clear();
		expect(await store.uploads()).toEqual([]);
	});
});

describe('IDBBlobStore', () => {
	it('keeps the queue across a reopen of the same database', async () => {
		const name = `blob-store-reopen-${dbCounter++}`;
		const first = await IDBBlobStore.open(name);
		const id = await staged(first, [ new Uint8Array([ 4, 5 ]) ]);
		await first.queue(record(id, digest(1), 2));
		await first.close();

		const second = await IDBBlobStore.open(name);
		const recs = await second.uploads();
		expect(recs.map((r) => r.digest)).toEqual([ digest(1) ]);
		expect(await chunkBytes(second, recs[0]!.bytesId, 0)).toEqual(new Uint8Array([ 4, 5 ]));
		await second.close();
	});
});

describe('IDBBlobStore where IndexedDB refuses Blob values', () => {
	// WebKit refuses Blob values in an ephemeral session (Safari private
	// browsing, Playwright contexts) with an UnknownError. These replace put
	// with one that refuses them the two ways a browser can: by throwing, and
	// by failing the request so its transaction aborts.
	const original = IDBObjectStore.prototype.put;
	const refusals: Record<
		string,
		(store: IDBObjectStore, value: unknown, key?: IDBValidKey) => IDBRequest
	> = {
		throws: () => {
			throw new DOMException(
				'Error preparing Blob/File data to be stored in object store',
				'UnknownError',
			);
		},
		aborts: (store, value, key) => {
			const req = original.call(store, typeof value, key);
			store.transaction.abort();
			return req;
		},
	};

	afterEach(() => {
		IDBObjectStore.prototype.put = original;
	});

	describe.each(Object.keys(refusals))('when put %s', (mode) => {
		function refuseBlobs(): void {
			IDBObjectStore.prototype.put = function(
				this: IDBObjectStore,
				value: unknown,
				key?: IDBValidKey,
			) {
				if (value instanceof Blob) return refusals[mode]!(this, value, key);
				return original.call(this, value, key);
			};
		}

		it('stores chunks as bytes and reads them back as Blobs', async () => {
			refuseBlobs();
			const store = await IDBBlobStore.open(`blob-store-noblob-${dbCounter++}`);
			const id = await staged(store, [ new Uint8Array([ 1, 2, 3 ]) ]);
			const chunk = await store.readChunk(id, 0);
			expect(chunk).toBeInstanceOf(Blob);
			expect(await chunkBytes(store, id, 0)).toEqual(new Uint8Array([ 1, 2, 3 ]));
			await store.close();
		});

		it('leaves no probe data behind', async () => {
			refuseBlobs();
			const name = `blob-store-noblob-${dbCounter++}`;
			const store = await IDBBlobStore.open(name);
			await store.close();
			IDBObjectStore.prototype.put = original;
			const db = await new Promise<IDBDatabase>((resolve, reject) => {
				const req = indexedDB.open(name);
				req.onsuccess = () => resolve(req.result);
				req.onerror = () => reject(req.error);
			});
			const count = await new Promise<number>((resolve, reject) => {
				const req = db.transaction('chunks').objectStore('chunks').count();
				req.onsuccess = () => resolve(req.result);
				req.onerror = () => reject(req.error);
			});
			db.close();
			expect(count).toBe(0);
		});
	});

	it('reads chunks written in either mode, whichever mode it opens in', async () => {
		const name = `blob-store-mixed-${dbCounter++}`;
		const normal = await IDBBlobStore.open(name);
		const asBlob = await staged(normal, [ new Uint8Array([ 1 ]) ]);
		await normal.close();

		IDBObjectStore.prototype.put = function(this: IDBObjectStore, value: unknown, key?: IDBValidKey) {
			if (value instanceof Blob) refusals.throws!(this, value, key);
			return original.call(this, value, key);
		};
		const fallback = await IDBBlobStore.open(name);
		const asBytes = await staged(fallback, [ new Uint8Array([ 2 ]) ]);
		expect(await chunkBytes(fallback, asBlob, 0)).toEqual(new Uint8Array([ 1 ]));
		await fallback.close();

		IDBObjectStore.prototype.put = original;
		const reopened = await IDBBlobStore.open(name);
		expect(await chunkBytes(reopened, asBytes, 0)).toEqual(new Uint8Array([ 2 ]));
		expect(await reopened.readChunk(asBytes, 0)).toBeInstanceOf(Blob);
		await reopened.close();
	});

	it('switches to bytes when a put is refused after the probe passed', async () => {
		const store = await IDBBlobStore.open(`blob-store-late-${dbCounter++}`);
		IDBObjectStore.prototype.put = function(this: IDBObjectStore, value: unknown, key?: IDBValidKey) {
			if (value instanceof Blob) refusals.throws!(this, value, key);
			return original.call(this, value, key);
		};
		const id = await staged(store, [ new Uint8Array([ 9 ]) ]);
		expect(await chunkBytes(store, id, 0)).toEqual(new Uint8Array([ 9 ]));
		await store.close();
	});
});
