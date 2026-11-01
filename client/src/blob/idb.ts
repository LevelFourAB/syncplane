// IndexedDB backend of the blob store: a database of its own beside the KV
// store's (StorageNames.blobStore), so its large values never share a
// transaction, a quota-error path or a version with the rows.
//
// Chunks are Blob values, except where this IndexedDB refuses them: WebKit
// does in an ephemeral session (Safari private browsing), with an
// UnknownError. After the first refusal the store writes the chunk's bytes
// instead. Every chunk record is either a Blob or bytes, and readChunk tells
// them apart by type, so a database written in one mode reads in the other.
//
// Nothing tests for the refusal on open. WebKit can tie a Blob record to the
// file of another Blob that was written and deleted in one transaction: the
// record keeps its size and reads back the other file's bytes. A test write
// that deletes its Blob is such a transaction, and corrupts the next chunk
// stored.

import { readBlobRange } from './digest.js';
import {
	type BlobStore,
	type CacheRecord,
	type EvictionKeep,
	pickEvictions,
	type UploadRecord,
} from './store.js';

const UPLOADS = 'uploads';
const CACHE = 'cache';
const CHUNKS = 'chunks';
const STAGING = 'staging';

interface StagingRecord {
	bytesId: string;
	startedAt: number;
}

function request<T>(req: IDBRequest<T>): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		req.onsuccess = () => resolve(req.result);
		req.onerror = () => reject(req.error);
	});
}

function chunkRange(bytesId: string): IDBKeyRange {
	return IDBKeyRange.bound([ bytesId, 0 ], [ bytesId, Infinity ]);
}

/**
 * BlobStore over one IndexedDB database. Every tab of the account opening the
 * same name shares its contents.
 */
export class IDBBlobStore implements BlobStore {
	private lost: string | undefined;

	/** Whether chunks are written as Blob values; see the file comment. */
	private blobValues = true;

	private constructor(private readonly db: IDBDatabase) {
		// Yield to a deleteDatabase or upgrade elsewhere, as the KV store does.
		db.onversionchange = () => this.lose('another connection is upgrading or deleting the database');
		db.onclose = () => this.lose('the browser closed the connection');
	}

	/** Opens (creating when absent) the blob database called `name`. */
	static async open(name: string): Promise<IDBBlobStore> {
		const db = await new Promise<IDBDatabase>((resolve, reject) => {
			const req = indexedDB.open(name, 1);
			req.onupgradeneeded = () => {
				const d = req.result;
				if (!d.objectStoreNames.contains(UPLOADS)) {
					d.createObjectStore(UPLOADS, { keyPath: [ 'workspaceId', 'digest' ] });
				}
				if (!d.objectStoreNames.contains(CACHE)) {
					d.createObjectStore(CACHE, { keyPath: [ 'workspaceId', 'digest' ] });
				}
				if (!d.objectStoreNames.contains(CHUNKS)) d.createObjectStore(CHUNKS);
				if (!d.objectStoreNames.contains(STAGING)) {
					d.createObjectStore(STAGING, { keyPath: 'bytesId' });
				}
			};
			req.onsuccess = () => resolve(req.result);
			req.onerror = () => reject(req.error);
			req.onblocked = () => {
				console.warn(
					`sync: IndexedDB open of "${name}" is waiting on another connection to close. `
						+ `If this persists, close other tabs of this app.`,
				);
			};
		});
		return new IDBBlobStore(db);
	}

	private lose(reason: string): void {
		this.lost ??= reason;
		this.db.close();
	}

	/**
	 * Runs `fn` in one transaction and settles once it has committed. `fn` may
	 * only await requests of `tx`: awaiting anything else lets the transaction
	 * commit under it.
	 */
	private async tx<T>(
		stores: string[],
		mode: IDBTransactionMode,
		fn: (tx: IDBTransaction) => Promise<T>,
	): Promise<T> {
		if (this.lost) throw new Error(`sync: blob store unavailable — ${this.lost}`);
		const tx = this.db.transaction(stores, mode);
		const done = new Promise<void>((resolve, reject) => {
			tx.oncomplete = () => resolve();
			tx.onerror = () => reject(tx.error);
			tx.onabort = () => reject(tx.error ?? new Error('blob store transaction aborted'));
		});
		done.catch(() => {});
		let result: T;
		try {
			result = await fn(tx);
		} catch (err) {
			try {
				tx.abort();
			} catch {
				// already finished
			}
			throw err;
		}
		await done;
		return result;
	}

	async stage(): Promise<string> {
		const bytesId = crypto.randomUUID();
		const rec: StagingRecord = { bytesId, startedAt: Date.now() };
		await this.tx([ STAGING ], 'readwrite', (tx) => request(tx.objectStore(STAGING).put(rec)));
		return bytesId;
	}

	async putChunk(bytesId: string, index: number, data: Blob): Promise<void> {
		const put = (value: Blob | Uint8Array) =>
			this.tx(
				[ CHUNKS ],
				'readwrite',
				(tx) => request(tx.objectStore(CHUNKS).put(value, [ bytesId, index ])),
			);
		if (this.blobValues) {
			try {
				await put(data);
				return;
			} catch (err) {
				// If bytes go where the Blob did not, it was the Blob, and every
				// later chunk goes as bytes.
				await put(await readBlobRange(data, 0, data.size)).catch(() => {
					throw err;
				});
				this.blobValues = false;
				return;
			}
		}
		await put(await readBlobRange(data, 0, data.size));
	}

	async readChunk(bytesId: string, index: number): Promise<Blob | undefined> {
		const v = await this.tx(
			[ CHUNKS ],
			'readonly',
			(tx) => request(tx.objectStore(CHUNKS).get([ bytesId, index ]) as IDBRequest<unknown>),
		);
		if (v === undefined || v instanceof Blob) return v;
		return new Blob([
			(v instanceof ArrayBuffer ? new Uint8Array(v) : v) as Uint8Array<ArrayBuffer>,
		]);
	}

	async dropStaged(bytesId: string): Promise<void> {
		await this.tx([ STAGING, CHUNKS ], 'readwrite', async (tx) => {
			dropStagedIn(tx, bytesId);
		});
	}

	async queue(rec: UploadRecord): Promise<UploadRecord> {
		return this.tx([ UPLOADS, CACHE, CHUNKS, STAGING ], 'readwrite', async (tx) => {
			const uploads = tx.objectStore(UPLOADS);
			const key = [ rec.workspaceId, rec.digest ];
			const existing = await request(uploads.get(key) as IDBRequest<UploadRecord | undefined>);
			if (existing !== undefined) {
				dropStagedIn(tx, rec.bytesId);
				if (existing.state === 'failed') {
					const next: UploadRecord = { ...existing, state: 'queued' };
					delete next.error;
					uploads.put(next);
					return next;
				}
				return existing;
			}
			const cache = tx.objectStore(CACHE);
			const cached = await request(cache.get(key) as IDBRequest<CacheRecord | undefined>);
			let next = rec;
			if (cached !== undefined) {
				dropStagedIn(tx, rec.bytesId);
				cache.delete(key);
				next = { ...rec, bytesId: cached.bytesId };
			} else {
				tx.objectStore(STAGING).delete(rec.bytesId);
			}
			uploads.put(withoutUndefined(next));
			return next;
		});
	}

	async upload(workspaceId: string, digest: string): Promise<UploadRecord | undefined> {
		return this.tx([ UPLOADS ], 'readonly', (tx) =>
			request(
				tx.objectStore(UPLOADS).get([ workspaceId, digest ]) as IDBRequest<
					UploadRecord | undefined
				>,
			));
	}

	async uploads(): Promise<UploadRecord[]> {
		return this.tx(
			[ UPLOADS ],
			'readonly',
			(tx) => request(tx.objectStore(UPLOADS).getAll() as IDBRequest<UploadRecord[]>),
		);
	}

	async updateUpload(rec: UploadRecord): Promise<boolean> {
		return this.tx([ UPLOADS ], 'readwrite', async (tx) => {
			const store = tx.objectStore(UPLOADS);
			const n = await request(store.count([ rec.workspaceId, rec.digest ]));
			if (n === 0) return false;
			store.put(withoutUndefined(rec));
			return true;
		});
	}

	async completeUpload(workspaceId: string, digest: string, usedAt: number): Promise<void> {
		await this.tx([ UPLOADS, CACHE ], 'readwrite', async (tx) => {
			const uploads = tx.objectStore(UPLOADS);
			const key = [ workspaceId, digest ];
			const rec = await request(uploads.get(key) as IDBRequest<UploadRecord | undefined>);
			if (rec === undefined) return;
			uploads.delete(key);
			const cached: CacheRecord = {
				workspaceId,
				digest,
				size: rec.size,
				mediaType: rec.mediaType,
				bytesId: rec.bytesId,
				usedAt,
			};
			tx.objectStore(CACHE).put(cached);
		});
	}

	async removeUpload(workspaceId: string, digest: string): Promise<boolean> {
		return this.tx([ UPLOADS, CHUNKS ], 'readwrite', async (tx) => {
			const uploads = tx.objectStore(UPLOADS);
			const key = [ workspaceId, digest ];
			const rec = await request(uploads.get(key) as IDBRequest<UploadRecord | undefined>);
			if (rec === undefined) return false;
			uploads.delete(key);
			tx.objectStore(CHUNKS).delete(chunkRange(rec.bytesId));
			return true;
		});
	}

	async cached(workspaceId: string, digest: string): Promise<CacheRecord | undefined> {
		return this.tx([ CACHE ], 'readonly', (tx) =>
			request(
				tx.objectStore(CACHE).get([ workspaceId, digest ]) as IDBRequest<CacheRecord | undefined>,
			));
	}

	async touch(workspaceId: string, digest: string, usedAt: number): Promise<void> {
		await this.tx([ CACHE ], 'readwrite', async (tx) => {
			const store = tx.objectStore(CACHE);
			const rec = await request(
				store.get([ workspaceId, digest ]) as IDBRequest<CacheRecord | undefined>,
			);
			if (rec !== undefined) store.put({ ...rec, usedAt });
		});
	}

	async dropCached(workspaceId: string, digest: string, bytesId: string): Promise<void> {
		await this.tx([ CACHE, CHUNKS ], 'readwrite', async (tx) => {
			const cache = tx.objectStore(CACHE);
			const key = [ workspaceId, digest ];
			const rec = await request(cache.get(key) as IDBRequest<CacheRecord | undefined>);
			if (rec?.bytesId !== bytesId) return;
			cache.delete(key);
			tx.objectStore(CHUNKS).delete(chunkRange(bytesId));
		});
	}

	async addCached(rec: CacheRecord): Promise<void> {
		await this.tx([ UPLOADS, CACHE, CHUNKS, STAGING ], 'readwrite', async (tx) => {
			const key = [ rec.workspaceId, rec.digest ];
			const queued = await request(tx.objectStore(UPLOADS).count(key));
			const cached = await request(tx.objectStore(CACHE).count(key));
			if (queued > 0 || cached > 0) {
				dropStagedIn(tx, rec.bytesId);
				return;
			}
			tx.objectStore(STAGING).delete(rec.bytesId);
			tx.objectStore(CACHE).put(rec);
		});
	}

	async evict(budgetBytes: number, keep?: EvictionKeep): Promise<void> {
		await this.tx([ CACHE, CHUNKS ], 'readwrite', async (tx) => {
			const cache = tx.objectStore(CACHE);
			const all = await request(cache.getAll() as IDBRequest<CacheRecord[]>);
			for (const rec of pickEvictions(all, budgetBytes, keep)) {
				cache.delete([ rec.workspaceId, rec.digest ]);
				tx.objectStore(CHUNKS).delete(chunkRange(rec.bytesId));
			}
		});
	}

	async sweep(before: number): Promise<void> {
		await this.tx([ STAGING, CHUNKS ], 'readwrite', async (tx) => {
			const all = await request(tx.objectStore(STAGING).getAll() as IDBRequest<StagingRecord[]>);
			for (const s of all) if (s.startedAt < before) dropStagedIn(tx, s.bytesId);
		});
	}

	async clear(workspaceId?: string): Promise<void> {
		await this.tx([ UPLOADS, CACHE, CHUNKS, STAGING ], 'readwrite', async (tx) => {
			if (workspaceId === undefined) {
				for (const name of [ UPLOADS, CACHE, CHUNKS, STAGING ]) tx.objectStore(name).clear();
				return;
			}
			for (const name of [ UPLOADS, CACHE ]) {
				const store = tx.objectStore(name);
				const all = await request(store.getAll() as IDBRequest<(UploadRecord | CacheRecord)[]>);
				for (const rec of all) {
					if (rec.workspaceId !== workspaceId) continue;
					store.delete([ rec.workspaceId, rec.digest ]);
					tx.objectStore(CHUNKS).delete(chunkRange(rec.bytesId));
				}
			}
		});
	}

	async close(): Promise<void> {
		this.lose('the store was closed');
	}
}

function dropStagedIn(tx: IDBTransaction, bytesId: string): void {
	tx.objectStore(STAGING).delete(bytesId);
	tx.objectStore(CHUNKS).delete(chunkRange(bytesId));
}

/** Structured clone keeps an `undefined` property; IndexedDB records need not. */
function withoutUndefined(rec: UploadRecord): UploadRecord {
	if (rec.error !== undefined) return rec;
	const { error: _error, ...rest } = rec;
	return rest;
}
