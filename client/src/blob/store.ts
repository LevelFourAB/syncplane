// The blob layer's local storage: the upload queue and the download cache, and
// the chunk bytes both of them own.
//
// # Ownership
//
// Chunk bytes live under a random `bytesId`, never under the digest. Each
// bytesId has exactly one owner at a time: a staging marker while put() or a
// download is still writing it, then an upload record or a cache record. Every
// move between owners is one transaction, so a reload at any point leaves
// either a staging marker (swept once it is old, see sweep) or an owner, and
// never bytes that nothing points at.
//
// The bytes cannot be keyed by digest because put() writes each chunk as it
// hashes it, before the digest is known, so a large file is never in memory
// whole.
//
// Chunks are stored as Blob values. A browser keeps a large IndexedDB Blob on
// disk and hands back a disk-backed Blob on read, so composing the stored
// chunks into one Blob for the app costs about one chunk of memory however
// large the blob is.

/** A failure, in the form the queue persists and tabs exchange. */
export interface StoredBlobError {
	code: string;
	message: string;
	transportCode?: string;
	errorCode?: string;
}

/** One blob in the upload queue. */
export interface UploadRecord {
	workspaceId: string;
	digest: string;
	size: number;
	mediaType: string;
	chunkHashes: Uint8Array[];
	bytesId: string;
	/** `Date.now()` when it was queued; the uploader takes the oldest first. */
	createdAt: number;
	state: 'queued' | 'failed';
	error?: StoredBlobError;
}

/** One blob in the download cache. */
export interface CacheRecord {
	workspaceId: string;
	digest: string;
	size: number;
	mediaType: string;
	bytesId: string;
	/** `Date.now()` of the last read; eviction drops the smallest first. */
	usedAt: number;
}

/**
 * Local storage for queued uploads and cached downloads, shared by every tab
 * of an account where the backend allows it. Each method is atomic.
 */
export interface BlobStore {
	/** Reserves a new bytesId for put() or a download to write chunks under. */
	stage(): Promise<string>;
	putChunk(bytesId: string, index: number, data: Blob): Promise<void>;
	readChunk(bytesId: string, index: number): Promise<Blob | undefined>;
	/** Deletes a staged bytesId and its chunks. */
	dropStaged(bytesId: string): Promise<void>;
	/**
	 * Queues `rec`, whose bytes are staged under `rec.bytesId`, and returns the
	 * record in effect afterwards. A blob already queued keeps its record, set
	 * back to `queued` when it had failed; a cached blob moves its bytes to the
	 * queue. Either way the staged bytes are dropped.
	 */
	queue(rec: UploadRecord): Promise<UploadRecord>;
	upload(workspaceId: string, digest: string): Promise<UploadRecord | undefined>;
	uploads(): Promise<UploadRecord[]>;
	/** Replaces an upload record that still exists; false when it is gone. */
	updateUpload(rec: UploadRecord): Promise<boolean>;
	/** Moves a queued blob's bytes to the cache. No-op when it is not queued. */
	completeUpload(workspaceId: string, digest: string, usedAt: number): Promise<void>;
	/** Deletes a queued blob and its bytes; false when it was not queued. */
	removeUpload(workspaceId: string, digest: string): Promise<boolean>;
	cached(workspaceId: string, digest: string): Promise<CacheRecord | undefined>;
	touch(workspaceId: string, digest: string, usedAt: number): Promise<void>;
	/**
	 * Caches a download whose bytes are staged under `rec.bytesId`. Drops the
	 * staged bytes instead when the blob is already queued or cached.
	 */
	addCached(rec: CacheRecord): Promise<void>;
	/**
	 * Deletes cached blobs, least recently used first, until the cache holds at
	 * most `budgetBytes`. Queued uploads do not count and are never deleted.
	 * Neither are cached blobs read after `keep.usedSince`, or whose
	 * bytesId is in `keep.bytesIds`; the cache can stay above the budget by
	 * what they hold.
	 */
	evict(budgetBytes: number, keep?: EvictionKeep): Promise<void>;
	/** Deletes staged bytes reserved before `before` (a `Date.now()` value). */
	sweep(before: number): Promise<void>;
	/** Deletes everything, or everything of one workspace. */
	clear(workspaceId?: string): Promise<void>;
	close(): Promise<void>;
}

/** Cached blobs that eviction must leave in place. */
export interface EvictionKeep {
	usedSince?: number;
	bytesIds?: ReadonlySet<string>;
}

/** The map key of one blob. */
export function blobKey(workspaceId: string, digest: string): string {
	return `${workspaceId}\u0000${digest}`;
}

/** In-process BlobStore. Holds every byte in memory; nothing survives the process. */
export class MemoryBlobStore implements BlobStore {
	private readonly staging = new Map<string, number>();
	private readonly chunks = new Map<string, Map<number, Blob>>();
	private readonly queueMap = new Map<string, UploadRecord>();
	private readonly cache = new Map<string, CacheRecord>();

	async stage(): Promise<string> {
		const id = crypto.randomUUID();
		this.staging.set(id, Date.now());
		return id;
	}

	async putChunk(bytesId: string, index: number, data: Blob): Promise<void> {
		let m = this.chunks.get(bytesId);
		if (m === undefined) {
			m = new Map();
			this.chunks.set(bytesId, m);
		}
		m.set(index, data);
	}

	async readChunk(bytesId: string, index: number): Promise<Blob | undefined> {
		return this.chunks.get(bytesId)?.get(index);
	}

	async dropStaged(bytesId: string): Promise<void> {
		this.staging.delete(bytesId);
		this.chunks.delete(bytesId);
	}

	async queue(rec: UploadRecord): Promise<UploadRecord> {
		const key = blobKey(rec.workspaceId, rec.digest);
		const existing = this.queueMap.get(key);
		if (existing !== undefined) {
			await this.dropStaged(rec.bytesId);
			if (existing.state === 'failed') {
				const next: UploadRecord = { ...existing, state: 'queued', error: undefined };
				this.queueMap.set(key, next);
				return clone(next);
			}
			return clone(existing);
		}
		const cached = this.cache.get(key);
		let next = clone(rec);
		if (cached !== undefined) {
			await this.dropStaged(rec.bytesId);
			this.cache.delete(key);
			next = { ...next, bytesId: cached.bytesId };
		} else {
			this.staging.delete(rec.bytesId);
		}
		this.queueMap.set(key, next);
		return clone(next);
	}

	async upload(workspaceId: string, digest: string): Promise<UploadRecord | undefined> {
		const rec = this.queueMap.get(blobKey(workspaceId, digest));
		return rec && clone(rec);
	}

	async uploads(): Promise<UploadRecord[]> {
		return [ ...this.queueMap.values() ].map(clone);
	}

	async updateUpload(rec: UploadRecord): Promise<boolean> {
		const key = blobKey(rec.workspaceId, rec.digest);
		if (!this.queueMap.has(key)) return false;
		this.queueMap.set(key, clone(rec));
		return true;
	}

	async completeUpload(workspaceId: string, digest: string, usedAt: number): Promise<void> {
		const key = blobKey(workspaceId, digest);
		const rec = this.queueMap.get(key);
		if (rec === undefined) return;
		this.queueMap.delete(key);
		this.cache.set(key, {
			workspaceId,
			digest,
			size: rec.size,
			mediaType: rec.mediaType,
			bytesId: rec.bytesId,
			usedAt,
		});
	}

	async removeUpload(workspaceId: string, digest: string): Promise<boolean> {
		const key = blobKey(workspaceId, digest);
		const rec = this.queueMap.get(key);
		if (rec === undefined) return false;
		this.queueMap.delete(key);
		this.chunks.delete(rec.bytesId);
		return true;
	}

	async cached(workspaceId: string, digest: string): Promise<CacheRecord | undefined> {
		const rec = this.cache.get(blobKey(workspaceId, digest));
		return rec && { ...rec };
	}

	async touch(workspaceId: string, digest: string, usedAt: number): Promise<void> {
		const rec = this.cache.get(blobKey(workspaceId, digest));
		if (rec !== undefined) rec.usedAt = usedAt;
	}

	async addCached(rec: CacheRecord): Promise<void> {
		const key = blobKey(rec.workspaceId, rec.digest);
		if (this.queueMap.has(key) || this.cache.has(key)) {
			await this.dropStaged(rec.bytesId);
			return;
		}
		this.staging.delete(rec.bytesId);
		this.cache.set(key, { ...rec });
	}

	async evict(budgetBytes: number, keep?: EvictionKeep): Promise<void> {
		const doomed = pickEvictions([ ...this.cache.values() ], budgetBytes, keep);
		for (const rec of doomed) {
			this.cache.delete(blobKey(rec.workspaceId, rec.digest));
			this.chunks.delete(rec.bytesId);
		}
	}

	async sweep(before: number): Promise<void> {
		for (const [ id, at ] of this.staging) {
			if (at < before) await this.dropStaged(id);
		}
	}

	async clear(workspaceId?: string): Promise<void> {
		for (const [ key, rec ] of this.queueMap) {
			if (workspaceId !== undefined && rec.workspaceId !== workspaceId) continue;
			this.queueMap.delete(key);
			this.chunks.delete(rec.bytesId);
		}
		for (const [ key, rec ] of this.cache) {
			if (workspaceId !== undefined && rec.workspaceId !== workspaceId) continue;
			this.cache.delete(key);
			this.chunks.delete(rec.bytesId);
		}
		if (workspaceId === undefined) {
			for (const id of this.staging.keys()) this.chunks.delete(id);
			this.staging.clear();
		}
	}

	async close(): Promise<void> {}
}

/**
 * The cache records to delete so the rest fit in `budgetBytes`, oldest first,
 * passing over the ones `keep` names.
 */
export function pickEvictions(
	records: CacheRecord[],
	budgetBytes: number,
	keep?: EvictionKeep,
): CacheRecord[] {
	let total = records.reduce((sum, r) => sum + r.size, 0);
	if (total <= budgetBytes) return [];
	const out: CacheRecord[] = [];
	for (const rec of [ ...records ].sort((a, b) => a.usedAt - b.usedAt)) {
		if (total <= budgetBytes) break;
		if (keep?.usedSince !== undefined && rec.usedAt > keep.usedSince) continue;
		if (keep?.bytesIds?.has(rec.bytesId)) continue;
		out.push(rec);
		total -= rec.size;
	}
	return out;
}

function clone(rec: UploadRecord): UploadRecord {
	return { ...rec, chunkHashes: rec.chunkHashes.map((h) => h.slice()) };
}
