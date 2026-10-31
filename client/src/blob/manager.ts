// The blob layer's client half: the upload queue's uploader, downloads and
// the cache, and the per-blob status that apps watch.
//
// Like the presence plane (sync/presence.ts) it is a separate service over the
// client's own transport, and shares nothing with the data plane but that
// transport. It never takes the KV mutex.
//
// # One uploader per account
//
// The queue is shared by every tab of the account (one IndexedDB database), so
// exactly one tab sends it: the holder of a Web Lock (StorageNames.blobLock).
// The term is not tied to visibility, because an upload in a hidden tab is
// still progress; it ends when the client stops or the page freezes. Where Web
// Locks are missing, or on a server runtime where each client holds a private
// store, every client sends its own queue.
//
// # Status across tabs
//
// The durable part of a status (`queued`, `failed`) is the queue record, which
// every tab reads. The live part — progress, a completion, a removal — goes
// out on a BroadcastChannel (StorageNames.blobChannel), and every tab folds it
// into its own memory and re-reads the blobs it watches. Messages are hints:
// a tab that misses one still converges on its next read of the store or the
// server.
//
// # Waiting for a blob another device uploads
//
// While this tab watches blobs that are `pending`, it holds one WatchBlobs
// stream per workspace for them, and shares each completion it hears with the
// other tabs like any other status change. The stream is per tab, not per
// account: the uploader's lock elects a tab for the queue, which need not be a
// tab that watches anything. Re-checks after a sync cycle and on a timer run
// only for blobs no working stream covers: a server without WatchBlobs, a
// stream waiting out a failure, or more than WATCH_MAX_DIGESTS blobs in one
// workspace.

import { create } from '@bufbuild/protobuf';
import { type Client, Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';

import {
	CodeBlobCorrupt,
	CodeBlobRefused,
	CodeBlobUnavailable,
	CodeWorkspaceAccessRevoked,
	CodeWorkspaceNoAccess,
	ErrorCodeMetadataKey,
} from '../codes.js';
import { isServerRuntime } from '../env.js';
import {
	type BlobRef,
	BlobRefSchema,
	BlobService,
	BlobState,
} from '../gen/syncplane/v1/blob_pb.js';
import { reportError, safeInvoke } from '../report.js';
import { Backoff, sleep } from '../sync/backoff.js';
import {
	BlobError,
	type BlobProgress,
	type BlobPutOptions,
	type BlobRefLike,
	type BlobStatus,
} from './api.js';
import {
	chunkCount,
	chunkLength,
	digestOf,
	hashBlob,
	hashChunk,
	isBlobDigest,
	readBlobRange,
	sameBytes,
} from './digest.js';
import {
	blobKey,
	type BlobStore,
	type EvictionKeep,
	type StoredBlobError,
	type UploadRecord,
} from './store.js';

/** Options for the blob manager. */
export interface BlobManagerOptions {
	transport: () => Promise<Transport>;
	/** Opens the store; called once, on first use. */
	openStore: () => Promise<BlobStore>;
	/** Web Lock whose holder runs the uploader. */
	lockName: string;
	/** BroadcastChannel the account's tabs share status on. */
	channelName: string;
	/** Byte budget of the download cache. */
	cacheBytes: number;
	/** How long after its last read a cached blob is safe from eviction, in milliseconds. */
	evictionGraceMs?: number;
	/** First and largest retry window of the uploader, in milliseconds. */
	minBackoffMs?: number;
	maxBackoffMs?: number;
	/**
	 * First and largest retry window of an upload the server refused because
	 * the caller holds no grant in the workspace yet, in milliseconds.
	 */
	minNoAccessBackoffMs?: number;
	maxNoAccessBackoffMs?: number;
	/** How long a change of the watched pending set waits before the streams reopen, in milliseconds. */
	watchDebounceMs?: number;
	/** First and largest window between checks of a watched `pending` blob, in milliseconds. */
	minRecheckMs?: number;
	maxRecheckMs?: number;
}

/** The default byte budget of the download cache: 256 MiB. */
export const DEFAULT_BLOB_CACHE_BYTES = 256 * 1024 * 1024;

/** The largest digest count one StatBlobs request carries. */
const STAT_BATCH = 256;

/**
 * Staged bytes older than this belong to a put() or a download that a reload
 * cut off; no live writer takes this long between two chunks.
 */
const STAGING_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * How often an idle uploader looks at the queue on its own: the safety net for
 * a put() in a tab whose wake message it did not receive.
 */
const IDLE_RESCAN_MS = 30_000;

/**
 * How long after its last read a cached blob is safe from eviction by any tab:
 * long enough for a Blob that get() returned to be rendered or read.
 */
const EVICTION_GRACE_MS = 10 * 60 * 1000;

/** The largest digest count one WatchBlobs request carries. */
const WATCH_MAX_DIGESTS = 256;

/** How many times one upload restarts from BeginUpload before it backs off. */
const MAX_RESTARTS = 3;

/** The status of one blob, in the form tabs exchange. Structured-clone safe. */
type SharedStatus =
	| { state: 'queued' }
	| { state: 'uploading'; progress: BlobProgress }
	| { state: 'uploaded' }
	| { state: 'failed'; error: StoredBlobError }
	/** The queue entry was discarded or purged. */
	| { state: 'removed' };

interface BlobMessage {
	from: string;
	workspaceId: string;
	digest: string;
	status: SharedStatus;
}

interface Listener {
	cb: (status: BlobStatus) => void;
	/** The status this listener was last given. */
	seen?: BlobStatus;
}

/** Every watcher of one blob. */
interface Watched {
	workspaceId: string;
	digest: string;
	listeners: Set<Listener>;
	/** The status last resolved. */
	last?: BlobStatus;
	/** Counts resolutions, so a slow one cannot overwrite a newer one. */
	version: number;
}

/** Reads one blob's chunks in order; see BlobManager.openReader. */
interface ChunkReader {
	count: number;
	/** Where a remote reader stores what it downloads. */
	bytesId?: string;
	read(index: number): Promise<Uint8Array>;
	/** Called after the last chunk was read. */
	finish(): Promise<void>;
	/** Called instead of finish() when the read stops early. */
	abort(): Promise<void>;
}

/** One WatchBlobs stream, for the pending blobs of one workspace. */
interface WatchStream {
	workspaceId: string;
	/** The digests it watches, sorted. */
	digests: string[];
	abort: AbortController;
	/** True from its first message until it fails. */
	healthy: boolean;
}

interface StatWaiter {
	resolve: (state: BlobState) => void;
	reject: (err: unknown) => void;
}

const PERMANENT_CODES = new Set([
	Code.InvalidArgument,
	Code.ResourceExhausted,
	Code.PermissionDenied,
	Code.Unimplemented,
]);

/**
 * Owns one account's blob queue, cache and status. One per SyncClient.
 *
 * @internal
 */
export class BlobManager {
	private readonly opts: Required<BlobManagerOptions>;
	private readonly tabId = crypto.randomUUID();
	private storePromise?: Promise<BlobStore>;
	private client?: Client<typeof BlobService>;
	private channel?: BroadcastChannel;
	private closed = false;
	private started = false;
	private swept = false;

	/** The running uploader term, while started. */
	private uploader?: { abort: AbortController; done: Promise<void> };
	/** Wakes an idle uploader loop. */
	private wake?: () => void;
	/**
	 * Set by kick() while the loop is not waiting, so a kick between its read
	 * of the queue and its wait is not lost.
	 */
	private kicked = false;
	/**
	 * Uploads that failed transiently, by blob key, with the time before which
	 * the uploader passes them over. Kept per upload, so one that keeps failing
	 * does not hold back the rest of the queue.
	 */
	private readonly retries = new Map<
		string,
		{ workspaceId: string; backoff: Backoff; notBefore: number; noAccess: boolean }
	>();
	/** Page lifecycle listeners added by start(), removed by stop(). */
	private readonly lifecycle: Array<[EventTarget, string, () => void]> = [];

	/** Uploads in flight, here or in the tab that runs the uploader, by blob key. */
	private readonly progress = new Map<string, BlobProgress>();
	/** Blobs the server reported complete. A blob never stops being complete. */
	private readonly complete = new Set<string>();
	private readonly watched = new Map<string, Watched>();
	private readonly downloads = new Map<string, Promise<Blob>>();
	/** Cached bytesIds this tab's eviction passes over, with a count of holders; see pin(). */
	private readonly pins = new Map<string, number>();
	private readonly unpinOnCollect = typeof FinalizationRegistry === 'undefined'
		? undefined
		: new FinalizationRegistry<() => void>((release) => release());
	/** Settled by every status change; see changed(). */
	private readonly changeWaiters = new Set<{ resolve: () => void; reject: (e: Error) => void }>();
	/** uploaded() calls still waiting, rejected by close(). */
	private readonly closeWaiters = new Set<(err: Error) => void>();

	private statQueue = new Map<string, Map<string, StatWaiter[]>>();
	private statScheduled = false;

	/** Open WatchBlobs streams, by workspace. */
	private readonly streams = new Map<string, WatchStream>();
	private streamSyncTimer?: ReturnType<typeof setTimeout>;
	/** The server has no WatchBlobs: re-checks are all there is. */
	private watchUnsupported = false;
	/** Workspaces whose access was revoked: nothing is watched or re-checked there. */
	private readonly revoked = new Set<string>();

	private recheckTimer?: ReturnType<typeof setTimeout>;
	private readonly recheckBackoff: Backoff;

	constructor(opts: BlobManagerOptions) {
		this.opts = {
			minBackoffMs: 1_000,
			maxBackoffMs: 30_000,
			evictionGraceMs: EVICTION_GRACE_MS,
			minNoAccessBackoffMs: 5_000,
			maxNoAccessBackoffMs: 300_000,
			watchDebounceMs: 50,
			minRecheckMs: 2_000,
			maxRecheckMs: 60_000,
			...opts,
		};
		this.recheckBackoff = new Backoff(this.opts.minRecheckMs, this.opts.maxRecheckMs);
	}

	// ---------------------------------------------------------------- lifecycle

	/** Starts the uploader. Idempotent. */
	start(): void {
		if (this.closed || this.started) return;
		this.started = true;
		this.openChannel();
		this.startTerm();
		this.listenLifecycle();
		if (!this.swept) {
			this.swept = true;
			void this.store()
				.then((s) => s.sweep(Date.now() - STAGING_TTL_MS))
				.catch((err: unknown) => reportError('blob staging sweep', err));
		}
		if (this.anyPendingUncovered()) this.scheduleRecheck();
		this.scheduleStreamSync();
	}

	/**
	 * Stops the uploader, aborting the chunk in flight, and the timed re-checks.
	 * The queue is kept; start() resumes it.
	 */
	async stop(): Promise<void> {
		if (!this.started) return;
		this.started = false;
		for (const [ target, type, fn ] of this.lifecycle.splice(0)) target.removeEventListener(type, fn);
		this.clearRecheck();
		this.closeStreams();
		await this.endTerm();
	}

	/** Stops, releases the lock and the channel, and closes the store. Idempotent. */
	async close(): Promise<void> {
		if (this.closed) return;
		await this.stop();
		this.closed = true;
		this.channel?.close();
		this.channel = undefined;
		const closedErr = new Error('SyncClient is closed');
		for (const w of [ ...this.changeWaiters ]) w.reject(closedErr);
		for (const reject of [ ...this.closeWaiters ]) reject(closedErr);
		this.changeWaiters.clear();
		this.closeWaiters.clear();
		this.watched.clear();
		if (this.storePromise) {
			const store = await this.storePromise.catch(() => undefined);
			await store?.close();
		}
	}

	/**
	 * Deletes the queue and the cache of one workspace, or of every workspace.
	 * Uploads in flight stop after their current chunk.
	 */
	async clear(workspaceId?: string): Promise<void> {
		const store = await this.store();
		const queued = (await store.uploads()).filter((r) =>
			workspaceId === undefined || r.workspaceId === workspaceId
		);
		await store.clear(workspaceId);
		for (const key of [ ...this.complete ]) {
			if (workspaceId === undefined || key.startsWith(`${workspaceId}\u0000`)) {
				this.complete.delete(key);
			}
		}
		for (const rec of queued) this.publish(rec.workspaceId, rec.digest, { state: 'removed' });
		for (const w of this.watched.values()) {
			if (workspaceId === undefined || w.workspaceId === workspaceId) this.refresh(w);
		}
	}

	private assertOpen(): void {
		if (this.closed) throw new Error('SyncClient is closed');
	}

	private store(): Promise<BlobStore> {
		if (!this.storePromise) {
			const p = this.opts.openStore();
			this.storePromise = p;
			p.catch(() => {
				if (this.storePromise === p) this.storePromise = undefined;
			});
		}
		return this.storePromise;
	}

	private async rpc(): Promise<Client<typeof BlobService>> {
		// The client's own transport, so its interceptors (authentication) and
		// tunnel apply to blob traffic too.
		this.client ??= createClient(BlobService, await this.opts.transport());
		return this.client;
	}

	// ------------------------------------------------------------------ the API

	async put(workspaceId: string, data: Blob, opts?: BlobPutOptions): Promise<BlobRef> {
		this.assertOpen();
		this.openChannel();
		const store = await this.store();
		const bytesId = await store.stage();
		let hashed: { digest: string; chunkHashes: Uint8Array[] };
		try {
			// The stored chunk is a Blob of the bytes that were hashed, not a slice
			// of `data`: a File changed on disk after this read must not reach
			// the queue under the old digest.
			hashed = await hashBlob(
				data,
				(index, bytes) =>
					store.putChunk(bytesId, index, new Blob([ bytes as Uint8Array<ArrayBuffer> ])),
			);
		} catch (err) {
			await store.dropStaged(bytesId).catch(() => {});
			throw err;
		}
		const mediaType = opts?.mediaType ?? data.type;
		const rec = await store.queue({
			workspaceId,
			digest: hashed.digest,
			size: data.size,
			mediaType,
			chunkHashes: hashed.chunkHashes,
			bytesId,
			createdAt: Date.now(),
			state: 'queued',
		});
		// Only the uploader's tab knows what is in flight; a re-put of a blob it
		// is sending must not reset the progress others show.
		if (!this.progress.has(blobKey(workspaceId, rec.digest))) {
			this.publish(workspaceId, rec.digest, { state: 'queued' });
		}
		this.kick();
		return create(BlobRefSchema, { digest: hashed.digest, size: BigInt(data.size), mediaType });
	}

	status(workspaceId: string, ref: BlobRefLike): Promise<BlobStatus> {
		try {
			this.assertOpen();
			checkRef(ref);
		} catch (err) {
			return Promise.reject(err);
		}
		return this.resolve(workspaceId, ref.digest);
	}

	watch(workspaceId: string, ref: BlobRefLike, cb: (status: BlobStatus) => void): () => void {
		this.assertOpen();
		checkRef(ref);
		this.openChannel();
		const key = blobKey(workspaceId, ref.digest);
		let w = this.watched.get(key);
		const fresh = w === undefined;
		if (w === undefined) {
			w = { workspaceId, digest: ref.digest, listeners: new Set(), version: 0 };
			this.watched.set(key, w);
		}
		const listener: Listener = { cb };
		w.listeners.add(listener);
		const entry = w;
		if (fresh) this.refresh(entry);
		else if (entry.last !== undefined) queueMicrotask(() => this.deliver(entry));
		return () => {
			entry.listeners.delete(listener);
			if (entry.listeners.size === 0 && this.watched.get(key) === entry) {
				this.watched.delete(key);
				if (entry.last?.state === 'pending') this.scheduleStreamSync();
			}
		};
	}

	uploaded(workspaceId: string, ref: BlobRefLike): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			let unsub: (() => void) | undefined;
			let settled = false;
			const onClose = (err: Error): void => finish(() => reject(err));
			const finish = (fn: () => void): void => {
				if (settled) return;
				settled = true;
				unsub?.();
				this.closeWaiters.delete(onClose);
				fn();
			};
			try {
				unsub = this.watch(workspaceId, ref, (s) => {
					if (s.state === 'uploaded') finish(resolve);
					else if (s.state === 'failed') finish(() => reject(s.error));
				});
			} catch (err) {
				reject(err);
				return;
			}
			this.closeWaiters.add(onClose);
		});
	}

	async get(workspaceId: string, ref: BlobRefLike): Promise<Blob> {
		this.assertOpen();
		checkRef(ref);
		const key = blobKey(workspaceId, ref.digest);
		const inflight = this.downloads.get(key);
		if (inflight) return inflight;
		const p = this.read(workspaceId, ref).finally(() => this.downloads.delete(key));
		this.downloads.set(key, p);
		return p;
	}

	async discard(workspaceId: string, ref: BlobRefLike): Promise<void> {
		this.assertOpen();
		checkRef(ref);
		const store = await this.store();
		if (await store.removeUpload(workspaceId, ref.digest)) {
			this.publish(workspaceId, ref.digest, { state: 'removed' });
		}
	}

	async pendingCount(): Promise<number> {
		this.assertOpen();
		const store = await this.store();
		return (await store.uploads()).filter((r) => r.state === 'queued').length;
	}

	async idle(): Promise<void> {
		this.assertOpen();
		this.openChannel();
		for (;;) {
			// Registered before the count, so a change between the two is not lost.
			const next = this.nextChange();
			if ((await this.pendingCount()) === 0) {
				next.cancel();
				return;
			}
			await next.promise;
		}
	}

	/**
	 * Re-checks the watched `pending` blobs of `workspaceId`, or of every
	 * workspace: a sync cycle or another tab's commit may have brought the row
	 * whose blob completed.
	 */
	noteSync(workspaceId?: string): void {
		if (this.closed) return;
		for (const w of this.watched.values()) {
			if (workspaceId !== undefined && w.workspaceId !== workspaceId) continue;
			if (w.last?.state === 'pending' && !this.covered(w)) this.refresh(w);
		}
	}

	/**
	 * Reacts to a change of what the caller holds in `workspaceId`: tries the
	 * uploads that wait for a grant again, and, when the workspace was
	 * revoked, watches and re-checks its pending blobs again.
	 */
	accessChanged(workspaceId: string): void {
		this.retryNoAccess(workspaceId);
		if (!this.revoked.delete(workspaceId)) return;
		this.scheduleStreamSync();
		this.noteSync(workspaceId);
	}

	/**
	 * Tries again, at once, each upload into `workspaceId` that waits for a
	 * grant: what the caller holds there changed.
	 */
	retryNoAccess(workspaceId: string): void {
		let any = false;
		for (const retry of this.retries.values()) {
			if (!retry.noAccess || retry.workspaceId !== workspaceId) continue;
			retry.notBefore = 0;
			any = true;
		}
		if (any) this.kick();
	}

	// ------------------------------------------------------------------ status

	/** The status of one blob: local queue, then memory and cache, then the server. */
	private async resolve(workspaceId: string, digest: string): Promise<BlobStatus> {
		const key = blobKey(workspaceId, digest);
		const store = await this.store();
		const rec = await store.upload(workspaceId, digest);
		if (rec !== undefined) {
			if (rec.state === 'failed') {
				return { state: 'failed', error: toBlobError(workspaceId, digest, rec.error) };
			}
			const p = this.progress.get(key);
			return p ? { state: 'uploading', progress: { ...p } } : { state: 'queued' };
		}
		if (this.complete.has(key) || (await store.cached(workspaceId, digest)) !== undefined) {
			return { state: 'uploaded' };
		}
		try {
			const state = await this.stat(workspaceId, digest);
			if (state === BlobState.COMPLETE) {
				this.complete.add(key);
				return { state: 'uploaded' };
			}
		} catch {
			// Unreachable server: the blob is not here, which is what pending means.
		}
		return { state: 'pending' };
	}

	/** Resolves `w` again and delivers the result when it differs. */
	private refresh(w: Watched): void {
		const version = ++w.version;
		void this.resolve(w.workspaceId, w.digest).then(
			(status) => {
				if (version !== w.version || this.closed) return;
				const wasPending = w.last?.state === 'pending';
				if (w.last === undefined || !sameStatus(w.last, status)) w.last = status;
				this.deliver(w);
				if (wasPending !== (status.state === 'pending')) this.scheduleStreamSync();
				if (status.state === 'pending' && !this.covered(w)) this.scheduleRecheck();
			},
			(err: unknown) => reportError('blob status', err),
		);
	}

	private deliver(w: Watched): void {
		const status = w.last;
		if (status === undefined) return;
		for (const l of [ ...w.listeners ]) {
			if (l.seen === status) continue;
			l.seen = status;
			safeInvoke(l.cb, status, 'blob watch callback');
		}
	}

	/** Applies a status change here, shares it with the other tabs, and re-reads watchers. */
	private publish(workspaceId: string, digest: string, status: SharedStatus): void {
		this.apply(workspaceId, digest, status);
		try {
			this.channel?.postMessage(
				{ from: this.tabId, workspaceId, digest, status } satisfies BlobMessage,
			);
		} catch {
			// closed channel — racing close(); this tab already applied it
		}
	}

	private apply(workspaceId: string, digest: string, status: SharedStatus): void {
		const key = blobKey(workspaceId, digest);
		if (status.state === 'uploading') this.progress.set(key, status.progress);
		else this.progress.delete(key);
		if (status.state === 'uploaded') this.complete.add(key);
		const w = this.watched.get(key);
		if (w) this.refresh(w);
		this.changed();
	}

	/** Settles everything waiting for the next status change (idle()). */
	private changed(): void {
		for (const w of [ ...this.changeWaiters ]) w.resolve();
	}

	private nextChange(): { promise: Promise<void>; cancel: () => void } {
		let waiter!: { resolve: () => void; reject: (e: Error) => void };
		let timer: ReturnType<typeof setTimeout> | undefined;
		const promise = new Promise<void>((resolve, reject) => {
			const done = (fn: () => void): void => {
				clearTimeout(timer);
				this.changeWaiters.delete(waiter);
				fn();
			};
			waiter = { resolve: () => done(resolve), reject: (e) => done(() => reject(e)) };
			// Another tab's upload may finish without a message reaching this one.
			timer = setTimeout(waiter.resolve, IDLE_RESCAN_MS);
		});
		this.changeWaiters.add(waiter);
		return {
			promise,
			cancel: () => {
				clearTimeout(timer);
				this.changeWaiters.delete(waiter);
			},
		};
	}

	/** Whether some watched blob is pending with no working stream covering it. */
	private anyPendingUncovered(): boolean {
		for (const w of this.watched.values()) {
			if (w.last?.state === 'pending' && !this.covered(w)) return true;
		}
		return false;
	}

	/**
	 * Whether `w` needs no re-check: a working WatchBlobs stream holds it, or
	 * its workspace was revoked and nothing about it can change.
	 */
	private covered(w: Watched): boolean {
		if (this.revoked.has(w.workspaceId)) return true;
		const stream = this.streams.get(w.workspaceId);
		return stream !== undefined && stream.healthy && stream.digests.includes(w.digest);
	}

	// ------------------------------------------------------------- WatchBlobs

	private scheduleStreamSync(): void {
		if (this.closed || this.streamSyncTimer !== undefined) return;
		this.streamSyncTimer = setTimeout(() => {
			this.streamSyncTimer = undefined;
			this.syncStreams();
		}, this.opts.watchDebounceMs);
	}

	/** Opens, reopens and closes streams to match the pending blobs watched now. */
	private syncStreams(): void {
		if (this.closed || !this.started || this.watchUnsupported) {
			this.closeStreams();
			return;
		}
		const want = new Map<string, string[]>();
		for (const w of this.watched.values()) {
			if (w.last?.state !== 'pending' || this.revoked.has(w.workspaceId)) continue;
			const list = want.get(w.workspaceId) ?? [];
			list.push(w.digest);
			want.set(w.workspaceId, list);
		}
		for (const [ ws, list ] of want) {
			want.set(ws, [ ...new Set(list) ].sort().slice(0, WATCH_MAX_DIGESTS));
		}
		for (const [ ws, stream ] of this.streams) {
			if ((want.get(ws) ?? []).join() === stream.digests.join()) {
				want.delete(ws);
				continue;
			}
			stream.abort.abort();
			this.streams.delete(ws);
		}
		for (const [ ws, digests ] of want) {
			const stream: WatchStream = {
				workspaceId: ws,
				digests,
				abort: new AbortController(),
				healthy: false,
			};
			this.streams.set(ws, stream);
			void this.runStream(stream);
		}
	}

	private closeStreams(): void {
		clearTimeout(this.streamSyncTimer);
		this.streamSyncTimer = undefined;
		for (const stream of this.streams.values()) stream.abort.abort();
		this.streams.clear();
	}

	private async runStream(stream: WatchStream): Promise<void> {
		const { workspaceId, digests, abort } = stream;
		const signal = abort.signal;
		const backoff = new Backoff(this.opts.minBackoffMs, this.opts.maxBackoffMs);
		try {
			while (!signal.aborted) {
				try {
					const rpc = await this.rpc();
					for await (const res of rpc.watchBlobs({ workspaceId, digests }, { signal })) {
						stream.healthy = true;
						backoff.reset();
						for (const b of res.blobs) {
							if (b.state === BlobState.COMPLETE && !this.complete.has(blobKey(workspaceId, b.digest))) {
								this.publish(workspaceId, b.digest, { state: 'uploaded' });
							}
						}
					}
					// The server ends the stream once it reported every digest complete.
					return;
				} catch (err) {
					if (signal.aborted) return;
					stream.healthy = false;
					const ce = ConnectError.from(err);
					if (ce.code === Code.Unimplemented) {
						this.watchUnsupported = true;
						this.closeStreams();
						this.fallBack(workspaceId);
						return;
					}
					if (
						ce.code === Code.PermissionDenied
						&& ce.metadata.get(ErrorCodeMetadataKey) === CodeWorkspaceAccessRevoked
					) {
						this.revoked.add(workspaceId);
						return;
					}
					// Transient: the re-checks stand in until the stream is back.
					this.fallBack(workspaceId);
					await sleep(backoff.next(), signal);
				}
			}
		} finally {
			if (this.streams.get(workspaceId) === stream) this.streams.delete(workspaceId);
			if (!signal.aborted) this.scheduleStreamSync();
		}
	}

	/** Re-checks the pending blobs of `workspaceId` now, and on the timer after. */
	private fallBack(workspaceId: string): void {
		this.noteSync(workspaceId);
		this.scheduleRecheck();
	}

	private scheduleRecheck(): void {
		if (!this.started || this.recheckTimer !== undefined) return;
		this.recheckTimer = setTimeout(() => {
			this.recheckTimer = undefined;
			if (!this.anyPendingUncovered()) {
				this.recheckBackoff.reset();
				return;
			}
			this.noteSync();
			this.scheduleRecheck();
		}, this.recheckBackoff.next());
	}

	private clearRecheck(): void {
		clearTimeout(this.recheckTimer);
		this.recheckTimer = undefined;
	}

	/** Asks the server for one blob's state, batched with the others asked in the same turn. */
	private stat(workspaceId: string, digest: string): Promise<BlobState> {
		return new Promise<BlobState>((resolve, reject) => {
			let byDigest = this.statQueue.get(workspaceId);
			if (byDigest === undefined) {
				byDigest = new Map();
				this.statQueue.set(workspaceId, byDigest);
			}
			const waiters = byDigest.get(digest) ?? [];
			waiters.push({ resolve, reject });
			byDigest.set(digest, waiters);
			if (!this.statScheduled) {
				this.statScheduled = true;
				queueMicrotask(() => void this.flushStats());
			}
		});
	}

	private async flushStats(): Promise<void> {
		this.statScheduled = false;
		const queue = this.statQueue;
		this.statQueue = new Map();
		for (const [ workspaceId, byDigest ] of queue) {
			const digests = [ ...byDigest.keys() ];
			for (let i = 0; i < digests.length; i += STAT_BATCH) {
				const batch = digests.slice(i, i + STAT_BATCH);
				try {
					const rpc = await this.rpc();
					const res = await rpc.statBlobs({ workspaceId, digests: batch });
					const states = new Map(res.blobs.map((b) => [ b.digest, b.state ]));
					for (const d of batch) {
						for (const w of byDigest.get(d) ?? []) w.resolve(states.get(d) ?? BlobState.MISSING);
					}
				} catch (err) {
					for (const d of batch) for (const w of byDigest.get(d) ?? []) w.reject(err);
				}
			}
		}
	}

	// -------------------------------------------------------------- downloads

	/**
	 * Keeps the cached bytes under `bytesId` from eviction for as long as
	 * `holder` is reachable.
	 *
	 * Chromium and Firefox keep the file behind a Blob read from IndexedDB
	 * alive while any Blob refers to it, even after its record is deleted; the
	 * WebKit code was not found to promise the same. Pinning bytes the app may
	 * still be reading is what keeps WebKit safe, and costs nothing elsewhere.
	 * A pin only stops this tab's evictions; another tab's are held off by
	 * EVICTION_GRACE_MS, measured from the last read.
	 */
	private pin(bytesId: string, holder?: object): () => void {
		this.pins.set(bytesId, (this.pins.get(bytesId) ?? 0) + 1);
		let released = false;
		const release = (): void => {
			if (released) return;
			released = true;
			const n = (this.pins.get(bytesId) ?? 1) - 1;
			if (n <= 0) this.pins.delete(bytesId);
			else this.pins.set(bytesId, n);
		};
		if (holder !== undefined) this.unpinOnCollect?.register(holder, release);
		return release;
	}

	/** What eviction must leave in place: recent reads, and this tab's pins. */
	private keep(): EvictionKeep {
		return { usedSince: Date.now() - this.opts.evictionGraceMs, bytesIds: new Set(this.pins.keys()) };
	}

	private async evict(store: BlobStore): Promise<void> {
		await store.evict(this.opts.cacheBytes, this.keep()).catch((err: unknown) =>
			reportError('blob cache evict', err)
		);
	}

	/** Where this device holds the blob's bytes. Counts as a read of a cached blob. */
	private async locate(
		store: BlobStore,
		workspaceId: string,
		digest: string,
	): Promise<{ bytesId: string; size: number } | undefined> {
		const rec = await store.upload(workspaceId, digest);
		if (rec !== undefined) return { bytesId: rec.bytesId, size: rec.size };
		const cached = await store.cached(workspaceId, digest);
		if (cached === undefined) return undefined;
		await store.touch(workspaceId, digest, Date.now());
		return { bytesId: cached.bytesId, size: cached.size };
	}

	/**
	 * The stored chunks under `bytesId` as one Blob, pinned while the Blob is
	 * reachable, or undefined when a chunk is gone (discarded, purged or
	 * evicted by another tab).
	 */
	private async compose(
		store: BlobStore,
		bytesId: string,
		size: number,
		mediaType: string,
	): Promise<Blob | undefined> {
		const release = this.pin(bytesId);
		try {
			const parts: Blob[] = [];
			for (let i = 0; i < chunkCount(size); i++) {
				const chunk = await store.readChunk(bytesId, i);
				if (chunk === undefined) return undefined;
				parts.push(chunk);
			}
			const blob = new Blob(parts, { type: mediaType });
			this.pin(bytesId, blob);
			return blob;
		} finally {
			release();
		}
	}

	private async read(workspaceId: string, ref: BlobRefLike): Promise<Blob> {
		const store = await this.store();
		const local = await this.locate(store, workspaceId, ref.digest);
		if (local !== undefined) {
			const blob = await this.compose(store, local.bytesId, local.size, ref.mediaType);
			if (blob !== undefined) return blob;
		}
		// Download into the cache, then compose from what was stored: the parts
		// of the result are then the stored Blobs, not chunks held in memory.
		const reader = await this.openRemote(store, workspaceId, ref);
		try {
			for (let i = 0; i < reader.count; i++) await reader.read(i);
		} catch (err) {
			await reader.abort();
			throw err;
		}
		// Held through finish(), whose eviction pass would otherwise be free to
		// drop a blob larger than the budget before it is composed.
		const hold = this.pin(reader.bytesId!);
		let blob: Blob | undefined;
		try {
			await reader.finish();
			const at = await this.locate(store, workspaceId, ref.digest);
			blob = at && (await this.compose(store, at.bytesId, at.size, ref.mediaType));
		} finally {
			hold();
		}
		if (blob === undefined) {
			throw new BlobError(
				CodeBlobUnavailable,
				`blob ${ref.digest} left the local store while it was read`,
				{
					workspaceId,
					digest: ref.digest,
				},
			);
		}
		return blob;
	}

	stream(workspaceId: string, ref: BlobRefLike): ReadableStream<Uint8Array> {
		this.assertOpen();
		checkRef(ref);
		const abort = new AbortController();
		let reader: ChunkReader | undefined;
		let index = 0;
		let settled = false;
		let pulling: Promise<void> | undefined;
		const settle = async (ok: boolean): Promise<void> => {
			if (settled) return;
			settled = true;
			if (reader === undefined) return;
			await (ok ? reader.finish() : reader.abort().catch(() => {}));
		};
		const step = async (controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> => {
			try {
				reader ??= await this.openReader(workspaceId, ref, abort.signal);
				if (index < reader.count) controller.enqueue(await reader.read(index++));
				if (index >= reader.count) {
					await settle(true);
					controller.close();
				}
			} catch (err) {
				await settle(false);
				controller.error(err);
			}
		};
		return new ReadableStream<Uint8Array>({
			pull: (controller) => {
				pulling = step(controller);
				return pulling;
			},
			cancel: async () => {
				abort.abort();
				// Abort the reader only once the pull in flight has stopped writing.
				await pulling?.catch(() => {});
				await settle(false);
			},
		}, { highWaterMark: 1 });
	}

	/** A reader over the blob's chunks: this device's bytes, or the server's. */
	private async openReader(
		workspaceId: string,
		ref: BlobRefLike,
		signal: AbortSignal,
	): Promise<ChunkReader> {
		const store = await this.store();
		const local = await this.locate(store, workspaceId, ref.digest);
		if (local === undefined) return this.openRemote(store, workspaceId, ref, signal);
		const release = this.pin(local.bytesId);
		return {
			count: chunkCount(local.size),
			read: async (index) => {
				const chunk = await store.readChunk(local.bytesId, index);
				if (chunk === undefined) {
					throw new BlobError(
						CodeBlobUnavailable,
						`blob ${ref.digest} left the local store while it was read`,
						{ workspaceId, digest: ref.digest },
					);
				}
				return readBlobRange(chunk, 0, chunk.size);
			},
			finish: async () => release(),
			abort: async () => release(),
		};
	}

	/**
	 * A reader that downloads the blob chunk by chunk, checks each chunk and
	 * stores it, and caches the blob on finish(). Fails before any chunk with
	 * blob_unavailable or blob_corrupt when the server's manifest does not
	 * allow the read.
	 */
	private async openRemote(
		store: BlobStore,
		workspaceId: string,
		ref: BlobRefLike,
		signal?: AbortSignal,
	): Promise<ChunkReader> {
		const { digest } = ref;
		const info = { workspaceId, digest };
		const rpc = await this.rpc();
		const res = await rpc.statBlobs({ workspaceId, digests: [ digest ] }, { signal });
		const st = res.blobs.find((b) => b.digest === digest);
		if (st === undefined || st.state !== BlobState.COMPLETE) {
			throw new BlobError(CodeBlobUnavailable, `blob ${digest} is not available`, info);
		}
		const size = Number(st.size);
		if (st.size !== ref.size) {
			throw new BlobError(
				CodeBlobCorrupt,
				`blob ${digest} is ${st.size} bytes on the server, the ref says ${ref.size}`,
				info,
			);
		}
		const hashes = st.chunkHashes;
		if (hashes.length !== chunkCount(size) || (await digestOf(size, hashes)) !== digest) {
			throw new BlobError(CodeBlobCorrupt, `the server's chunk hashes do not produce ${digest}`, info);
		}
		this.complete.add(blobKey(workspaceId, digest));
		const bytesId = await store.stage();
		const release = this.pin(bytesId);
		return {
			count: hashes.length,
			bytesId,
			read: async (index) => {
				let data: Uint8Array;
				try {
					data = (await rpc.getChunk({ workspaceId, digest, index }, { signal })).data;
				} catch (err) {
					const ce = ConnectError.from(err);
					if (ce.code === Code.NotFound) {
						throw new BlobError(CodeBlobUnavailable, `blob ${digest} is not available`, {
							...info,
							transportCode: Code[ce.code],
							cause: err,
						});
					}
					throw err;
				}
				if (
					data.length !== chunkLength(size, index) || !sameBytes(await hashChunk(data), hashes[index]!)
				) {
					throw new BlobError(
						CodeBlobCorrupt,
						`chunk ${index} of blob ${digest} does not match its hash`,
						info,
					);
				}
				await store.putChunk(bytesId, index, new Blob([ data as Uint8Array<ArrayBuffer> ]));
				return data;
			},
			finish: async () => {
				try {
					await store.addCached({
						workspaceId,
						digest,
						size,
						mediaType: ref.mediaType,
						bytesId,
						usedAt: Date.now(),
					});
				} finally {
					release();
				}
				await this.evict(store);
				this.publish(workspaceId, digest, { state: 'uploaded' });
			},
			abort: async () => {
				release();
				await store.dropStaged(bytesId).catch(() => {});
			},
		};
	}

	// --------------------------------------------------------------- uploader

	private openChannel(): void {
		if (this.channel !== undefined || this.closed) return;
		if (isServerRuntime() || typeof BroadcastChannel === 'undefined') return;
		const channel = new BroadcastChannel(this.opts.channelName);
		channel.onmessage = (e: MessageEvent) => {
			const msg = e.data as BlobMessage | undefined;
			if (msg === undefined || msg.from === this.tabId || typeof msg.digest !== 'string') return;
			this.apply(msg.workspaceId, msg.digest, msg.status);
			if (msg.status.state === 'queued') this.kick();
		};
		this.channel = channel;
	}

	private canLock(): boolean {
		return !isServerRuntime() && typeof navigator !== 'undefined' && navigator.locks !== undefined;
	}

	/** Begins an uploader term: under the Web Lock where there is one, else at once. */
	private startTerm(): void {
		if (!this.started || this.uploader !== undefined) return;
		const abort = new AbortController();
		const signal = abort.signal;
		let done: Promise<void>;
		if (this.canLock()) {
			done = navigator.locks
				.request(this.opts.lockName, { signal }, () => this.uploadLoop(signal))
				.then(
					() => {},
					(err: unknown) => {
						if (signal.aborted) return;
						reportError('blob uploader lock unavailable; uploading from this tab', err);
						return this.uploadLoop(signal);
					},
				);
		} else {
			done = this.uploadLoop(signal);
		}
		this.uploader = { abort, done };
	}

	private async endTerm(): Promise<void> {
		const term = this.uploader;
		if (term === undefined) return;
		this.uploader = undefined;
		term.abort.abort();
		this.wake?.();
		await term.done.catch(() => {});
	}

	/**
	 * Ends the term while the page is frozen, so a frozen tab does not hold the
	 * uploader lock from the tabs that still run.
	 */
	private listenLifecycle(): void {
		if (typeof document === 'undefined' || this.lifecycle.length > 0) return;
		const pause = (): void => void this.endTerm();
		const resume = (): void => this.startTerm();
		const doc = document as unknown as EventTarget;
		const win = typeof window !== 'undefined' ? (window as unknown as EventTarget) : undefined;
		const entries: Array<[EventTarget | undefined, string, () => void]> = [
			[ doc, 'freeze', pause ],
			[ doc, 'resume', resume ],
			[ win, 'pagehide', pause ],
			[ win, 'pageshow', resume ],
		];
		for (const [ target, type, fn ] of entries) {
			if (target === undefined) continue;
			target.addEventListener(type, fn);
			this.lifecycle.push([ target, type, fn ]);
		}
	}

	private async uploadLoop(signal: AbortSignal): Promise<void> {
		const backoff = new Backoff(this.opts.minBackoffMs, this.opts.maxBackoffMs);
		while (!signal.aborted) {
			this.kicked = false;
			let picked: { next?: UploadRecord; retryAt?: number };
			try {
				picked = await this.nextQueued();
				backoff.reset();
			} catch (err) {
				if (signal.aborted) return;
				reportError('blob queue read', err);
				await sleep(backoff.next(), signal);
				continue;
			}
			const { next, retryAt } = picked;
			if (next === undefined) {
				await this.waitForWork(
					signal,
					retryAt === undefined ? IDLE_RESCAN_MS : Math.max(0, retryAt - Date.now()),
				);
				continue;
			}
			const key = blobKey(next.workspaceId, next.digest);
			try {
				await this.uploadOne(next, signal);
				this.retries.delete(key);
			} catch (err) {
				if (this.progress.has(key)) this.publish(next.workspaceId, next.digest, { state: 'queued' });
				if (signal.aborted) return;
				const refusal = refusalOf(next, err);
				if (refusal !== undefined) {
					this.retries.delete(key);
					await this.markFailed(next, refusal).catch((e: unknown) => reportError('blob queue write', e));
					continue;
				}
				// A wait for a grant can last as long as the app takes to grant, so it
				// backs off further than an outage; retryNoAccess ends it early.
				const noAccess = isNoAccess(err);
				let retry = this.retries.get(key);
				if (retry === undefined || retry.noAccess !== noAccess) {
					retry = {
						workspaceId: next.workspaceId,
						backoff: noAccess
							? new Backoff(this.opts.minNoAccessBackoffMs, this.opts.maxNoAccessBackoffMs)
							: new Backoff(this.opts.minBackoffMs, this.opts.maxBackoffMs),
						notBefore: 0,
						noAccess,
					};
					this.retries.set(key, retry);
				}
				retry.notBefore = Date.now() + retry.backoff.next();
			}
		}
	}

	/**
	 * The oldest queued upload not waiting out a retry, or else the earliest
	 * time one of them may be tried again.
	 */
	private async nextQueued(): Promise<{ next?: UploadRecord; retryAt?: number }> {
		const all = await (await this.store()).uploads();
		const now = Date.now();
		const queued = new Set<string>();
		let next: UploadRecord | undefined;
		let retryAt: number | undefined;
		for (const r of all) {
			if (r.state !== 'queued') continue;
			const key = blobKey(r.workspaceId, r.digest);
			queued.add(key);
			const notBefore = this.retries.get(key)?.notBefore ?? 0;
			if (notBefore > now) {
				retryAt = retryAt === undefined ? notBefore : Math.min(retryAt, notBefore);
			} else if (next === undefined || r.createdAt < next.createdAt) {
				next = r;
			}
		}
		// Uploads that left the queue (done elsewhere, discarded, purged).
		for (const key of this.retries.keys()) if (!queued.has(key)) this.retries.delete(key);
		return { next, retryAt };
	}

	/** Wakes the uploader loop, now or as soon as it next waits. */
	private kick(): void {
		if (this.wake) this.wake();
		else this.kicked = true;
	}

	/** Waits for a kick(), an abort, or `ms` milliseconds. */
	private waitForWork(signal: AbortSignal, ms: number): Promise<void> {
		return new Promise<void>((resolve) => {
			// stop() may have aborted, or a kick arrived, while the queue was read.
			if (signal.aborted || this.kicked) {
				resolve();
				return;
			}
			const done = (): void => {
				clearTimeout(timer);
				signal.removeEventListener('abort', done);
				if (this.wake === done) this.wake = undefined;
				resolve();
			};
			const timer = setTimeout(done, ms);
			signal.addEventListener('abort', done);
			this.wake = done;
		});
	}

	private async uploadOne(rec: UploadRecord, signal: AbortSignal): Promise<void> {
		const { workspaceId, digest } = rec;
		const store = await this.store();
		const rpc = await this.rpc();
		const total = rec.chunkHashes.length;
		let restarts = 0;
		for (;;) {
			const begin = await rpc.beginUpload({
				workspaceId,
				digest,
				size: BigInt(rec.size),
				chunkHashes: rec.chunkHashes,
			}, { signal });
			if (begin.state === BlobState.COMPLETE) break;
			const missing = [ ...begin.missingChunks ].sort((a, b) => a - b);
			const progress: BlobProgress = {
				sentChunks: total - missing.length,
				totalChunks: total,
				sentBytes: rec.size - missing.reduce((n, i) => n + chunkLength(rec.size, i), 0),
				totalBytes: rec.size,
			};
			this.publish(workspaceId, digest, { state: 'uploading', progress: { ...progress } });
			let completed = false;
			let expired = false;
			for (const index of missing) {
				const chunk = await store.readChunk(rec.bytesId, index);
				if (chunk === undefined) {
					// Discarded or purged under us: nothing left to send.
					if ((await store.upload(workspaceId, digest)) === undefined) return;
					throw new BlobError(
						CodeBlobRefused,
						`chunk ${index} of queued blob ${digest} is missing from the local store`,
						{ workspaceId, digest },
					);
				}
				const data = await readBlobRange(chunk, 0, chunk.size);
				let state: BlobState;
				try {
					state = (await rpc.putChunk({ workspaceId, digest, index, data }, { signal })).state;
				} catch (err) {
					// The staged upload expired on the server; BeginUpload stages it again.
					if (ConnectError.from(err).code === Code.FailedPrecondition && restarts < MAX_RESTARTS) {
						expired = true;
						break;
					}
					throw err;
				}
				progress.sentChunks++;
				progress.sentBytes += data.length;
				this.publish(workspaceId, digest, { state: 'uploading', progress: { ...progress } });
				if (state === BlobState.COMPLETE) {
					completed = true;
					break;
				}
			}
			if (completed) break;
			if (++restarts > MAX_RESTARTS) {
				throw new Error(`the server did not complete blob ${digest} after every chunk was sent`);
			}
			if (!expired && missing.length === 0) {
				throw new Error(`the server reported blob ${digest} incomplete with no chunk missing`);
			}
		}
		await store.completeUpload(workspaceId, digest, Date.now());
		this.publish(workspaceId, digest, { state: 'uploaded' });
		await this.evict(store);
	}

	private async markFailed(rec: UploadRecord, error: BlobError): Promise<void> {
		const stored: StoredBlobError = { code: error.code, message: error.message };
		if (error.transportCode !== undefined) stored.transportCode = error.transportCode;
		if (error.errorCode !== undefined) stored.errorCode = error.errorCode;
		const store = await this.store();
		if (await store.updateUpload({ ...rec, state: 'failed', error: stored })) {
			this.publish(rec.workspaceId, rec.digest, { state: 'failed', error: stored });
		}
	}
}

/**
 * Whether `err` is the workspace gate's answer to a caller that holds no grant
 * yet. It arrives as permission_denied, and the grant can still come.
 */
function isNoAccess(err: unknown): boolean {
	return err instanceof ConnectError
		&& err.code === Code.PermissionDenied
		&& err.metadata.get(ErrorCodeMetadataKey) === CodeWorkspaceNoAccess;
}

/** The permanent refusal `err` stands for, or undefined for a transient failure. */
function refusalOf(rec: UploadRecord, err: unknown): BlobError | undefined {
	if (err instanceof BlobError) return err;
	if (!(err instanceof ConnectError)) return undefined;
	if (!PERMANENT_CODES.has(err.code) || isNoAccess(err)) return undefined;
	return new BlobError(CodeBlobRefused, err.rawMessage || err.message, {
		workspaceId: rec.workspaceId,
		digest: rec.digest,
		transportCode: Code[err.code],
		errorCode: err.metadata.get(ErrorCodeMetadataKey) ?? undefined,
		cause: err,
	});
}

function toBlobError(
	workspaceId: string,
	digest: string,
	e: StoredBlobError | undefined,
): BlobError {
	return new BlobError(
		(e?.code ?? CodeBlobRefused) as BlobError['code'],
		e?.message ?? 'the server refused the upload',
		{ workspaceId, digest, transportCode: e?.transportCode, errorCode: e?.errorCode },
	);
}

function checkRef(ref: BlobRefLike): void {
	if (!isBlobDigest(ref.digest)) {
		throw new TypeError(`"${ref.digest}" is not a blob digest (sp1:<64 lowercase hex digits>)`);
	}
}

function sameStatus(a: BlobStatus, b: BlobStatus): boolean {
	if (a.state !== b.state) return false;
	if (a.state === 'uploading' && b.state === 'uploading') {
		return a.progress.sentChunks === b.progress.sentChunks
			&& a.progress.sentBytes === b.progress.sentBytes
			&& a.progress.totalChunks === b.progress.totalChunks;
	}
	if (a.state === 'failed' && b.state === 'failed') {
		return a.error.code === b.error.code && a.error.message === b.error.message;
	}
	return true;
}
