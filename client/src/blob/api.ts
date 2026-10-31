import { CodeBlobCorrupt, CodeBlobRefused, CodeBlobUnavailable } from '../codes.js';
import type { BlobRef } from '../gen/syncplane/v1/blob_pb.js';

export type { BlobRef } from '../gen/syncplane/v1/blob_pb.js';

/**
 * Any value shaped like a `syncplane.v1.BlobRef`: the client's own generated
 * type, or the one an app's generated entity types embed.
 */
export interface BlobRefLike {
	readonly digest: string;
	readonly size: bigint;
	readonly mediaType: string;
}

/** How far an upload has come. Byte counts include chunks the server had before. */
export interface BlobProgress {
	sentChunks: number;
	totalChunks: number;
	sentBytes: number;
	totalBytes: number;
}

/**
 * What one client knows about one blob.
 *
 * - `queued`: the bytes are in this device's upload queue and wait for the
 *   uploader. Also reported between retries after a transient failure, and
 *   while the caller holds no grant in the workspace yet.
 * - `uploading`: a tab of this account is sending the chunks.
 * - `uploaded`: the server holds every chunk.
 * - `pending`: this device has no bytes to send and the server has not
 *   reported the blob complete, or could not be asked.
 * - `failed`: the server refused the upload, and retrying the same bytes meets
 *   the same answer. The bytes stay in the queue, readable through `get`,
 *   until `discard` or another `put` of them.
 */
export type BlobStatus =
	| { readonly state: 'queued' }
	| { readonly state: 'uploading'; readonly progress: BlobProgress }
	| { readonly state: 'uploaded' }
	| { readonly state: 'pending' }
	| { readonly state: 'failed'; readonly error: BlobError };

/** Options for `WorkspaceBlobs.put`. */
export interface BlobPutOptions {
	/**
	 * The media type the returned ref carries.
	 *
	 * @defaultValue the `type` of the Blob, which is `''` when it has none
	 */
	mediaType?: string;
}

/** The codes a {@link BlobError} carries. */
export type BlobErrorCode =
	| typeof CodeBlobRefused
	| typeof CodeBlobUnavailable
	| typeof CodeBlobCorrupt;

/** A blob operation that cannot succeed by retrying it. */
export class BlobError extends Error {
	readonly code: BlobErrorCode;
	readonly workspaceId: string;
	readonly digest: string;
	/**
	 * The Connect code name of the server's answer (for example
	 * `InvalidArgument`), when the server gave one.
	 */
	readonly transportCode?: string;
	/**
	 * The code the server's application tagged its refusal with, for example
	 * the code of a `CanUploadBlob` rejection such as `upload_too_large`. Set
	 * only when the server sent one; codes are the app's to define, as with
	 * `RejectionInfo.errorCode`.
	 */
	readonly errorCode?: string;

	constructor(
		code: BlobErrorCode,
		message: string,
		info: {
			workspaceId: string;
			digest: string;
			transportCode?: string;
			errorCode?: string;
			cause?: unknown;
		},
	) {
		super(message, info.cause === undefined ? undefined : { cause: info.cause });
		this.name = 'BlobError';
		this.code = code;
		this.workspaceId = info.workspaceId;
		this.digest = info.digest;
		if (info.transportCode !== undefined) this.transportCode = info.transportCode;
		if (info.errorCode !== undefined) this.errorCode = info.errorCode;
	}
}

/**
 * The blobs of one workspace. Reached through `client.ws(id).blobs`.
 *
 * Every method that takes a ref rejects with a `TypeError` when its digest is
 * not of the form `sp1:<64 lowercase hex digits>`; `watch` throws it.
 */
export interface WorkspaceBlobs {
	/**
	 * Hashes `data`, stores it in this device's upload queue and returns the ref
	 * to put in a row. Does not wait for the upload, and works offline: the
	 * uploader sends the queue while the client is started, across reloads.
	 *
	 * Reads `data` once, one 8 MiB chunk at a time; the caller may drop it after
	 * the promise resolves. Putting bytes that are already queued returns the
	 * same digest and queues nothing new; it sets a `failed` upload back to
	 * `queued`.
	 *
	 * Rejects when the local store cannot hold the bytes, for example with the
	 * browser's `QuotaExceededError`.
	 */
	put(data: Blob, opts?: BlobPutOptions): Promise<BlobRef>;
	/**
	 * The blob's current status. Asks the server when this device has neither
	 * queued nor cached the blob and has not seen it complete. Never rejects for
	 * a network failure: it reports `pending`.
	 */
	status(ref: BlobRefLike): Promise<BlobStatus>;
	/**
	 * Calls `cb` with the blob's status, then again each time it changes, until
	 * the returned function is called. Never calls `cb` synchronously.
	 *
	 * Changes made by other tabs of the account arrive too. While the status is
	 * `pending` and the client is started, the server is asked again after each
	 * sync cycle and on a timer.
	 */
	watch(ref: BlobRefLike, cb: (status: BlobStatus) => void): () => void;
	/**
	 * Resolves once the status is `uploaded`. Rejects with the
	 * {@link BlobError} of a `failed` status, and with an `Error` when the
	 * client closes first. Waits through `queued` and `pending`, so it does not
	 * settle while the client is not started and the bytes are only local.
	 */
	uploaded(ref: BlobRefLike): Promise<void>;
	/**
	 * The blob's bytes, typed with `ref.mediaType`.
	 *
	 * Reads this device's upload queue first, then its download cache, then the
	 * server, which costs one request per 8 MiB chunk. A download is checked
	 * against the digest chunk by chunk and stored in the cache as it arrives,
	 * whatever its size; the returned Blob is made of the stored chunks, so in
	 * a browser it is backed by IndexedDB on disk and holds about one chunk in
	 * memory. Where IndexedDB refuses Blob values (Safari private browsing),
	 * chunks are stored as bytes and the returned Blob holds the whole blob in
	 * memory; `stream` holds one chunk at a time there too. The cache does not evict a blob read in the last 10 minutes, nor,
	 * in this tab, one whose returned Blob is still reachable.
	 *
	 * Rejects with a {@link BlobError}: `blob_unavailable` when the server has
	 * no complete blob the caller can read, `blob_corrupt` when the server's
	 * bytes or size do not match the ref. Rejects with the `ConnectError` of a
	 * request that failed for another reason, such as the network.
	 */
	get(ref: BlobRefLike): Promise<Blob>;
	/**
	 * The blob's bytes as a stream of chunks, in order, each up to 8 MiB.
	 *
	 * Reads from the same places as `get`, and caches what it downloads. It
	 * reads at most one chunk ahead of the consumer. A chunk from the server is
	 * checked before it is enqueued. Cancelling stops the download in flight
	 * and caches nothing. The stream errors with the reasons `get` rejects with.
	 */
	stream(ref: BlobRefLike): ReadableStream<Uint8Array>;
	/**
	 * Deletes the blob from this device's upload queue, whatever its state.
	 * A tab uploading it stops after the chunk in flight. Rows that refer to it
	 * keep the ref. No-op when the blob is not queued.
	 */
	discard(ref: BlobRefLike): Promise<void>;
}

/**
 * The blobs of the client's default workspace, plus the upload queue of the
 * whole account. Reached through `client.blobs`.
 *
 * The {@link WorkspaceBlobs} methods throw when the client has no default
 * workspace.
 */
export interface ClientBlobs extends WorkspaceBlobs {
	/**
	 * Resolves once no upload of any workspace is `queued` or `uploading`: each
	 * is `uploaded` or `failed`. Rejects with an `Error` when the client closes
	 * first. Does not settle while the client is not started and the queue is
	 * not empty.
	 */
	idle(): Promise<void>;
	/** The number of uploads, across workspaces and tabs, that are not done or failed. */
	pendingCount(): Promise<number>;
}
