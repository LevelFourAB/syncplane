// An in-memory BlobService for the unit and react suites. It checks manifests
// and chunks the way the RPC contract in proto/contract/syncplane/v1/blob.proto
// says the server does, and lets a spec intercept each call.

import {
	Code,
	ConnectError,
	createRouterTransport,
	type HandlerContext,
	type Transport,
} from '@connectrpc/connect';

import { chunkCount, chunkLength, digestOf, hashChunk, sameBytes } from '../../src/blob/digest.js';
import {
	type BeginUploadRequest,
	BlobService,
	BlobState,
	type GetChunkRequest,
	type PutChunkRequest,
	type WatchBlobsRequest,
} from '../../src/gen/syncplane/v1/blob_pb.js';

interface StoredBlob {
	size: number;
	chunkHashes: Uint8Array[];
	chunks: Map<number, Uint8Array>;
	complete: boolean;
}

/** Calls a spec can intercept. A hook that throws fails the call. */
export interface BlobServerHooks {
	beginUpload?: (req: BeginUploadRequest) => void | Promise<void>;
	putChunk?: (req: PutChunkRequest, ctx: HandlerContext) => void | Promise<void>;
	/** Replaces the bytes a GetChunk answers with. */
	getChunk?: (req: GetChunkRequest, data: Uint8Array) => Uint8Array;
	/** Whether StatBlobs, GetChunk and WatchBlobs may reveal a blob; default yes. */
	readable?: (workspaceId: string, digest: string) => boolean;
	/** Called as each WatchBlobs stream opens; throwing fails it. */
	watchBlobs?: (req: WatchBlobsRequest) => void;
}

export interface FakeBlobServer {
	transport: Transport;
	hooks: BlobServerHooks;
	/** Every PutChunk that reached the handler, in order. */
	puts: PutChunkRequest[];
	begins: BeginUploadRequest[];
	stats: number;
	gets: number;
	/** Most PutChunk calls in flight at once. */
	maxPutsInFlight: number;
	/** Every WatchBlobs request, in the order the streams opened. */
	watches: WatchBlobsRequest[];
	/** WatchBlobs streams open now. */
	activeWatches: number;
	/** Tells open WatchBlobs streams to look again, as a row change would. */
	changed(): void;
	has(workspaceId: string, digest: string): boolean;
	bytes(workspaceId: string, digest: string): Uint8Array | undefined;
	/** Stores `data` as a complete blob and returns its digest. */
	seed(workspaceId: string, data: Uint8Array): Promise<string>;
	/** Stages chunk `index` of `data` as part of an unfinished upload. */
	stage(workspaceId: string, data: Uint8Array, indexes: number[]): Promise<string>;
}

export async function manifestOf(
	data: Uint8Array,
): Promise<{ digest: string; chunkHashes: Uint8Array[] }> {
	const chunkHashes: Uint8Array[] = [];
	for (let i = 0; i < chunkCount(data.length); i++) {
		const start = i * 8 * 1024 * 1024;
		chunkHashes.push(await hashChunk(data.subarray(start, start + chunkLength(data.length, i))));
	}
	return { digest: await digestOf(data.length, chunkHashes), chunkHashes };
}

function chunkOf(data: Uint8Array, index: number): Uint8Array {
	const start = index * 8 * 1024 * 1024;
	return data.slice(start, start + chunkLength(data.length, index));
}

export function fakeBlobServer(): FakeBlobServer {
	const blobs = new Map<string, StoredBlob>();
	const key = (ws: string, digest: string): string => `${ws}\u0000${digest}`;
	let inFlight = 0;
	const waiters = new Set<() => void>();
	const changed = (): void => {
		for (const w of [ ...waiters ]) w();
	};

	const server: FakeBlobServer = {
		transport: undefined as unknown as Transport,
		hooks: {},
		puts: [],
		begins: [],
		stats: 0,
		gets: 0,
		maxPutsInFlight: 0,
		watches: [],
		activeWatches: 0,
		changed,
		has: (ws, digest) => blobs.get(key(ws, digest))?.complete === true,
		bytes(ws, digest) {
			const b = blobs.get(key(ws, digest));
			if (!b?.complete) return undefined;
			const out = new Uint8Array(b.size);
			for (const [ i, c ] of b.chunks) out.set(c, i * 8 * 1024 * 1024);
			return out;
		},
		async seed(ws, data) {
			const { digest, chunkHashes } = await manifestOf(data);
			const chunks = new Map<number, Uint8Array>();
			for (let i = 0; i < chunkHashes.length; i++) chunks.set(i, chunkOf(data, i));
			blobs.set(key(ws, digest), { size: data.length, chunkHashes, chunks, complete: true });
			changed();
			return digest;
		},
		async stage(ws, data, indexes) {
			const { digest, chunkHashes } = await manifestOf(data);
			const chunks = new Map<number, Uint8Array>();
			for (const i of indexes) chunks.set(i, chunkOf(data, i));
			blobs.set(key(ws, digest), { size: data.length, chunkHashes, chunks, complete: false });
			return digest;
		},
	};

	const readable = (ws: string, digest: string): boolean =>
		server.hooks.readable?.(ws, digest) ?? true;

	server.transport = createRouterTransport(({ service }) => {
		service(BlobService, {
			async beginUpload(req) {
				server.begins.push(req);
				await server.hooks.beginUpload?.(req);
				const size = Number(req.size);
				if (
					req.chunkHashes.length !== chunkCount(size)
					|| (await digestOf(size, req.chunkHashes)) !== req.digest
				) {
					throw new ConnectError('manifest does not match digest', Code.InvalidArgument);
				}
				let b = blobs.get(key(req.workspaceId, req.digest));
				if (b === undefined) {
					b = { size, chunkHashes: req.chunkHashes, chunks: new Map(), complete: size === 0 };
					blobs.set(key(req.workspaceId, req.digest), b);
				}
				if (b.complete) return { state: BlobState.COMPLETE, missingChunks: [] };
				const missing: number[] = [];
				for (let i = 0; i < b.chunkHashes.length; i++) if (!b.chunks.has(i)) missing.push(i);
				return { state: BlobState.UPLOADING, missingChunks: missing };
			},
			async putChunk(req, ctx) {
				inFlight++;
				server.maxPutsInFlight = Math.max(server.maxPutsInFlight, inFlight);
				try {
					server.puts.push(req);
					await server.hooks.putChunk?.(req, ctx);
					const b = blobs.get(key(req.workspaceId, req.digest));
					if (b === undefined) {
						throw new ConnectError('no upload started', Code.FailedPrecondition);
					}
					if (b.complete) return { state: BlobState.COMPLETE };
					const want = b.chunkHashes[req.index];
					if (want === undefined || !sameBytes(await hashChunk(req.data), want)) {
						throw new ConnectError('chunk does not match its hash', Code.InvalidArgument);
					}
					b.chunks.set(req.index, req.data.slice());
					if (b.chunks.size === b.chunkHashes.length) {
						b.complete = true;
						changed();
					}
					return { state: b.complete ? BlobState.COMPLETE : BlobState.UPLOADING };
				} finally {
					inFlight--;
				}
			},
			async statBlobs(req) {
				server.stats++;
				return {
					blobs: req.digests.map((digest) => {
						const b = blobs.get(key(req.workspaceId, digest));
						if (b === undefined || !readable(req.workspaceId, digest)) {
							return { digest, state: BlobState.MISSING, size: 0n, chunkHashes: [] };
						}
						return {
							digest,
							state: b.complete ? BlobState.COMPLETE : BlobState.UPLOADING,
							size: BigInt(b.size),
							chunkHashes: b.complete ? b.chunkHashes : [],
						};
					}),
				};
			},
			async *watchBlobs(req, ctx) {
				server.watches.push(req);
				server.hooks.watchBlobs?.(req);
				server.activeWatches++;
				const stateOf = (digest: string): BlobState => {
					const b = blobs.get(key(req.workspaceId, digest));
					if (b === undefined || !readable(req.workspaceId, digest)) return BlobState.MISSING;
					return b.complete ? BlobState.COMPLETE : BlobState.UPLOADING;
				};
				const status = (digest: string, state: BlobState) => ({
					digest,
					state,
					size: 0n,
					chunkHashes: [],
				});
				try {
					const last = new Map<string, BlobState>();
					for (const d of req.digests) last.set(d, stateOf(d));
					yield { blobs: req.digests.map((d) => status(d, last.get(d)!)) };
					while ([ ...last.values() ].some((st) => st !== BlobState.COMPLETE)) {
						await new Promise<void>((resolve) => {
							const done = (): void => {
								waiters.delete(done);
								resolve();
							};
							waiters.add(done);
							ctx.signal.addEventListener('abort', done, { once: true });
						});
						if (ctx.signal.aborted) return;
						const moved: string[] = [];
						for (const d of last.keys()) {
							const st = stateOf(d);
							if (st !== last.get(d)) {
								last.set(d, st);
								moved.push(d);
							}
						}
						if (moved.length > 0) yield { blobs: moved.map((d) => status(d, last.get(d)!)) };
					}
				} finally {
					server.activeWatches--;
				}
			},
			async getChunk(req) {
				server.gets++;
				const b = blobs.get(key(req.workspaceId, req.digest));
				if (b === undefined || !b.complete || !readable(req.workspaceId, req.digest)) {
					throw new ConnectError('blob not found', Code.NotFound);
				}
				const data = b.chunks.get(req.index);
				if (data === undefined) throw new ConnectError('index out of range', Code.OutOfRange);
				return { data: server.hooks.getChunk?.(req, data) ?? data };
			},
		});
	});
	return server;
}

/** `n` bytes where byte i is i % 251, the `mod251` pattern of testdata/blob. */
export function mod251(n: number): Uint8Array {
	const out = new Uint8Array(n);
	for (let i = 0; i < n; i++) out[i] = i % 251;
	return out;
}
