// The blob layer against a scripted BlobService: the uploader (one chunk in
// flight, resume, dedupe, refusal, retry, stop), downloads with verification
// and the cache, and status across watchers and tabs.

import 'fake-indexeddb/auto';

import { Code, ConnectError, createRouterTransport } from '@connectrpc/connect';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { BlobError, type BlobRefLike, type BlobStatus } from '../../src/blob/api.js';
import { BLOB_CHUNK_SIZE, sameBytes } from '../../src/blob/digest.js';
import { IDBBlobStore } from '../../src/blob/idb.js';
import { BlobManager, type BlobManagerOptions } from '../../src/blob/manager.js';
import { type BlobStore, MemoryBlobStore } from '../../src/blob/store.js';
import {
	CodeBlobCorrupt,
	CodeBlobRefused,
	CodeBlobUnavailable,
	CodeWorkspaceAccessRevoked,
	CodeWorkspaceNoAccess,
	ErrorCodeMetadataKey,
} from '../../src/codes.js';
import { type FakeBlobServer, fakeBlobServer, manifestOf, mod251 } from '../support/blobserver.js';

let dbCounter = 0;
const open: BlobManager[] = [];

afterEach(async () => {
	for (const m of open.splice(0)) await m.close();
	vi.unstubAllGlobals();
});

function newManager(
	server: FakeBlobServer,
	opts: Partial<BlobManagerOptions> & { store?: BlobStore } = {},
): BlobManager {
	const store = opts.store ?? new MemoryBlobStore();
	const m = new BlobManager({
		transport: async () => server.transport,
		openStore: async () => store,
		lockName: 'blob-lock',
		channelName: 'blob-channel',
		cacheBytes: 1 << 30,
		minBackoffMs: 5,
		maxBackoffMs: 20,
		minRecheckMs: 5,
		maxRecheckMs: 20,
		...opts,
	});
	open.push(m);
	return m;
}

function blobOf(data: Uint8Array, type = ''): Blob {
	return new Blob([ data as Uint8Array<ArrayBuffer> ], { type });
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<Uint8Array[]> {
	const out: Uint8Array[] = [];
	const reader = stream.getReader();
	for (;;) {
		const { done, value } = await reader.read();
		if (done) return out;
		out.push(value);
	}
}

function concat(chunks: Uint8Array[]): Uint8Array {
	const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
	let at = 0;
	for (const c of chunks) {
		out.set(c, at);
		at += c.length;
	}
	return out;
}

async function bytesOf(blob: Blob): Promise<Uint8Array> {
	return new Uint8Array(await blob.arrayBuffer());
}

/**
 * Compares bytes without a deep equality, which walks every index of a
 * multi-megabyte array as a key and runs the worker out of memory.
 */
function expectBytes(got: Uint8Array | undefined, want: Uint8Array): void {
	expect(got?.length).toBe(want.length);
	expect(got !== undefined && sameBytes(got, want)).toBe(true);
}

async function refOf(data: Uint8Array, mediaType = ''): Promise<BlobRefLike> {
	return { digest: (await manifestOf(data)).digest, size: BigInt(data.length), mediaType };
}

/** Records every status a watch delivers. */
function recorder(m: BlobManager, ref: BlobRefLike, ws = 'ws1') {
	const seen: BlobStatus[] = [];
	const unsub = m.watch(ws, ref, (s) => seen.push(s));
	return { seen, states: () => seen.map((s) => s.state), unsub };
}

/** A server from before WatchBlobs. */
function noWatchBlobs(): never {
	throw new ConnectError('no WatchBlobs', Code.Unimplemented);
}

/** A minimal Web Locks fake: one holder per name, the rest queued, waits cancellable. */
function fakeLocks() {
	const queues = new Map<string, Array<() => void>>();
	const held = new Set<string>();
	return {
		request: async (
			name: string,
			options: { signal?: AbortSignal },
			cb: () => Promise<unknown>,
		): Promise<unknown> => {
			if (held.has(name)) {
				await new Promise<void>((resolve, reject) => {
					const queue = queues.get(name) ?? [];
					queue.push(resolve);
					queues.set(name, queue);
					options.signal?.addEventListener('abort', () => {
						const q = queues.get(name) ?? [];
						const i = q.indexOf(resolve);
						if (i >= 0) q.splice(i, 1);
						reject(new DOMException('aborted', 'AbortError'));
					});
				});
			}
			held.add(name);
			try {
				return await cb();
			} finally {
				held.delete(name);
				queues.get(name)?.shift()?.();
			}
		},
	};
}

describe('blob uploads', () => {
	it('queues while stopped and uploads once started', async () => {
		const server = fakeBlobServer();
		const m = newManager(server);
		const data = mod251(1000);
		const ref = await m.put('ws1', blobOf(data, 'image/png'));

		expect(ref.digest).toBe((await manifestOf(data)).digest);
		expect(ref.size).toBe(1000n);
		expect(ref.mediaType).toBe('image/png');
		expect(await m.status('ws1', ref)).toEqual({ state: 'queued' });
		expect(await m.pendingCount()).toBe(1);
		expect(server.begins).toHaveLength(0);

		m.start();
		await m.uploaded('ws1', ref);
		await m.idle();
		expectBytes(server.bytes('ws1', ref.digest), data);
		expect(await m.pendingCount()).toBe(0);
		expect(await m.status('ws1', ref)).toEqual({ state: 'uploaded' });

		// The bytes moved to the cache: reading them needs no network.
		const blob = await m.get('ws1', ref);
		expectBytes(await bytesOf(blob), data);
		expect(blob.type).toBe('image/png');
		expect(server.gets).toBe(0);
		expect(server.stats).toBe(0);
	});

	it('takes the media type from the option over the Blob', async () => {
		const m = newManager(fakeBlobServer());
		const ref = await m.put('ws1', blobOf(mod251(3), 'text/plain'), { mediaType: 'image/webp' });
		expect(ref.mediaType).toBe('image/webp');
	});

	it('uploads an empty blob', async () => {
		const server = fakeBlobServer();
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(new Uint8Array(0)));
		m.start();
		await m.uploaded('ws1', ref);
		expect(server.puts).toHaveLength(0);
		expect(await bytesOf(await m.get('ws1', ref))).toEqual(new Uint8Array(0));
	});

	it('keeps one chunk in flight', async () => {
		const server = fakeBlobServer();
		server.hooks.putChunk = () => new Promise((r) => setTimeout(r, 5));
		const m = newManager(server);
		const big = mod251(2 * BLOB_CHUNK_SIZE + 5);
		const small = mod251(10);
		const a = await m.put('ws1', blobOf(big));
		const b = await m.put('ws1', blobOf(small));
		m.start();
		await Promise.all([ m.uploaded('ws1', a), m.uploaded('ws1', b) ]);
		expect(server.maxPutsInFlight).toBe(1);
		expect(server.puts.map((p) => [ p.digest === a.digest ? 'a' : 'b', p.index ])).toEqual([
			[ 'a', 0 ],
			[ 'a', 1 ],
			[ 'a', 2 ],
			[ 'b', 0 ],
		]);
	});

	it('reports progress while it uploads', async () => {
		const server = fakeBlobServer();
		const m = newManager(server);
		const data = mod251(BLOB_CHUNK_SIZE + 1);
		const ref = await m.put('ws1', blobOf(data));
		const rec = recorder(m, ref);
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'queued' ]));
		m.start();
		await m.uploaded('ws1', ref);
		await vi.waitFor(() => expect(rec.states().at(-1)).toBe('uploaded'));
		const progress = rec.seen.flatMap((s) => (s.state === 'uploading' ? [ s.progress ] : []));
		expect(progress[0]).toEqual({
			sentChunks: 0,
			totalChunks: 2,
			sentBytes: 0,
			totalBytes: BLOB_CHUNK_SIZE + 1,
		});
		// Watchers see the newest status, so intermediate progress may be skipped.
		for (const p of progress) expect(p.totalChunks).toBe(2);
	});

	it('sends only the chunks BeginUpload reports missing', async () => {
		const server = fakeBlobServer();
		const data = mod251(2 * BLOB_CHUNK_SIZE + 5);
		await server.stage('ws1', data, [ 0, 2 ]);
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(data));
		m.start();
		await m.uploaded('ws1', ref);
		expect(server.puts.map((p) => p.index)).toEqual([ 1 ]);
		expectBytes(server.bytes('ws1', ref.digest), data);
	});

	it('skips the upload of a blob the server already has', async () => {
		const server = fakeBlobServer();
		const data = mod251(500);
		await server.seed('ws1', data);
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(data));
		m.start();
		await m.uploaded('ws1', ref);
		expect(server.begins).toHaveLength(1);
		expect(server.puts).toHaveLength(0);
	});

	it('names blobs per workspace', async () => {
		const server = fakeBlobServer();
		const data = mod251(50);
		await server.seed('ws1', data);
		const m = newManager(server);
		const ref = await m.put('ws2', blobOf(data));
		m.start();
		await m.uploaded('ws2', ref);
		expect(server.puts.map((p) => p.workspaceId)).toEqual([ 'ws2' ]);
	});

	it('stops cleanly mid-chunk and resumes after a restart', async () => {
		const server = fakeBlobServer();
		let block = true;
		server.hooks.putChunk = (req, ctx) => {
			if (!block || req.index !== 1) return;
			return new Promise<void>((_resolve, reject) => {
				ctx.signal.addEventListener('abort', () => reject(new ConnectError('aborted', Code.Canceled)));
			});
		};
		const name = `blob-manager-restart-${dbCounter++}`;
		const first = newManager(server, { openStore: () => IDBBlobStore.open(name) });
		const data = mod251(2 * BLOB_CHUNK_SIZE + 5);
		const ref = await first.put('ws1', blobOf(data));
		first.start();
		await vi.waitFor(() => expect(server.puts.map((p) => p.index)).toEqual([ 0, 1 ]));
		expect((await first.status('ws1', ref)).state).toBe('uploading');

		await first.stop();
		expect(await first.status('ws1', ref)).toEqual({ state: 'queued' });
		await first.close();

		block = false;
		const second = newManager(server, { openStore: () => IDBBlobStore.open(name) });
		expect(await second.pendingCount()).toBe(1);
		second.start();
		await second.uploaded('ws1', ref);
		// Chunk 0 landed before the stop; only 1 (aborted) and 2 are sent again.
		expect(server.puts.map((p) => p.index)).toEqual([ 0, 1, 1, 2 ]);
		expectBytes(server.bytes('ws1', ref.digest), data);
	});

	it('marks a refused upload failed and goes on with the queue', async () => {
		const server = fakeBlobServer();
		const bad = mod251(20);
		const badDigest = (await manifestOf(bad)).digest;
		server.hooks.beginUpload = (req) => {
			if (req.digest === badDigest) {
				throw new ConnectError('too large', Code.ResourceExhausted);
			}
		};
		const m = newManager(server);
		const badRef = await m.put('ws1', blobOf(bad));
		const goodRef = await m.put('ws1', blobOf(mod251(30)));
		m.start();

		const err = await m.uploaded('ws1', badRef).then(() => undefined, (e: unknown) => e);
		expect(err).toBeInstanceOf(BlobError);
		expect((err as BlobError).code).toBe(CodeBlobRefused);
		expect((err as BlobError).transportCode).toBe('ResourceExhausted');
		expect((err as BlobError).digest).toBe(badDigest);
		await m.uploaded('ws1', goodRef);
		await m.idle();

		const status = await m.status('ws1', badRef);
		expect(status.state).toBe('failed');
		expect(await m.pendingCount()).toBe(0);
		// The refused bytes stay readable on this device.
		expectBytes(await bytesOf(await m.get('ws1', badRef)), bad);

		// Putting them again queues them again.
		server.hooks.beginUpload = undefined;
		await m.put('ws1', blobOf(bad));
		await m.uploaded('ws1', badRef);
	});

	it.each([
		[ 'InvalidArgument', Code.InvalidArgument ],
		[ 'PermissionDenied', Code.PermissionDenied ],
		[ 'Unimplemented', Code.Unimplemented ],
	])('treats %s as a refusal', async (name, code) => {
		const server = fakeBlobServer();
		server.hooks.beginUpload = () => {
			throw new ConnectError('no', code);
		};
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(mod251(5)));
		m.start();
		await expect(m.uploaded('ws1', ref)).rejects.toMatchObject({ transportCode: name });
		expect(server.begins).toHaveLength(1);
	});

	it('keeps an upload queued while the caller waits for a grant', async () => {
		const server = fakeBlobServer();
		let granted = false;
		server.hooks.beginUpload = () => {
			if (!granted) {
				throw new ConnectError('no access yet', Code.PermissionDenied, {
					[ErrorCodeMetadataKey]: CodeWorkspaceNoAccess,
				});
			}
		};
		// A wait for a grant backs off far longer than the test runs, so only
		// retryNoAccess can bring the upload back.
		const m = newManager(server, { minNoAccessBackoffMs: 60_000, maxNoAccessBackoffMs: 60_000 });
		const ref = await m.put('ws1', blobOf(mod251(5)));
		const other = await m.put('ws2', blobOf(mod251(6)));
		m.start();
		await vi.waitFor(() => expect(server.begins.length).toBeGreaterThanOrEqual(1));
		await new Promise((r) => setTimeout(r, 30));
		expect(await m.status('ws1', ref)).toEqual({ state: 'queued' });
		expect(await m.pendingCount()).toBe(2);

		granted = true;
		m.retryNoAccess('ws2');
		await m.uploaded('ws2', other);
		expect(await m.status('ws1', ref)).toEqual({ state: 'queued' });
		m.retryNoAccess('ws1');
		await m.uploaded('ws1', ref);
	});

	it.each([
		[ 'a revoked workspace', CodeWorkspaceAccessRevoked ],
		[ 'an upload hook refusal', undefined ],
	])('fails an upload on a permission denial for %s', async (_name, tag) => {
		const server = fakeBlobServer();
		server.hooks.beginUpload = () => {
			throw new ConnectError(
				'denied',
				Code.PermissionDenied,
				tag === undefined ? undefined : { [ErrorCodeMetadataKey]: tag },
			);
		};
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(mod251(5)));
		m.start();
		await expect(m.uploaded('ws1', ref)).rejects.toMatchObject({
			code: CodeBlobRefused,
			transportCode: 'PermissionDenied',
		});
	});

	it("carries the server's error code on a refusal, across a reload and to other tabs", async () => {
		vi.stubGlobal('navigator', { userAgent: 'Mozilla/5.0', locks: fakeLocks() });
		const server = fakeBlobServer();
		server.hooks.beginUpload = () => {
			throw new ConnectError('too large for this plan', Code.PermissionDenied, {
				[ErrorCodeMetadataKey]: 'upload_too_large',
			});
		};
		const name = `blob-manager-errorcode-${dbCounter++}`;
		const opts = {
			openStore: () => IDBBlobStore.open(name),
			lockName: `lock-${name}`,
			channelName: `channel-${name}`,
		};
		const tabA = newManager(server, opts);
		const tabB = newManager(server, opts);
		const ref = await tabA.put('ws1', blobOf(mod251(5)));
		const rec = recorder(tabB, ref);
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'queued' ]));
		tabA.start();

		await expect(tabA.uploaded('ws1', ref)).rejects.toMatchObject({
			code: CodeBlobRefused,
			transportCode: 'PermissionDenied',
			errorCode: 'upload_too_large',
		});
		await vi.waitFor(() => expect(rec.states().at(-1)).toBe('failed'));
		const shared = rec.seen.at(-1);
		expect(shared?.state === 'failed' && shared.error.errorCode).toBe('upload_too_large');

		await tabA.close();
		await tabB.close();
		const reloaded = newManager(server, opts);
		const status = await reloaded.status('ws1', ref);
		expect(status.state === 'failed' && status.error.errorCode).toBe('upload_too_large');
	});

	it('retries a transient failure with backoff', async () => {
		const server = fakeBlobServer();
		let failures = 2;
		server.hooks.beginUpload = () => {
			if (failures-- > 0) throw new ConnectError('down', Code.Unavailable);
		};
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(mod251(5)));
		m.start();
		await m.uploaded('ws1', ref);
		expect(server.begins).toHaveLength(3);
	});

	it('goes on with the queue while one upload keeps failing transiently', async () => {
		const server = fakeBlobServer();
		const stuck = mod251(20);
		const stuckDigest = (await manifestOf(stuck)).digest;
		server.hooks.beginUpload = (req) => {
			if (req.digest === stuckDigest) throw new ConnectError('down', Code.Unavailable);
		};
		const m = newManager(server);
		const stuckRef = await m.put('ws1', blobOf(stuck));
		const next = await m.put('ws1', blobOf(mod251(30)));
		m.start();
		await m.uploaded('ws1', next);

		// The stuck upload is retried, never failed.
		await vi.waitFor(() =>
			expect(server.begins.filter((b) => b.digest === stuckDigest).length).toBeGreaterThan(2)
		);
		expect(await m.status('ws1', stuckRef)).toEqual({ state: 'queued' });
		expect(await m.pendingCount()).toBe(1);

		server.hooks.beginUpload = undefined;
		await m.uploaded('ws1', stuckRef);
	});

	it('goes on with the queue while the server never completes an upload', async () => {
		const server = fakeBlobServer();
		const stuck = mod251(20);
		const stuckDigest = (await manifestOf(stuck)).digest;
		// Every PutChunk of the stuck blob answers that the server had it
		// staged and then lost it, past the restarts one attempt allows.
		server.hooks.putChunk = (req) => {
			if (req.digest === stuckDigest) throw new ConnectError('expired', Code.FailedPrecondition);
		};
		const m = newManager(server);
		const stuckRef = await m.put('ws1', blobOf(stuck));
		const next = await m.put('ws1', blobOf(mod251(30)));
		m.start();
		await m.uploaded('ws1', next);
		expect(await m.status('ws1', stuckRef)).toEqual({ state: 'queued' });

		server.hooks.putChunk = undefined;
		await m.uploaded('ws1', stuckRef);
	});

	it('stages the upload again when the server lost it', async () => {
		const server = fakeBlobServer();
		let expired = false;
		server.hooks.putChunk = (req) => {
			if (!expired && req.index === 0) {
				expired = true;
				throw new ConnectError('expired', Code.FailedPrecondition);
			}
		};
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(mod251(5)));
		m.start();
		await m.uploaded('ws1', ref);
		expect(server.begins).toHaveLength(2);
	});

	it('discards a queued upload', async () => {
		const server = fakeBlobServer();
		const m = newManager(server);
		const ref = await m.put('ws1', blobOf(mod251(5)));
		await m.discard('ws1', ref);
		expect(await m.pendingCount()).toBe(0);
		expect(await m.status('ws1', ref)).toEqual({ state: 'pending' });
	});

	it('rejects uploaded() when the manager closes first', async () => {
		const m = newManager(fakeBlobServer());
		const ref = await m.put('ws1', blobOf(mod251(5)));
		const waiting = m.uploaded('ws1', ref);
		await m.close();
		await expect(waiting).rejects.toThrow('closed');
	});

	it('rejects a malformed digest', async () => {
		const m = newManager(fakeBlobServer());
		const ref = { digest: 'sp1:nope', size: 1n, mediaType: '' };
		await expect(m.get('ws1', ref)).rejects.toBeInstanceOf(TypeError);
		await expect(m.status('ws1', ref)).rejects.toBeInstanceOf(TypeError);
		expect(() => m.watch('ws1', ref, () => {})).toThrow(TypeError);
	});
});

describe('blob downloads', () => {
	it('downloads, checks and caches a blob', async () => {
		const server = fakeBlobServer();
		const data = mod251(BLOB_CHUNK_SIZE + 7);
		await server.seed('ws1', data);
		const m = newManager(server);
		const ref = await refOf(data, 'video/mp4');

		const blob = await m.get('ws1', ref);
		expect(blob.type).toBe('video/mp4');
		expectBytes(await bytesOf(blob), data);
		expect(server.gets).toBe(2);

		await m.get('ws1', ref);
		expect(server.gets).toBe(2);
		expect(await m.status('ws1', ref)).toEqual({ state: 'uploaded' });
	});

	it('shares one download between concurrent readers', async () => {
		const server = fakeBlobServer();
		const data = mod251(100);
		await server.seed('ws1', data);
		const m = newManager(server);
		const ref = await refOf(data);
		await Promise.all([ m.get('ws1', ref), m.get('ws1', ref) ]);
		expect(server.gets).toBe(1);
	});

	it('refuses a corrupt chunk and caches nothing', async () => {
		const server = fakeBlobServer();
		const data = mod251(100);
		await server.seed('ws1', data);
		server.hooks.getChunk = (_req, chunk) => {
			const bad = chunk.slice();
			bad[3] = bad[3]! ^ 0xff;
			return bad;
		};
		const m = newManager(server);
		const ref = await refOf(data);
		await expect(m.get('ws1', ref)).rejects.toMatchObject({ code: CodeBlobCorrupt });

		server.hooks.getChunk = undefined;
		expectBytes(await bytesOf(await m.get('ws1', ref)), data);
		expect(server.gets).toBe(2);
	});

	it('downloads again when the cached bytes do not match the digest', async () => {
		// WebKit can hand back other bytes than it stored, with the size it
		// stored: the cache must not trust what it reads.
		const server = fakeBlobServer();
		const data = mod251(BLOB_CHUNK_SIZE + 7);
		await server.seed('ws1', data);
		const store = new MemoryBlobStore();
		const ref = await refOf(data);
		await newManager(server, { store }).get('ws1', ref);
		const before = (await store.cached('ws1', ref.digest))!;
		await store.putChunk(before.bytesId, 0, blobOf(new Uint8Array(BLOB_CHUNK_SIZE)));

		const m = newManager(server, { store });
		expectBytes(await bytesOf(await m.get('ws1', ref)), data);
		expect(server.gets).toBe(4);
		const after = (await store.cached('ws1', ref.digest))!;
		expect(after.bytesId).not.toBe(before.bytesId);
		expect(await store.readChunk(before.bytesId, 1)).toBeUndefined();

		await m.get('ws1', ref);
		expect(server.gets).toBe(4);
	});

	it('refuses a queued blob whose stored bytes do not match its digest', async () => {
		const server = fakeBlobServer();
		const store = new MemoryBlobStore();
		const data = mod251(100);
		const ref = await newManager(server, { store }).put('ws1', blobOf(data));
		const rec = (await store.upload('ws1', ref.digest))!;
		await store.putChunk(rec.bytesId, 0, blobOf(new Uint8Array(100)));

		await expect(newManager(server, { store }).get('ws1', ref)).rejects.toMatchObject({
			code: CodeBlobCorrupt,
		});
		expect(await store.upload('ws1', ref.digest)).toBeDefined();
	});

	it('refuses a ref whose size disagrees with the server', async () => {
		const server = fakeBlobServer();
		const data = mod251(100);
		await server.seed('ws1', data);
		const m = newManager(server);
		const ref = { ...(await refOf(data)), size: 99n };
		await expect(m.get('ws1', ref)).rejects.toMatchObject({ code: CodeBlobCorrupt });
		expect(server.gets).toBe(0);
	});

	it('reports a blob the caller cannot read as unavailable', async () => {
		const server = fakeBlobServer();
		const data = mod251(100);
		await server.seed('ws1', data);
		server.hooks.readable = () => false;
		const m = newManager(server);
		const err = await m.get('ws1', await refOf(data)).then(() => undefined, (e: unknown) => e);
		expect(err).toBeInstanceOf(BlobError);
		expect((err as BlobError).code).toBe(CodeBlobUnavailable);
	});

	it('rejects with the transport error when the server cannot be reached', async () => {
		const transport = createRouterTransport(() => {});
		const m = new BlobManager({
			transport: async () => transport,
			openStore: async () => new MemoryBlobStore(),
			lockName: 'l',
			channelName: 'c',
			cacheBytes: 1 << 20,
		});
		open.push(m);
		const ref = await refOf(mod251(3));
		await expect(m.get('ws1', ref)).rejects.toBeInstanceOf(ConnectError);
		expect(await m.status('ws1', ref)).toEqual({ state: 'pending' });
	});

	it('evicts the least recently read download and never a queued upload', async () => {
		const server = fakeBlobServer();
		const a = mod251(10);
		const b = mod251(11);
		await server.seed('ws1', a);
		await server.seed('ws1', b);
		const m = newManager(server, { cacheBytes: 15, evictionGraceMs: 0 });
		const queued = mod251(100);
		const queuedRef = await m.put('ws1', blobOf(queued));

		const refA = await refOf(a);
		const refB = await refOf(b);
		// Streams hold no Blob the app keeps, so nothing pins what they cached.
		await drain(m.stream('ws1', refA));
		await drain(m.stream('ws1', refB));
		expect(server.gets).toBe(2);
		await drain(m.stream('ws1', refB));
		expect(server.gets).toBe(2);
		await drain(m.stream('ws1', refA));
		expect(server.gets).toBe(3);

		expectBytes(await bytesOf(await m.get('ws1', queuedRef)), queued);
		expect(await m.pendingCount()).toBe(1);
	});

	it('does not evict a blob read within the grace period', async () => {
		const server = fakeBlobServer();
		const a = mod251(10);
		const b = mod251(11);
		await server.seed('ws1', a);
		await server.seed('ws1', b);
		const m = newManager(server, { cacheBytes: 15 });
		await drain(m.stream('ws1', await refOf(a)));
		await drain(m.stream('ws1', await refOf(b)));
		await drain(m.stream('ws1', await refOf(a)));
		expect(server.gets).toBe(2);
	});

	it('keeps the parts of a Blob get() returned while the app holds it', async () => {
		const server = fakeBlobServer();
		const a = mod251(10);
		const b = mod251(11);
		await server.seed('ws1', a);
		await server.seed('ws1', b);
		const m = newManager(server, { cacheBytes: 15, evictionGraceMs: 0 });
		const held = await m.get('ws1', await refOf(a));
		await drain(m.stream('ws1', await refOf(b)));
		expectBytes(await bytesOf(held), a);
		await drain(m.stream('ws1', await refOf(a)));
		expect(server.gets).toBe(2);
	});

	it('returns a blob larger than the budget from stored parts', async () => {
		const server = fakeBlobServer();
		const data = mod251(BLOB_CHUNK_SIZE + 100);
		await server.seed('ws1', data);
		const store = new MemoryBlobStore();
		const m = newManager(server, { store, cacheBytes: 10, evictionGraceMs: 0 });
		const ref = await refOf(data);
		const blob = await m.get('ws1', ref);
		expectBytes(await bytesOf(blob), data);
		// Still cached while the Blob is held.
		expect(await store.cached('ws1', ref.digest)).toBeDefined();
	});
});

describe('blob streams', () => {
	it('yields verified chunks in order, from the network and then the cache', async () => {
		const server = fakeBlobServer();
		const data = mod251(2 * BLOB_CHUNK_SIZE + 5);
		await server.seed('ws1', data);
		const m = newManager(server);
		const ref = await refOf(data);

		const chunks = await drain(m.stream('ws1', ref));
		expect(chunks.map((c) => c.length)).toEqual([ BLOB_CHUNK_SIZE, BLOB_CHUNK_SIZE, 5 ]);
		expectBytes(concat(chunks), data);
		expect(server.gets).toBe(3);

		expectBytes(concat(await drain(m.stream('ws1', ref))), data);
		expect(server.gets).toBe(3);
		expect(await m.status('ws1', ref)).toEqual({ state: 'uploaded' });
	});

	it('reads a queued blob without the network', async () => {
		const server = fakeBlobServer();
		const m = newManager(server);
		const data = mod251(BLOB_CHUNK_SIZE + 1);
		const ref = await m.put('ws1', blobOf(data));
		expectBytes(concat(await drain(m.stream('ws1', ref))), data);
		expect(server.stats + server.gets).toBe(0);
	});

	it('fetches at most one chunk ahead of the reader', async () => {
		const server = fakeBlobServer();
		const data = mod251(3 * BLOB_CHUNK_SIZE);
		await server.seed('ws1', data);
		const m = newManager(server);
		const reader = m.stream('ws1', await refOf(data)).getReader();
		await reader.read();
		await new Promise((r) => setTimeout(r, 50));
		expect(server.gets).toBeLessThanOrEqual(2);
		await reader.cancel();
	});

	it('stops and caches nothing when cancelled mid-download', async () => {
		const server = fakeBlobServer();
		const data = mod251(3 * BLOB_CHUNK_SIZE);
		await server.seed('ws1', data);
		const store = new MemoryBlobStore();
		const m = newManager(server, { store });
		const ref = await refOf(data);
		const reader = m.stream('ws1', ref).getReader();
		await reader.read();
		await reader.cancel();
		const gets = server.gets;
		await new Promise((r) => setTimeout(r, 50));
		expect(server.gets).toBe(gets);
		expect(await store.cached('ws1', ref.digest)).toBeUndefined();
	});

	it('errors on a corrupt chunk', async () => {
		const server = fakeBlobServer();
		const data = mod251(100);
		await server.seed('ws1', data);
		server.hooks.getChunk = (_req, chunk) => chunk.subarray(1);
		const m = newManager(server);
		await expect(drain(m.stream('ws1', await refOf(data)))).rejects.toMatchObject({
			code: CodeBlobCorrupt,
		});
	});

	it('closes at once for the empty blob', async () => {
		const server = fakeBlobServer();
		await server.seed('ws1', new Uint8Array(0));
		const m = newManager(server);
		expect(await drain(m.stream('ws1', await refOf(new Uint8Array(0))))).toEqual([]);
	});
});

describe('blob status', () => {
	it('re-checks a watched pending blob after a sync', async () => {
		const server = fakeBlobServer();
		server.hooks.watchBlobs = noWatchBlobs;
		const m = newManager(server, { minRecheckMs: 60_000, maxRecheckMs: 60_000 });
		m.start();
		const data = mod251(40);
		const ref = await refOf(data);
		const rec = recorder(m, ref);
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'pending' ]));

		await server.seed('ws1', data);
		m.noteSync('ws2');
		await new Promise((r) => setTimeout(r, 10));
		expect(rec.states()).toEqual([ 'pending' ]);
		m.noteSync('ws1');
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'pending', 'uploaded' ]));
	});

	it('re-checks a watched pending blob on a timer while started', async () => {
		const server = fakeBlobServer();
		server.hooks.watchBlobs = noWatchBlobs;
		const m = newManager(server);
		m.start();
		const data = mod251(40);
		const ref = await refOf(data);
		const rec = recorder(m, ref);
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'pending' ]));
		await server.seed('ws1', data);
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'pending', 'uploaded' ]));
	});

	it('batches the status questions of one turn into one request', async () => {
		const server = fakeBlobServer();
		const m = newManager(server);
		const refs = await Promise.all([ 1, 2, 3 ].map((n) => refOf(mod251(n))));
		await Promise.all(refs.map((r) => m.status('ws1', r)));
		expect(server.stats).toBe(1);
	});
});

describe('blob status across tabs', () => {
	it('shares status between tabs and uploads from one of them', async () => {
		vi.stubGlobal('navigator', { userAgent: 'Mozilla/5.0', locks: fakeLocks() });
		const server = fakeBlobServer();
		let release: (() => void) | undefined;
		server.hooks.putChunk = (req) => {
			if (req.index === 0 && server.puts.length === 1) return new Promise<void>((r) => (release = r));
		};
		const name = `blob-manager-tabs-${dbCounter++}`;
		const opts = {
			openStore: () => IDBBlobStore.open(name),
			lockName: `lock-${name}`,
			channelName: `channel-${name}`,
		};
		const tabA = newManager(server, opts);
		const tabB = newManager(server, opts);
		tabA.start();
		tabB.start();

		const data = mod251(BLOB_CHUNK_SIZE + 3);
		const ref = await refOf(data);
		const rec = recorder(tabB, ref);
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'pending' ]));

		await tabA.put('ws1', blobOf(data));
		await vi.waitFor(() => expect(rec.states()).toContain('uploading'));
		release?.();
		await vi.waitFor(() => expect(rec.states().at(-1)).toBe('uploaded'));
		expect(rec.states()).toContain('queued');
		expect(server.begins).toHaveLength(1);

		// Closing the uploading tab hands the lock to the other.
		await tabA.close();
		const next = await tabB.put('ws1', blobOf(mod251(9)));
		await tabB.uploaded('ws1', next);
		expect(server.begins).toHaveLength(2);
	});
});

describe('blob status through WatchBlobs', () => {
	/** No re-check timer fires inside a test, so only the stream can deliver. */
	const quiet = { minRecheckMs: 60_000, maxRecheckMs: 60_000, watchDebounceMs: 1 };

	it('learns that a watched pending blob completed, with no re-check', async () => {
		const server = fakeBlobServer();
		const m = newManager(server, quiet);
		m.start();
		const data = mod251(40);
		const ref = await refOf(data);
		const rec = recorder(m, ref);
		await vi.waitFor(() => expect(server.activeWatches).toBe(1));
		expect(server.watches[0]?.digests).toEqual([ ref.digest ]);
		const stats = server.stats;

		await server.seed('ws1', data);
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'pending', 'uploaded' ]));
		expect(server.stats).toBe(stats);
		// The server ends the stream once every digest is complete.
		await vi.waitFor(() => expect(server.activeWatches).toBe(0));
		await new Promise((r) => setTimeout(r, 20));
		expect(server.watches).toHaveLength(1);
	});

	it('holds one stream per workspace and reopens it when the set changes', async () => {
		const server = fakeBlobServer();
		const m = newManager(server, quiet);
		m.start();
		const [ r1, r2, r3, other ] = await Promise.all([ 1, 2, 3, 4 ].map((n) => refOf(mod251(n))));
		recorder(m, r1!);
		recorder(m, r2!);
		recorder(m, other!, 'ws2');
		await vi.waitFor(() => expect(server.activeWatches).toBe(2));
		const first = server.watches.find((w) => w.workspaceId === 'ws1');
		expect(first?.digests).toEqual([ r1!.digest, r2!.digest ].sort());

		const third = recorder(m, r3!);
		await vi.waitFor(() =>
			expect(server.watches.filter((w) => w.workspaceId === 'ws1').at(-1)?.digests).toHaveLength(3)
		);
		await vi.waitFor(() => expect(server.activeWatches).toBe(2));

		third.unsub();
		await vi.waitFor(() =>
			expect(server.watches.filter((w) => w.workspaceId === 'ws1').at(-1)?.digests).toHaveLength(2)
		);
	});

	it('closes the stream once nothing pending is watched, and on stop', async () => {
		const server = fakeBlobServer();
		const m = newManager(server, quiet);
		m.start();
		const rec = recorder(m, await refOf(mod251(5)));
		await vi.waitFor(() => expect(server.activeWatches).toBe(1));
		rec.unsub();
		await vi.waitFor(() => expect(server.activeWatches).toBe(0));

		recorder(m, await refOf(mod251(6)));
		await vi.waitFor(() => expect(server.activeWatches).toBe(1));
		await m.stop();
		await vi.waitFor(() => expect(server.activeWatches).toBe(0));
	});

	it('opens no stream while stopped', async () => {
		const server = fakeBlobServer();
		const m = newManager(server, quiet);
		const rec = recorder(m, await refOf(mod251(5)));
		await vi.waitFor(() => expect(rec.states()).toEqual([ 'pending' ]));
		await new Promise((r) => setTimeout(r, 20));
		expect(server.watches).toHaveLength(0);
	});

	it('falls back to re-checks for good on a server without WatchBlobs', async () => {
		const server = fakeBlobServer();
		server.hooks.watchBlobs = noWatchBlobs;
		const m = newManager(server, { watchDebounceMs: 1 });
		m.start();
		const data = mod251(40);
		const ref = await refOf(data);
		const rec = recorder(m, ref);
		await vi.waitFor(() => expect(server.watches).toHaveLength(1));
		await server.seed('ws1', data);
		await vi.waitFor(() => expect(rec.states().at(-1)).toBe('uploaded'));

		recorder(m, await refOf(mod251(41)));
		await new Promise((r) => setTimeout(r, 30));
		expect(server.watches).toHaveLength(1);
	});

	it('reopens the stream after a transient failure', async () => {
		const server = fakeBlobServer();
		let failures = 1;
		server.hooks.watchBlobs = () => {
			if (failures-- > 0) throw new ConnectError('down', Code.Unavailable);
		};
		const m = newManager(server, quiet);
		m.start();
		const data = mod251(40);
		const ref = await refOf(data);
		const rec = recorder(m, ref);
		await vi.waitFor(() => expect(server.activeWatches).toBe(1));
		expect(server.watches).toHaveLength(2);
		await server.seed('ws1', data);
		await vi.waitFor(() => expect(rec.states().at(-1)).toBe('uploaded'));
	});

	it('stops watching a revoked workspace without re-checking it', async () => {
		const server = fakeBlobServer();
		server.hooks.watchBlobs = () => {
			throw new ConnectError('revoked', Code.PermissionDenied, {
				[ErrorCodeMetadataKey]: CodeWorkspaceAccessRevoked,
			});
		};
		const m = newManager(server, { watchDebounceMs: 1 });
		m.start();
		const rec = recorder(m, await refOf(mod251(40)));
		await vi.waitFor(() => expect(server.watches).toHaveLength(1));
		const stats = server.stats;
		m.noteSync('ws1');
		await new Promise((r) => setTimeout(r, 50));
		expect(server.watches).toHaveLength(1);
		expect(server.stats).toBe(stats);
		expect(rec.states()).toEqual([ 'pending' ]);
	});

	it('watches a revoked workspace again once access changes', async () => {
		const server = fakeBlobServer();
		let revoked = true;
		server.hooks.watchBlobs = () => {
			if (revoked) {
				throw new ConnectError('revoked', Code.PermissionDenied, {
					[ErrorCodeMetadataKey]: CodeWorkspaceAccessRevoked,
				});
			}
		};
		const m = newManager(server, quiet);
		m.start();
		const data = mod251(40);
		const ref = await refOf(data);
		const rec = recorder(m, ref);
		await vi.waitFor(() => expect(server.watches).toHaveLength(1));
		await new Promise((r) => setTimeout(r, 20));
		expect(server.watches).toHaveLength(1);

		revoked = false;
		m.accessChanged('ws1');
		await vi.waitFor(() => expect(server.activeWatches).toBe(1));
		await server.seed('ws1', data);
		await vi.waitFor(() => expect(rec.states().at(-1)).toBe('uploaded'));
	});

	it('shares a completion it hears with the other tabs', async () => {
		vi.stubGlobal('navigator', { userAgent: 'Mozilla/5.0', locks: fakeLocks() });
		const server = fakeBlobServer();
		const name = `blob-manager-watch-${dbCounter++}`;
		const opts = {
			openStore: () => IDBBlobStore.open(name),
			lockName: `lock-${name}`,
			channelName: `channel-${name}`,
			...quiet,
		};
		const watching = newManager(server, opts);
		const other = newManager(server, opts);
		watching.start();
		const data = mod251(40);
		const ref = await refOf(data);
		recorder(watching, ref);
		const seen = recorder(other, ref);
		await vi.waitFor(() => expect(seen.states()).toEqual([ 'pending' ]));
		// Only the started tab streams.
		await vi.waitFor(() => expect(server.activeWatches).toBe(1));
		await server.seed('ws1', data);
		await vi.waitFor(() => expect(seen.states()).toEqual([ 'pending', 'uploaded' ]));
	});
});
