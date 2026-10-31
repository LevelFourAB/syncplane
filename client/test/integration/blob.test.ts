// Blobs against the real engine: upload and download between two clients,
// dedupe, resume, the empty blob, and the read authorization the server
// derives from the rows that refer to a blob.
//
// Each test uses a fresh workspace. The /debug/rpc counters are global to the
// spawned server, and this is the only file that calls BlobService, so the
// deltas a test reads are its own: vitest runs the tests of one file in order.

import 'fake-indexeddb/auto';

import { connect } from 'node:net';
import { Duplex } from 'node:stream';

import type { Interceptor } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { afterEach, describe, expect, inject, it } from 'vitest';

import { BlobError, type BlobRefLike, type BlobStatus } from '../../src/blob/api.js';
import { BLOB_CHUNK_SIZE } from '../../src/blob/digest.js';
import type { SyncClient, SyncClientOptions } from '../../src/client.js';
import { CodeBlobRefused, CodeBlobUnavailable } from '../../src/codes.js';
import {
	BlobHolderSchema,
	BlobTreeSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { BlobRef, PutChunkRequest } from '../../src/gen/syncplane/v1/blob_pb.js';
import { type ByteStream, createTunnelTransport, fromByteStream } from '../../src/tunnel/index.js';
import { mod251 } from '../support/blobserver.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

const clients: SyncClient[] = [];

afterEach(async () => {
	for (const c of clients.splice(0)) await c.close();
});

function blobClient(ws: string, overrides: Partial<SyncClientOptions> = {}): SyncClient {
	const c = newClient(ws, { entities: [ BlobHolderSchema, BlobTreeSchema ], ...overrides });
	clients.push(c);
	return c;
}

/** A transport that authenticates as `subject` and runs `interceptors` first. */
function transport(
	subject?: string,
	interceptors: Interceptor[] = [],
): SyncClientOptions['transport'] {
	const auth: Interceptor = (next) => (req) => {
		if (subject !== undefined) req.header.set('Authorization', `Bearer ${subject}`);
		return next(req);
	};
	return createConnectTransport({
		baseUrl: inject('serverUrl'),
		httpVersion: '1.1',
		interceptors: [ auth, ...interceptors ],
	});
}

/** Records the index of every PutChunk that left this client. */
function recordPuts(into: number[]): Interceptor {
	return (next) => (req) => {
		if (!req.stream && req.method.name === 'PutChunk') {
			into.push((req.message as PutChunkRequest).index);
		}
		return next(req);
	};
}

async function rpcStats(): Promise<
	{ total: Record<string, number>; active: Record<string, number> }
> {
	const res = await fetch(`${inject('serverUrl')}/debug/rpc`);
	if (!res.ok) throw new Error(`rpc stats failed: ${res.status}`);
	return (await res.json()) as { total: Record<string, number>; active: Record<string, number> };
}

async function rpcTotal(method: string): Promise<number> {
	return (await rpcStats()).total[method] ?? 0;
}

async function rpcActive(method: string): Promise<number> {
	return (await rpcStats()).active[method] ?? 0;
}

function tunnelTransport(): SyncClientOptions['transport'] {
	const [ host, port ] = inject('tunnelAddr').split(':');
	return createTunnelTransport(() =>
		fromByteStream(Duplex.toWeb(connect(Number(port), host)) as unknown as ByteStream)
	);
}

/** A transport whose PutChunk calls wait until `open()`. */
function gatedUpload(): { transport: SyncClientOptions['transport']; open: () => void } {
	let open!: () => void;
	const gate = new Promise<void>((resolve) => (open = resolve));
	const hold: Interceptor = (next) => async (req) => {
		if (!req.stream && req.method.name === 'PutChunk') await gate;
		return next(req);
	};
	return { transport: transport(undefined, [ hold ]), open };
}

function blobOf(data: Uint8Array, type = 'application/octet-stream'): Blob {
	return new Blob([ data as Uint8Array<ArrayBuffer> ], { type });
}

/** Distinct bytes per call, so no two tests (or runs) share a digest. */
function uniqueBytes(n: number): Uint8Array {
	const data = mod251(n);
	const salt = crypto.getRandomValues(new Uint8Array(Math.min(16, n)));
	data.set(salt, 0);
	return data;
}

async function bytesOf(blob: Blob): Promise<Uint8Array> {
	return new Uint8Array(await blob.arrayBuffer());
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
	return a.length === b.length && a.every((v, i) => v === b[i]);
}

/** Uploads `data` from `c` and writes a BlobHolder row built by `row`. */
async function uploadAndRefer(
	c: SyncClient,
	key: string,
	data: Uint8Array,
	row: (ref: BlobRef) => Record<string, unknown>,
): Promise<BlobRef> {
	const ref = await c.blobs.put(blobOf(data));
	await c.blobs.uploaded(ref);
	await c.mutate.put(BlobHolderSchema, { id: key, title: key, ...row(ref) });
	await c.syncNow();
	return ref;
}

async function unavailable(c: SyncClient, ref: BlobRefLike): Promise<boolean> {
	const err = await c.blobs.get(ref).then(() => undefined, (e: unknown) => e);
	return err instanceof BlobError && err.code === CodeBlobUnavailable;
}

describe('blobs over the engine', () => {
	it('moves a multi-chunk blob from one client to another through a row', async () => {
		const ws = freshWorkspace();
		// Holds A's upload back until B watches, so B sees the blob pending.
		let open!: () => void;
		const gate = new Promise<void>((resolve) => (open = resolve));
		const holdUpload: Interceptor = (next) => async (req) => {
			if (!req.stream && req.method.name === 'PutChunk') await gate;
			return next(req);
		};
		const a = blobClient(ws, { transport: transport(undefined, [ holdUpload ]) });
		const b = blobClient(ws);
		await b.start();

		const data = uniqueBytes(BLOB_CHUNK_SIZE + 1000);
		const ref = await a.blobs.put(blobOf(data, 'image/png'));
		await a.mutate.put(BlobHolderSchema, { id: 'h1', title: 'photo', photo: ref });

		// B sees the row before the bytes arrive: the blob is pending.
		await a.start();
		await a.syncNow();
		await b.syncNow();
		const row = await b.get(BlobHolderSchema, 'h1');
		expect(row?.photo?.digest).toBe(ref.digest);
		const seen: BlobStatus[] = [];
		const unsub = b.blobs.watch(row!.photo!, (s) => seen.push(s));
		await eventually(async () => seen.length > 0);
		expect(seen).toEqual([ { state: 'pending' } ]);

		open();
		await a.blobs.uploaded(ref);
		// A pull is what re-checks a watched pending blob.
		await b.syncNow();
		await eventually(async () => seen.at(-1)?.state === 'uploaded');
		unsub();

		const blob = await b.blobs.get(row!.photo!);
		expect(blob.type).toBe('image/png');
		expect(sameBytes(await bytesOf(blob), data)).toBe(true);
	});

	it('re-checks a pending blob on a streaming client when a row change arrives', async () => {
		const ws = freshWorkspace();
		let open!: () => void;
		const gate = new Promise<void>((resolve) => (open = resolve));
		const holdUpload: Interceptor = (next) => async (req) => {
			if (!req.stream && req.method.name === 'PutChunk') await gate;
			return next(req);
		};
		const a = blobClient(ws, { transport: transport(undefined, [ holdUpload ]) });
		const b = blobClient(ws, { streamEnabled: true });
		await a.start();
		await b.start();

		const ref = await a.blobs.put(blobOf(uniqueBytes(300)));
		await a.mutate.put(BlobHolderSchema, { id: 'h1', title: 'photo', photo: ref });
		await a.syncNow();
		await eventually(async () => (await b.get(BlobHolderSchema, 'h1')) !== undefined);
		const seen: BlobStatus[] = [];
		const unsub = b.blobs.watch(ref, (st) => seen.push(st));
		await eventually(async () => seen.length > 0);
		expect(seen[0]).toEqual({ state: 'pending' });

		open();
		await a.blobs.uploaded(ref);
		// The completion alone sends no frame on a stream with no row change;
		// the next row change does, and its poke re-checks the blob.
		await a.mutate.put(BlobHolderSchema, { id: 'h2', title: 'later' });
		await a.syncNow();
		await eventually(async () => seen.at(-1)?.state === 'uploaded');
		unsub();
	});

	it('answers status() from the server without waiting for a pull', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		const b = blobClient(ws);
		await a.start();
		const ref = await uploadAndRefer(a, 'h1', uniqueBytes(100), (r) => ({ photo: r }));
		expect(await b.blobs.status(ref)).toEqual({ state: 'uploaded' });
	});

	it('skips PutChunk for bytes the workspace already holds', async () => {
		const ws = freshWorkspace();
		const data = uniqueBytes(500);
		const a = blobClient(ws);
		await a.start();
		const ref = await uploadAndRefer(a, 'h1', data, (r) => ({ photo: r }));

		const before = await rpcTotal('PutChunk');
		const b = blobClient(ws);
		await b.start();
		const again = await b.blobs.put(blobOf(data));
		expect(again.digest).toBe(ref.digest);
		await b.blobs.uploaded(again);
		expect(await rpcTotal('PutChunk')).toBe(before);
	});

	it('uploads into each workspace on its own', async () => {
		const data = uniqueBytes(64);
		const a = blobClient(freshWorkspace());
		const b = blobClient(freshWorkspace());
		await a.start();
		await b.start();
		await uploadAndRefer(a, 'h1', data, (r) => ({ photo: r }));
		const before = await rpcTotal('PutChunk');
		await uploadAndRefer(b, 'h1', data, (r) => ({ photo: r }));
		expect(await rpcTotal('PutChunk')).toBe(before + 1);
	});

	it('resumes an interrupted upload with only the missing chunks', async () => {
		const ws = freshWorkspace();
		const storage = {
			kvStore: undefined,
			accountId: `acct-${crypto.randomUUID()}`,
			storagePrefix: `blobresume${Date.now()}`,
		};
		// Holds chunk 1 back until the client closes, which aborts it before it
		// reaches the server.
		const holdChunkOne: Interceptor = (next) => async (req) => {
			if (
				!req.stream && req.method.name === 'PutChunk' && (req.message as PutChunkRequest).index === 1
			) {
				await new Promise<void>((resolve) => req.signal.addEventListener('abort', () => resolve()));
				req.signal.throwIfAborted();
			}
			return next(req);
		};
		const firstPuts: number[] = [];
		const first = blobClient(ws, {
			...storage,
			transport: transport(undefined, [ recordPuts(firstPuts), holdChunkOne ]),
		});
		const data = uniqueBytes(BLOB_CHUNK_SIZE + 77);
		const ref = await first.blobs.put(blobOf(data));
		const before = await rpcTotal('PutChunk');
		await first.start();
		await eventually(async () => firstPuts.length === 2);
		await first.close();
		expect(await rpcTotal('PutChunk')).toBe(before + 1);

		const secondPuts: number[] = [];
		const second = blobClient(ws, {
			...storage,
			transport: transport(undefined, [ recordPuts(secondPuts) ]),
		});
		expect(await second.blobs.pendingCount()).toBe(1);
		await second.start();
		await second.blobs.uploaded(ref);
		expect(secondPuts).toEqual([ 1 ]);

		await second.mutate.put(BlobHolderSchema, { id: 'h1', title: 't', photo: ref });
		await second.syncNow();
		const reader = blobClient(ws);
		expect(sameBytes(await bytesOf(await reader.blobs.get(ref)), data)).toBe(true);
	});

	it('uploads and reads the empty blob', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		await a.start();
		const before = await rpcTotal('PutChunk');
		const ref = await uploadAndRefer(a, 'h1', new Uint8Array(0), (r) => ({ photo: r }));
		expect(await rpcTotal('PutChunk')).toBe(before);
		const b = blobClient(ws);
		expect((await b.blobs.get(ref)).size).toBe(0);
	});

	it('uploads through the tunnel', async () => {
		const ws = freshWorkspace();
		const [ host, port ] = inject('tunnelAddr').split(':');
		const tunneled = blobClient(ws, {
			transport: createTunnelTransport(() =>
				fromByteStream(Duplex.toWeb(connect(Number(port), host)) as unknown as ByteStream)
			),
		});
		await tunneled.start();
		const data = uniqueBytes(BLOB_CHUNK_SIZE + 9);
		const ref = await uploadAndRefer(tunneled, 'h1', data, (r) => ({ photo: r }));
		const direct = blobClient(ws);
		expect(sameBytes(await bytesOf(await direct.blobs.get(ref)), data)).toBe(true);
	});
});

describe('blob read authorization over the engine', () => {
	it('serves no blob that no row refers to', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		await a.start();
		const ref = await a.blobs.put(blobOf(uniqueBytes(10)));
		await a.blobs.uploaded(ref);
		const b = blobClient(ws);
		expect(await unavailable(b, ref)).toBe(true);
		expect(await b.blobs.status(ref)).toEqual({ state: 'pending' });
	});

	it('hides the blob of a hidden row', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		await a.start();
		const ref = await uploadAndRefer(a, 'h1', uniqueBytes(10), (r) => ({ photo: r, hidden: true }));
		expect(await unavailable(blobClient(ws), ref)).toBe(true);
	});

	it('serves a guarded field only to the caller who may read it', async () => {
		const ws = freshWorkspace();
		const alice = blobClient(ws, { transport: transport('alice') });
		await alice.start();
		const ref = await uploadAndRefer(
			alice,
			'h1',
			uniqueBytes(10),
			(r) => ({ secret: r, ownerId: 'alice' }),
		);

		const bob = blobClient(ws, { transport: transport('bob') });
		expect(await unavailable(bob, ref)).toBe(true);
		const aliceAgain = blobClient(ws, { transport: transport('alice') });
		expect((await aliceAgain.blobs.get(ref)).size).toBe(10);
	});

	it('stops serving a blob once its only row is deleted', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		await a.start();
		const ref = await uploadAndRefer(a, 'h1', uniqueBytes(10), (r) => ({ photo: r }));
		expect((await blobClient(ws).blobs.get(ref)).size).toBe(10);

		await a.mutate.del(BlobHolderSchema, 'h1');
		await a.syncNow();
		expect(await unavailable(blobClient(ws), ref)).toBe(true);
	});

	it.each([
		[ 'a nested message', (r: BlobRef) => ({ album: { name: 'a', cover: r } }) ],
		[ 'a repeated field', (r: BlobRef) => ({ files: [ r ] }) ],
		[ 'a keyed list element', (r: BlobRef) => ({ pages: [ { id: 'p1', scan: r } ] }) ],
		[ 'a map value', (r: BlobRef) => ({ byName: { cover: r } }) ],
	])('authorizes a read through %s', async (_name, row) => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		await a.start();
		const ref = await uploadAndRefer(a, 'h1', uniqueBytes(10), row);
		expect((await blobClient(ws).blobs.get(ref)).size).toBe(10);
	});

	it('authorizes a read through a blob_tree row the server scans', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		await a.start();
		const shelf = await a.blobs.put(blobOf(uniqueBytes(11)));
		const deep = await a.blobs.put(blobOf(uniqueBytes(12)));
		await a.blobs.uploaded(shelf);
		await a.blobs.uploaded(deep);
		await a.mutate.put(BlobTreeSchema, {
			id: 't1',
			shelves: [ { id: 's1', files: [ shelf ] } ],
			root: { children: [ { children: [ { icon: deep } ] } ] },
		});
		await a.syncNow();
		const b = blobClient(ws);
		expect((await b.blobs.get(shelf)).size).toBe(11);
		expect((await b.blobs.get(deep)).size).toBe(12);
	});

	it('marks an upload above the server limit failed', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		await a.start();
		const putsBefore = await rpcTotal('PutChunk');
		const ref = await a.blobs.put(blobOf(new Uint8Array(inject('maxBlobBytes') + 1)));

		const err = await a.blobs.uploaded(ref).then(() => undefined, (e: unknown) => e);
		expect(err).toBeInstanceOf(BlobError);
		expect((err as BlobError).code).toBe(CodeBlobRefused);
		expect((err as BlobError).transportCode).toBe('ResourceExhausted');
		expect((await a.blobs.status(ref)).state).toBe('failed');
		expect(await a.blobs.pendingCount()).toBe(0);
		expect(await rpcTotal('PutChunk')).toBe(putsBefore);
	});
});

// These tests share the server's WatchBlobs counters with every other test in
// this file, which vitest runs one at a time; no other file opens WatchBlobs.
describe('WatchBlobs over the engine', () => {
	/** Waits until `c` watches `ref` and reports it pending, with its stream open. */
	async function watchPending(c: SyncClient, ref: BlobRefLike): Promise<BlobStatus[]> {
		const seen: BlobStatus[] = [];
		c.blobs.watch(ref, (st) => seen.push(st));
		await eventually(async () => seen[0]?.state === 'pending');
		await eventually(async () => (await rpcActive('WatchBlobs')) >= 1);
		return seen;
	}

	it('tells a watching client that an upload completed, with no row change and no sync', async () => {
		const ws = freshWorkspace();
		const gated = gatedUpload();
		const a = blobClient(ws, { transport: gated.transport });
		const b = blobClient(ws);
		await a.start();
		await b.start();
		const ref = await a.blobs.put(blobOf(uniqueBytes(BLOB_CHUNK_SIZE + 3)));
		await a.mutate.put(BlobHolderSchema, { id: 'h1', title: 'photo', photo: ref });
		await a.syncNow();
		await b.syncNow();

		const opened = await rpcTotal('WatchBlobs');
		const seen = await watchPending(b, ref);
		expect(await rpcTotal('WatchBlobs')).toBeGreaterThan(opened);
		const stats = await rpcTotal('StatBlobs');

		gated.open();
		await a.blobs.uploaded(ref);
		await eventually(async () => seen.at(-1)?.state === 'uploaded');
		// The stream told B, not a re-check: B asked StatBlobs nothing since.
		expect(await rpcTotal('StatBlobs')).toBe(stats);
		// The server ends the stream once every digest is complete.
		await eventually(async () => (await rpcActive('WatchBlobs')) === 0);
	});

	it('reports a blob complete only once a row naming it arrives', async () => {
		const ws = freshWorkspace();
		const a = blobClient(ws);
		const b = blobClient(ws);
		await a.start();
		await b.start();
		const ref = await a.blobs.put(blobOf(uniqueBytes(50)));
		await a.blobs.uploaded(ref);

		// No row refers to it yet, so B sees it missing even though it is stored.
		const seen = await watchPending(b, ref);
		await new Promise((r) => setTimeout(r, 300));
		expect(seen.map((st) => st.state)).toEqual([ 'pending' ]);

		await a.mutate.put(BlobHolderSchema, { id: 'h1', title: 'photo', photo: ref });
		await a.syncNow();
		await eventually(async () => seen.at(-1)?.state === 'uploaded');
		await eventually(async () => (await rpcActive('WatchBlobs')) === 0);
	});

	it('streams through the tunnel', async () => {
		const ws = freshWorkspace();
		const gated = gatedUpload();
		const a = blobClient(ws, { transport: gated.transport });
		const b = blobClient(ws, { transport: tunnelTransport() });
		await a.start();
		await b.start();
		const ref = await a.blobs.put(blobOf(uniqueBytes(70)));
		await a.mutate.put(BlobHolderSchema, { id: 'h1', title: 'photo', photo: ref });
		await a.syncNow();

		const seen = await watchPending(b, ref);
		gated.open();
		await eventually(async () => seen.at(-1)?.state === 'uploaded');
	});
});
