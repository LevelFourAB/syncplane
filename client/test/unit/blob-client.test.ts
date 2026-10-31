// The blob layer as SyncClient wires it: the queue's IndexedDB database
// outlives a client, the uploader follows start() and close(), and the purge
// paths delete blob data with the rows.

import 'fake-indexeddb/auto';

import { Code, ConnectError } from '@connectrpc/connect';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	CodeWorkspaceAccessRevoked,
	CodeWorkspaceNoAccess,
	ErrorCodeMetadataKey,
} from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { type FakeBlobServer, fakeBlobServer, mod251 } from '../support/blobserver.js';

let prefixCounter = 0;
const clients: SyncClient[] = [];

afterEach(async () => {
	for (const c of clients.splice(0)) await c.close();
});

function newClient(
	server: FakeBlobServer,
	opts: { storagePrefix?: string; workspaces?: string[]; memory?: boolean } = {},
): SyncClient<readonly [typeof TodoSchema]> {
	const workspaces = opts.workspaces ?? [ 'ws1' ];
	const c = new SyncClient({
		transport: server.transport,
		workspaces,
		defaultWorkspace: workspaces[0],
		accountId: 'acct',
		storagePrefix: opts.storagePrefix ?? `blobclient${prefixCounter++}`,
		kvStore: opts.memory ? new MemoryKVStore() : undefined,
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
	clients.push(c);
	return c;
}

function blobOf(data: Uint8Array): Blob {
	return new Blob([ data as Uint8Array<ArrayBuffer> ]);
}

describe('SyncClient blobs', () => {
	it('keeps queued uploads across a restart and sends them once started', async () => {
		const server = fakeBlobServer();
		const prefix = `blobclient${prefixCounter++}`;
		const first = newClient(server, { storagePrefix: prefix });
		const ref = await first.ws('ws1').blobs.put(blobOf(mod251(300)));
		await first.close();
		expect(server.begins).toHaveLength(0);

		const second = newClient(server, { storagePrefix: prefix });
		expect(await second.blobs.pendingCount()).toBe(1);
		expect(await second.blobs.status(ref)).toEqual({ state: 'queued' });
		await second.start();
		await second.blobs.uploaded(ref);
		await second.blobs.idle();
		expect(server.has('ws1', ref.digest)).toBe(true);
	});

	it('does not upload before start()', async () => {
		const server = fakeBlobServer();
		const c = newClient(server);
		await c.blobs.put(blobOf(mod251(3)));
		await new Promise((r) => setTimeout(r, 20));
		expect(server.begins).toHaveLength(0);
	});

	it('aborts the chunk in flight when the client closes', async () => {
		const server = fakeBlobServer();
		let aborted = false;
		server.hooks.putChunk = (_req, ctx) =>
			new Promise<void>((_resolve, reject) => {
				ctx.signal.addEventListener('abort', () => {
					aborted = true;
					reject(new ConnectError('aborted', Code.Canceled));
				});
			});
		const c = newClient(server);
		const ref = await c.blobs.put(blobOf(mod251(10)));
		const waiting = c.blobs.uploaded(ref).catch((e: unknown) => e);
		await c.start();
		await vi.waitFor(() => expect(server.puts).toHaveLength(1));
		await c.close();
		expect(aborted).toBe(true);
		expect(await waiting).toBeInstanceOf(Error);
		await expect(c.blobs.pendingCount()).rejects.toThrow('closed');
	});

	it('retries an upload that waits for a grant once the workspace is granted', async () => {
		const server = fakeBlobServer();
		let granted = false;
		server.hooks.beginUpload = () => {
			if (!granted) {
				throw new ConnectError('no access yet', Code.PermissionDenied, {
					[ErrorCodeMetadataKey]: CodeWorkspaceNoAccess,
				});
			}
		};
		const c = newClient(server, { memory: true });
		const ref = await c.blobs.put(blobOf(mod251(10)));
		await c.start();
		await vi.waitFor(() => expect(server.begins.length).toBeGreaterThanOrEqual(1));
		expect(await c.blobs.status(ref)).toEqual({ state: 'queued' });

		// What the coordinator reports when a wait for a grant ends.
		const status = (c as unknown as {
			statusTracker: { updateWorkspace(ws: string, patch: { access: string }): void };
		}).statusTracker;
		granted = true;
		status.updateWorkspace('ws1', { access: 'none' });
		status.updateWorkspace('ws1', { access: 'granted' });
		await c.blobs.uploaded(ref);
	});

	it('watches a workspace again when its revoked access comes back', async () => {
		const server = fakeBlobServer();
		let revoked = true;
		server.hooks.watchBlobs = () => {
			if (revoked) {
				throw new ConnectError('revoked', Code.PermissionDenied, {
					[ErrorCodeMetadataKey]: CodeWorkspaceAccessRevoked,
				});
			}
		};
		const c = newClient(server, { memory: true });
		await c.start();
		const data = mod251(12);
		const ref = await (async () => {
			const other = newClient(server, { memory: true });
			return other.blobs.put(blobOf(data));
		})();
		const seen: string[] = [];
		c.blobs.watch(ref, (st) => seen.push(st.state));
		await vi.waitFor(() => expect(server.watches.length).toBeGreaterThanOrEqual(1));

		const status = (c as unknown as {
			statusTracker: { updateWorkspace(ws: string, patch: { access: string }): void };
		}).statusTracker;
		status.updateWorkspace('ws1', { access: 'revoked' });
		revoked = false;
		status.updateWorkspace('ws1', { access: 'granted' });
		await vi.waitFor(() => expect(server.activeWatches).toBe(1), { timeout: 2_000 });
		await server.seed('ws1', data);
		await vi.waitFor(() => expect(seen.at(-1)).toBe('uploaded'), { timeout: 2_000 });
	});

	it('targets the default workspace from client.blobs', async () => {
		const server = fakeBlobServer();
		const c = newClient(server, { workspaces: [ 'ws1', 'ws2' ] });
		const a = await c.blobs.put(blobOf(mod251(4)));
		const b = await c.ws('ws2').blobs.put(blobOf(mod251(5)));
		await c.start();
		await c.blobs.uploaded(a);
		await c.ws('ws2').blobs.uploaded(b);
		expect(server.has('ws1', a.digest)).toBe(true);
		expect(server.has('ws2', b.digest)).toBe(true);
		expect(server.has('ws1', b.digest)).toBe(false);
	});

	it('deletes a workspace blob queue on a purging removeWorkspace', async () => {
		const server = fakeBlobServer();
		const c = newClient(server, { workspaces: [ 'ws1', 'ws2' ], memory: true });
		await c.ws('ws1').blobs.put(blobOf(mod251(4)));
		await c.ws('ws2').blobs.put(blobOf(mod251(5)));
		await c.removeWorkspace('ws2', { purge: true });
		expect(await c.blobs.pendingCount()).toBe(1);
	});

	it('deletes the blob queue with purgeLocalData()', async () => {
		const server = fakeBlobServer();
		const prefix = `blobclient${prefixCounter++}`;
		const first = newClient(server, { storagePrefix: prefix });
		await first.blobs.put(blobOf(mod251(4)));
		await first.purgeLocalData();

		const second = newClient(server, { storagePrefix: prefix });
		expect(await second.blobs.pendingCount()).toBe(0);
	});
});
