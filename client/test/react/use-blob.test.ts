// useBlob: status, bytes and the object URL's lifetime across unmount and a
// changed ref, and the wait for a blob that is not on the server yet.

import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { BlobRefLike } from '../../src/blob/api.js';
import { SyncClient } from '../../src/client.js';
import { NoteSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync, useBlob } from '../../src/react/index.js';
import { type FakeBlobServer, fakeBlobServer, manifestOf, mod251 } from '../support/blobserver.js';

const { SyncProvider } = createReactSync([ NoteSchema ]);

let prefixCounter = 0;
let urls = 0;
const created = vi.fn((_b: Blob) => `blob:test/${++urls}`);
const revoked = vi.fn((_url: string) => {});
const clients: SyncClient[] = [];

beforeEach(() => {
	created.mockClear();
	revoked.mockClear();
	vi.stubGlobal('URL', Object.assign(URL, { createObjectURL: created, revokeObjectURL: revoked }));
});

afterEach(async () => {
	vi.unstubAllGlobals();
	for (const c of clients.splice(0)) await c.close();
});

function newClient(server: FakeBlobServer): SyncClient<readonly [typeof NoteSchema]> {
	const c = new SyncClient({
		transport: server.transport,
		workspaceId: 'ws1',
		storagePrefix: `useblob${prefixCounter++}`,
		kvStore: new MemoryKVStore(),
		entities: [ NoteSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
	clients.push(c);
	return c;
}

function wrapper(client: SyncClient) {
	return ({ children }: { children?: ReactNode }) =>
		createElement(SyncProvider, { client }, children);
}

function blobOf(data: Uint8Array, type = ''): Blob {
	return new Blob([ data as Uint8Array<ArrayBuffer> ], { type });
}

describe('useBlob', () => {
	it('reads a queued blob and revokes its URL on unmount', async () => {
		const client = newClient(fakeBlobServer());
		const ref = await client.blobs.put(blobOf(mod251(20), 'image/png'));

		const { result, unmount } = renderHook(() => useBlob(ref), { wrapper: wrapper(client) });
		await waitFor(() => expect(result.current.url).toBe('blob:test/1'));
		expect(result.current.status).toEqual({ state: 'queued' });
		expect(result.current.blob?.size).toBe(20);
		expect(result.current.blob?.type).toBe('image/png');

		unmount();
		expect(revoked).toHaveBeenCalledWith('blob:test/1');
	});

	it('revokes the old URL when the ref changes', async () => {
		const client = newClient(fakeBlobServer());
		const a = await client.blobs.put(blobOf(mod251(5)));
		const b = await client.blobs.put(blobOf(mod251(6)));

		const { result, rerender } = renderHook(({ r }: { r: BlobRefLike }) => useBlob(r), {
			wrapper: wrapper(client),
			initialProps: { r: a },
		});
		await waitFor(() => expect(result.current.url).toBeDefined());
		const first = result.current.url;

		rerender({ r: b });
		expect(revoked).toHaveBeenCalledWith(first);
		await waitFor(() => expect(result.current.blob?.size).toBe(6));
		expect(result.current.url).not.toBe(first);
	});

	it('stays empty for a row with no blob', () => {
		const client = newClient(fakeBlobServer());
		const { result } = renderHook(() => useBlob(undefined), { wrapper: wrapper(client) });
		expect(result.current).toEqual({
			status: undefined,
			blob: undefined,
			url: undefined,
			error: undefined,
		});
	});

	it('waits for a pending blob and reads it once the server has it', async () => {
		const server = fakeBlobServer();
		const client = newClient(server);
		await client.start();
		const data = mod251(30);
		const ref = { digest: (await manifestOf(data)).digest, size: 30n, mediaType: 'text/plain' };

		const { result } = renderHook(() => useBlob(ref), { wrapper: wrapper(client) });
		await waitFor(() => expect(result.current.status).toEqual({ state: 'pending' }));
		expect(result.current.url).toBeUndefined();
		expect(server.gets).toBe(0);

		await server.seed('ws1', data);
		await act(async () => {
			// What a completed pull does: re-check the watched pending blobs.
			(client as unknown as { blobMgr: { noteSync(ws?: string): void } }).blobMgr.noteSync('ws1');
		});
		await waitFor(() => expect(result.current.url).toBeDefined());
		expect(result.current.status).toEqual({ state: 'uploaded' });
		expect(result.current.blob?.size).toBe(30);
	});

	it('reports a failed read', async () => {
		const server = fakeBlobServer();
		const data = mod251(30);
		await server.seed('ws1', data);
		server.hooks.getChunk = (_req, chunk) => chunk.subarray(1);
		const client = newClient(server);
		const ref = { digest: (await manifestOf(data)).digest, size: 30n, mediaType: '' };

		const { result } = renderHook(() => useBlob(ref), { wrapper: wrapper(client) });
		await waitFor(() => expect(result.current.error).toBeDefined());
		expect(result.current.url).toBeUndefined();
	});
});
