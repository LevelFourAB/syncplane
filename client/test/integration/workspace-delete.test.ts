// A workspace the server deletes (Server.DeleteWorkspace) answers every client
// that syncs it with the permanent workspace denial. The client stops syncing
// it and reports the workspace as revoked, whether the denial reaches it on a
// push, on a pull, or through an open stream that the deletion resets.

import { describe, expect, inject, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { CodeWorkspaceReplaced } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

async function deleteWorkspace(ws: string, restore = false): Promise<void> {
	const res = await fetch(`${inject('serverUrl')}/debug/delete-workspace`, {
		method: 'POST',
		body: JSON.stringify({ workspace: ws, restore }),
	});
	if (!res.ok) throw new Error(`delete failed: ${res.status} ${await res.text()}`);
}

describe('a deleted workspace', () => {
	it('revokes access for a client that syncs it, and keeps its unsent writes out', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'before' });
		await client.syncNow();
		expect(client.status().workspaces[ws]?.access).not.toBe('revoked');
		expect(client.status().workspaces[ws]?.pendingMutations).toBe(0);

		await deleteWorkspace(ws);
		await client.mutate.put(TodoSchema, { id: 't2', title: 'after' });
		await client.syncNow();

		expect(client.status().workspaces[ws]?.access).toBe('revoked');
		// The write did not bring the workspace back for anyone else.
		const other = newClient(ws);
		await other.start();
		await other.syncNow();
		expect(other.status().workspaces[ws]?.access).toBe('revoked');
		expect(await other.get(TodoSchema, 't2')).toBeUndefined();
		await client.close();
		await other.close();
	});

	it('reaches a client through its open stream', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws, { streamEnabled: true });
		await client.start();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'before' });
		await eventually(async () => client.status().workspaces[ws]?.pendingMutations === 0);

		await deleteWorkspace(ws);

		await eventually(async () => client.status().workspaces[ws]?.access === 'revoked');
		await client.close();
	});

	it('syncs again as a new, empty workspace once restored', async () => {
		const ws = freshWorkspace();
		const before = newClient(ws);
		await before.start();
		await before.mutate.put(TodoSchema, { id: 't1', title: 'before' });
		await before.syncNow();
		await deleteWorkspace(ws);

		await deleteWorkspace(ws, true);

		const fresh = newClient(ws);
		await fresh.start();
		await fresh.mutate.put(TodoSchema, { id: 't2', title: 'new' });
		await fresh.syncNow();
		expect(fresh.status().workspaces[ws]?.access).not.toBe('revoked');
		expect(fresh.status().workspaces[ws]?.pendingMutations).toBe(0);
		expect(await fresh.get(TodoSchema, 't1')).toBeUndefined();
		expect((await fresh.get(TodoSchema, 't2'))?.title).toBe('new');
		await before.close();
		await fresh.close();
	});

	it('drops a queue written before the deletion instead of applying it to the restored workspace', async () => {
		// Given a client that synced the workspace, then queued a write and stopped
		const ws = freshWorkspace();
		const kvStore = new MemoryKVStore();
		const hold = { pushDebounceMs: 60_000, pushMaxDelayMs: 60_000 };
		const before = newClient(ws, { ...hold, kvStore });
		await before.start();
		await before.mutate.put(TodoSchema, { id: 't1', title: 'synced' });
		await before.syncNow();
		await before.mutate.put(TodoSchema, { id: 't2', title: 'queued' });
		await before.close();

		// When the server deletes and restores the workspace, another client
		// writes to the new one, and the first client comes back
		await deleteWorkspace(ws);
		await deleteWorkspace(ws, true);
		const fresh = newClient(ws);
		await fresh.start();
		await fresh.mutate.put(TodoSchema, { id: 'n1', title: 'new' });
		await fresh.syncNow();
		const rejections: RejectionInfo[] = [];
		const back = newClient(ws, { ...hold, kvStore, onMutationRejected: (i) => rejections.push(i) });
		await back.start();
		await back.syncNow().catch(() => undefined);
		await back.syncNow();

		// Then the queued write is reported and dropped, not applied
		expect(rejections.map((r) => [ r.mutationId > 0n, r.errorCode ])).toEqual([ [
			true,
			CodeWorkspaceReplaced,
		] ]);
		expect(back.status().workspaces[ws]?.pendingMutations).toBe(0);
		await fresh.syncNow();
		expect(await fresh.get(TodoSchema, 't2')).toBeUndefined();
		// And the client holds the new workspace
		expect(await back.get(TodoSchema, 't1')).toBeUndefined();
		expect((await back.get(TodoSchema, 'n1'))?.title).toBe('new');
		await fresh.close();
		await back.close();
	});
});
