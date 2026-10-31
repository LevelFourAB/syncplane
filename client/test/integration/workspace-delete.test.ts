// A workspace the server deletes (Server.DeleteWorkspace) answers every client
// that syncs it with the permanent workspace denial. The client stops syncing
// it and reports the workspace as revoked, whether the denial reaches it on a
// push, on a pull, or through an open stream that the deletion resets.

import { describe, expect, inject, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
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
});
