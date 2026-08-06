import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CodeWorkspacePurged } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { Row } from '../../src/read.js';
import type { RejectionInfo } from '../../src/sync/coordinator.js';

// One client, several workspaces, one store. These specs stay entirely local
// (no start(), so no transport): they cover the partitioning the account-scoped
// store gives — reads, writes, watchers, undo and purge all scoped to one
// workspace, with the cross-workspace reads on top.

const A = 'ws-a';
const B = 'ws-b';

function newClient(
	opts: { undo?: boolean; onMutationRejected?: (info: RejectionInfo) => void } = {},
) {
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ A, B ],
		defaultWorkspace: A,
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		undo: opts.undo ? { enabled: true } : undefined,
		onMutationRejected: opts.onMutationRejected,
	});
}

const flush = () => new Promise((r) => setTimeout(r, 5));

describe('multi-workspace client', () => {
	it('requires an accountId once it holds more than one workspace', () => {
		expect(() =>
			new SyncClient({
				workspaces: [ A, B ],
				kvStore: new MemoryKVStore(),
				entities: [ TodoSchema ],
			})
		).toThrow(/accountId is required/);

		// A single-workspace client keeps its store where it always was: the
		// workspace id doubles as the account id.
		const solo = new SyncClient({
			workspaceId: A,
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema ],
		});
		expect(solo.accountId).toBe(A);
		expect(solo.workspaceId).toBe(A);
	});

	it('keeps rows of the same key in different workspaces apart', async () => {
		const client = newClient();
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'in A' });
		await client.ws(B).mutate.put(TodoSchema, { id: 't1', title: 'in B' });

		expect((await client.ws(A).get(TodoSchema, 't1'))?.title).toBe('in A');
		expect((await client.ws(B).get(TodoSchema, 't1'))?.title).toBe('in B');
		// The client-level API is the default workspace's.
		expect((await client.get(TodoSchema, 't1'))?.title).toBe('in A');

		const inA = await client.ws(A).scan(TodoSchema);
		expect(inA.map((r) => r.workspaceId)).toEqual([ A ]);

		// One prefix scan sees both, each row naming where it lives.
		const all = await client.scanAll(TodoSchema);
		expect(all.map((r) => [ r.workspaceId, r.value.title ])).toEqual([
			[ A, 'in A' ],
			[ B, 'in B' ],
		]);
		await client.close();
	});

	it('evaluates a cross-workspace query over the merged set, not per workspace', async () => {
		const client = newClient();
		await client.ws(A).mutate.put(TodoSchema, { id: 'a1', title: 'bbb' });
		await client.ws(B).mutate.put(TodoSchema, { id: 'b1', title: 'aaa' });

		// Ordering and limit apply to the union: the top row comes from B even
		// though A is scanned first.
		const top = await client.queryAll(TodoSchema, {
			orderBy: (t) => [ t.title.asc() ],
			limit: 1,
		});
		expect(top.map((r) => [ r.workspaceId, r.value.title ])).toEqual([ [ B, 'aaa' ] ]);
		await client.close();
	});

	it('counts pending mutations per workspace and totals them on the status', async () => {
		const client = newClient();
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'a' });
		await client.ws(B).mutate.put(TodoSchema, { id: 't2', title: 'b' });
		await client.ws(B).mutate.put(TodoSchema, { id: 't3', title: 'c' });

		expect(await client.ws(A).pendingCount()).toBe(1);
		expect(await client.ws(B).pendingCount()).toBe(2);
		expect(client.status().pendingMutations).toBe(3);
		expect(client.status().workspaces[A]?.pendingMutations).toBe(1);
		expect(client.ws(B).status().pendingMutations).toBe(2);
		await client.close();
	});

	it('numbers mutation ids per workspace, so neither queue gaps', async () => {
		const client = newClient();
		await client.ws(A).mutate.put(TodoSchema, { id: 'a1', title: 'a1' });
		await client.ws(B).mutate.put(TodoSchema, { id: 'b1', title: 'b1' });
		await client.ws(A).mutate.put(TodoSchema, { id: 'a2', title: 'a2' });

		// The server requires id == watermark + 1 per (workspace, client), so an
		// account-wide counter would leave B starting at 2.
		expect(await client.ws(A).pendingCount()).toBe(2);
		expect(await client.ws(B).pendingCount()).toBe(1);
		await client.close();
	});

	it('does not wake a workspace-scoped watcher for another workspace commit', async () => {
		const client = newClient();
		const seenA: Row<unknown>[][] = [];
		const seenAll: Row<unknown>[][] = [];
		const unsubA = client.ws(A).watch(TodoSchema, (rows) => seenA.push(rows));
		const unsubAll = client.watchQueryAll(TodoSchema, {}, (rows) => seenAll.push(rows));
		await flush();
		const afterInitialA = seenA.length;
		const afterInitialAll = seenAll.length;

		await client.ws(B).mutate.put(TodoSchema, { id: 't1', title: 'in B' });
		await vi.waitFor(() => expect(seenAll.length).toBeGreaterThan(afterInitialAll));
		// A's watcher is bound to A, so B's commit is not its business.
		expect(seenA.length).toBe(afterInitialA);

		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'in A' });
		await vi.waitFor(() => expect(seenA.length).toBeGreaterThan(afterInitialA));
		expect(seenA.at(-1)?.map((r) => r.workspaceId)).toEqual([ A ]);

		unsubA.close();
		unsubAll.close();
		await client.close();
	});

	it('keeps undo stacks per workspace', async () => {
		const client = newClient({ undo: true });
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'a' });
		expect(client.ws(A).canUndo()).toBe(true);
		// B never wrote anything, so it has nothing to undo — the stacks cannot
		// span workspaces (a batch is one savepoint in one sequence).
		expect(client.ws(B).canUndo()).toBe(false);

		await client.ws(A).undo();
		expect(await client.ws(A).get(TodoSchema, 't1')).toBeUndefined();
		await client.close();
	});

	it('addWorkspace starts an empty partition; removeWorkspace with purge clears it', async () => {
		const client = newClient();
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'in A' });
		await client.ws(B).mutate.put(TodoSchema, { id: 't1', title: 'in B' });

		const c = client.addWorkspace('ws-c');
		expect(client.workspaces).toEqual([ A, B, 'ws-c' ]);
		expect(await c.scan(TodoSchema)).toEqual([]);

		// Purging B deletes its rows and its queue, and leaves A alone.
		await client.removeWorkspace(B, { purge: true });
		expect(client.workspaces).toEqual([ A, 'ws-c' ]);
		expect(() => client.ws(B)).toThrow(/not held by this client/);
		expect((await client.scanAll(TodoSchema)).map((r) => [ r.workspaceId, r.value.title ]))
			.toEqual([ [ A, 'in A' ] ]);
		expect(client.status().pendingMutations).toBe(1);
		await client.close();
	});

	it('hands back the mutations a purge discards, and reports each one', async () => {
		const rejections: RejectionInfo[] = [];
		const client = newClient({ onMutationRejected: (info) => rejections.push(info) });
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'stays queued' });
		await client.ws(B).mutate.put(TodoSchema, { id: 't1', title: 'unsent' });
		await client.ws(B).mutate.patch(TodoSchema, 't1', { completed: true });

		const result = await client.removeWorkspace(B, { purge: true });

		// The queue is gone from the store, so the result is the only copy left —
		// an app that wants to keep the unsent work takes it from here.
		expect(result.removed).toBe(true);
		expect(result.purged).toBe(true);
		expect(result.discarded.map((d) => d.mutationId)).toEqual([ 1n, 2n ]);
		expect(result.discarded.every((d) => d.workspaceId === B)).toBe(true);
		const first = result.discarded[0]?.mutation;
		expect(first?.payload.case).toBe('crud');
		expect(first?.mutationId).toBe(1n);

		// Ignoring the result still does not lose the loss silently.
		expect(rejections.map((r) => [ r.workspaceId, r.errorCode ])).toEqual([
			[ B, CodeWorkspacePurged ],
			[ B, CodeWorkspacePurged ],
		]);

		// A's queue is untouched — a purge is scoped to the one workspace.
		expect(client.status().workspaces[A]?.pendingMutations).toBe(1);
		await client.close();
	});

	it('isolates a throwing onMutationRejected from the purge reporting it', async () => {
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			const seen: bigint[] = [];
			const client = newClient({
				onMutationRejected: (info) => {
					seen.push(info.mutationId);
					throw new Error('boom');
				},
			});
			await client.ws(B).mutate.put(TodoSchema, { id: 't1', title: 'unsent' });
			await client.ws(B).mutate.patch(TodoSchema, 't1', { completed: true });

			// The queue is already gone when the callbacks fire, so a throwing app
			// handler must neither fail removeWorkspace nor cost the mutations after
			// it their report (the coordinator reports rejection batches the same way).
			const result = await client.removeWorkspace(B, { purge: true });
			expect(result.discarded.map((d) => d.mutationId)).toEqual([ 1n, 2n ]);
			expect(seen).toEqual([ 1n, 2n ]);
			expect(errSpy).toHaveBeenCalled();
			await client.close();
		} finally {
			errSpy.mockRestore();
		}
	});

	it('reports nothing discarded when the workspace is removed without a purge', async () => {
		const rejections: RejectionInfo[] = [];
		const client = newClient({ onMutationRejected: (info) => rejections.push(info) });
		await client.ws(B).mutate.put(TodoSchema, { id: 't1', title: 'survives' });

		const result = await client.removeWorkspace(B);
		expect(result).toEqual({ removed: true, purged: false, discarded: [] });
		expect(rejections).toEqual([]);
		// The data is still there: re-adding the workspace finds its rows.
		expect((await client.addWorkspace(B).get(TodoSchema, 't1'))?.title).toBe('survives');

		// A workspace the client does not hold is a no-op, not an error.
		expect(await client.removeWorkspace('never-held', { purge: true }))
			.toEqual({ removed: false, purged: false, discarded: [] });
		await client.close();
	});

	it('removing the default workspace leaves the client-level API without one', async () => {
		const client = newClient();
		await client.removeWorkspace(A);
		expect(client.defaultWorkspace).toBeUndefined();
		expect(() => client.mutate).toThrow(/no default workspace/);
		// The remaining workspace is still fully usable through ws().
		await client.ws(B).mutate.put(TodoSchema, { id: 't1', title: 'in B' });
		expect((await client.ws(B).get(TodoSchema, 't1'))?.title).toBe('in B');
		await client.close();
	});

	it('scopes a transaction to one workspace', async () => {
		const client = newClient();
		await client.ws(B).transact(async (tx) => {
			expect(tx.workspaceId).toBe(B);
			await tx.mutate.put(TodoSchema, { id: 't1', title: 'one' });
			await tx.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		});
		// One batch, so one queue entry — in B only.
		expect(await client.ws(B).pendingCount()).toBe(1);
		expect(await client.ws(A).pendingCount()).toBe(0);
		expect((await client.ws(B).scan(TodoSchema)).map((r) => r.key)).toEqual([ 't1', 't2' ]);
		expect(await client.ws(A).scan(TodoSchema)).toEqual([]);
		await client.close();
	});

	// `:` is legal in a workspace id but is percent-encoded by wsSegment, so this
	// covers a key segment that is not the id verbatim.
	it('keeps namespaced workspace ids in their own partition', async () => {
		const client = new SyncClient({
			accountId: 'acct',
			workspaces: [ 'org:one', 'org:two' ],
			defaultWorkspace: 'org:one',
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		await client.ws('org:one').mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await client.ws('org:two').mutate.put(TodoSchema, { id: 't1', title: 'two' });
		expect((await client.ws('org:one').get(TodoSchema, 't1'))?.title).toBe('one');
		expect((await client.ws('org:two').get(TodoSchema, 't1'))?.title).toBe('two');
		// A row key may itself contain slashes; it is always the last segment.
		await client.ws('org:one').mutate.put(TodoSchema, { id: 'a/b', title: 'nested' });
		expect((await client.ws('org:one').get(TodoSchema, 'a/b'))?.title).toBe('nested');
		await client.close();
	});
});
