import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { CodeConflict } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { PairSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

describe('sync (poll-driven)', () => {
	it('mutates locally before any sync, then converges two clients', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);

		// Offline-first: mutations apply locally before start().
		await a.mutate.put(TodoSchema, { id: 't1', title: 'from A' });
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('from A');
		expect(await a.pendingCount()).toBe(1);

		await a.start();
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);

		await b.start();
		await b.syncNow();
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('from A');

		await a.close();
		await b.close();
	});

	it('keeps composite-key rows distinct when a field value contains the separator', async () => {
		// ("a/b", "c") and ("a", "b/c") both naively join to "a/b/c". The client
		// derives an escaped key the server validates, so the rows stay distinct
		// and survive a round-trip to a second client.
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();

		await a.mutate.put(PairSchema, { ns: 'a/b', id: 'c', label: 'first' });
		await a.mutate.put(PairSchema, { ns: 'a', id: 'b/c', label: 'second' });
		await a.syncNow();

		await b.start();
		await b.syncNow();

		for (const client of [ a, b ]) {
			expect((await client.get(PairSchema, 'a\\/b/c'))?.label).toBe('first');
			expect((await client.get(PairSchema, 'a/b\\/c'))?.label).toBe('second');
		}

		await a.close();
		await b.close();
	});

	it('merges concurrent field-level edits from both clients (LWW per field)', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'original' });
		await a.syncNow();
		await b.syncNow();

		// Concurrent edits to different fields, pushed in opposite order.
		await a.mutate.patch(TodoSchema, 't1', { title: 'renamed by A' });
		await b.mutate.patch(TodoSchema, 't1', { completed: true });
		await b.syncNow();
		await a.syncNow();
		await b.syncNow();

		for (const client of [ a, b ]) {
			const todo = await client.get(TodoSchema, 't1');
			expect(todo?.title).toBe('renamed by A');
			expect(todo?.completed).toBe(true);
		}

		await a.close();
		await b.close();
	});

	it("a PATCH after another client's DELETE is rejected, not resurrected (delete-wins)", async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const rejections: RejectionInfo[] = [];
		const b = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await b.start();

		// A creates the row and both clients converge on it.
		await a.mutate.put(TodoSchema, { id: 't1', title: 'original' });
		await a.syncNow();
		await b.syncNow();
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('original');

		// A deletes the row; B, still offline, renames the stale copy.
		await a.mutate.del(TodoSchema, 't1');
		await a.syncNow();
		await b.mutate.patch(TodoSchema, 't1', { title: 'renamed by B' });
		// B's optimistic copy still shows the rename until it learns of the delete.
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('renamed by B');

		// B pushes: the server rejects the rename as a conflict and B rolls back.
		await b.syncNow();
		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodeConflict);
		expect(await b.pendingCount()).toBe(0);

		// No ghost survives on either client: the delete wins.
		await b.syncNow();
		expect(await a.get(TodoSchema, 't1')).toBeUndefined();
		expect(await b.get(TodoSchema, 't1')).toBeUndefined();

		await a.close();
		await b.close();
	});

	it('keeps optimistic state visible while pending, server state after ack', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't1', title: 'v1' });
		await a.mutate.patch(TodoSchema, 't1', { title: 'v2 (optimistic)' });

		// Both pending; local read shows the latest optimistic value.
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('v2 (optimistic)');

		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('v2 (optimistic)');
		await a.close();
	});

	it('bootstraps a late-joining client via the stream', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		for (let i = 0; i < 10; i++) {
			await a.mutate.put(TodoSchema, { id: `t${i}`, title: `todo ${i}` });
		}
		await a.mutate.del(TodoSchema, 't3');
		await a.syncNow();

		const late = newClient(ws);
		await late.start();
		await late.syncNow(); // no cursor → bootstrap stream
		const rows = await late.scan(TodoSchema);
		expect(rows.map((r) => r.key)).toEqual(
			[ 't0', 't1', 't2', 't4', 't5', 't6', 't7', 't8', 't9' ],
		);

		await a.close();
		await late.close();
	});

	it('deletes propagate', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'doomed' });
		await a.syncNow();
		await b.syncNow();
		expect(await b.get(TodoSchema, 't1')).toBeDefined();

		await b.mutate.del(TodoSchema, 't1');
		await b.syncNow();
		await a.syncNow();
		expect(await a.get(TodoSchema, 't1')).toBeUndefined();

		await a.close();
		await b.close();
	});

	it('fires watchers as state changes', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();

		const snapshots: string[][] = [];
		const unsub = a.watch(TodoSchema, (rows) => {
			snapshots.push(rows.map((r) => r.value.title));
		});

		await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await a.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		await a.syncNow();
		// Allow the async watcher emits to flush.
		await new Promise((r) => setTimeout(r, 50));

		expect(snapshots.at(-1)).toEqual([ 'one', 'two' ]);
		unsub();
		await a.close();
	});

	it('converges a transact() batch to a second client as one mutation', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);

		await a.transact(async (tx) => {
			await tx.mutate.put(TodoSchema, { id: 't1', title: 'one' });
			await tx.mutate.put(TodoSchema, { id: 't2', title: 'two' });
			await tx.mutate.patch(TodoSchema, 't1', { completed: true });
		});
		// Three writes, one atomic mutation on the queue.
		expect(await a.pendingCount()).toBe(1);

		await a.start();
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);

		await b.start();
		await b.syncNow();
		const t1 = await b.get(TodoSchema, 't1');
		expect(t1?.title).toBe('one');
		expect(t1?.completed).toBe(true); // intra-batch PATCH after PUT
		expect((await b.get(TodoSchema, 't2'))?.title).toBe('two');

		await a.close();
		await b.close();
	});

	it('rolls a whole transact() batch back when the server rejects any part', async () => {
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();

		await a.transact(async (tx) => {
			await tx.mutate.put(TodoSchema, { id: 'good', title: 'sibling' });
			// PATCH of a row that never existed: the server rejects (delete-wins),
			// which must roll the whole batch back — including the PUT above.
			await tx.mutate.patch(TodoSchema, 'ghost', { title: 'nope' });
		});
		// Optimistically the PUT half is visible while the batch is pending.
		expect((await a.get(TodoSchema, 'good'))?.title).toBe('sibling');
		expect(await a.pendingCount()).toBe(1);

		await a.syncNow();

		// One rejection for the one batch envelope; both halves are gone.
		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodeConflict);
		expect(await a.pendingCount()).toBe(0);
		expect(await a.get(TodoSchema, 'good')).toBeUndefined();
		expect(await a.get(TodoSchema, 'ghost')).toBeUndefined();

		await a.close();
	});
});
