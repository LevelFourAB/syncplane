// transact() as a real atomic batch — no server needed: the batch applies to
// the optimistic overlay and queues as ONE mutation, which is what we assert.

import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CompleteAllArgsSchema, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

function newClient(undo?: { enabled?: boolean; maxDepth?: number }) {
	return new SyncClient({
		workspaceId: 'ws-transact',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, WidgetSchema ],
		undo,
	});
}

describe('transact()', () => {
	it('applies every mutation but queues exactly one (batch) mutation', async () => {
		const c = newClient();
		await c.transact(async (tx) => {
			await tx.mutate.put(WidgetSchema, { id: 'a', label: 'A' });
			await tx.mutate.put(WidgetSchema, { id: 'b', label: 'B' });
			await tx.mutate.put(WidgetSchema, { id: 'c', label: 'C' });
		});
		expect(await c.pendingCount()).toBe(1);
		expect((await c.get(WidgetSchema, 'a'))?.label).toBe('A');
		expect((await c.get(WidgetSchema, 'b'))?.label).toBe('B');
		expect((await c.get(WidgetSchema, 'c'))?.label).toBe('C');
		await c.close();
	});

	it('applies sub-mutations in order so a later op sees an earlier one', async () => {
		const c = newClient();
		await c.transact(async (tx) => {
			await tx.mutate.put(WidgetSchema, { id: 'a', label: 'first', count: 5 });
			await tx.mutate.patch(WidgetSchema, 'a', { label: 'second' });
		});
		const w = await c.get(WidgetSchema, 'a');
		expect(w?.label).toBe('second');
		expect(w?.count).toBe(5); // unmasked field from the PUT survives
		await c.close();
	});

	it('does not expose buffered writes until the transaction commits', async () => {
		const c = newClient();
		let midRead: unknown;
		await c.transact(async (tx) => {
			await tx.mutate.put(WidgetSchema, { id: 'a', label: 'A' });
			midRead = await tx.get(WidgetSchema, 'a');
		});
		expect(midRead).toBeUndefined();
		expect(await c.get(WidgetSchema, 'a')).toBeDefined();
		await c.close();
	});

	it('discards the whole batch when fn throws — nothing applies or queues', async () => {
		const c = newClient();
		await expect(
			c.transact(async (tx) => {
				await tx.mutate.put(WidgetSchema, { id: 'a', label: 'A' });
				throw new Error('boom');
			}),
		).rejects.toThrow('boom');
		expect(await c.get(WidgetSchema, 'a')).toBeUndefined();
		expect(await c.pendingCount()).toBe(0);
		await c.close();
	});

	it('is atomic without undo enabled too', async () => {
		const c = newClient(undefined); // undo off
		await c.transact(async (tx) => {
			await tx.mutate.put(WidgetSchema, { id: 'a', label: 'A' });
			await tx.mutate.put(WidgetSchema, { id: 'b', label: 'B' });
		});
		expect(await c.pendingCount()).toBe(1);
		expect(c.canUndo()).toBe(false);
		await c.close();
	});

	it('does not sweep concurrent client.mutate.* writes into the batch', async () => {
		const c = newClient();
		// A write issued through the client (not the handle) while fn is suspended
		// on an await must queue as its OWN mutation, not silently join the batch.
		let release: () => void;
		const gate = new Promise<void>((r) => {
			release = r;
		});
		await c.transact(async (tx) => {
			await tx.mutate.put(WidgetSchema, { id: 'in', label: 'in-batch' });
			const concurrent = c.mutate.put(WidgetSchema, { id: 'out', label: 'concurrent' });
			release();
			await gate;
			await concurrent;
		});
		// One batch (id 'in' alone is a single CRUD) + one independent write = 2.
		expect(await c.pendingCount()).toBe(2);
		expect((await c.get(WidgetSchema, 'in'))?.label).toBe('in-batch');
		expect((await c.get(WidgetSchema, 'out'))?.label).toBe('concurrent');
		await c.close();
	});

	it('lets a client custom mutation run during a transaction', async () => {
		const c = newClient();
		// Custom mutations cannot batch and are not on the handle; issued through
		// the client during fn they queue independently rather than throwing.
		await c.transact(async (tx) => {
			await tx.mutate.put(WidgetSchema, { id: 'a', label: 'A' });
			await c.mutate.custom('complete_all', CompleteAllArgsSchema, {});
		});
		expect(await c.pendingCount()).toBe(2);
		await c.close();
	});

	it('nested transact() commits an independent batch', async () => {
		const c = newClient();
		await c.transact(async (outer) => {
			await outer.mutate.put(WidgetSchema, { id: 'a', label: 'A' });
			await c.transact(async (inner) => {
				await inner.mutate.put(WidgetSchema, { id: 'b', label: 'B' });
				await inner.mutate.put(WidgetSchema, { id: 'c', label: 'C' });
			});
		});
		// Inner batch (b+c) commits as one, outer (a alone) as one: two mutations.
		expect(await c.pendingCount()).toBe(2);
		expect((await c.get(WidgetSchema, 'a'))?.label).toBe('A');
		expect((await c.get(WidgetSchema, 'b'))?.label).toBe('B');
		await c.close();
	});

	it('undoes and redoes the whole batch atomically as one step', async () => {
		const c = newClient({});
		await c.transact(async (tx) => {
			await tx.mutate.put(WidgetSchema, { id: 'a', label: 'A' });
			await tx.mutate.put(WidgetSchema, { id: 'b', label: 'B' });
		});

		expect(await c.undo()).toBe(true);
		expect(await c.get(WidgetSchema, 'a')).toBeUndefined();
		expect(await c.get(WidgetSchema, 'b')).toBeUndefined();
		// The undo itself is one atomic batch mutation on the queue.
		expect(await c.pendingCount()).toBe(2);

		expect(await c.redo()).toBe(true);
		expect((await c.get(WidgetSchema, 'a'))?.label).toBe('A');
		expect((await c.get(WidgetSchema, 'b'))?.label).toBe('B');
		await c.close();
	});
});
