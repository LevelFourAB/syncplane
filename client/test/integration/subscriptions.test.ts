import { describe, expect, it } from 'vitest';

import { subscription } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

const highCount = subscription(WidgetSchema, (w) => w.count.gte(10));

describe('partial sync (subscriptions)', () => {
	it('bootstraps only rows matching the predicate and skips other entities', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w-low', count: 5 });
		await writer.mutate.put(WidgetSchema, { id: 'w-high', count: 50 });
		await writer.mutate.put(TodoSchema, { id: 't1', title: 'todo row' });
		await writer.syncNow();

		const reader = newClient(ws, { subscriptions: [ highCount ] });
		await reader.start();
		await reader.syncNow();

		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-high' ]);
		expect(await reader.scan(TodoSchema)).toEqual([]);

		await writer.close();
		await reader.close();
	});

	it('moves rows in and out of the local set as edits cross the boundary', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		const reader = newClient(ws, { subscriptions: [ highCount ] });
		await writer.start();
		await reader.start();

		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 5 });
		await writer.syncNow();
		await reader.syncNow();
		expect(await reader.get(WidgetSchema, 'w1')).toBeUndefined();

		// Edit into scope → the row appears.
		await writer.mutate.patch(WidgetSchema, 'w1', { count: 25 });
		await writer.syncNow();
		await reader.syncNow();
		expect((await reader.get(WidgetSchema, 'w1'))?.count).toBe(25);

		// Edit out of scope → the row vanishes (tombstoned, not stranded).
		await writer.mutate.patch(WidgetSchema, 'w1', { count: 1 });
		await writer.syncNow();
		await reader.syncNow();
		expect(await reader.get(WidgetSchema, 'w1')).toBeUndefined();

		await writer.close();
		await reader.close();
	});

	it('syncs into the new scope after setSubscriptions', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w-low', count: 5 });
		await writer.mutate.put(WidgetSchema, { id: 'w-high', count: 50 });
		await writer.syncNow();

		const reader = newClient(ws, { subscriptions: [ highCount ] });
		await reader.start();
		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-high' ]);

		// Swap to the complementary predicate: the server delta-resyncs the
		// difference (w-high tombstoned, w-low shipped) through ordinary pulls.
		reader.setSubscriptions([ subscription(WidgetSchema, (w) => w.count.lt(10)) ]);
		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-low' ]);

		// Back to full sync.
		reader.setSubscriptions([]);
		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-high', 'w-low' ]);

		await writer.close();
		await reader.close();
	});

	it('resolves setSubscriptions once the new scope is in the local store', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w-low', count: 5 });
		await writer.mutate.put(WidgetSchema, { id: 'w-high', count: 50 });
		await writer.syncNow();

		const reader = newClient(ws, { subscriptions: [ highCount ] });
		await reader.start();
		await reader.syncNow();

		// No syncNow(): the promise alone must cover the pull that widens scope.
		await reader.setSubscriptions([ subscription(WidgetSchema, (w) => w.count.lt(10)) ]);
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-low' ]);

		await reader.ws(ws).setSubscriptions([]);
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-high', 'w-low' ]);

		await writer.close();
		await reader.close();
	});

	it('resolves a setSubscriptions made before start() after the first pull', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w-low', count: 5 });
		await writer.mutate.put(WidgetSchema, { id: 'w-high', count: 50 });
		await writer.syncNow();

		const reader = newClient(ws, { subscriptions: [ highCount ] });
		const ready = reader.setSubscriptions([ subscription(WidgetSchema, (w) => w.count.lt(10)) ]);
		await reader.start();
		await ready;
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-low' ]);

		await writer.close();
		await reader.close();
	});

	it('resolves a replaced setSubscriptions once the later set is local', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w-low', count: 5 });
		await writer.mutate.put(WidgetSchema, { id: 'w-high', count: 50 });
		await writer.syncNow();

		const reader = newClient(ws, { subscriptions: [ highCount ] });
		await reader.start();
		await reader.syncNow();

		const first = reader.setSubscriptions([]);
		const second = reader.setSubscriptions([ subscription(WidgetSchema, (w) => w.count.lt(10)) ]);
		await first;
		await second;
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w-low' ]);

		await writer.close();
		await reader.close();
	});

	it("converges a pending patch that moves a row out of the client's own scope", async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { subscriptions: [ highCount ] });
		await a.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', count: 20 });
		await a.syncNow();
		expect((await a.get(WidgetSchema, 'w1'))?.count).toBe(20);

		// The patch leaves the row visible optimistically (transient ghost)
		// until the ack lands; after sync the row is out of scope and gone.
		await a.mutate.patch(WidgetSchema, 'w1', { count: 1 });
		expect((await a.get(WidgetSchema, 'w1'))?.count).toBe(1);
		await a.syncNow();
		await a.syncNow(); // second cycle pulls the tombstone past the ack
		expect(await a.get(WidgetSchema, 'w1')).toBeUndefined();

		// The row still exists for a full-sync client.
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w1'))?.count).toBe(1);

		await a.close();
		await b.close();
	});

	it('does not regress full sync when no subscriptions are set', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', count: 5 });
		await a.mutate.put(TodoSchema, { id: 't1', title: 'x' });
		await a.syncNow();
		await b.syncNow();

		expect((await b.get(WidgetSchema, 'w1'))?.count).toBe(5);
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('x');

		await a.close();
		await b.close();
	});
});
