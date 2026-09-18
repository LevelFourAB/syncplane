import { describe, expect, it } from 'vitest';

import { subscription } from '../../src/client.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// Key-set subscriptions end to end: a client names the rows it holds, the
// server ships those and nothing else, and moving a key in or out of the set
// delivers exactly that difference.
describe('key-set subscriptions', () => {
	/** Writes one widget per id with the given count, then pushes them. */
	async function seed(ws: string, counts: Record<string, number>): Promise<void> {
		const writer = newClient(ws);
		await writer.start();
		for (const [ id, count ] of Object.entries(counts)) {
			await writer.mutate.put(WidgetSchema, { id, count });
		}
		await writer.syncNow();
		await writer.close();
	}

	const keys = async (client: ReturnType<typeof newClient>) =>
		(await client.scan(WidgetSchema)).map((r) => r.key).sort();

	it('syncs the named rows and nothing else', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 1, b: 2, c: 3, d: 4 });

		// "z" names no row and is simply absent.
		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, { keys: [ 'c', 'a', 'z' ] }) ],
		});
		await reader.start();
		await reader.syncNow();

		expect(await keys(reader)).toEqual([ 'a', 'c' ]);
		await reader.close();
	});

	it('follows a write to a named row and ignores writes to the rest', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 1, b: 2 });

		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, { keys: [ 'a' ] }) ],
		});
		await reader.start();
		await reader.syncNow();

		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'a', count: 10 });
		await writer.mutate.put(WidgetSchema, { id: 'b', count: 20 });
		await writer.syncNow();
		await writer.close();

		await reader.syncNow();
		expect((await reader.get(WidgetSchema, 'a'))?.count).toBe(10);
		expect(await reader.get(WidgetSchema, 'b')).toBeUndefined();
		await reader.close();
	});

	it('delta-resyncs the difference when the set changes', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 1, b: 2, c: 3 });

		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, { keys: [ 'a', 'b' ] }) ],
		});
		await reader.start();
		await reader.syncNow();
		expect(await keys(reader)).toEqual([ 'a', 'b' ]);

		// b stays, a leaves, c enters.
		reader.setSubscriptions([ subscription(WidgetSchema, { keys: [ 'c', 'b' ] }) ]);
		await reader.syncNow();
		expect(await keys(reader)).toEqual([ 'b', 'c' ]);

		// An empty set keeps the entity subscribed but holds nothing.
		reader.setSubscriptions([ subscription(WidgetSchema, { keys: [] }) ]);
		await reader.syncNow();
		expect(await keys(reader)).toEqual([]);
		await reader.close();
	});

	it('reports the predicate bound on local reads', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 1, b: 2 });

		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, { keys: [ 'a' ] }) ],
		});
		await reader.start();
		await reader.syncNow();

		const rows = await reader.query(WidgetSchema, {});
		expect(rows).toHaveLength(1);
		expect(rows.coverage).toEqual({ complete: false, bounds: [ 'predicate' ] });
		await reader.close();
	});

	it('rejects a set above the server maximum', async () => {
		const ws = freshWorkspace();
		const many = Array.from({ length: 5_000 }, (_, i) => `k${i}`);
		const reader = newClient(ws, { subscriptions: [ subscription(WidgetSchema, { keys: many }) ] });
		await reader.start();
		await expect(reader.syncNow()).rejects.toThrow(/MaxSubscriptionKeys/);
		await reader.close();
	});
});
