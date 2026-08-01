import { describe, expect, it } from 'vitest';

import { subscription } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// Windowed subscriptions end to end: the client syncs a rank-bounded working
// set, churn evicts what fell off the tail, and every read says how much of the
// entity it could actually see.
describe('windowed subscriptions', () => {
	/** Writes n widgets with ascending counts, then pushes them. */
	async function seed(ws: string, counts: Record<string, number>): Promise<void> {
		const writer = newClient(ws);
		await writer.start();
		for (const [ id, count ] of Object.entries(counts)) {
			await writer.mutate.put(WidgetSchema, { id, count });
		}
		await writer.syncNow();
		await writer.close();
	}

	const topTwoByCount = subscription(WidgetSchema, undefined, {
		limit: 2,
		orderBy: (w) => [ w.count.desc() ],
	});

	it('syncs the window and nothing beyond it', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 10, b: 30, c: 20, d: 5 });

		const reader = newClient(ws, { subscriptions: [ topTwoByCount ] });
		await reader.start();
		await reader.syncNow();

		const rows = await reader.scan(WidgetSchema);
		expect(rows.map((r) => r.key).sort()).toEqual([ 'b', 'c' ]);
		await reader.close();
	});

	it('drops a row from the local store once it falls off the tail', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 10, b: 20 });

		const reader = newClient(ws, { subscriptions: [ topTwoByCount ] });
		await reader.start();
		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key).sort()).toEqual([ 'a', 'b' ]);

		// A new top row evicts "a" — a change no predicate could have expressed.
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'c', count: 30 });
		await writer.syncNow();
		await writer.close();

		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key).sort()).toEqual([ 'b', 'c' ]);
		expect(await reader.get(WidgetSchema, 'a')).toBeUndefined();
		await reader.close();
	});

	it('re-ranks a byRecency window on every write', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 1, b: 1, c: 1 });

		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, undefined, { limit: 2, byRecency: true }) ],
		});
		await reader.start();
		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key).sort()).toEqual([ 'b', 'c' ]);

		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'a', count: 2 });
		await writer.syncNow();
		await writer.close();

		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key).sort()).toEqual([ 'a', 'c' ]);
		await reader.close();
	});

	it('delta-resyncs into a wider window instead of re-bootstrapping', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 30, b: 20, c: 10 });

		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, undefined, { limit: 1, byRecency: true }) ],
		});
		await reader.start();
		await reader.syncNow();
		expect(await reader.scan(WidgetSchema)).toHaveLength(1);

		reader.setSubscriptions([
			subscription(WidgetSchema, undefined, { limit: 3, byRecency: true }),
		]);
		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key).sort()).toEqual([ 'a', 'b', 'c' ]);
		await reader.close();
	});

	it('rejects a limit above the server maximum', async () => {
		const ws = freshWorkspace();
		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, undefined, { limit: 100_000, byRecency: true }) ],
		});
		await reader.start();
		await expect(reader.syncNow()).rejects.toThrow();
		await reader.close();
	});

	describe('coverage', () => {
		it('reports the window and its limit for a bounded entity', async () => {
			const ws = freshWorkspace();
			await seed(ws, { a: 10, b: 20, c: 30 });
			const reader = newClient(ws, { subscriptions: [ topTwoByCount ] });
			await reader.start();
			await reader.syncNow();

			const rows = await reader.query(WidgetSchema, {});
			expect(rows.coverage).toEqual({ complete: false, bounds: [ 'window' ], limit: 2 });
			await reader.close();
		});

		it('reports unsubscribed for an entity the set never names', async () => {
			const ws = freshWorkspace();
			const reader = newClient(ws, { subscriptions: [ topTwoByCount ] });
			await reader.start();
			await reader.syncNow();

			// Empty because nothing synced, not because nothing exists.
			const todos = await reader.scan(TodoSchema);
			expect(todos).toHaveLength(0);
			expect(todos.coverage).toEqual({ complete: false, bounds: [ 'unsubscribed' ] });
			await reader.close();
		});

		it('claims complete under full sync', async () => {
			const ws = freshWorkspace();
			await seed(ws, { a: 1 });
			const reader = newClient(ws);
			await reader.start();
			await reader.syncNow();

			expect((await reader.scan(WidgetSchema)).coverage).toEqual({ complete: true, bounds: [] });
			await reader.close();
		});
	});

	it('reaches an evicted row through queryServer', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 10, b: 20, c: 30 });

		const reader = newClient(ws, { subscriptions: [ topTwoByCount ] });
		await reader.start();
		await reader.syncNow();
		// "a" is outside the window, so the local store does not hold it...
		expect(await reader.get(WidgetSchema, 'a')).toBeUndefined();

		// ...but the server still answers for it. That escape hatch is the whole
		// reason a bounded working set is usable.
		const found = await reader.queryServer(WidgetSchema, { where: (w) => w.id.eq('a') });
		expect(found.map((r) => r.key)).toEqual([ 'a' ]);
		expect(found.coverage.complete).toBe(true);
		await reader.close();
	});

	it('reports a truncated server query as incomplete', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: 10, b: 20, c: 30 });
		const reader = newClient(ws, { subscriptions: [ topTwoByCount ] });
		await reader.start();

		const page = await reader.queryServer(WidgetSchema, { limit: 2 });
		expect(page).toHaveLength(2);
		expect(page.coverage).toEqual({ complete: false, bounds: [ 'window' ], limit: 2 });
		await reader.close();
	});
});
