import { describe, expect, it } from 'vitest';

import { subscription } from '../../src/client.js';
import type { EntityApi } from '../../src/db.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// A grouped read means the same thing on both sides or it means nothing: the
// group of a missing value, the order groups come back in, and what a repeated
// path does to the counts are all decisions the two implementations make
// independently. This is the tier that can watch them agree — the TS client
// against the real Go server, on rows the client also holds locally.
describe('grouped reads over the wire', () => {
	it('counts the same groups the local read does', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 30, label: 'x' });
		await writer.mutate.put(WidgetSchema, { id: 'w2', count: 5, label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w3', count: 20, label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w4', count: 20, label: 'a' });
		await writer.syncNow();

		const q = { by: (w: { label: unknown }) => w.label } as never;
		const remote = await writer.groupCountServer(WidgetSchema, q);
		expect(remote).toEqual([
			{ value: 'a', count: 1 },
			{ value: 'x', count: 1 },
			{ value: 'y', count: 2 },
		]);
		expect(await writer.groupCount(WidgetSchema, q)).toEqual(remote);

		await writer.close();
	});

	it('reaches rows outside the synced subset', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 30, label: 'x' });
		await writer.mutate.put(WidgetSchema, { id: 'w2', count: 5, label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w3', count: 5, label: 'y' });
		await writer.syncNow();

		// The reader syncs only count >= 25, so its local groups see one row and
		// the server's see all three — which is the whole point of the push-down.
		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, (w) => w.count.gte(25)) ],
		});
		await reader.start();
		await reader.syncNow();

		const q = { by: (w: { label: unknown }) => w.label } as never;
		expect(await reader.groupCount(WidgetSchema, q)).toEqual([ { value: 'x', count: 1 } ]);
		expect(await reader.groupCountServer(WidgetSchema, q)).toEqual([
			{ value: 'x', count: 1 },
			{ value: 'y', count: 2 },
		]);

		await writer.close();
		await reader.close();
	});

	it('agrees on where a missing value sorts and what it counts', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		// `maybe` has explicit presence, so a row that never set it resolves to
		// nothing — a group of its own, ordered last.
		await writer.mutate.put(WidgetSchema, { id: 'w1', maybe: 'b' });
		await writer.mutate.put(WidgetSchema, { id: 'w2', maybe: 'a' });
		await writer.mutate.put(WidgetSchema, { id: 'w3' });
		await writer.mutate.put(WidgetSchema, { id: 'w4' });
		await writer.syncNow();

		const q = { by: (w: { maybe: unknown }) => w.maybe } as never;
		const remote = await writer.groupCountServer(WidgetSchema, q);
		expect(remote).toEqual([
			{ value: 'a', count: 1 },
			{ value: 'b', count: 1 },
			{ value: undefined, count: 2 },
		]);
		expect(await writer.groupCount(WidgetSchema, q)).toEqual(remote);

		await writer.close();
	});

	it('counts a repeated path per element on both sides', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', tags: [ 'ui', 'bug' ] });
		await writer.mutate.put(WidgetSchema, { id: 'w2', tags: [ 'ui' ] });
		await writer.mutate.put(WidgetSchema, { id: 'w3', tags: [] });
		await writer.syncNow();

		const q = { by: (w: { tags: unknown }) => w.tags } as never;
		const remote = await writer.groupCountServer(WidgetSchema, q);
		// Three rows, four counted memberships: the counts describe elements, not
		// rows, which is what a label facet asks for.
		expect(remote).toEqual([
			{ value: 'bug', count: 1 },
			{ value: 'ui', count: 2 },
			{ value: undefined, count: 1 },
		]);
		expect(await writer.groupCount(WidgetSchema, q)).toEqual(remote);

		await writer.close();
	});

	it('narrows by a predicate and caps the groups it returns', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		for (
			const [ id, label, count ] of [
				[ 'w1', 'a', 30 ],
				[ 'w2', 'b', 30 ],
				[ 'w3', 'c', 30 ],
				[ 'w4', 'd', 1 ],
			] as const
		) {
			await writer.mutate.put(WidgetSchema, { id, label, count });
		}
		await writer.syncNow();

		const q = {
			by: (w: { label: unknown }) => w.label,
			where: (w: { count: { gte(n: number): unknown } }) => w.count.gte(10),
			limit: 2,
		} as never;
		expect(await writer.groupCountServer(WidgetSchema, q)).toEqual([
			{ value: 'a', count: 1 },
			{ value: 'b', count: 1 },
		]);

		await writer.close();
	});

	it('names the distinct values, with no counts', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w2', label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w3', label: 'x' });
		await writer.syncNow();

		const q = { by: (w: { label: unknown }) => w.label } as never;
		expect(await writer.distinctServer(WidgetSchema, q)).toEqual([ 'x', 'y' ]);
		expect(await writer.distinct(WidgetSchema, q)).toEqual([ 'x', 'y' ]);

		await writer.close();
	});

	it('is reachable from the schema-bound db facade', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', label: 'x' });
		await writer.syncNow();

		const cold = newClient(ws);
		const widgets = cold.db.widget as EntityApi<typeof WidgetSchema>;
		expect(await widgets.groupCount({ by: (w) => w.label })).toEqual([]);
		expect(await widgets.groupCountServer({ by: (w) => w.label })).toEqual([
			{ value: 'x', count: 1 },
		]);

		await writer.close();
		await cold.close();
	});

	it('rejects a path nothing can be grouped by, before it reaches the wire', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await expect(
			client.groupCountServer(WidgetSchema, { by: (w: { settings: unknown }) => w.settings } as never),
		).rejects.toThrow(/not comparable/);
		await client.close();
	});
});
