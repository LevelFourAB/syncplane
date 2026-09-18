import { describe, expect, it } from 'vitest';

import { subscription } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { textOf } from '../../src/index.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

// Holds end to end: a row outside every declared subscription is brought into
// scope on demand, behaves like any local row while held — watchers, text
// handles, server writes and deletes — and leaves when the last hold does.
describe('holds', () => {
	/** Writes one widget per id, then pushes them. */
	async function seed(ws: string, widgets: Record<string, { count?: number; body?: string }>) {
		const writer = newClient(ws);
		await writer.start();
		for (const [ id, fields ] of Object.entries(widgets)) {
			await writer.mutate.put(WidgetSchema, { id, ...fields });
		}
		await writer.syncNow();
		await writer.close();
	}

	/** A client that syncs todos only: widgets are outside its scope. */
	const todoReader = (ws: string) =>
		newClient(ws, { subscriptions: [ subscription(TodoSchema) ], holdGraceMs: 0 });

	it('brings a row outside every subscription into scope and drops it on release', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: { count: 1 }, b: { count: 2 } });

		const reader = todoReader(ws);
		await reader.start();
		await reader.syncNow();
		expect(await reader.get(WidgetSchema, 'a')).toBeUndefined();
		expect((await reader.query(WidgetSchema, {})).coverage.bounds).toEqual([ 'unsubscribed' ]);

		const hold = reader.hold(WidgetSchema, 'a');
		await hold.ready;
		expect((await reader.get(WidgetSchema, 'a'))?.count).toBe(1);
		expect(await reader.get(WidgetSchema, 'b')).toBeUndefined();
		expect((await reader.query(WidgetSchema, {})).coverage.bounds).toEqual([ 'predicate' ]);

		hold.release();
		await eventually(async () => (await reader.get(WidgetSchema, 'a')) === undefined);
		expect((await reader.query(WidgetSchema, {})).coverage.bounds).toEqual([ 'unsubscribed' ]);
		await reader.close();
	});

	it('edits a held row through a text handle', async () => {
		const ws = freshWorkspace();
		await seed(ws, { doc: { body: 'hello' } });

		const reader = todoReader(ws);
		await reader.start();
		const hold = reader.hold(WidgetSchema, 'doc');
		await hold.ready;
		const text = await reader.text(WidgetSchema, 'doc', 'body');
		await text.insert(5, ' world');
		await text.close();
		await reader.syncNow();
		hold.release();
		await reader.close();

		const checker = newClient(ws);
		await checker.start();
		await checker.syncNow();
		expect(textOf((await checker.get(WidgetSchema, 'doc'))!.body)).toBe('hello world');
		await checker.close();
	});

	it('follows server writes and deletes of a held row', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: { count: 1 } });

		const reader = todoReader(ws);
		await reader.start();
		const hold = reader.hold(WidgetSchema, 'a');
		await hold.ready;

		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'a', count: 10 });
		await writer.syncNow();
		await reader.syncNow();
		expect((await reader.get(WidgetSchema, 'a'))?.count).toBe(10);

		// The tombstone arrives; the hold stays, so a re-created row returns.
		await writer.mutate.del(WidgetSchema, 'a');
		await writer.syncNow();
		await reader.syncNow();
		expect(await reader.get(WidgetSchema, 'a')).toBeUndefined();

		await writer.mutate.put(WidgetSchema, { id: 'a', count: 11 });
		await writer.syncNow();
		await reader.syncNow();
		expect((await reader.get(WidgetSchema, 'a'))?.count).toBe(11);

		hold.release();
		await writer.close();
		await reader.close();
	});

	it('includes a hold taken before start() in the first snapshot', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: { count: 1 }, b: { count: 2 } });

		const reader = todoReader(ws);
		const hold = reader.hold(WidgetSchema, 'a');
		await reader.start();
		await hold.ready;
		expect((await reader.get(WidgetSchema, 'a'))?.count).toBe(1);
		expect(await reader.get(WidgetSchema, 'b')).toBeUndefined();
		hold.release();
		await reader.close();
	});

	it('keeps a row while any hold on it remains', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: { count: 1 } });

		const reader = todoReader(ws);
		await reader.start();
		const first = reader.hold(WidgetSchema, 'a');
		const second = reader.hold(WidgetSchema, 'a');
		await Promise.all([ first.ready, second.ready ]);
		first.release();
		await reader.syncNow();
		expect((await reader.get(WidgetSchema, 'a'))?.count).toBe(1);
		second.release();
		await eventually(async () => (await reader.get(WidgetSchema, 'a')) === undefined);
		await reader.close();
	});

	it('reports a held row through watchers', async () => {
		const ws = freshWorkspace();
		await seed(ws, { a: { count: 1 } });

		const reader = todoReader(ws);
		await reader.start();
		await reader.syncNow();
		const seen: (number | undefined)[] = [];
		const watch = reader.watchRows(WidgetSchema, [ 'a' ], (rows) => {
			seen.push(rows.get('a')?.value.count);
		});
		await eventually(async () => seen.length === 1);
		expect(seen).toEqual([ undefined ]);

		const hold = reader.hold(WidgetSchema, 'a');
		await hold.ready;
		await eventually(async () => seen.length === 2);
		expect(seen[1]).toBe(1);

		hold.release();
		await eventually(async () => seen.length === 3);
		expect(seen[2]).toBeUndefined();
		watch.close();
		await reader.close();
	});
});
