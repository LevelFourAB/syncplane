import { describe, expect, it } from 'vitest';

import { subscription, type SubscriptionSpec, SyncClient } from '../../src/client.js';
import { attachDevtools } from '../../src/devtools.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

function newClient(subs?: SubscriptionSpec[]) {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		subscriptions: subs,
	});
}

describe('attachDevtools', () => {
	it('snapshots the pending queue and the optimistic overlay', async () => {
		const client = newClient();
		const devtools = attachDevtools(client, { expose: false });
		const todo = client.entityRegistry.bySchema(TodoSchema).name;

		await client.db.todo.mutate.put({ id: 't1', title: 'first' });
		await client.db.todo.mutate.put({ id: 't2', title: 'second' });

		const snap = await devtools.snapshot();
		expect(snap.sessionId).toBe(client.sessionId);
		expect(snap.status.pendingMutations).toBe(2);

		const ws = snap.workspaces.find((w) => w.workspaceId === 'ws1')!;
		expect(ws.pending).toHaveLength(2);
		expect(ws.pending[0]!.targets).toEqual([ { entity: todo, key: 't1' } ]);
		// Nothing has pushed: no server, no start().
		expect(ws.pending.every((m) => !m.pushed)).toBe(true);
		expect(ws.pushedThrough).toBe(0n);
		// Full sync: every entity complete.
		expect(ws.coverage[todo]!.complete).toBe(true);

		const overlays = snap.overlay.filter((o) => o.entity === todo);
		expect(overlays.map((o) => o.key).sort()).toEqual([ 't1', 't2' ]);
		expect(overlays.every((o) => !o.deleted && o.textEdits === 0)).toBe(true);

		devtools.detach();
		await client.close();
	});

	it('explains a visible optimistic row and a local tombstone', async () => {
		const client = newClient();
		const devtools = attachDevtools(client, { expose: false });
		await client.db.todo.mutate.put({ id: 't1', title: 'first' });

		const created = await devtools.explain(TodoSchema, 't1');
		expect(created.visible).toBe(true);
		expect(created.confirmed).toBe(false);
		expect(created.overlay).toBe('edit');
		expect(created.queued).toHaveLength(1);
		expect(created.verdict).toMatch(/optimistically/);

		await client.db.todo.mutate.del('t1');
		const deleted = await devtools.explain(TodoSchema, 't1');
		expect(deleted.visible).toBe(false);
		expect(deleted.overlay).toBe('tombstone');
		expect(deleted.verdict).toMatch(/Deleted locally/);

		devtools.detach();
		await client.close();
	});

	it('explains a row an unsubscribed entity cannot hold', async () => {
		const client = newClient([ subscription(WidgetSchema) ]);
		const devtools = attachDevtools(client, { expose: false });
		const todo = client.entityRegistry.bySchema(TodoSchema).name;

		const explanation = await devtools.explain(todo, 'missing');
		expect(explanation.visible).toBe(false);
		expect(explanation.coverage.bounds).toEqual([ 'unsubscribed' ]);
		expect(explanation.verdict).toMatch(/subscriptions do not name/);

		const snap = await devtools.snapshot();
		const ws = snap.workspaces.find((w) => w.workspaceId === 'ws1')!;
		expect(ws.coverage[todo]!.complete).toBe(false);

		devtools.detach();
		await client.close();
	});

	it('reports complete coverage for a row that is genuinely absent', async () => {
		const client = newClient();
		const devtools = attachDevtools(client, { expose: false });

		const explanation = await devtools.explain(TodoSchema, 'nope');
		expect(explanation.visible).toBe(false);
		expect(explanation.coverage.complete).toBe(true);
		expect(explanation.verdict).toMatch(/coverage is complete/);

		devtools.detach();
		await client.close();
	});

	it('retains rejections in the buffer', async () => {
		const client = newClient();
		const devtools = attachDevtools(client, { expose: false });
		const todo = client.entityRegistry.bySchema(TodoSchema).name;

		await client.db.todo.mutate.put({ id: 't1', title: 'doomed' });
		await client.removeWorkspace('ws1', { purge: true });

		const rejections = devtools.rejections();
		expect(rejections).toHaveLength(1);
		expect(rejections[0]!.errorCode).toBe('workspace_purged');
		expect(rejections[0]!.targets).toEqual([ { entity: todo, key: 't1' } ]);
		expect(rejections[0]!.at).toBeGreaterThan(0);

		devtools.detach();
		await client.close();
	});

	it('captures read-path events through the multi-listener seam', async () => {
		const client = newClient();
		const devtools = attachDevtools(client, { expose: false });

		await client.db.todo.mutate.put({ id: 't1', title: 'first' });
		await client.db.todo.query({});

		const queries = devtools.events().filter((e) => e.type === 'query');
		expect(queries.length).toBeGreaterThan(0);
		expect(queries[0]!.origin).toBe('query');

		devtools.detach();
		// Detached: nothing more is retained.
		await client.db.todo.query({});
		expect(devtools.events()).toHaveLength(0);
		await client.close();
	});

	it('publishes and withdraws the globalThis handle', async () => {
		const client = newClient();
		const globals = globalThis as Record<string, unknown>;
		const devtools = attachDevtools(client, { expose: '__syncplaneTest' });
		expect(globals.__syncplaneTest).toBe(devtools);
		devtools.detach();
		expect(globals.__syncplaneTest).toBeUndefined();
		await client.close();
	});

	it('counts optimistic text edits per row and reports held text peers', async () => {
		const client = newClient();
		const devtools = attachDevtools(client, { expose: false });
		const widget = client.entityRegistry.bySchema(WidgetSchema).name;
		await client.db.widget.mutate.put({ id: 'w1', label: 'l', body: 'hello' });
		const handle = await client.db.widget.text('w1', 'body');
		await handle.insert(5, '!');

		const snap = await devtools.snapshot();
		const row = snap.overlay.find((o) => o.entity === widget && o.key === 'w1')!;
		expect(row.textEdits).toBeGreaterThan(0);
		expect(snap.textPeers).toHaveLength(1);

		handle.close();
		devtools.detach();
		await client.close();
	});
});
