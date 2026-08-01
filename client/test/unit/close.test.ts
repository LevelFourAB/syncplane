import { createRouterTransport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

/** A client whose start() can reach the Coordinator (needs a transport). */
function newStartableClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		transport: createRouterTransport(() => {}),
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('SyncClient.close', () => {
	it('is idempotent', async () => {
		const client = newClient();
		await client.close();
		await client.close();
	});

	it('rejects reads, writes and lifecycle calls after close', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await client.close();

		const closed = /SyncClient is closed/;
		await expect(client.mutate.put(TodoSchema, { id: 't2', title: 'two' })).rejects.toThrow(closed);
		await expect(client.mutate.patch(TodoSchema, 't1', { title: 'x' })).rejects.toThrow(closed);
		await expect(client.mutate.del(TodoSchema, 't1')).rejects.toThrow(closed);
		await expect(client.mutate.custom('complete_all')).rejects.toThrow(closed);
		await expect(client.start()).rejects.toThrow(closed);
		await expect(client.syncNow()).rejects.toThrow(closed);
		await expect(client.get(TodoSchema, 't1')).rejects.toThrow(closed);
		await expect(client.scan(TodoSchema)).rejects.toThrow(closed);
		await expect(client.query(TodoSchema, {})).rejects.toThrow(closed);
		await expect(client.pendingCount()).rejects.toThrow(closed);
	});

	it('turns watch and watchQuery into no-ops after close', async () => {
		const client = newClient();
		await client.close();

		const seen: unknown[] = [];
		const unsub = client.watch(TodoSchema, (rows) => seen.push(rows));
		const unsubQuery = client.watchQuery(TodoSchema, {}, (rows) => seen.push(rows));
		await new Promise((r) => setTimeout(r, 10));
		expect(seen).toEqual([]);
		unsub();
		unsubQuery();
	});

	it('leaves no live coordinator when close races an in-flight start', async () => {
		const client = newStartableClient();
		// start() is suspended on its awaits (transport, format check, hub) when
		// close() runs; it must observe the teardown and not build a Coordinator.
		const starting = client.start();
		await client.close();
		await expect(starting).resolves.toBeUndefined();

		expect((client as unknown as { coordinator?: unknown }).coordinator).toBeUndefined();
		await expect(client.syncNow()).rejects.toThrow(/SyncClient is closed/);
	});

	it('does not resurrect the hub when close races an in-flight enqueue', async () => {
		const client = newClient();
		// The enqueue commits locally then routes through notifyChange(), which
		// lazily builds the hub + cross-tab channel — it must not do so post-close.
		const enqueued = client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await client.close();
		await enqueued.catch(() => undefined);

		const internals = client as unknown as { hubInstance?: unknown; changeShare?: unknown };
		expect(internals.hubInstance).toBeUndefined();
		expect(internals.changeShare).toBeUndefined();
	});

	it('stops live watchers when the client closes', async () => {
		const client = newClient();
		const seen: string[][] = [];
		const unsub = client.watch(TodoSchema, (rows) => seen.push(rows.map((r) => r.value.title)));
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await new Promise((r) => setTimeout(r, 10));
		const before = seen.length;
		expect(before).toBeGreaterThan(0);

		await client.close();
		// The hub is closed: no further emissions even if something notified.
		await new Promise((r) => setTimeout(r, 10));
		expect(seen.length).toBe(before);
		unsub();
	});
});
