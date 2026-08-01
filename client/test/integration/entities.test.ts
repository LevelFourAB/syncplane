import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { confirmedPrefix } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { freshWorkspace, newClient } from './helpers.js';

/** Keys the store holds under one entity's confirmed prefix. */
async function confirmedKeys(kv: MemoryKVStore, entity: string): Promise<string[]> {
	const tx = await kv.read();
	const keys: string[] = [];
	for await (const [ k ] of tx.scan(confirmedPrefix(entity))) keys.push(k);
	tx.release();
	return keys;
}

// A client is routinely a schema BEHIND the server it syncs with — a deploy
// adds an entity, or a build only ever declares the subset it needs — and a
// full-sync client (no subscriptions) is sent every entity the server holds.
// These pin what happens to the rows it cannot read.
describe('entities the client does not register', () => {
	it('are not stored by a full-sync client', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(TodoSchema, { id: 't1', title: 'seen' });
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 3 });
		await writer.syncNow();

		const kv = new MemoryKVStore();
		const reader = newClient(ws, { entities: [ TodoSchema ], kvStore: kv });
		await reader.start();
		await reader.syncNow();

		expect((await reader.get(TodoSchema, 't1'))?.title).toBe('seen');
		expect(await confirmedKeys(kv, 'widget')).toEqual([]);

		await writer.close();
		await reader.close();
	});

	// The other half of not storing them: a build that later declares the entity
	// has no rows for it and cannot pull them — they were all written below its
	// cursor — so registering it re-bootstraps the workspace.
	it('arrive when a later build registers them', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(TodoSchema, { id: 't1', title: 'seen' });
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 3 });
		await writer.syncNow();

		// The store outlives the build that opened it: the same KV store, reopened
		// by a client whose registry has grown.
		const kv = new MemoryKVStore();
		const before = newClient(ws, { entities: [ TodoSchema ], kvStore: kv });
		await before.start();
		await before.syncNow();
		await before.close();
		expect(await confirmedKeys(kv, 'widget')).toEqual([]);

		const after = newClient(ws, { entities: [ TodoSchema, WidgetSchema ], kvStore: kv });
		await after.start();
		await after.syncNow();

		expect((await after.get(WidgetSchema, 'w1'))?.count).toBe(3);
		expect((await after.get(TodoSchema, 't1'))?.title).toBe('seen');

		await writer.close();
		await after.close();
	});
});
