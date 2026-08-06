import type { DescMessage } from '@bufbuild/protobuf';
import { describe, expect, expectTypeOf, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { buildEntityApis } from '../../src/db.js';
import {
	CompleteAllArgsSchema,
	type Todo,
	TodoSchema,
} from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import type { Row } from '../../src/read.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('client.db', () => {
	it('derives a camelCase key per entity with full typing', () => {
		const client = newClient();
		expectTypeOf(client.db).toHaveProperty('todo');
		expectTypeOf(client.db.todo.get).returns.resolves.toEqualTypeOf<Todo | undefined>();
		expectTypeOf(client.db.todo.scan).returns.resolves.toEqualTypeOf<RowSet<Todo>>();
	});

	it('round-trips put/get/scan through the facade', async () => {
		const client = newClient();
		await client.db.todo.mutate.put({ id: 't1', title: 'first' });
		await client.db.todo.mutate.put({ id: 't2', title: 'second' });

		const t1 = await client.db.todo.get('t1');
		expect(t1?.title).toBe('first');
		expect(await client.db.todo.scan()).toHaveLength(2);
		// Same data is visible through the schema-first API.
		expect((await client.get(TodoSchema, 't2'))?.title).toBe('second');
		await client.close();
	});

	it('create() mints an id, writes the row, and returns the key', async () => {
		const client = newClient();
		const id = await client.db.todo.mutate.create({ title: 'auto' });
		expect(id).toHaveLength(26);
		expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/);
		expectTypeOf(client.db.todo.mutate.create).returns.resolves.toEqualTypeOf<string>();

		const row = await client.db.todo.get(id);
		expect(row?.id).toBe(id);
		expect(row?.title).toBe('auto');
		await client.close();
	});

	it('create() respects a supplied key', async () => {
		const client = newClient();
		const id = await client.db.todo.mutate.create({ id: 'mine', title: 'kept' });
		expect(id).toBe('mine');
		expect((await client.db.todo.get('mine'))?.title).toBe('kept');
		await client.close();
	});

	it('supports patch and del', async () => {
		const client = newClient();
		await client.db.todo.mutate.put({ id: 't1', title: 'first' });
		await client.db.todo.mutate.patch('t1', { completed: true });
		expect((await client.db.todo.get('t1'))?.completed).toBe(true);
		expect((await client.db.todo.get('t1'))?.title).toBe('first');

		await client.db.todo.mutate.del('t1');
		expect(await client.db.todo.get('t1')).toBeUndefined();
		await client.close();
	});

	it('watch delivers rows for the bound entity', async () => {
		const client = newClient();
		const snapshots: Row<Todo>[][] = [];
		const unsub = client.db.todo.watch((rows) => snapshots.push(rows));
		await client.db.todo.mutate.put({ id: 't1', title: 'watched' });
		await new Promise((r) => setTimeout(r, 0));
		expect(snapshots.at(-1)?.map((r) => r.value.title)).toEqual([ 'watched' ]);
		unsub.close();
		await client.close();
	});

	it('watch/watchQuery throw synchronously on an unregistered schema', () => {
		// Mirrors scan/get/query: the registration error reaches the caller here
		// rather than escaping the deferred then-chain as an unhandledrejection.
		const client = newClient();
		expect(() => client.watch(CompleteAllArgsSchema, () => {})).toThrow(/is not a registered entity/);
		expect(() => client.watchQuery(CompleteAllArgsSchema, {}, () => {})).toThrow(
			/is not a registered entity/,
		);
	});

	it('is built once and reused', () => {
		const client = newClient();
		expect(client.db).toBe(client.db);
	});

	it('throws on simple-name collisions', () => {
		const a = { name: 'Todo', typeName: 'a.v1.Todo' } as DescMessage;
		const b = { name: 'Todo', typeName: 'b.v1.Todo' } as DescMessage;
		const stub = newClient();
		const registry = new EntityRegistry([ TodoSchema ]);
		expect(() => buildEntityApis([ a, b ], stub, registry)).toThrow(
			/maps to "todo" which is already taken/,
		);
	});
});
