import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';

const { db, SyncProvider, WorkspaceProvider } = createReactSync([ TodoSchema ]);

function newClient(): SyncClient<readonly [typeof TodoSchema]> {
	return new SyncClient({
		workspaces: [ 'ws1', 'ws2', 'ws3' ],
		accountId: 'acct',
		defaultWorkspace: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

function wrapper(client: SyncClient, workspaceId?: string) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		const inner = workspaceId === undefined
			? children
			: createElement(WorkspaceProvider, { workspaceId }, children);
		return createElement(SyncProvider, { client }, inner);
	};
}

describe('useQueryAll', () => {
	it('reads every workspace and names the one each row came from', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'two' });
		await client.ws('ws3').mutate.put(TodoSchema, { id: 'c', title: 'three' });

		const { result } = renderHook(() => db.todo.useQueryAll({}), {
			wrapper: wrapper(client),
		});
		await waitFor(() => expect(result.current).toHaveLength(3));

		expect(
			result.current
				.map((row) => [ row.workspaceId, row.value.id ])
				.sort(),
		).toEqual([ [ 'ws1', 'a' ], [ 'ws2', 'b' ], [ 'ws3', 'c' ] ]);
		await client.close();
	});

	it('ignores the enclosing workspace provider', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'two' });

		// A provider naming ws2 would scope useQuery to one row; this read spans
		// the client, so it must see both.
		const { result } = renderHook(() => db.todo.useQueryAll({}), {
			wrapper: wrapper(client, 'ws2'),
		});
		await waitFor(() => expect(result.current).toHaveLength(2));
		await client.close();
	});

	it('re-emits when any workspace commits', async () => {
		const client = newClient();
		const { result } = renderHook(() => db.todo.useQueryAll({}), {
			wrapper: wrapper(client),
		});
		await waitFor(() => expect(result.current.loading).toBe(false));
		expect(result.current).toHaveLength(0);

		await act(async () => {
			await client.ws('ws3').mutate.put(TodoSchema, { id: 'late', title: 'arrived' });
		});
		await waitFor(() => expect(result.current).toHaveLength(1));
		expect(result.current[0]?.workspaceId).toBe('ws3');
		await client.close();
	});

	it('narrows by the predicate across the merged set', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'x', completed: true });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'y', completed: false });
		await client.ws('ws3').mutate.put(TodoSchema, { id: 'c', title: 'z', completed: false });

		const { result } = renderHook(
			() => db.todo.useQueryAll({ where: (t) => t.completed.eq(false) }),
			{ wrapper: wrapper(client) },
		);
		await waitFor(() => expect(result.current).toHaveLength(2));
		expect(result.current.map((row) => row.value.id).sort()).toEqual([ 'b', 'c' ]);
		await client.close();
	});

	it('does not share an identity with the single-workspace read of the same query', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'two' });

		const { result } = renderHook(
			() => ({ all: db.todo.useQueryAll({}), one: db.todo.useQuery({}) }),
			{ wrapper: wrapper(client) },
		);
		await waitFor(() => expect(result.current.all).toHaveLength(2));
		expect(result.current.one).toHaveLength(1);
		await client.close();
	});
});
