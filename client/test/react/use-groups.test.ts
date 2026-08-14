import { act, renderHook } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';

const { db, SyncProvider } = createReactSync([ TodoSchema ]);

function newClient(): SyncClient<readonly [typeof TodoSchema]> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

function providerWrapper(client: SyncClient) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		return createElement(SyncProvider, { client }, children);
	};
}

/** Flushes pending microtasks plus a macrotask — the async watch dispatch. */
function flush(): Promise<void> {
	return act(async () => {
		await new Promise((r) => setTimeout(r, 0));
	});
}

describe('useGroupCount', () => {
	it('reports loading before the first callback, then the groups', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'a', completed: false });
		await client.mutate.put(TodoSchema, { id: 't2', title: 'b', completed: true });
		const { result } = renderHook(() => db.todo.useGroupCount({ by: (t) => t.completed }), {
			wrapper: providerWrapper(client),
		});
		expect(result.current).toEqual([]);
		expect(result.current.loading).toBe(true);
		await flush();
		expect([ ...result.current ]).toEqual([
			{ value: false, count: 1 },
			{ value: true, count: 1 },
		]);
		expect(result.current.loading).toBe(false);
		await client.close();
	});

	it('re-emits when a write moves a row between groups', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'a', completed: false });
		const { result } = renderHook(() => db.todo.useGroupCount({ by: (t) => t.completed }), {
			wrapper: providerWrapper(client),
		});
		await flush();
		expect([ ...result.current ]).toEqual([ { value: false, count: 1 } ]);
		await act(async () => {
			await client.mutate.patch(TodoSchema, 't1', { completed: true });
		});
		await flush();
		expect([ ...result.current ]).toEqual([ { value: true, count: 1 } ]);
		await client.close();
	});

	it('keeps the same reference when a write does not move the groups', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'a', completed: false });
		const { result } = renderHook(() => db.todo.useGroupCount({ by: (t) => t.completed }), {
			wrapper: providerWrapper(client),
		});
		await flush();
		const first = result.current;
		await act(async () => {
			// A title edit changes no group, so the hook must not re-render.
			await client.mutate.patch(TodoSchema, 't1', { title: 'renamed' });
		});
		await flush();
		expect(result.current).toBe(first);
		await client.close();
	});

	it('closes its subscription on unmount', async () => {
		const client = newClient();
		const { unmount } = renderHook(() => db.todo.useGroupCount({ by: (t) => t.completed }), {
			wrapper: providerWrapper(client),
		});
		await flush();
		const hub = (client as unknown as {
			hubInstance?: { groupWatchers: Set<unknown> };
		}).hubInstance;
		expect(hub?.groupWatchers.size).toBe(1);
		unmount();
		await flush();
		expect(hub?.groupWatchers.size).toBe(0);
		await client.close();
	});
});

describe('useDistinct', () => {
	it('reports the values alone, in ascending order', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'b' });
		await client.mutate.put(TodoSchema, { id: 't2', title: 'a' });
		await client.mutate.put(TodoSchema, { id: 't3', title: 'a' });
		const { result } = renderHook(() => db.todo.useDistinct({ by: (t) => t.title }), {
			wrapper: providerWrapper(client),
		});
		await flush();
		expect([ ...result.current ]).toEqual([ 'a', 'b' ]);
		await client.close();
	});
});
