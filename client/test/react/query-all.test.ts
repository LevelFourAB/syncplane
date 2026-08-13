import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';

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

	it('spans the workspaces it is given, and counts only them', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'two' });
		await client.ws('ws3').mutate.put(TodoSchema, { id: 'c', title: 'three' });

		const { result } = renderHook(
			() => db.todo.useQueryAll({}, { workspaces: [ 'ws1', 'ws3' ] }),
			{ wrapper: wrapper(client) },
		);
		await waitFor(() => expect(result.current).toHaveLength(2));
		expect(result.current.map((row) => row.workspaceId).sort()).toEqual([ 'ws1', 'ws3' ]);
		expect(result.current.total).toBe(2);
		await client.close();
	});

	it('resubscribes when the workspace set changes, but not when it is reordered', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'two' });
		await client.ws('ws3').mutate.put(TodoSchema, { id: 'c', title: 'three' });
		const subscribed = vi.spyOn(client, 'watchQueryAll');

		const { result, rerender } = renderHook(
			({ workspaces }: { workspaces: readonly string[] }) => db.todo.useQueryAll({}, { workspaces }),
			{ wrapper: wrapper(client), initialProps: { workspaces: [ 'ws1', 'ws2' ] } },
		);
		await waitFor(() => expect(result.current).toHaveLength(2));
		expect(subscribed).toHaveBeenCalledTimes(1);

		// A fresh array naming the same workspaces in another order asks the same
		// question, so the watch stays where it is.
		rerender({ workspaces: [ 'ws2', 'ws1' ] });
		expect(subscribed).toHaveBeenCalledTimes(1);

		rerender({ workspaces: [ 'ws1', 'ws3' ] });
		await waitFor(() =>
			expect(result.current.map((row) => row.workspaceId).sort()).toEqual([ 'ws1', 'ws3' ])
		);
		expect(subscribed).toHaveBeenCalledTimes(2);
		await client.close();
	});

	it('keeps a per-workspace predicate on its subscription across renders', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'mine-1' });
		const subscribed = vi.spyOn(client, 'watchQueryAll');

		// The callback is a new function every render; only what it RETURNS can
		// decide whether this is still the same question.
		const { result, rerender } = renderHook(
			({ title }: { title: string }) =>
				db.todo.useQueryAll({
					byWorkspace: () => (t) => t.title.eq(title),
				}, { workspaces: [ 'ws1' ] }),
			{ wrapper: wrapper(client), initialProps: { title: 'mine-1' } },
		);
		await waitFor(() => expect(result.current).toHaveLength(1));
		expect(subscribed).toHaveBeenCalledTimes(1);

		rerender({ title: 'mine-1' });
		expect(subscribed).toHaveBeenCalledTimes(1);

		rerender({ title: 'mine-2' });
		await waitFor(() => expect(result.current).toHaveLength(0));
		expect(subscribed).toHaveBeenCalledTimes(2);
		await client.close();
	});

	it('gives each workspace its own predicate under one subscription', async () => {
		const client = newClient();
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'mine-1' });
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'b', title: 'mine-2' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'c', title: 'mine-2' });

		// "mine" is a different literal in each workspace, which is the whole
		// reason one `where` cannot say it.
		const mine: Record<string, string> = { ws1: 'mine-1', ws2: 'mine-2' };
		const { result } = renderHook(
			() =>
				db.todo.useQueryAll({
					byWorkspace: (workspaceId) => {
						const title = mine[workspaceId];
						return title === undefined ? undefined : ((t) => t.title.eq(title));
					},
				}, { workspaces: [ 'ws1', 'ws2', 'ws3' ] }),
			{ wrapper: wrapper(client) },
		);
		await waitFor(() => expect(result.current).toHaveLength(2));
		expect(
			result.current.map((row) => [ row.workspaceId, row.value.id ]).sort(),
		).toEqual([ [ 'ws1', 'a' ], [ 'ws2', 'c' ] ]);
		await client.close();
	});
});
