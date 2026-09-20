import { act, render, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode, StrictMode, Suspense } from 'react';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient, type SyncClientOptions } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync, useSyncClient } from '../../src/react/index.js';

const { db, SyncProvider, useSyncStatus, useUndo } = createReactSync([ TodoSchema ]);

type Opts = Partial<SyncClientOptions<readonly [typeof TodoSchema]>>;

function newClient(opts?: Opts): SyncClient<readonly [typeof TodoSchema]> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		...opts,
	});
}

/** A wrapper that supplies the client; optionally under StrictMode. */
function providerWrapper(client: SyncClient, strict = false) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		const tree = createElement(SyncProvider, { client }, children);
		return strict ? createElement(StrictMode, null, tree) : tree;
	};
}

/** Flushes pending microtasks + a macrotask (covers async watch + GC timers). */
function flush(): Promise<void> {
	return act(async () => {
		await new Promise((r) => setTimeout(r, 0));
	});
}

function hubOf(client: SyncClient): { watchers: Set<unknown> } | undefined {
	return (client as unknown as { hubInstance?: { watchers: Set<unknown> } }).hubInstance;
}

describe('createReactSync hooks', () => {
	it('useQuery returns [] synchronously then fills after a flush', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		const { result } = renderHook(() => db.todo.useQuery({}), {
			wrapper: providerWrapper(client),
		});
		expect(result.current).toEqual([]);
		await flush();
		expect(result.current.map((r) => r.value.id)).toEqual([ 't1' ]);
		await client.close();
	});

	it('useQuery carries every facet axis empty while loading, then filled', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one', completed: true });
		await client.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		const { result } = renderHook(
			() => db.todo.useQuery({ facets: { done: (t) => t.completed } }),
			{ wrapper: providerWrapper(client) },
		);
		expect(result.current.loading).toBe(true);
		expect(result.current.facets).toEqual({ done: [] });
		await flush();
		expect(result.current.facets).toEqual({
			done: [
				{ value: false, count: 1, workspaceId: 'ws1' },
				{ value: true, count: 1, workspaceId: 'ws1' },
			],
		});
		await client.close();
	});

	it('keeps the same rows reference across an unrelated re-render', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		const { result, rerender } = renderHook(() => db.todo.useQuery({}), {
			wrapper: providerWrapper(client),
		});
		await flush();
		const first = result.current;
		rerender();
		expect(result.current).toBe(first);
		await client.close();
	});

	it('keeps the same reference when a mutation does not change the result', async () => {
		const client = newClient();
		const { result } = renderHook(
			() => db.todo.useQuery({ where: (t) => t.completed.eq(true) }),
			{ wrapper: providerWrapper(client) },
		);
		await flush();
		const empty = result.current;
		expect(empty).toEqual([]);
		// A row that does not match the filter must not re-emit (WatcherHub memo).
		await act(async () => {
			await client.mutate.put(TodoSchema, { id: 'x', title: 'n', completed: false });
		});
		await flush();
		expect(result.current).toBe(empty);
		await client.close();
	});

	it('resubscribes only when the query structurally changes', async () => {
		const client = newClient();
		// The hooks read and write through the workspace scope, not the
		// client-level shorthand, so that is where the subscription lands.
		const spy = vi.spyOn(client.ws('ws1'), 'watchQuery');
		let limit = 1;
		const { rerender } = renderHook(
			() => db.todo.useQuery({ where: (t) => t.completed.eq(false), limit }),
			{ wrapper: providerWrapper(client) },
		);
		await flush();
		expect(spy).toHaveBeenCalledTimes(1);
		// Structurally identical query (new lambdas, same shape) → no resubscribe.
		rerender();
		await flush();
		expect(spy).toHaveBeenCalledTimes(1);
		// Structurally different → resubscribe.
		limit = 2;
		rerender();
		await flush();
		expect(spy).toHaveBeenCalledTimes(2);
		await client.close();
	});

	it('suspends on first render, resolves to rows, tears down on unmount', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		function View(): ReactNode {
			const rows = db.todo.useSuspenseQuery({});
			return createElement('div', { 'data-testid': 'list' }, rows.map((r) => r.value.id).join(','));
		}
		const ui = createElement(
			StrictMode,
			null,
			createElement(
				SyncProvider,
				{ client },
				createElement(
					Suspense,
					{ fallback: createElement('div', { 'data-testid': 'fallback' }, 'loading') },
					createElement(View),
				),
			),
		);
		const { getByTestId, queryByTestId, unmount } = render(ui);
		expect(queryByTestId('fallback')).not.toBeNull();
		await waitFor(() => expect(queryByTestId('list')).not.toBeNull());
		expect(getByTestId('list').textContent).toBe('t1');
		expect(hubOf(client)?.watchers.size).toBe(1);
		unmount();
		await flush();
		expect(hubOf(client)?.watchers.size).toBe(0);
		await client.close();
	});

	it('useMutate().put updates useQuery rows (optimistic overlay)', async () => {
		const client = newClient();
		const { result } = renderHook(
			() => ({ rows: db.todo.useQuery({}), mutate: db.todo.useMutate() }),
			{ wrapper: providerWrapper(client) },
		);
		await flush();
		expect(result.current.rows).toEqual([]);
		await act(async () => {
			await result.current.mutate.put({ id: 't1', title: 'one' });
		});
		await flush();
		expect(result.current.rows.map((r) => r.value.id)).toEqual([ 't1' ]);
		await client.close();
	});

	it('useRow does not guard writes unless asked, and binds the version when it is', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		const spy = vi.spyOn(client.mutate, 'patch');
		const { result } = renderHook(() => db.todo.useRow('t1'), {
			wrapper: providerWrapper(client),
		});
		await flush();
		expect(result.current.row?.value.id).toBe('t1');
		const version = result.current.row!.version;

		// Plain LWW by default: row.version is confirmed-only, so a guard cut from
		// it would make a burst of edits conflict with its own earlier writes.
		await act(async () => {
			await result.current.patch({ title: 'two' });
		});
		expect(spy).toHaveBeenLastCalledWith(TodoSchema, 't1', { title: 'two' }, undefined);

		// `cas: true` binds the hook's version, and does not leak to the mutate call.
		await act(async () => {
			await result.current.patch({ title: 'three' }, { cas: true });
		});
		expect(spy).toHaveBeenLastCalledWith(TodoSchema, 't1', { title: 'three' }, {
			expectedVersion: version,
		});

		// An explicit guard wins over `cas`.
		await act(async () => {
			await result.current.patch({ title: 'four' }, { cas: true, expectedVersion: 99n });
		});
		expect(spy).toHaveBeenLastCalledWith(TodoSchema, 't1', { title: 'four' }, {
			expectedVersion: 99n,
		});

		await flush();
		expect(result.current.row?.value.title).toBe('four');
		await client.close();
	});

	it('useSyncStatus reflects status and is stable when unchanged', async () => {
		const client = newClient();
		const { result, rerender } = renderHook(() => useSyncStatus(), {
			wrapper: providerWrapper(client),
		});
		expect(result.current.pendingMutations).toBe(0);
		const ref = result.current;
		rerender();
		expect(result.current).toBe(ref);
		await act(async () => {
			await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		});
		expect(result.current.pendingMutations).toBe(1);
		await client.close();
	});

	it('useUndo tracks canUndo/canRedo via the new emitter', async () => {
		const client = newClient({ undo: {} });
		const { result } = renderHook(() => useUndo(), {
			wrapper: providerWrapper(client),
		});
		expect(result.current.canUndo).toBe(false);
		await act(async () => {
			await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		});
		expect(result.current.canUndo).toBe(true);
		expect(result.current.canRedo).toBe(false);
		await act(async () => {
			await result.current.undo();
		});
		expect(result.current.canUndo).toBe(false);
		expect(result.current.canRedo).toBe(true);
		// A custom mutation is a barrier: both stacks clear.
		await act(async () => {
			await client.mutate.custom('complete_all');
		});
		expect(result.current.canUndo).toBe(false);
		expect(result.current.canRedo).toBe(false);
		await client.close();
	});

	it('balances watcher subscribe/unsubscribe under StrictMode', async () => {
		const client = newClient();
		const { unmount } = renderHook(() => db.todo.useQuery({}), {
			wrapper: providerWrapper(client, true),
		});
		await flush();
		expect(hubOf(client)?.watchers.size).toBe(1);
		unmount();
		await flush();
		expect(hubOf(client)?.watchers.size).toBe(0);
		await client.close();
	});

	it('useSyncClient throws outside a provider', () => {
		expect(() => renderHook(() => useSyncClient())).toThrow(/no SyncProvider/);
	});

	it('createReactSync throws on a colliding entity key', () => {
		expect(() => createReactSync([ TodoSchema, TodoSchema ])).toThrow(/already taken/);
	});
});
