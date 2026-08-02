// <UndoScopeProvider> — the React face of a nested undo scope. What matters
// here is the wiring the vanilla suite cannot see: which stack a hook below the
// provider writes to, and that the scope's lifetime really is the subtree's
// (StrictMode's mount → unmount → mount included).

import { act, render, renderHook } from '@testing-library/react';
import { createElement, type ReactNode, StrictMode } from 'react';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';

const { db, SyncProvider, UndoScopeProvider, useUndo, useUndoScope } = createReactSync([
	TodoSchema,
]);

function newClient(): SyncClient<readonly [typeof TodoSchema]> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		undo: {},
	});
}

/** Supplies the client, optionally under StrictMode. */
function providerWrapper(client: SyncClient, strict = false) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		const tree = createElement(SyncProvider, { client }, children);
		return strict ? createElement(StrictMode, null, tree) : tree;
	};
}

/** The client's undo scopes, reached through its (private) root scope. */
function openScopeCount(client: SyncClient): number {
	const scopes = (client as unknown as {
		scopes: Map<string, { undoManagers(): unknown[] }>;
	}).scopes;
	// One manager is the workspace's own; the rest are open scopes.
	return scopes.get('ws1')!.undoManagers().length - 1;
}

describe('UndoScopeProvider', () => {
	it("routes writes below it to the scope's stack, not the workspace's", async () => {
		const client = newClient();
		const { result } = renderHook(
			() => ({
				scoped: useUndo(),
				workspace: useUndo({ undo: 'workspace' }),
				mutate: db.todo.useMutate(),
			}),
			{
				wrapper: ({ children }: { children: ReactNode }) =>
					createElement(
						providerWrapper(client),
						null,
						createElement(UndoScopeProvider, null, children),
					),
			},
		);

		await act(async () => {
			await result.current.mutate.put({ id: 't1', title: 'in the dialog' });
		});
		// The dialog's stack has the step; the view behind it has nothing.
		expect(result.current.scoped.canUndo).toBe(true);
		expect(result.current.workspace.canUndo).toBe(false);

		await act(async () => {
			await result.current.scoped.undo();
		});
		expect(await client.get(TodoSchema, 't1')).toBeUndefined();
		expect(result.current.workspace.canUndo).toBe(false);
		await client.close();
	});

	it("sends a { undo: 'workspace' } write to the workspace stack from inside a scope", async () => {
		const client = newClient();
		const { result } = renderHook(
			() => ({
				scoped: useUndo(),
				workspace: useUndo({ undo: 'workspace' }),
				global: db.todo.useMutate({ undo: 'workspace' }),
			}),
			{
				wrapper: ({ children }: { children: ReactNode }) =>
					createElement(
						providerWrapper(client),
						null,
						createElement(UndoScopeProvider, null, children),
					),
			},
		);

		await act(async () => {
			await result.current.global.put({ id: 't1', title: 'belongs to the view behind' });
		});
		expect(result.current.workspace.canUndo).toBe(true);
		expect(result.current.scoped.canUndo).toBe(false);
		await client.close();
	});

	it('closes the scope when the subtree unmounts, dropping its stack', async () => {
		const client = newClient();
		let scoped: ReturnType<typeof useUndo> | undefined;
		function Dialog(): null {
			scoped = useUndo();
			return null;
		}
		const view = render(
			createElement(UndoScopeProvider, null, createElement(Dialog)),
			{ wrapper: providerWrapper(client) },
		);
		expect(openScopeCount(client)).toBe(1);

		await act(async () => {
			await client.ws('ws1').mutate.put(TodoSchema, { id: 't1', title: 'board' });
		});
		view.unmount();

		// The dialog's scope is gone; the workspace's own stack survives it.
		expect(openScopeCount(client)).toBe(0);
		expect(scoped).toBeDefined();
		expect(client.canUndo()).toBe(true);
		await client.close();
	});

	it('keeps exactly one scope open under StrictMode', async () => {
		const client = newClient();
		let mutate: ReturnType<typeof db.todo.useMutate> | undefined;
		let scoped: ReturnType<typeof useUndo> | undefined;
		function Dialog(): null {
			mutate = db.todo.useMutate();
			scoped = useUndo();
			return null;
		}
		// StrictMode double-invokes the render AND remounts the effects, which is
		// where a scope can be orphaned (never closed) or left closed (recording
		// nothing). Neither may happen: one live scope, and it still records.
		const view = render(
			createElement(UndoScopeProvider, null, createElement(Dialog)),
			{ wrapper: providerWrapper(client, true) },
		);
		expect(openScopeCount(client)).toBe(1);

		await act(async () => {
			await mutate!.put({ id: 't1', title: 'after the remount' });
		});
		expect(scoped!.canUndo).toBe(true);
		expect(client.canUndo()).toBe(false);

		view.unmount();
		expect(openScopeCount(client)).toBe(0);
		await client.close();
	});

	it("useUndoScope hands out a scope with the component's lifetime", async () => {
		const client = newClient();
		const { result, unmount } = renderHook(() => useUndoScope(), {
			wrapper: providerWrapper(client),
		});
		expect(openScopeCount(client)).toBe(1);

		await act(async () => {
			await result.current.mutate.put(TodoSchema, { id: 't1', title: 'x' });
		});
		expect(result.current.canUndo()).toBe(true);
		expect(client.canUndo()).toBe(false);

		unmount();
		expect(result.current.closed).toBe(true);
		expect(openScopeCount(client)).toBe(0);
		await client.close();
	});
});
