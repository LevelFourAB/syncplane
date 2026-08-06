import { act, render, renderHook, waitFor } from '@testing-library/react';
import { Component, createElement, type ReactNode, Suspense } from 'react';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { type Coverage, type RowSet, withCoverage } from '../../src/coverage.js';
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

function wrapper(client: SyncClient) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		return createElement(SyncProvider, { client }, children);
	};
}

function flush(): Promise<void> {
	return act(async () => {
		await new Promise((r) => setTimeout(r, 0));
	});
}

/** Renders `fallback` once anything below it throws. */
class Boundary extends Component<{ children?: ReactNode; fallback: string }, { failed: boolean }> {
	state = { failed: false };
	static getDerivedStateFromError(): { failed: boolean } {
		return { failed: true };
	}
	override render(): ReactNode {
		return this.state.failed ? this.props.fallback : this.props.children;
	}
}

describe('useQuery load state', () => {
	it('tells "not read yet" apart from "read and empty"', async () => {
		const client = newClient();
		const { result } = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) });

		// Both are [], which is exactly why the flag has to exist.
		expect(result.current).toEqual([]);
		expect(result.current.loading).toBe(true);
		expect(result.current.error).toBeUndefined();

		await flush();
		expect(result.current).toEqual([]);
		expect(result.current.loading).toBe(false);
		await client.close();
	});

	it('is done loading once rows arrive, and stays an ordinary array', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		const { result } = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) });
		await flush();

		expect(result.current.loading).toBe(false);
		expect(result.current.map((r) => r.value.id)).toEqual([ 't1' ]);
		// loading/error are non-enumerable, like coverage: structural comparisons
		// and JSON must not see them.
		expect(Object.keys(result.current)).toEqual([ '0' ]);
		expect(result.current.coverage).toBeDefined();
		await client.close();
	});

	it('reports a watch that could never be established', async () => {
		const client = newClient();
		const scope = client.ws('ws1');
		vi.spyOn(scope, 'watchQuery').mockImplementation(() => {
			throw new Error('entity not registered');
		});
		const { result } = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) });
		await flush();

		expect(result.current.loading).toBe(false);
		expect(result.current.error?.message).toMatch(/not registered/);
		vi.restoreAllMocks();
		await client.close();
	});

	it('reports a watch that failed asynchronously, keeping the rows it had', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		const scope = client.ws('ws1');
		const real = scope.watchQuery.bind(scope);
		let failNow = (): void => {};
		vi.spyOn(scope, 'watchQuery').mockImplementation((schema, q, cb, opts) => {
			failNow = () => opts?.onError?.(new Error('store never came up'));
			return real(schema, q, cb, opts);
		});
		const { result } = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) });
		await flush();
		expect(result.current).toHaveLength(1);

		act(() => failNow());
		expect(result.current.error?.message).toMatch(/never came up/);
		// The rows the user was already reading survive the watch dying.
		expect(result.current.map((r) => r.value.id)).toEqual([ 't1' ]);
		vi.restoreAllMocks();
		await client.close();
	});

	it('throws a failed subscription to the error boundary under suspense', async () => {
		vi.spyOn(console, 'error').mockImplementation(() => {});
		const client = newClient();
		const scope = client.ws('ws1');
		vi.spyOn(scope, 'watchQuery').mockImplementation(() => {
			throw new Error('entity not registered');
		});
		function View(): ReactNode {
			db.todo.useSuspenseQuery({});
			return 'rows';
		}
		const { container } = render(
			createElement(
				Boundary,
				{ fallback: 'query failed' },
				createElement(
					SyncProvider,
					{ client },
					createElement(Suspense, { fallback: 'loading' }, createElement(View)),
				),
			),
		);
		await waitFor(() => expect(container.textContent).toBe('query failed'));
		vi.restoreAllMocks();
		await client.close();
	});
});

// A cold client fills entity by entity, so a result can be empty simply
// because its entity has not started downloading. `hydrating` is what a
// component checks before it renders an empty state; the coverage it mirrors is
// produced by the coordinator (see the client's hydration specs), so these
// specs drive the watch directly and assert only the React plumbing.
describe('useQuery hydration state', () => {
	const hydrating: Coverage = { complete: false, bounds: [ 'hydrating' ] };
	const complete: Coverage = { complete: true, bounds: [] };

	/** Replaces the watch with one the test can feed row sets to by hand. */
	function drivable(client: SyncClient): (coverage: Coverage) => void {
		const scope = client.ws('ws1');
		let emit!: (rows: RowSet<unknown>) => void;
		vi.spyOn(scope, 'watchQuery').mockImplementation(
			((_s: unknown, _q: unknown, cb: (rows: RowSet<unknown>) => void) => {
				emit = cb;
				return { close: () => {}, setWindow: () => {} };
			}) as unknown as typeof scope.watchQuery,
		);
		return (coverage) => act(() => emit(withCoverage([], coverage)));
	}

	it('flags a result whose entity is still arriving, and clears it when it lands', async () => {
		const client = newClient();
		const deliver = drivable(client);
		const { result } = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) });
		await flush();

		// Empty because it has not arrived — not because there is nothing.
		deliver(hydrating);
		expect(result.current.loading).toBe(false);
		expect(result.current.hydrating).toBe(true);
		expect(result.current).toEqual([]);

		deliver(complete);
		expect(result.current.hydrating).toBe(false);

		// Non-enumerable, like loading/error/coverage.
		expect(Object.keys(result.current)).toEqual([]);
		vi.restoreAllMocks();
		await client.close();
	});

	it('is false for an ordinary result', async () => {
		const client = newClient();
		const { result } = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) });
		await flush();
		expect(result.current.hydrating).toBe(false);
		await client.close();
	});
});

describe('useRow load state', () => {
	it('separates "entity not read yet" from "no such row"', async () => {
		const client = newClient();
		const { result } = renderHook(() => db.todo.useRow('nope'), { wrapper: wrapper(client) });
		expect(result.current.row).toBeUndefined();
		expect(result.current.loading).toBe(true);

		await flush();
		expect(result.current.row).toBeUndefined();
		expect(result.current.loading).toBe(false);
		expect(result.current.error).toBeUndefined();
		await client.close();
	});

	it('says a missing row may still be coming while the entity hydrates', async () => {
		// The key lookup cannot tell "no such row" from "not downloaded yet", so
		// the entity's coverage has to.
		const client = newClient();
		const scope = client.ws('ws1');
		let emit!: (rows: RowSet<unknown>) => void;
		vi.spyOn(scope, 'watch').mockImplementation(
			((_s: unknown, cb: (rows: RowSet<unknown>) => void) => {
				emit = cb;
				return { close: () => {}, setWindow: () => {} };
			}) as unknown as typeof scope.watch,
		);
		const { result } = renderHook(() => db.todo.useRow('t1'), { wrapper: wrapper(client) });
		await flush();

		act(() => emit(withCoverage([], { complete: false, bounds: [ 'hydrating' ] })));
		expect(result.current.loading).toBe(false);
		expect(result.current.hydrating).toBe(true);
		expect(result.current.row).toBeUndefined();

		act(() => emit(withCoverage([], { complete: true, bounds: [] })));
		expect(result.current.hydrating).toBe(false);
		expect(result.current.row).toBeUndefined();

		vi.restoreAllMocks();
		await client.close();
	});

	it('suspends until the entity has been read', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		function View(): ReactNode {
			const { row, loading } = db.todo.useSuspenseRow('t1');
			expect(loading).toBe(false); // never renders in a loading state
			return createElement('div', { 'data-testid': 'row' }, row?.value.title ?? 'missing');
		}
		const { queryByTestId, getByTestId } = render(
			createElement(
				SyncProvider,
				{ client },
				createElement(Suspense, { fallback: 'loading' }, createElement(View)),
			),
		);
		expect(queryByTestId('row')).toBeNull();
		await waitFor(() => expect(queryByTestId('row')).not.toBeNull());
		expect(getByTestId('row').textContent).toBe('one');
		await client.close();
	});
});
