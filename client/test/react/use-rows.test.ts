// useRows reads a key SET through the subscription useRow already shares, so a
// component holding ten keys and ten components holding one each cost the same
// watch and the same reads. The specs here pin what a caller can see of that:
// alignment with the keys asked for, a load state that waits for all of them,
// a result array stable enough for a memoized child, and a key list that can
// grow without tearing the subscription down.

import { act, render, renderHook, type RenderHookResult } from '@testing-library/react';
import { Component, createElement, memo, type ReactNode, Suspense } from 'react';
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

/** The hub's live watchers of both kinds — one entry per underlying subscription. */
function watcherCount(client: SyncClient): number {
	const hub = (client as unknown as {
		hubInstance?: { watchers: Set<unknown>; keyWatchers: Set<unknown> };
	}).hubInstance;
	return (hub?.watchers.size ?? 0) + (hub?.keyWatchers.size ?? 0);
}

/** The keys the shared keyed watch is currently reading. */
function watchedKeys(client: SyncClient): string[] {
	const hub = (client as unknown as {
		hubInstance?: { keyWatchers: Set<{ refs: Map<string, { key: string }> }> };
	}).hubInstance;
	const out: string[] = [];
	for (const w of hub?.keyWatchers ?? []) {
		for (const ref of w.refs.values()) out.push(ref.key);
	}
	return out.sort();
}

async function seeded(keys: string[]): Promise<SyncClient<readonly [typeof TodoSchema]>> {
	const client = newClient();
	for (const key of keys) {
		await client.mutate.put(TodoSchema, { id: key, title: `title ${key}` });
	}
	return client;
}

describe('useRows', () => {
	it('returns one slot per key, in the order asked for', async () => {
		const client = await seeded([ 'a', 'b', 'c' ]);
		const { result, unmount } = renderHook(() => db.todo.useRows([ 'c', 'ghost', 'a' ]), {
			wrapper: wrapper(client),
		});
		await flush();

		expect(result.current.map((r) => r?.value.title)).toEqual([
			'title c',
			undefined,
			'title a',
		]);
		// The state rides on the array, so it survives spreading and stays out of
		// structural comparisons.
		expect(result.current.loading).toBe(false);
		expect(Object.keys(result.current)).toEqual([ '0', '1', '2' ]);

		unmount();
		await client.close();
	});

	it('stays loading until every requested key has been answered', async () => {
		const client = await seeded([ 'a' ]);
		const { result, unmount } = renderHook(() => db.todo.useRows([ 'a', 'ghost' ]), {
			wrapper: wrapper(client),
		});
		// Aligned from the first render — `rows[i]` names `keys[i]` throughout —
		// so a slot full of undefined must not read as a settled answer.
		expect(result.current.loading).toBe(true);
		expect([ ...result.current ]).toEqual([ undefined, undefined ]);

		await flush();
		// "No such row" is an answer, so a missing key does not keep it loading.
		expect(result.current.loading).toBe(false);
		expect(result.current[1]).toBeUndefined();

		unmount();
		await client.close();
	});

	it('shares one subscription with useRow on the same entity', async () => {
		const client = await seeded([ 'a', 'b', 'c' ]);
		const { unmount } = renderHook(
			() => ({
				many: db.todo.useRows([ 'a', 'b' ]),
				one: db.todo.useRow('c'),
			}),
			{ wrapper: wrapper(client) },
		);
		await flush();

		expect(watcherCount(client)).toBe(1);
		expect(watchedKeys(client)).toEqual([ 'a', 'b', 'c' ]);

		unmount();
		await flush();
		expect(watcherCount(client)).toBe(0);
		await client.close();
	});

	it('re-renders only when one of its own keys moves', async () => {
		const client = await seeded([ 'a', 'b', 'other' ]);
		let renders = 0;
		const View = memo(function View(): ReactNode {
			const rows = db.todo.useRows([ 'a', 'b' ]);
			renders++;
			return createElement('span', null, rows.map((r) => r?.value.title ?? '').join(','));
		});
		render(createElement(SyncProvider, { client }, createElement(View)));
		await flush();
		const settled = renders;

		await act(async () => {
			await client.mutate.patch(TodoSchema, 'other', { title: 'moved' });
		});
		await flush();
		// A commit elsewhere in the entity reaches no key this call site holds.
		expect(renders).toBe(settled);

		await act(async () => {
			await client.mutate.patch(TodoSchema, 'b', { title: 'b2' });
		});
		await flush();
		expect(renders).toBeGreaterThan(settled);

		await client.close();
	});

	it('picks up keys added to the list without resubscribing', async () => {
		const client = await seeded([ 'a', 'b' ]);
		let keys = [ 'a' ];
		const { result, rerender, unmount }: RenderHookResult<
			ReturnType<typeof db.todo.useRows>,
			unknown
		> = renderHook(() => db.todo.useRows(keys), { wrapper: wrapper(client) });
		await flush();
		expect(result.current.map((r) => r?.value.title)).toEqual([ 'title a' ]);

		keys = [ 'a', 'b' ];
		rerender();
		await flush();

		expect(result.current.map((r) => r?.value.title)).toEqual([ 'title a', 'title b' ]);
		// One watch throughout: the key set moved, the subscription did not.
		expect(watcherCount(client)).toBe(1);
		expect(watchedKeys(client)).toEqual([ 'a', 'b' ]);

		unmount();
		await client.close();
	});

	it('hands back the same array while nothing it holds moves', async () => {
		const client = await seeded([ 'a', 'other' ]);
		const { result, rerender, unmount } = renderHook(() => db.todo.useRows([ 'a' ]), {
			wrapper: wrapper(client),
		});
		await flush();
		const first = result.current;

		rerender();
		expect(result.current).toBe(first);

		await act(async () => {
			await client.mutate.patch(TodoSchema, 'other', { title: 'moved' });
		});
		await flush();
		expect(result.current).toBe(first);

		await act(async () => {
			await client.mutate.patch(TodoSchema, 'a', { title: 'a2' });
		});
		await flush();
		expect(result.current).not.toBe(first);
		expect(result.current[0]?.value.title).toBe('a2');

		unmount();
		await client.close();
	});

	it('suspends until every key has been answered', async () => {
		const client = await seeded([ 'a', 'b' ]);
		function View(): ReactNode {
			const rows = db.todo.useSuspenseRows([ 'a', 'b' ]);
			expect(rows.loading).toBe(false); // never renders in a loading state
			return createElement(
				'div',
				{ 'data-testid': 'rows' },
				rows.map((r) => r?.value.title ?? 'missing').join(','),
			);
		}
		const { queryByTestId, findByTestId } = render(
			createElement(
				SyncProvider,
				{ client },
				createElement(
					Suspense,
					{ fallback: createElement('div', { 'data-testid': 'fallback' }) },
					createElement(View),
				),
			),
		);
		expect(queryByTestId('rows')).toBeNull();

		const shown = await findByTestId('rows');
		expect(shown.textContent).toBe('title a,title b');
		await client.close();
	});

	it('throws a failed subscription to the error boundary under suspense', async () => {
		const client = await seeded([]);
		await client.close();

		class Boundary extends Component<{ children: ReactNode }, { failed: boolean }> {
			override state = { failed: false };
			static getDerivedStateFromError(): { failed: boolean } {
				return { failed: true };
			}
			override render(): ReactNode {
				return this.state.failed
					? createElement('div', { 'data-testid': 'boom' })
					: this.props.children;
			}
		}
		function View(): ReactNode {
			db.todo.useSuspenseRows([ 'a' ]);
			return createElement('div', { 'data-testid': 'rows' });
		}
		const { queryByTestId } = render(
			createElement(
				SyncProvider,
				{ client },
				createElement(
					Boundary,
					null,
					createElement(Suspense, { fallback: null }, createElement(View)),
				),
			),
		);
		await flush();
		expect(queryByTestId('rows')).toBeNull();
	});
});
