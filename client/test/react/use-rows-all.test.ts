// useRowsAll reads a set of rows named by workspace AND key, which is the case
// no `useQueryAll` predicate covers: a list assembled outside the entity — a
// personal task list, a pinned set — whose members happen to live in different
// workspaces. The specs here pin what a caller can see of that: alignment with
// the refs asked for, two workspaces' copies of one key kept apart, a
// subscription shared across call sites and separate from the per-workspace one,
// and a commit reaching only the rows it actually moved.

import { act, render, renderHook, type RenderHookResult } from '@testing-library/react';
import { createElement, memo, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';
import type { RowRef } from '../../src/watch.js';

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

/** Every ref the hub's keyed watches are reading, as `<workspace>/<key>`. */
function watchedRefs(client: SyncClient): string[] {
	const hub = (client as unknown as {
		hubInstance?: { keyWatchers: Set<{ refs: Map<string, RowRef> }> };
	}).hubInstance;
	const out: string[] = [];
	for (const w of hub?.keyWatchers ?? []) {
		for (const ref of w.refs.values()) out.push(`${ref.workspaceId}/${ref.key}`);
	}
	return out.sort();
}

async function seeded(rows: [string, string][]): Promise<SyncClient<readonly [typeof TodoSchema]>> {
	const client = newClient();
	for (const [ ws, key ] of rows) {
		await client.ws(ws).mutate.put(TodoSchema, { id: key, title: `${ws}:${key}` });
	}
	return client;
}

describe('useRowsAll', () => {
	it('returns one slot per ref, in the order asked for, across workspaces', async () => {
		const client = await seeded([ [ 'ws1', 'a' ], [ 'ws2', 'b' ], [ 'ws3', 'c' ] ]);
		const { result, unmount } = renderHook(
			() =>
				db.todo.useRowsAll([
					{ workspaceId: 'ws3', key: 'c' },
					{ workspaceId: 'ws1', key: 'ghost' },
					{ workspaceId: 'ws2', key: 'b' },
				]),
			{ wrapper: wrapper(client) },
		);
		await flush();

		expect(result.current.map((r) => r?.value.title)).toEqual([
			'ws3:c',
			undefined,
			'ws2:b',
		]);
		// Each row names the workspace it came from, which is what routes a write
		// back through client.ws(...).
		expect(result.current.map((r) => r?.workspaceId)).toEqual([ 'ws3', undefined, 'ws2' ]);
		expect(result.current.loading).toBe(false);

		unmount();
		await client.close();
	});

	it('keeps the same key in two workspaces apart', async () => {
		const client = await seeded([ [ 'ws1', 'a' ], [ 'ws2', 'a' ] ]);
		const { result, unmount } = renderHook(
			() =>
				db.todo.useRowsAll([
					{ workspaceId: 'ws1', key: 'a' },
					{ workspaceId: 'ws2', key: 'a' },
				]),
			{ wrapper: wrapper(client) },
		);
		await flush();

		expect(result.current.map((r) => r?.value.title)).toEqual([ 'ws1:a', 'ws2:a' ]);

		// And they move independently: one workspace's copy changing leaves the
		// other's row object untouched.
		const second = result.current[1];
		await act(async () => {
			await client.ws('ws1').mutate.patch(TodoSchema, 'a', { title: 'edited' });
		});
		await flush();
		expect(result.current[0]?.value.title).toBe('edited');
		expect(result.current[1]).toBe(second);

		unmount();
		await client.close();
	});

	it('ignores the enclosing workspace provider', async () => {
		const client = await seeded([ [ 'ws1', 'a' ], [ 'ws2', 'b' ] ]);
		// A provider naming ws2 scopes useRow; these refs name their own workspaces
		// and must be unaffected by it.
		const { result, unmount } = renderHook(
			() =>
				db.todo.useRowsAll([
					{ workspaceId: 'ws1', key: 'a' },
					{ workspaceId: 'ws2', key: 'b' },
				]),
			{ wrapper: wrapper(client, 'ws2') },
		);
		await flush();

		expect(result.current.map((r) => r?.value.title)).toEqual([ 'ws1:a', 'ws2:b' ]);
		unmount();
		await client.close();
	});

	it('stays loading until every requested ref has been answered', async () => {
		const client = await seeded([ [ 'ws1', 'a' ] ]);
		const { result, unmount } = renderHook(
			() =>
				db.todo.useRowsAll([
					{ workspaceId: 'ws1', key: 'a' },
					{ workspaceId: 'ws2', key: 'ghost' },
				]),
			{ wrapper: wrapper(client) },
		);
		expect(result.current.loading).toBe(true);
		expect([ ...result.current ]).toEqual([ undefined, undefined ]);

		await flush();
		// "No such row" is an answer, so a missing ref does not keep it loading.
		expect(result.current.loading).toBe(false);
		expect(result.current[1]).toBeUndefined();

		unmount();
		await client.close();
	});

	it('shares one subscription across call sites, separate from the per-workspace one', async () => {
		const client = await seeded([ [ 'ws1', 'a' ], [ 'ws2', 'b' ] ]);
		const { unmount } = renderHook(
			() => ({
				first: db.todo.useRowsAll([ { workspaceId: 'ws1', key: 'a' } ]),
				second: db.todo.useRowsAll([ { workspaceId: 'ws2', key: 'b' } ]),
			}),
			{ wrapper: wrapper(client) },
		);
		await flush();

		// Both call sites, however many workspaces they span, ride one watch.
		expect(watcherCount(client)).toBe(1);
		expect(watchedRefs(client)).toEqual([ 'ws1/a', 'ws2/b' ]);

		unmount();
		await flush();
		expect(watcherCount(client)).toBe(0);
		await client.close();
	});

	it('does not share the watch useRow uses for the same row', async () => {
		const client = await seeded([ [ 'ws1', 'a' ] ]);
		const { result, unmount } = renderHook(
			() => ({
				all: db.todo.useRowsAll([ { workspaceId: 'ws1', key: 'a' } ]),
				one: db.todo.useRow('a'),
			}),
			{ wrapper: wrapper(client) },
		);
		await flush();

		expect(result.current.all[0]?.value.title).toBe('ws1:a');
		expect(result.current.one.row?.value.title).toBe('ws1:a');
		// Two scan scopes, so two watches and two decodes of the same row.
		expect(watcherCount(client)).toBe(2);

		unmount();
		await client.close();
	});

	it('re-renders only when one of its own rows moves', async () => {
		const client = await seeded([ [ 'ws1', 'a' ], [ 'ws2', 'b' ], [ 'ws3', 'other' ] ]);
		let renders = 0;
		const View = memo(function View(): ReactNode {
			const rows = db.todo.useRowsAll([
				{ workspaceId: 'ws1', key: 'a' },
				{ workspaceId: 'ws2', key: 'b' },
			]);
			renders++;
			return createElement('span', null, rows.map((r) => r?.value.title ?? '').join(','));
		});
		render(createElement(SyncProvider, { client }, createElement(View)));
		await flush();
		const settled = renders;

		// A commit in a workspace this call site holds nothing in.
		await act(async () => {
			await client.ws('ws3').mutate.patch(TodoSchema, 'other', { title: 'moved' });
		});
		await flush();
		expect(renders).toBe(settled);

		// And one in a workspace it does hold, but at another key.
		await act(async () => {
			await client.ws('ws1').mutate.put(TodoSchema, { id: 'unwatched', title: 'x' });
		});
		await flush();
		expect(renders).toBe(settled);

		await act(async () => {
			await client.ws('ws2').mutate.patch(TodoSchema, 'b', { title: 'b2' });
		});
		await flush();
		expect(renders).toBeGreaterThan(settled);

		await client.close();
	});

	it('picks up refs added to the list without resubscribing', async () => {
		const client = await seeded([ [ 'ws1', 'a' ], [ 'ws2', 'b' ] ]);
		let refs: RowRef[] = [ { workspaceId: 'ws1', key: 'a' } ];
		const { result, rerender, unmount }: RenderHookResult<
			ReturnType<typeof db.todo.useRowsAll>,
			unknown
		> = renderHook(() => db.todo.useRowsAll(refs), { wrapper: wrapper(client) });
		await flush();
		expect(result.current.map((r) => r?.value.title)).toEqual([ 'ws1:a' ]);

		refs = [ { workspaceId: 'ws1', key: 'a' }, { workspaceId: 'ws2', key: 'b' } ];
		rerender();
		await flush();

		expect(result.current.map((r) => r?.value.title)).toEqual([ 'ws1:a', 'ws2:b' ]);
		// One watch throughout: the ref set moved, the subscription did not.
		expect(watcherCount(client)).toBe(1);
		expect(watchedRefs(client)).toEqual([ 'ws1/a', 'ws2/b' ]);

		unmount();
		await client.close();
	});

	it('hands back the same array while nothing it holds moves', async () => {
		const client = await seeded([ [ 'ws1', 'a' ], [ 'ws2', 'other' ] ]);
		const { result, rerender, unmount } = renderHook(
			// A fresh ref object every render: only what it names may decide whether
			// the result is rebuilt.
			() => db.todo.useRowsAll([ { workspaceId: 'ws1', key: 'a' } ]),
			{ wrapper: wrapper(client) },
		);
		await flush();
		const first = result.current;

		rerender();
		expect(result.current).toBe(first);

		await act(async () => {
			await client.ws('ws2').mutate.patch(TodoSchema, 'other', { title: 'moved' });
		});
		await flush();
		expect(result.current).toBe(first);

		await act(async () => {
			await client.ws('ws1').mutate.patch(TodoSchema, 'a', { title: 'a2' });
		});
		await flush();
		expect(result.current).not.toBe(first);
		expect(result.current[0]?.value.title).toBe('a2');

		unmount();
		await client.close();
	});
});
