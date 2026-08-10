import { act, render, renderHook } from '@testing-library/react';
import { createElement, memo, type ReactNode } from 'react';
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

/** The keys one shared keyed watch is currently reading. */
function watchedKeys(client: SyncClient): string[] {
	const hub = (client as unknown as {
		hubInstance?: { keyWatchers: Set<{ keys: Set<string> }> };
	}).hubInstance;
	const out: string[] = [];
	for (const w of hub?.keyWatchers ?? []) out.push(...w.keys);
	return out.sort();
}

/** Rows the hub holds decoded across every scan scope. */
function decodedRowCount(client: SyncClient): number {
	const hub = (client as unknown as {
		hubInstance?: { scopes: Map<string, { rows: Map<string, unknown> }> };
	}).hubInstance;
	let total = 0;
	for (const scope of hub?.scopes.values() ?? []) total += scope.rows.size;
	return total;
}

describe('useRow subscription sharing', () => {
	it('serves many rows of one entity from a single watch', async () => {
		const client = newClient();
		for (let i = 0; i < 5; i++) {
			await client.mutate.put(TodoSchema, { id: `t${i}`, title: `row ${i}` });
		}
		const { result, unmount } = renderHook(
			() => [ 0, 1, 2, 3, 4 ].map((i) => db.todo.useRow(`t${i}`)),
			{ wrapper: wrapper(client) },
		);
		await flush();

		expect(result.current.map((r) => r.row?.value.title)).toEqual([
			'row 0',
			'row 1',
			'row 2',
			'row 3',
			'row 4',
		]);
		// Five hooks, one subscription — the whole point of the shared store.
		expect(watcherCount(client)).toBe(1);

		unmount();
		await flush();
		expect(watcherCount(client)).toBe(0);
		await client.close();
	});

	it('re-renders only the hook whose own row changed', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 'a', title: 'a1' });
		await client.mutate.put(TodoSchema, { id: 'b', title: 'b1' });

		const renders: Record<string, number> = { a: 0, b: 0 };
		const RowView = memo(function RowView({ id }: { id: string }): ReactNode {
			const { row } = db.todo.useRow(id);
			renders[id] = (renders[id] ?? 0) + 1;
			return createElement('span', null, row?.value.title ?? '');
		});
		render(
			createElement(
				SyncProvider,
				{ client },
				createElement(RowView, { id: 'a' }),
				createElement(RowView, { id: 'b' }),
			),
		);
		await flush();
		const before = { ...renders };

		await act(async () => {
			await client.mutate.patch(TodoSchema, 'a', { title: 'a2' });
		});
		await flush();

		expect(renders.a).toBeGreaterThan(before.a!);
		// b's row object did not move, so its hook must not have been notified.
		expect(renders.b).toBe(before.b);
		await client.close();
	});

	it('shares one watch between call sites reading the same key', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		const { result, unmount } = renderHook(
			() => ({
				first: db.todo.useRow('t1'),
				second: db.todo.useRow('t1'),
			}),
			{ wrapper: wrapper(client) },
		);
		await flush();
		expect(watcherCount(client)).toBe(1);
		// One cell serves both, so they are the same row object, not two decodes.
		expect(result.current.first.row).toBe(result.current.second.row);
		unmount();
		await flush();
		await client.close();
	});

	it('costs its own row and nothing for the entity around it', async () => {
		const client = newClient();
		for (let i = 0; i < 50; i++) {
			await client.mutate.put(TodoSchema, { id: `t${i}`, title: `row ${i}` });
		}
		const { result, unmount } = renderHook(() => db.todo.useRow('t7'), {
			wrapper: wrapper(client),
		});
		await flush();

		expect(result.current.row?.value.title).toBe('row 7');
		expect(watchedKeys(client)).toEqual([ 't7' ]);
		// The hub holds the watched row and nothing around it: the entity was
		// never derived to find it, so neither the mount nor the memory it keeps
		// scales with how much is in there.
		expect(decodedRowCount(client)).toBe(1);

		unmount();
		await client.close();
	});

	it('drops the shared watch only after the last row hook leaves', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		const first = renderHook(() => db.todo.useRow('t1'), { wrapper: wrapper(client) });
		const second = renderHook(() => db.todo.useRow('t2'), { wrapper: wrapper(client) });
		await flush();
		expect(watcherCount(client)).toBe(1);
		expect(watchedKeys(client)).toEqual([ 't1', 't2' ]);

		first.unmount();
		await flush();
		// Still one subscriber left, so the watch stays up — reading only the key
		// that subscriber holds.
		expect(watcherCount(client)).toBe(1);
		expect(watchedKeys(client)).toEqual([ 't2' ]);
		expect(second.result.current.loading).toBe(false);

		second.unmount();
		await flush();
		expect(watcherCount(client)).toBe(0);
		await client.close();
	});
});
