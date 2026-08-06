// `useQuery` with a window is the virtualization path: the subscription holds
// the whole match set, the hook renders a page of it, and moving the page must
// neither resubscribe nor re-read. The specs here pin all three, plus the
// `total` a scroller sizes itself from.

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

/** Live watchers on the hub — how a resubscription makes itself visible. */
function watcherCount(client: SyncClient): number {
	return (client as unknown as { hubInstance?: { watchers: Set<unknown> } })
		.hubInstance?.watchers.size ?? 0;
}

async function seeded(): Promise<SyncClient<readonly [typeof TodoSchema]>> {
	const client = newClient();
	for (let i = 0; i < 12; i++) {
		await client.mutate.put(TodoSchema, { id: `t${String(i).padStart(2, '0')}`, title: `#${i}` });
	}
	return client;
}

describe('useQuery windowing', () => {
	it('renders a page and reports what it was cut from', async () => {
		const client = await seeded();
		const { result } = renderHook(
			() => db.todo.useQuery({}, { window: { offset: 0, limit: 4 } }),
			{ wrapper: wrapper(client) },
		);
		await flush();

		expect(result.current.map((r) => r.value.id)).toEqual([ 't00', 't01', 't02', 't03' ]);
		expect(result.current.total).toBe(12);
		expect(result.current.loading).toBe(false);
		await client.close();
	});

	it('keeps one subscription as the window moves', async () => {
		const client = await seeded();
		const { result, rerender } = renderHook(
			({ offset }: { offset: number }) => db.todo.useQuery({}, { window: { offset, limit: 3 } }),
			{ wrapper: wrapper(client), initialProps: { offset: 0 } },
		);
		await flush();
		expect(result.current.map((r) => r.value.id)).toEqual([ 't00', 't01', 't02' ]);
		const watchers = watcherCount(client);
		expect(watchers).toBe(1);

		rerender({ offset: 6 });
		await flush();

		expect(result.current.map((r) => r.value.id)).toEqual([ 't06', 't07', 't08' ]);
		expect(result.current.total).toBe(12);
		// The whole point: the page moved over a result that was never re-read.
		expect(watcherCount(client)).toBe(watchers);
		await client.close();
	});

	it('follows writes under the page it is showing', async () => {
		const client = await seeded();
		const { result } = renderHook(
			() => db.todo.useQuery({}, { window: { offset: 0, limit: 3 } }),
			{ wrapper: wrapper(client) },
		);
		await flush();
		expect(result.current.map((r) => r.value.id)).toEqual([ 't00', 't01', 't02' ]);

		// Sorts ahead of everything, so it enters the page and pushes one out.
		await act(async () => {
			await client.mutate.put(TodoSchema, { id: 't__first', title: 'first' });
		});
		await flush();
		expect(result.current.map((r) => r.value.id)).toEqual([ 't00', 't01', 't02' ]);
		expect(result.current.total).toBe(13);

		// A row past the page moves `total` without moving the rows.
		const before = result.current;
		await act(async () => {
			await client.mutate.put(TodoSchema, { id: 't99', title: 'last' });
		});
		await flush();
		expect(result.current.map((r) => r.value.id)).toEqual([ 't00', 't01', 't02' ]);
		expect(result.current.total).toBe(14);
		expect(result.current).not.toBe(before);
		await client.close();
	});

	it('does not re-render for an edit the page cannot see', async () => {
		const client = await seeded();
		let renders = 0;
		const { result } = renderHook(() => {
			renders++;
			return db.todo.useQuery({}, { window: { offset: 0, limit: 3 } });
		}, { wrapper: wrapper(client) });
		await flush();
		expect(result.current.map((r) => r.value.id)).toEqual([ 't00', 't01', 't02' ]);
		const settled = renders;

		// An in-place edit past the page: neither the rows nor the total move.
		await act(async () => {
			await client.mutate.put(TodoSchema, { id: 't09', title: 'edited' });
		});
		await flush();
		expect(renders).toBe(settled);
		await client.close();
	});
});
