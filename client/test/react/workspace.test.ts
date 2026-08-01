import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';

const { db, SyncProvider, WorkspaceProvider, useWorkspaceStatus } = createReactSync([ TodoSchema ]);

type Client = SyncClient<readonly [typeof TodoSchema]>;

/** A two-workspace client; `defaultWorkspace` is set only when asked for. */
function newClient(defaultWorkspace?: string): Client {
	return new SyncClient({
		workspaces: [ 'ws1', 'ws2' ],
		accountId: 'acct',
		defaultWorkspace,
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

/** Provides the client, optionally under a workspace override. */
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

describe('workspace-scoped hooks', () => {
	it('reads the workspace named by the provider, not the default', async () => {
		const client = newClient('ws1');
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'in one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'in two' });

		const one = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) });
		const two = renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client, 'ws2') });
		await flush();

		expect(one.result.current.map((r) => r.value.id)).toEqual([ 'a' ]);
		expect(two.result.current.map((r) => r.value.id)).toEqual([ 'b' ]);
		await client.close();
	});

	it('takes the per-call workspaceId over the provider and the default', async () => {
		const client = newClient('ws1');
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'in two' });
		const { result } = renderHook(
			() => db.todo.useQuery({}, { workspaceId: 'ws2' }),
			// The provider says ws1; the option must win.
			{ wrapper: wrapper(client, 'ws1') },
		);
		await flush();
		expect(result.current.map((r) => r.value.id)).toEqual([ 'b' ]);
		await client.close();
	});

	it('writes through the resolved workspace', async () => {
		const client = newClient('ws1');
		const { result } = renderHook(() => db.todo.useMutate(), {
			wrapper: wrapper(client, 'ws2'),
		});
		await act(async () => {
			await result.current.put({ id: 'w', title: 'written in two' });
		});
		expect(await client.ws('ws2').get(TodoSchema, 'w')).toBeDefined();
		expect(await client.ws('ws1').get(TodoSchema, 'w')).toBeUndefined();
		await client.close();
	});

	it('does not let two workspaces share one suspense cache entry', async () => {
		const client = newClient('ws1');
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'a', title: 'in one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'b', title: 'in two' });

		// Same client, same entity, same query — only the workspace differs, so
		// an identity that ignored it would hand ws2 the rows cached for ws1.
		const one = renderHook(() => db.todo.useSuspenseQuery({}), {
			wrapper: wrapper(client),
		});
		const two = renderHook(() => db.todo.useSuspenseQuery({}), {
			wrapper: wrapper(client, 'ws2'),
		});
		await waitFor(() => expect(one.result.current).toHaveLength(1));
		await waitFor(() => expect(two.result.current).toHaveLength(1));
		expect(one.result.current[0]?.value.id).toBe('a');
		expect(two.result.current[0]?.value.id).toBe('b');
		await client.close();
	});

	it('scopes useRow (and its CAS) to the resolved workspace', async () => {
		const client = newClient('ws1');
		await client.ws('ws1').mutate.put(TodoSchema, { id: 'r', title: 'one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 'r', title: 'two' });

		const { result } = renderHook(() => db.todo.useRow('r'), {
			wrapper: wrapper(client, 'ws2'),
		});
		await waitFor(() => expect(result.current.row).toBeDefined());
		expect(result.current.row?.value.title).toBe('two');

		await act(async () => {
			await result.current.update((t) => {
				t.title = 'two, edited';
			});
		});
		expect((await client.ws('ws2').get(TodoSchema, 'r'))?.title).toBe('two, edited');
		expect((await client.ws('ws1').get(TodoSchema, 'r'))?.title).toBe('one');
		await client.close();
	});

	it('reports the resolved workspace on useWorkspaceStatus, stably', async () => {
		const client = newClient('ws1');
		const { result, rerender } = renderHook(() => useWorkspaceStatus(), {
			wrapper: wrapper(client, 'ws2'),
		});
		expect(result.current.workspaceId).toBe('ws2');
		const first = result.current;
		rerender();
		expect(result.current).toBe(first);

		// A write to the OTHER workspace must not disturb this one's snapshot.
		await act(async () => {
			await client.ws('ws1').mutate.put(TodoSchema, { id: 'x', title: 'elsewhere' });
		});
		expect(result.current).toBe(first);

		await act(async () => {
			await client.ws('ws2').mutate.put(TodoSchema, { id: 'y', title: 'here' });
		});
		expect(result.current.pendingMutations).toBe(1);
		await client.close();
	});

	it('explains itself when there is no default workspace and no provider', () => {
		const client = newClient();
		expect(() => renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client) })).toThrow(
			/none of them is the default/,
		);
		void client.close();
	});

	it('rejects a workspace the client does not hold', () => {
		const client = newClient('ws1');
		expect(() => renderHook(() => db.todo.useQuery({}), { wrapper: wrapper(client, 'nope') }))
			.toThrow(/not held by this client/);
		void client.close();
	});
});
