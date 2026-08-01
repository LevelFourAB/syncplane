import { act, renderHook } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { type RejectionInfo, SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync, useOnMutationRejected } from '../../src/react/index.js';

const { db, SyncProvider, WorkspaceProvider } = createReactSync([ TodoSchema ]);

/** Two workspaces, so the hook's per-workspace filter has something to filter. */
function newClient(): SyncClient<readonly [typeof TodoSchema]> {
	return new SyncClient({
		workspaces: [ 'ws1', 'ws2' ],
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

/**
 * Purging a workspace discards whatever it still had queued and reports every
 * discarded mutation on the dead-letter surface — the one rejection path that
 * needs no server, and it carries the same shape a server rejection does.
 */
async function purge(client: SyncClient, workspaceId: string): Promise<void> {
	await act(async () => {
		await client.removeWorkspace(workspaceId, { purge: true });
	});
}

describe('useOnMutationRejected', () => {
	it('delivers the rejection, naming the row it would have written', async () => {
		const client = newClient();
		const seen: RejectionInfo[] = [];
		renderHook(() => useOnMutationRejected((info) => seen.push(info)), {
			wrapper: wrapper(client),
		});
		await client.ws('ws1').mutate.put(TodoSchema, { id: 't1', title: 'never sent' });

		await purge(client, 'ws1');

		expect(seen).toHaveLength(1);
		expect(seen[0]!.errorCode).toBe('workspace_purged');
		expect(seen[0]!.workspaceId).toBe('ws1');
		// The point of `targets`: a toast can say WHICH row failed.
		expect(seen[0]!.targets.map((t) => t.key)).toEqual([ 't1' ]);
		expect(seen[0]!.targets[0]!.entity).toMatch(/Todo/i);
		await client.close();
	});

	it('describes every member of a rejected batch', async () => {
		const client = newClient();
		const seen: RejectionInfo[] = [];
		renderHook(() => useOnMutationRejected((info) => seen.push(info)), {
			wrapper: wrapper(client),
		});
		await client.ws('ws1').transact(async (tx) => {
			await tx.mutate.put(TodoSchema, { id: 'a', title: 'one' });
			await tx.mutate.put(TodoSchema, { id: 'b', title: 'two' });
		});

		await purge(client, 'ws1');

		expect(seen).toHaveLength(1);
		expect(seen[0]!.targets.map((t) => t.key)).toEqual([ 'a', 'b' ]);
		await client.close();
	});

	it('delivers only the resolved workspace by default', async () => {
		const client = newClient();
		const seen: RejectionInfo[] = [];
		renderHook(() => useOnMutationRejected((info) => seen.push(info)), {
			// Scoped to ws2; ws1's failures belong to someone else's view.
			wrapper: wrapper(client, 'ws2'),
		});
		await client.ws('ws1').mutate.put(TodoSchema, { id: 't1', title: 'in one' });
		await client.ws('ws2').mutate.put(TodoSchema, { id: 't2', title: 'in two' });

		await purge(client, 'ws1');
		expect(seen).toHaveLength(0);

		await purge(client, 'ws2');
		expect(seen.map((i) => i.workspaceId)).toEqual([ 'ws2' ]);
		await client.close();
	});

	it('delivers every workspace with allWorkspaces', async () => {
		const client = newClient();
		const seen: RejectionInfo[] = [];
		renderHook(
			() => useOnMutationRejected((info) => seen.push(info), { allWorkspaces: true }),
			{ wrapper: wrapper(client, 'ws2') },
		);
		await client.ws('ws1').mutate.put(TodoSchema, { id: 't1', title: 'in one' });

		await purge(client, 'ws1');
		expect(seen.map((i) => i.workspaceId)).toEqual([ 'ws1' ]);
		await client.close();
	});

	it('calls the newest handler without resubscribing, and stops on unmount', async () => {
		const client = newClient();
		const calls: string[] = [];
		const { rerender, unmount } = renderHook(
			({ tag }: { tag: string }) =>
				useOnMutationRejected(() => {
					calls.push(tag);
				}),
			{ wrapper: wrapper(client), initialProps: { tag: 'first' } },
		);
		rerender({ tag: 'second' });

		await client.ws('ws1').mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await purge(client, 'ws1');
		expect(calls).toEqual([ 'second' ]);

		unmount();
		await client.ws('ws2').mutate.put(TodoSchema, { id: 't2', title: 'two' });
		await purge(client, 'ws2');
		expect(calls).toEqual([ 'second' ]);
		await client.close();
	});

	it('leaves the constructor option working alongside the hook', async () => {
		const viaOption: RejectionInfo[] = [];
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
			onMutationRejected: (info) => viaOption.push(info),
		});
		const viaHook: RejectionInfo[] = [];
		renderHook(() => useOnMutationRejected((info) => viaHook.push(info)), {
			wrapper: wrapper(client),
		});
		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });

		await purge(client, 'ws1');

		expect(viaOption).toHaveLength(1);
		expect(viaHook).toHaveLength(1);
		await client.close();
	});

	it('keeps mutations optimistic — the write promise resolves, the rejection follows', async () => {
		const client = newClient();
		const seen: RejectionInfo[] = [];
		const { result } = renderHook(
			() => ({
				mutate: db.todo.useMutate(),
				rejected: useOnMutationRejected((info) => seen.push(info)),
			}),
			{ wrapper: wrapper(client) },
		);
		// Resolves on local commit, long before anything is pushed.
		await act(async () => {
			await result.current.mutate.put({ id: 't1', title: 'optimistic' });
		});
		expect(seen).toHaveLength(0);

		await purge(client, 'ws1');
		expect(seen).toHaveLength(1);
		await client.close();
	});
});
