import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CodeUnavailable } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

describe('sync status', () => {
	it('tracks dirty through enqueue and clean+online after sync', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		expect(a.status()).toMatchObject({ state: 'idle', online: false, dirty: false });

		await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		expect(a.status()).toMatchObject({ dirty: true, pendingMutations: 1 });
		await a.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		expect(a.status().pendingMutations).toBe(2);

		await a.start();
		await a.syncNow();
		expect(a.status()).toMatchObject({ dirty: false, pendingMutations: 0, online: true });

		await a.close();
	});

	it('emits status changes to subscribers', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const states: boolean[] = [];
		const unsub = a.onStatusChange((s) => states.push(s.dirty));

		await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await a.start();
		await a.syncNow();

		expect(states[0]).toBe(true);
		expect(states.at(-1)).toBe(false);
		unsub();
		await a.close();
	});

	it('surfaces errors and keeps mutations queued when the server is unreachable', async () => {
		const dead = new SyncClient({
			// Port 9 (discard) refuses connections immediately.
			transport: createConnectTransport({ baseUrl: 'http://127.0.0.1:9', httpVersion: '1.1' }),
			workspaceId: freshWorkspace(),
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema ],
			pollIntervalMs: 0,
			streamEnabled: false,
		});

		await dead.mutate.put(TodoSchema, { id: 't1', title: 'queued' });
		await dead.start();
		await expect(dead.syncNow()).rejects.toThrow();

		await eventually(async () => dead.status().state === 'error');
		const status = dead.status();
		expect(status.online).toBe(false);
		expect(status.lastError?.message).toBeTruthy();
		// An unreachable server is classified in the library's own namespace, not
		// Connect's — with the transport's code kept alongside for the rare caller
		// that wants it.
		expect(status.lastError?.code).toBe(CodeUnavailable);
		expect(status.lastError?.transportCode).toBe('Unavailable');
		expect(status.dirty).toBe(true);
		expect(await dead.pendingCount()).toBe(1);
		expect((await dead.get(TodoSchema, 't1'))?.title).toBe('queued');

		await dead.close();
	});
});
