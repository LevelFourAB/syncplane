// Two SyncClients on one store stand in for two tabs. Tab A pushes and so is
// the tab that hears the server; tab B never pushes. Events that tab A handles —
// a rejection, a custom mutation — must still reach tab B's undo stacks, through
// the journal in the shared store rather than through tab A's memory.

import { Code, ConnectError, type Transport } from '@connectrpc/connect';
import { describe, expect, it, onTestFinished, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { META_CLIENT_ID } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createTestServer } from '../../src/testing/index.js';

const entities = [ TodoSchema ] as const;

/** Delegates to `t`, but every push fails as if the network were down. */
function failingPushes(t: Transport): Transport {
	return {
		unary: (method, ...rest) =>
			method.localName === 'push'
				? Promise.reject(new ConnectError('offline', Code.Unavailable))
				: t.unary(method, ...rest),
		stream: t.stream.bind(t),
	} as Transport;
}

async function tabs() {
	const server = createTestServer({ entities });
	const kv = new MemoryKVStore();
	const seed = await kv.write();
	await seed.put(META_CLIENT_ID, 'tab-client');
	await seed.commit();
	const make = (pushes: boolean) =>
		new SyncClient({
			entities,
			transport: pushes ? server.transport() : failingPushes(server.transport()),
			kvStore: kv,
			workspaceId: 'main',
			streamEnabled: false,
			presenceEnabled: false,
			pollIntervalMs: 0,
			pushDebounceMs: 2 ** 31 - 1,
			pushMaxDelayMs: 2 ** 31 - 1,
			undo: {},
		});
	const a = make(true);
	const b = make(false);
	await a.start();
	await b.start();
	const close = async () => {
		await a.close();
		await b.close();
	};
	return { server, a, b, close };
}

describe('given two tabs on one store, and a peer tab that pushes', () => {
	it("drops a group whose mutation the peer's push got rejected", async () => {
		const { server, a, b, close } = await tabs();
		await b.mutate.put(TodoSchema, { id: 't1', title: 'from b' });
		expect(b.canUndo()).toBe(true);

		server.reject({ key: 't1' });
		await a.syncNow();
		expect(await b.get(TodoSchema, 't1')).toBeUndefined();

		expect(await b.undo()).toBe(false);
		expect(b.canUndo()).toBe(false);
		await close();
	});

	it('skips the rejected group and undoes the one below it', async () => {
		const { server, a, b, close } = await tabs();
		await b.mutate.put(TodoSchema, { id: 't2', title: 'kept' });
		await b.mutate.put(TodoSchema, { id: 't1', title: 'refused' });

		server.reject({ key: 't1' });
		await a.syncNow();

		expect(await b.undo()).toBe(true);
		expect(await b.get(TodoSchema, 't2')).toBeUndefined();
		expect(b.canUndo()).toBe(false);
		await close();
	});

	it('updates canUndo() when the peer reports the rejection, before any undo', async () => {
		// The cross-tab change channel is off on a server runtime; pass for a
		// browser so the peer's broadcast reaches tab B.
		vi.stubGlobal('navigator', { userAgent: 'Mozilla/5.0' });
		onTestFinished(() => {
			vi.unstubAllGlobals();
		});
		const { server, a, b, close } = await tabs();
		await b.mutate.put(TodoSchema, { id: 't1', title: 'from b' });

		server.reject({ key: 't1' });
		await a.syncNow();

		await vi.waitFor(() => expect(b.canUndo()).toBe(false));
		await close();
	});

	it('clears the stacks when the peer issues a custom mutation', async () => {
		const { a, b, close } = await tabs();
		await b.mutate.put(TodoSchema, { id: 't1', title: 'before the barrier' });

		await a.mutate.custom('anything', TodoSchema, { id: 't9' });

		expect(await b.undo()).toBe(false);
		expect(await b.get(TodoSchema, 't1')).toBeDefined();
		expect(b.canUndo()).toBe(false);
		await close();
	});

	it("keeps a group recorded after the peer's custom mutation", async () => {
		const { a, b, close } = await tabs();
		await b.mutate.put(TodoSchema, { id: 't1', title: 'before the barrier' });
		await a.mutate.custom('anything', TodoSchema, { id: 't9' });
		await b.mutate.put(TodoSchema, { id: 't2', title: 'after the barrier' });

		expect(await b.undo()).toBe(true);
		expect(await b.get(TodoSchema, 't2')).toBeUndefined();
		expect(await b.undo()).toBe(false);
		expect(await b.get(TodoSchema, 't1')).toBeDefined();
		await close();
	});

	it('drops the redo step of a group the peer clears', async () => {
		const { a, b, close } = await tabs();
		await b.mutate.put(TodoSchema, { id: 't1', title: 'x' });
		expect(await b.undo()).toBe(true);
		expect(b.canRedo()).toBe(true);

		await a.mutate.custom('anything', TodoSchema, { id: 't9' });

		expect(await b.redo()).toBe(false);
		expect(await b.get(TodoSchema, 't1')).toBeUndefined();
		await close();
	});
});
