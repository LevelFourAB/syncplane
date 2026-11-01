// Two SyncClients on one store stand in for two tabs. They share one pending
// queue, so a tab can renumber mutations that the other tab's undo stack
// references. The undo stack must still drop exactly the group whose mutation
// the server rejects.

import { Code, ConnectError, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

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
	// Tab A never gets a push through; tab B pushes normally.
	const make = (undo: boolean) =>
		new SyncClient({
			entities,
			transport: undo ? server.transport() : failingPushes(server.transport()),
			kvStore: kv,
			workspaceId: 'main',
			streamEnabled: false,
			presenceEnabled: false,
			pollIntervalMs: 0,
			pushDebounceMs: 2 ** 31 - 1,
			pushMaxDelayMs: 2 ** 31 - 1,
			undo: undo ? {} : undefined,
		});
	const a = make(false);
	const b = make(true);
	await a.start();
	await b.start();
	return { server, a, b };
}

/**
 * Queues two foldable writes from tab A and one write from tab B behind them,
 * then lets tab A compact the queue without pushing it. B's mutation moves
 * from id 3 to id 2, and the counter drops to 2.
 */
async function renumberUnderB(a: SyncClient, b: SyncClient): Promise<void> {
	await a.mutate.put(TodoSchema, { id: 't9', title: 'first' });
	await a.mutate.put(TodoSchema, { id: 't9', title: 'second' });
	await b.mutate.put(TodoSchema, { id: 't1', title: 'from b' });
	expect(b.canUndo()).toBe(true);

	// A compacts before it pushes; the push then fails and leaves the
	// compacted queue in place.
	await expect(a.syncNow()).rejects.toThrow('offline');
}

describe('given two tabs on one store, and a peer tab that renumbers the queue', () => {
	it('drops the group whose mutation the server rejects', async () => {
		const { server, a, b } = await tabs();
		await renumberUnderB(a, b);

		server.reject({ key: 't1' });
		const rejected: bigint[] = [];
		b.onMutationRejected((info) => rejected.push(info.mutationId));
		await b.syncNow();

		expect(rejected).toEqual([ 2n ]);
		expect(b.canUndo()).toBe(false);
		await a.close();
		await b.close();
	});

	it('keeps a group whose old id was minted again for a rejected mutation', async () => {
		const { server, a, b } = await tabs();
		await renumberUnderB(a, b);

		// The counter now stands at 2, so this write takes id 3: the id the
		// first group referenced before the renumber.
		await b.mutate.put(TodoSchema, { id: 't2', title: 'also from b' });

		server.reject({ key: 't2' });
		const rejected: bigint[] = [];
		b.onMutationRejected((info) => rejected.push(info.mutationId));
		await b.syncNow();

		expect(rejected).toEqual([ 3n ]);
		// The put of t1 was applied, so its group stays and undo still deletes t1.
		expect(b.canUndo()).toBe(true);
		expect(await b.undo()).toBe(true);
		expect(await b.get(TodoSchema, 't1')).toBeUndefined();
		await a.close();
		await b.close();
	});
});
