// The sign-out wipe seen from a second client on the same store, the way a
// second browser tab shares one IndexedDB. The wipe deletes the client id and
// the mutation-id counters. A client that keeps running must not then write
// under its old client id from mutation id 1: the server holds a watermark for
// that id, and answers ids at or below it as already applied.

import { describe, expect, it } from 'vitest';

import type { RejectionInfo, SyncClient } from '../../src/client.js';
import { CodeConflict } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { freshWorkspace, newClient } from './helpers.js';

/** Keeps writes in the queue until the spec calls syncNow. */
const HOLD = { pushDebounceMs: 60_000, pushMaxDelayMs: 60_000 };

/** Two clients on one store: `a` signs out, `b` stays open. */
async function twoTabs() {
	const ws = freshWorkspace();
	const kvStore = new MemoryKVStore();
	const rejections: RejectionInfo[] = [];
	const a = newClient(ws, { ...HOLD, kvStore });
	const b = newClient(ws, { ...HOLD, kvStore, onMutationRejected: (info) => rejections.push(info) });
	await a.start();
	await b.start();
	return { ws, a, b, rejections };
}

/** Puts a todo through `b`, or returns the error if `b` refuses the write. */
async function tryPut(b: SyncClient, id: string, title: string): Promise<unknown> {
	return b.mutate.put(TodoSchema, { id, title }).then(() => undefined, (err: unknown) => err);
}

async function serverTitle(ws: string, id: string): Promise<string | undefined> {
	const c = newClient(ws);
	await c.start();
	await c.syncNow();
	const title = (await c.get(TodoSchema, id))?.title;
	await c.close();
	return title;
}

describe('a client whose store another client purges', () => {
	it('does not lose a write it makes after the purge', async () => {
		// Given client b has pushed three writes
		const { ws, a, b, rejections } = await twoTabs();
		for (const n of [ 1, 2, 3 ]) {
			await b.mutate.put(TodoSchema, { id: `t${n}`, title: `before ${n}` });
		}
		await b.syncNow();

		// When client a purges the shared store, and b then writes and syncs
		await a.purgeLocalData();
		const refused = await tryPut(b, 'after', 'written after the purge');
		// Refusing the write at the call site is also correct: the app sees it.
		if (refused !== undefined) return;
		await b.syncNow();
		await b.syncNow();

		// Then the server stores the write
		// And the write must not vanish without a rejection
		expect({ onServer: await serverTitle(ws, 'after'), rejections }).toEqual({
			onServer: 'written after the purge',
			rejections: [],
		});
		await b.close();
	});

	it('does not get an old rejection for a write it makes after the purge', async () => {
		// Given client b has pushed a write, and a second write the server
		// refused as a conflict
		const { ws, a, b, rejections } = await twoTabs();
		await b.mutate.put(TodoSchema, { id: 't1', title: 'before' });
		await b.mutate.patch(TodoSchema, 'ghost', { title: 'no such row' });
		await b.syncNow();
		expect(rejections.map((r) => r.errorCode)).toEqual([ CodeConflict ]);
		rejections.length = 0;

		// When client a purges the shared store, and b then makes two writes
		await a.purgeLocalData();
		const refused = await tryPut(b, 'x', 'first after the purge');
		if (refused !== undefined) return;
		await b.mutate.put(TodoSchema, { id: 'y', title: 'second after the purge' });
		await b.syncNow();
		await b.syncNow();

		// Then the server stores both writes
		// And b must not get the conflict that an earlier write got
		expect({
			x: await serverTitle(ws, 'x'),
			y: await serverTitle(ws, 'y'),
			rejections: rejections.map((r) => r.errorCode),
		}).toEqual({ x: 'first after the purge', y: 'second after the purge', rejections: [] });
		await b.close();
	});

	it('control: stores a write after the purge when it had pushed nothing before', async () => {
		// Given client b has pushed nothing
		const { ws, a, b, rejections } = await twoTabs();

		// When client a purges the shared store, and b then writes and syncs
		await a.purgeLocalData();
		const refused = await tryPut(b, 'after', 'written after the purge');
		if (refused !== undefined) return;
		await b.syncNow();

		// Then the server stores the write
		expect(rejections).toEqual([]);
		expect(await serverTitle(ws, 'after')).toBe('written after the purge');
		await b.close();
	});
});
