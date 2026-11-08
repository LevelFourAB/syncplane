// A local store that goes back in time while the server keeps its watermark:
// a device backup restored, a profile copied from an older state, a commit
// lost on power failure. The store holds the same client id with an older
// mutation-id counter, and the server has applied ids above that counter. The
// writes made after the restore must still reach the server.

import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { freshWorkspace, newClient } from './helpers.js';

/** Keeps writes in the queue until the spec calls syncNow. */
const HOLD = { pushDebounceMs: 60_000, pushMaxDelayMs: 60_000 };

/** A copy of every key in `src`, as a backup of the store holds it. */
async function copyStore(src: MemoryKVStore): Promise<MemoryKVStore> {
	const out = new MemoryKVStore();
	const r = await src.read();
	const w = await out.write();
	for await (const [ k, v ] of r.scan('')) await w.put(k, structuredClone(v));
	r.release();
	await w.commit();
	return out;
}

async function serverTitle(ws: string, id: string): Promise<string | undefined> {
	const c = newClient(ws);
	await c.start();
	await c.syncNow();
	const title = (await c.get(TodoSchema, id))?.title;
	await c.close();
	return title;
}

/**
 * Pushes one write, takes a backup of the store, then makes `later` (writes
 * that the server applies after the backup) and closes. Returns the backup.
 */
async function backupThenMoreWrites(
	ws: string,
	later: (c: ReturnType<typeof newClient>) => Promise<void>,
): Promise<MemoryKVStore> {
	const kvStore = new MemoryKVStore();
	const a = newClient(ws, { ...HOLD, kvStore });
	await a.start();
	await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
	await a.syncNow();
	const backup = await copyStore(kvStore);
	await later(a);
	await a.syncNow();
	await a.close();
	return backup;
}

describe('a store restored from an older copy', () => {
	it('keeps a write made after the restore', async () => {
		// Given the server has applied three more writes than the backup holds
		const ws = freshWorkspace();
		const backup = await backupThenMoreWrites(ws, async (a) => {
			for (const n of [ 2, 3, 4 ]) await a.mutate.put(TodoSchema, { id: `t${n}`, title: `n${n}` });
		});

		// When a client opens the restored store, syncs, writes and syncs again
		const rejections: RejectionInfo[] = [];
		const b = newClient(ws, {
			...HOLD,
			kvStore: backup,
			onMutationRejected: (info) => rejections.push(info),
		});
		await b.start();
		await b.syncNow();
		await b.mutate.put(TodoSchema, { id: 'after', title: 'written after the restore' });
		await b.syncNow();
		await b.syncNow();

		// Then the server stores the write
		// And the write must not vanish without a rejection
		expect({ onServer: await serverTitle(ws, 'after'), rejections }).toEqual({
			onServer: 'written after the restore',
			rejections: [],
		});
		expect(await serverTitle(ws, 't4')).toBe('n4');
		await b.close();
	});

	it('keeps writes made on the restored store before its first pull', async () => {
		// Given the server has applied three more writes than the backup holds
		const ws = freshWorkspace();
		const backup = await backupThenMoreWrites(ws, async (a) => {
			for (const n of [ 2, 3, 4 ]) await a.mutate.put(TodoSchema, { id: `t${n}`, title: `n${n}` });
		});

		// When a client on the restored store writes twice before it syncs
		const rejections: RejectionInfo[] = [];
		const b = newClient(ws, {
			...HOLD,
			kvStore: backup,
			onMutationRejected: (info) => rejections.push(info),
		});
		await b.mutate.put(TodoSchema, { id: 'offline-1', title: 'first' });
		await b.mutate.put(TodoSchema, { id: 'offline-2', title: 'second' });
		// And then comes online
		await b.start();
		await b.syncNow();
		await b.syncNow();

		// Then both writes reach the server
		// And the server's watermark must not retire writes that were never pushed
		expect({
			first: await serverTitle(ws, 'offline-1'),
			second: await serverTitle(ws, 'offline-2'),
			rejections,
		}).toEqual({ first: 'first', second: 'second', rejections: [] });
		await b.close();
	});

	it('does not report the old verdict of a reused mutation id for a new write', async () => {
		// Given the server refused a write after the backup was taken
		const ws = freshWorkspace();
		const backup = await backupThenMoreWrites(ws, async (a) => {
			await a.mutate.patch(TodoSchema, 'ghost', { title: 'no such row' });
			await a.mutate.put(TodoSchema, { id: 't3', title: 'n3' });
		});

		// When a client on the restored store syncs, writes and syncs again
		const rejections: RejectionInfo[] = [];
		const b = newClient(ws, {
			...HOLD,
			kvStore: backup,
			onMutationRejected: (info) => rejections.push(info),
		});
		await b.start();
		await b.syncNow();
		await b.mutate.put(TodoSchema, { id: 'after', title: 'written after the restore' });
		await b.syncNow();
		await b.syncNow();

		// Then the server stores the new write
		// And the app must not get the verdict of the refused write for it
		expect({
			onServer: await serverTitle(ws, 'after'),
			rejections: rejections.map((r) => r.errorCode),
		}).toEqual({ onServer: 'written after the restore', rejections: [] });
		await b.close();
	});

	it('control: a store that is not rolled back keeps its client id and pushes the next write', async () => {
		// Given a client that has pushed four writes
		const ws = freshWorkspace();
		const kvStore = new MemoryKVStore();
		const a = newClient(ws, { ...HOLD, kvStore });
		await a.start();
		for (const n of [ 1, 2, 3, 4 ]) await a.mutate.put(TodoSchema, { id: `t${n}`, title: `n${n}` });
		await a.syncNow();
		const clientId = await a.clientId();
		await a.close();

		// When a new client opens the same store, writes and syncs
		const b = newClient(ws, { ...HOLD, kvStore });
		await b.start();
		await b.syncNow();
		await b.mutate.put(TodoSchema, { id: 'after', title: 'next write' });
		await b.syncNow();

		// Then the write is stored under the same client id
		expect(await serverTitle(ws, 'after')).toBe('next write');
		expect(await b.clientId()).toBe(clientId);
		expect(await b.pendingCount()).toBe(0);
		await b.close();
	});
});
