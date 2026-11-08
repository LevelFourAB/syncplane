// Two installs that run at once from copies of one store: a desktop data
// directory copied to a second machine, a cloned VM or browser profile. They
// share a client id and its mutation-id counter, so both mint the same ids.
// The server keeps the first write of each id, and refuses the other
// install's push as a whole. That install must keep its write and send it
// again past the first install's mutations.

import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { freshWorkspace, newClient } from './helpers.js';

/** Keeps writes in the queue until the spec calls syncNow. */
const HOLD = { pushDebounceMs: 60_000, pushMaxDelayMs: 60_000 };

/** A copy of every key in `src`, as a copied data directory holds it. */
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

describe('two installs on copies of one store', () => {
	it('store or report the write of each install', async () => {
		// Given a store whose client has pushed one write, and a copy of it
		const ws = freshWorkspace();
		const kvA = new MemoryKVStore();
		const seed = newClient(ws, { ...HOLD, kvStore: kvA });
		await seed.start();
		await seed.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await seed.syncNow();
		await seed.close();
		const kvB = await copyStore(kvA);

		// And two installs that run at once, one on each copy
		const rejectionsA: RejectionInfo[] = [];
		const rejectionsB: RejectionInfo[] = [];
		const a = newClient(ws, {
			...HOLD,
			kvStore: kvA,
			onMutationRejected: (i) => rejectionsA.push(i),
		});
		const b = newClient(ws, {
			...HOLD,
			kvStore: kvB,
			onMutationRejected: (i) => rejectionsB.push(i),
		});
		await a.start();
		await b.start();

		// When each install writes a row, and both sync
		await a.mutate.put(TodoSchema, { id: 'fromA', title: 'machine A' });
		await b.mutate.put(TodoSchema, { id: 'fromB', title: 'machine B' });
		// The install that pushes second meets the other's mutation under the
		// shared id: the server refuses its push as a whole, and that cycle
		// reports it. Which install that is depends on their first cycles.
		const failures: string[] = [];
		for (const c of [ a, b, a, b, a, b ]) {
			await c.syncNow().catch((err: unknown) => failures.push(String(err)));
		}
		expect(failures).toEqual([ expect.stringContaining("another writer used this client's id") ]);
		await a.syncNow();
		await b.syncNow();

		// Then each row is on the server, and neither install lost a write
		expect({
			a: await serverTitle(ws, 'fromA'),
			b: await serverTitle(ws, 'fromB'),
			rejectionsA,
			rejectionsB,
			pendingA: await a.pendingCount(),
			pendingB: await b.pendingCount(),
		}).toEqual({
			a: 'machine A',
			b: 'machine B',
			rejectionsA: [],
			rejectionsB: [],
			pendingA: 0,
			pendingB: 0,
		});

		await a.close();
		await b.close();
	});

	it('control: a resent write the server already applied counts as acked', async () => {
		// Given a client whose first push response is lost after the server
		// applied the push
		const ws = freshWorkspace();
		let lost = false;
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, {
			...HOLD,
			transport: createConnectTransport({
				baseUrl: inject('serverUrl'),
				httpVersion: '1.1',
				interceptors: [ (next) => async (req) => {
					const res = await next(req);
					if (req.method.name === 'Push' && !lost) {
						lost = true;
						throw new Error('push response lost');
					}
					return res;
				} ],
			}),
			onMutationRejected: (info) => rejections.push(info),
		});
		await a.start();
		const clientId = await a.clientId();
		await a.mutate.put(TodoSchema, { id: 't1', title: 'sent twice' });

		// When the client syncs, and resends the write
		await a.syncNow().catch(() => undefined);
		expect(lost).toBe(true);
		await a.syncNow();
		await a.syncNow();

		// Then the write is acked, stored once, and the client keeps its id
		expect(rejections).toEqual([]);
		expect(await a.pendingCount()).toBe(0);
		expect(await a.clientId()).toBe(clientId);
		expect(await serverTitle(ws, 't1')).toBe('sent twice');
		await a.close();
	});
});
