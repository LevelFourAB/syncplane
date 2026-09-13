// What a local write costs is the store round trips it makes, not the JS it
// runs: enqueue() takes the mutation-id watermark and every crud's prior row in
// ONE batched read, so a write opens a single read-only transaction instead of
// one per key (see kv/idb.ts, where each get is its own transaction).
//
// The count is the point of these specs, together with the semantics the
// batched read must not lose: a later crud of a batch still sees what an
// earlier one wrote, even though the read happened before either applied.

import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead, KVStore, KVWrite } from '../../src/kv/store.js';

/** Point reads served, by the verb that served them. */
interface ReadCounts {
	get: number;
	getMany: number;
}

/** A store whose transactions count the point reads they serve. */
class CountingStore implements KVStore {
	readonly counts: ReadCounts = { get: 0, getMany: 0 };

	constructor(private inner: KVStore) {}

	async read(): Promise<KVRead> {
		return this.counting(await this.inner.read());
	}

	async write(): Promise<KVWrite> {
		return this.counting(await this.inner.write());
	}

	close(): Promise<void> {
		return this.inner.close();
	}

	reset(): void {
		this.counts.get = 0;
		this.counts.getMany = 0;
	}

	private counting<T extends KVRead>(tx: T): T {
		const counts = this.counts;
		return new Proxy(tx, {
			get(target, prop) {
				const value = Reflect.get(target, prop) as unknown;
				if (typeof value !== 'function') return value;
				if (prop === 'get' || prop === 'getMany') {
					return (...args: unknown[]) => {
						counts[prop]++;
						return (value as (...a: unknown[]) => unknown).apply(target, args);
					};
				}
				return value.bind(target);
			},
		});
	}
}

function newClient(kvStore: KVStore): SyncClient<[typeof TodoSchema]> {
	return new SyncClient({
		workspaceId: 'ws-reads',
		kvStore,
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('enqueue() reads', () => {
	it('serves a patch of a text-free entity from one batched read', async () => {
		const store = new CountingStore(new MemoryKVStore());
		const c = newClient(store);
		await c.mutate.put(TodoSchema, { id: 't1', title: 'first' });
		store.reset();
		await c.mutate.patch(TodoSchema, 't1', { title: 'second' });
		expect(store.counts).toEqual({ get: 0, getMany: 1 });
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('second');
		await c.close();
	});

	it('reads nothing but the watermark for a put of a text-free entity', async () => {
		const store = new CountingStore(new MemoryKVStore());
		const c = newClient(store);
		await c.mutate.put(TodoSchema, { id: 't1', title: 'first' });
		store.reset();
		await c.mutate.put(TodoSchema, { id: 't2', title: 'second' });
		expect(store.counts).toEqual({ get: 0, getMany: 1 });
		await c.close();
	});

	it('lets a later crud of a batch see the row an earlier one wrote', async () => {
		const store = new CountingStore(new MemoryKVStore());
		const c = newClient(store);
		await c.transact(async (tx) => {
			await tx.mutate.put(TodoSchema, { id: 't1', title: 'first', completed: false });
			await tx.mutate.patch(TodoSchema, 't1', { completed: true });
		});
		const row = await c.get(TodoSchema, 't1');
		// The PATCH merged onto the PUT rather than onto what was on disk before
		// the batch: an unmasked field of the PUT survives, and the PATCH applied
		// instead of finding no row at all.
		expect(row?.title).toBe('first');
		expect(row?.completed).toBe(true);
		expect(await c.pendingCount()).toBe(1);
		await c.close();
	});
});
