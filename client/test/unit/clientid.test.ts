import { createRouterTransport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { META_CLIENT_ID } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVWrite } from '../../src/kv/store.js';

/**
 * A store that counts how often the client id is looked up, and how many write
 * transactions were opened to do it — the two costs memoizing clientId() takes
 * off the mutation path.
 */
class CountingKVStore extends MemoryKVStore {
	idReads = 0;
	writes = 0;

	override async write(): Promise<KVWrite> {
		this.writes++;
		const tx = await super.write();
		const get = tx.get.bind(tx);
		tx.get = (key: string) => {
			if (key === META_CLIENT_ID) this.idReads++;
			return get(key);
		};
		return tx;
	}
}

/** A transport that answers nothing: these tests never start sync. */
const transport = createRouterTransport(() => {});

function newClient(kv: MemoryKVStore): SyncClient<readonly [typeof TodoSchema]> {
	return new SyncClient({
		transport,
		workspaceId: 'ws1',
		kvStore: kv,
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('SyncClient.clientId', () => {
	it('mints once and reuses the value', async () => {
		const kv = new CountingKVStore();
		const client = newClient(kv);

		const first = await client.clientId();
		expect(await client.clientId()).toBe(first);
		expect(await client.clientId()).toBe(first);
		expect(kv.idReads).toBe(1);
		await client.close();
	});

	it('does not re-read the store per mutation', async () => {
		const kv = new CountingKVStore();
		const client = newClient(kv);
		await client.clientId();
		const writesAfterMint = kv.writes;

		await client.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await client.mutate.put(TodoSchema, { id: 't2', title: 'two' });

		// One enqueue transaction each, and no second lookup of an id that
		// cannot have changed.
		expect(kv.writes - writesAfterMint).toBe(2);
		expect(kv.idReads).toBe(1);
		await client.close();
	});

	it('mints one id for callers racing in the same tick', async () => {
		const kv = new CountingKVStore();
		const client = newClient(kv);

		const ids = await Promise.all([ client.clientId(), client.clientId(), client.clientId() ]);
		expect(new Set(ids).size).toBe(1);
		expect(kv.idReads).toBe(1);
		await client.close();
	});

	it('does not cache a failed mint', async () => {
		// The store fails the first write transaction, as a flaky IndexedDB
		// would; the retry must be able to mint.
		let failures = 1;
		const kv = new class extends MemoryKVStore {
			override async write(): Promise<KVWrite> {
				if (failures > 0) {
					failures--;
					throw new Error('kv unavailable');
				}
				return super.write();
			}
		}();
		const client = newClient(kv);

		await expect(client.clientId()).rejects.toThrow('kv unavailable');
		await expect(client.clientId()).resolves.toMatch(/./);
		await client.close();
	});

	it('is shared by every client over the same store', async () => {
		const kv = new CountingKVStore();
		const a = newClient(kv);
		const b = newClient(kv);

		expect(await b.clientId()).toBe(await a.clientId());
		await a.close();
		await b.close();
	});
});
