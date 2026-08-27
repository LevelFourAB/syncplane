import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CodeWorkspacePurged } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import type { Row } from '../../src/read.js';
import type { RejectionInfo } from '../../src/sync/coordinator.js';

// The sign-out wipe. These specs stay local (no start(), so no transport): what
// matters is that nothing of the account is left in the store the next user's
// client would open, and that the unsent work is handed over on the way out.

const A = 'ws-a';
const B = 'ws-b';

function newClient(
	kvStore: KVStore,
	opts: { onMutationRejected?: (info: RejectionInfo) => void } = {},
) {
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ A, B ],
		defaultWorkspace: A,
		kvStore,
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		onMutationRejected: opts.onMutationRejected,
	});
}

const flush = () => new Promise((r) => setTimeout(r, 5));

async function storeKeys(kv: KVStore): Promise<string[]> {
	const tx = await kv.read();
	try {
		const keys: string[] = [];
		for await (const [ k ] of tx.scan('')) keys.push(k);
		return keys;
	} finally {
		tx.release();
	}
}

describe('SyncClient.purgeLocalData', () => {
	it('empties the store of every workspace and closes the client', async () => {
		const kv = new MemoryKVStore();
		const client = newClient(kv);
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'in A' });
		await client.ws(B).mutate.put(TodoSchema, { id: 't2', title: 'in B' });
		expect((await storeKeys(kv)).length).toBeGreaterThan(0);

		await client.purgeLocalData();

		expect(await storeKeys(kv)).toEqual([]);
		await expect(client.mutate.put(TodoSchema, { id: 't3', title: 'after' }))
			.rejects.toThrow(/SyncClient is closed/);
	});

	it('leaves the next client on the store empty and under a new device identity', async () => {
		const kv = new MemoryKVStore();
		const first = newClient(kv);
		await first.mutate.put(TodoSchema, { id: 't1', title: 'signed-out user' });
		const firstId = await first.clientId();

		await first.purgeLocalData();

		// A fresh identity is the point: the server's watermarks are keyed by it,
		// and keeping the handle that addresses them keeps a trace of the account
		// the device is done with.
		const second = newClient(kv);
		expect(await second.scanAll(TodoSchema)).toEqual([]);
		expect(await second.clientId()).not.toBe(firstId);
		await second.close();
	});

	it('hands back the queued mutations and reports each one', async () => {
		const rejections: RejectionInfo[] = [];
		const client = newClient(new MemoryKVStore(), {
			onMutationRejected: (info) => rejections.push(info),
		});
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'unsent in A' });
		await client.ws(B).mutate.put(TodoSchema, { id: 't2', title: 'unsent in B' });
		await client.ws(B).mutate.patch(TodoSchema, 't2', { completed: true });

		const discarded = await client.purgeLocalData();

		// Grouped by workspace, in queue order within each — and this is the only
		// copy left, so an app keeping unsent work takes it from here.
		expect(discarded.map((d) => [ d.workspaceId, d.mutationId ])).toEqual([
			[ A, 1n ],
			[ B, 1n ],
			[ B, 2n ],
		]);
		expect(discarded[0]?.mutation.payload.case).toBe('crud');

		// Ignoring the result still does not lose the loss silently.
		expect(rejections.map((r) => [ r.workspaceId, r.errorCode ])).toEqual([
			[ A, CodeWorkspacePurged ],
			[ B, CodeWorkspacePurged ],
			[ B, CodeWorkspacePurged ],
		]);
	});

	it('is idempotent', async () => {
		const client = newClient(new MemoryKVStore());
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'unsent' });

		expect((await client.purgeLocalData()).length).toBe(1);
		expect(await client.purgeLocalData()).toEqual([]);
		await client.close();
	});

	it('wakes watchers before it closes, so the UI drops the rows', async () => {
		const client = newClient(new MemoryKVStore());
		await client.ws(A).mutate.put(TodoSchema, { id: 't1', title: 'in A' });
		const seen: Row<unknown>[][] = [];
		const unsub = client.ws(A).watch(TodoSchema, (rows) => seen.push(rows));
		await flush();
		expect(seen.at(-1)?.length).toBe(1);

		await client.purgeLocalData();

		await vi.waitFor(() => expect(seen.at(-1)).toEqual([]));
		unsub.close();
	});
});
