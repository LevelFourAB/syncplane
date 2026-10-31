import { describe, expect, it } from 'vitest';

import { subscription, SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

// The promise setSubscriptions() returns resolves once a pull has run under the
// new set. That half needs a server and lives in the integration tier; these
// specs pin the ways it settles with no sync at all, so a view awaiting it
// never hangs on a client that went away.

const entities = [ TodoSchema, WidgetSchema ] as const;

function newClient(workspaces: string[] = [ 'ws1' ]): SyncClient<typeof entities> {
	return new SyncClient({
		accountId: 'acct',
		workspaces,
		defaultWorkspace: workspaces[0],
		kvStore: new MemoryKVStore(),
		entities,
		streamEnabled: false,
		pollIntervalMs: 0,
		subscriptions: [ subscription(TodoSchema) ],
	});
}

describe('setSubscriptions ready', () => {
	it('stays pending while no sync runs', async () => {
		const client = newClient();
		let settled = false;
		void client.setSubscriptions([ subscription(WidgetSchema) ]).then(() => {
			settled = true;
		});
		await new Promise((r) => setTimeout(r, 20));
		expect(settled).toBe(false);
		await client.close();
	});

	it('resolves on close', async () => {
		const client = newClient();
		const ready = client.setSubscriptions([ subscription(WidgetSchema) ]);
		const workspace = client.ws('ws1').setSubscriptions([]);
		await client.close();
		await expect(ready).resolves.toBeUndefined();
		await expect(workspace).resolves.toBeUndefined();
	});

	it('resolves when the workspace is removed', async () => {
		const client = newClient([ 'ws1', 'ws2' ]);
		const ready = client.ws('ws2').setSubscriptions([ subscription(WidgetSchema) ]);
		await client.removeWorkspace('ws2');
		await expect(ready).resolves.toBeUndefined();
		await client.close();
	});

	it('throws at the call for an invalid set and a closed client', async () => {
		const client = newClient();
		expect(() => client.setSubscriptions([ { schema: { typeName: 'nope' } as never } ])).toThrow();
		await client.close();
		expect(() => client.setSubscriptions([])).toThrow();
	});
});
