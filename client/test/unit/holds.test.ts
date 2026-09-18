import { describe, expect, it } from 'vitest';

import { subscription, SyncClient, type SyncClientOptions } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Subscription } from '../../src/gen/syncplane/v1/patch_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

// A hold is a runtime-managed key-set subscription layered on the declared
// scope. These specs pin the bookkeeping that never reaches the wire: what the
// effective scope says while a row is held, how holds count and expire, and
// what a hold cannot add to. Everything here is local — no start(), no
// transport — because the scope is readable through coverage before any sync
// runs. The wire half is in the integration tier.

const entities = [ TodoSchema, WidgetSchema ] as const;

function newClient(
	overrides: Partial<SyncClientOptions<typeof entities>> = {},
): SyncClient<typeof entities> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities,
		streamEnabled: false,
		pollIntervalMs: 0,
		subscriptions: [ subscription(TodoSchema) ],
		holdGraceMs: 0,
		...overrides,
	});
}

/** Coverage bounds of a widget read, which is how the effective scope shows. */
async function widgetBounds(client: SyncClient): Promise<string[]> {
	return (await client.query(WidgetSchema, {})).coverage.bounds;
}

/** The scope map the coordinator would be handed, declared subscriptions plus holds. */
function effectiveScopes(client: SyncClient): Map<string, Subscription[]> {
	return (client as unknown as { ownScopes(): Map<string, Subscription[]> }).ownScopes();
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('holds', () => {
	it('scopes an unsubscribed entity to the held rows', async () => {
		const client = newClient();
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);

		const a = client.db.widget.hold('a');
		const b = client.hold(WidgetSchema, 'b');
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);

		const subs = effectiveScopes(client).get('ws1')!;
		expect(subs).toHaveLength(2);
		expect(subs[1]!.entity).toBe('widget');
		expect(subs[1]!.keySet?.keys).toEqual([ 'a', 'b' ]);

		a.release();
		b.release();
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);
		expect(effectiveScopes(client).get('ws1')).toHaveLength(1);
		await client.close();
	});

	it('keeps a row until every hold on it is released', async () => {
		const client = newClient();
		const first = client.db.widget.hold('a');
		const second = client.db.widget.hold('a');
		first.release();
		first.release(); // idempotent: a second release is not a third hold's worth
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);
		second.release();
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);
		await client.close();
	});

	it('keeps a released row for the grace period', async () => {
		const client = newClient({ holdGraceMs: 30 });
		client.db.widget.hold('a').release();
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);
		await sleep(60);
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);
		await client.close();
	});

	it('re-holding inside the grace cancels the drop', async () => {
		const client = newClient({ holdGraceMs: 30 });
		client.db.widget.hold('a').release();
		const again = client.db.widget.hold('a');
		await sleep(60);
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);
		again.release();
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);
		await sleep(60);
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);
		await client.close();
	});

	it('changes nothing under full sync', async () => {
		const client = newClient({ subscriptions: [] });
		const hold = client.db.widget.hold('a');
		expect((await client.query(WidgetSchema, {})).coverage.complete).toBe(true);
		expect(effectiveScopes(client).get('ws1')).toEqual([]);
		hold.release();
		await client.close();
	});

	it('adds nothing for an entity a declared subscription covers whole', async () => {
		const client = newClient({
			subscriptions: [ subscription(TodoSchema), subscription(WidgetSchema) ],
		});
		const hold = client.db.widget.hold('a');
		expect((await client.query(WidgetSchema, {})).coverage.complete).toBe(true);
		expect(effectiveScopes(client).get('ws1')).toHaveLength(2);
		hold.release();
		await client.close();
	});

	it('joins a declared predicate scope of the same entity', async () => {
		const client = newClient({
			subscriptions: [ subscription(WidgetSchema, (w) => w.count.eq(1)) ],
		});
		const hold = client.db.widget.hold('a');
		const subs = effectiveScopes(client).get('ws1')!;
		expect(subs.map((s) => s.entity)).toEqual([ 'widget', 'widget' ]);
		expect(subs[0]!.predicate).toBeDefined();
		expect(subs[1]!.keySet?.keys).toEqual([ 'a' ]);
		hold.release();
		await client.close();
	});

	it('survives a declared scope change', async () => {
		const client = newClient();
		const hold = client.db.widget.hold('a');
		client.setSubscriptions([ subscription(TodoSchema, (t) => t.completed.eq(false)) ]);
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);
		hold.release();
		await client.close();
	});

	it('resolves ready when released before any sync', async () => {
		const client = newClient();
		const hold = client.db.widget.hold('a');
		hold.release();
		await expect(hold.ready).resolves.toBeUndefined();
		await client.close();
	});

	it('resolves ready on close', async () => {
		const client = newClient();
		const hold = client.db.widget.hold('a');
		await client.close();
		await expect(hold.ready).resolves.toBeUndefined();
	});

	it('refuses a malformed key and a closed client', async () => {
		const client = newClient();
		expect(() => client.db.widget.hold('')).toThrow(/row key is empty/);
		await client.close();
		expect(() => client.db.widget.hold('a')).toThrow(/closed/);
	});

	it('is not available inside a transaction', async () => {
		const client = newClient();
		await expect(
			client.transact(async (tx) => {
				tx.db.widget.hold('a');
			}),
		).rejects.toThrow(/holds are not available in transactions/);
		await client.close();
	});
});
