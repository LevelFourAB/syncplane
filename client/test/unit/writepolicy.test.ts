// The client refuses a write its schema says can never land, at the call site,
// instead of applying it to the overlay and rolling it back when the rejection
// ack arrives. The fixtures mirror the engine's: "activity" is server-owned
// (all three policies trustedOnly), "comment" is append-only (creates allowed,
// edits and deletes not) and "ledger" restricts deletes only.

import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	ActivitySchema,
	CommentSchema,
	LedgerSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, ActivitySchema, CommentSchema, LedgerSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

/** Nothing reached the local store, so no rollback was needed. */
async function isEmpty(client: SyncClient, schema: Parameters<SyncClient['get']>[0], key: string) {
	return (await client.get(schema, key)) === undefined;
}

describe('entity write policies', () => {
	it('leaves an unrestricted entity alone', async () => {
		const client = newClient();
		await client.mutate.put(TodoSchema, { id: 't1', title: 'first' });
		await client.mutate.patch(TodoSchema, 't1', { title: 'edited' });
		await client.mutate.del(TodoSchema, 't1');
		await client.close();
	});

	it('refuses every write to a server-owned entity', async () => {
		const client = newClient();

		await expect(client.mutate.put(ActivitySchema, { id: 'a1', what: 'made' }))
			.rejects.toThrow(/createPolicy and updatePolicy are trustedOnly/);
		await expect(
			client.mutate.put(ActivitySchema, { id: 'a1', what: 'made' }, { expectedVersion: 0n }),
		).rejects.toThrow(/createPolicy is trustedOnly/);
		await expect(client.mutate.patch(ActivitySchema, 'a1', { what: 'edited' }))
			.rejects.toThrow(/updatePolicy is trustedOnly/);
		await expect(client.mutate.del(ActivitySchema, 'a1'))
			.rejects.toThrow(/deletePolicy is trustedOnly/);

		expect(await isEmpty(client, ActivitySchema, 'a1')).toBe(true);
		await client.close();
	});

	it('lets an append-only entity be created, but only when the write says so', async () => {
		const client = newClient();

		// An unguarded put could replace a row, so it is refused — and the
		// message has to teach the fix, since this is the case an app hits first.
		await expect(client.mutate.put(CommentSchema, { id: 'c1', body: 'posted' }))
			.rejects.toThrow(/pass expectedVersion 0n to assert a create/);
		expect(await isEmpty(client, CommentSchema, 'c1')).toBe(true);

		await client.mutate.put(CommentSchema, { id: 'c1', body: 'posted' }, { expectedVersion: 0n });
		expect((await client.get(CommentSchema, 'c1'))?.body).toBe('posted');

		await client.close();
	});

	it('refuses to edit or remove an append-only row', async () => {
		const client = newClient();
		await client.mutate.put(CommentSchema, { id: 'c1', body: 'posted' }, { expectedVersion: 0n });

		await expect(client.mutate.patch(CommentSchema, 'c1', { body: 'edited' }))
			.rejects.toThrow(/updatePolicy is trustedOnly/);
		await expect(
			client.mutate.put(CommentSchema, { id: 'c1', body: 'edited' }, { expectedVersion: 1n }),
		).rejects.toThrow(/updatePolicy is trustedOnly/);
		await expect(client.mutate.del(CommentSchema, 'c1'))
			.rejects.toThrow(/deletePolicy is trustedOnly/);

		// The refusals left the row exactly as it was posted.
		expect((await client.get(CommentSchema, 'c1'))?.body).toBe('posted');
		await client.close();
	});

	it('refuses through update(), which is a patch underneath', async () => {
		const client = newClient();
		await client.mutate.put(CommentSchema, { id: 'c1', body: 'posted' }, { expectedVersion: 0n });

		await expect(client.mutate.update(CommentSchema, 'c1', (draft) => {
			draft.body = 'edited';
		})).rejects.toThrow(/updatePolicy is trustedOnly/);

		expect((await client.get(CommentSchema, 'c1'))?.body).toBe('posted');
		await client.close();
	});

	it('refuses through the typed db facade', async () => {
		const client = newClient();

		await expect(client.db.comment.mutate.put({ id: 'c1', body: 'posted' }))
			.rejects.toThrow(/updatePolicy is trustedOnly/);
		await expect(client.db.activity.mutate.del('a1'))
			.rejects.toThrow(/deletePolicy is trustedOnly/);

		await client.close();
	});

	it('restricts only what the schema restricts', async () => {
		const client = newClient();

		// "ledger" restricts deletes and nothing else, so an unguarded put still
		// passes: the check never asks about a policy the entity did not set.
		await client.mutate.put(LedgerSchema, { id: 'l1', label: 'made' });
		await client.mutate.patch(LedgerSchema, 'l1', { label: 'edited' });
		await expect(client.mutate.del(LedgerSchema, 'l1'))
			.rejects.toThrow(/deletePolicy is trustedOnly/);

		expect((await client.get(LedgerSchema, 'l1'))?.label).toBe('edited');
		await client.close();
	});
});
