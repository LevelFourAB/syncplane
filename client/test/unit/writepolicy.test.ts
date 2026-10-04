// The client refuses a write its schema says can never land, at the call site,
// instead of applying it to the overlay and rolling it back when the rejection
// ack arrives. An entity's access rules decide which writes those are: an
// operation no rule admits is open only to trusted server-side writes.

import type { DescMessage } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	EntrySchema,
	LabelSchema,
	NoteSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { type AccessRuleDecl, defineSchema, entity, text } from '../../src/schema/builder.js';

const ruled = (name: string, ...allow: AccessRuleDecl[]) =>
	entity(name, { id: text().key(), body: text() }, { access: { allow } });

const bundle = defineSchema(
	// Server-owned: clients read its rows, and no rule admits a write.
	ruled('activity', { read: true, permission: 'view' }),
	// Append-only: a client posts a row, but cannot change or remove it.
	ruled('comment', { read: true, create: true, permission: 'view' }),
	// Everything but a delete.
	ruled('ledger', { read: true, permission: 'view' }, {
		create: true,
		update: true,
		permission: 'edit',
	}),
	// Rows a client changes but never creates.
	ruled('setting', { read: true, update: true, permission: 'view' }),
	// Every admitted caller writes.
	ruled('memo', { read: true, write: true, admitted: true }),
);
const Activity = bundle.descMessage('activity');
const Comment = bundle.descMessage('comment');
const Ledger = bundle.descMessage('ledger');
const Setting = bundle.descMessage('setting');
const Memo = bundle.descMessage('memo');

// Outside the access model, so in a schema of its own.
const Plain = defineSchema(entity('plain', { id: text().key(), body: text() })).descMessage(
	'plain',
);

function newClient(entities: DescMessage[] = [ ...bundle.descMessages() ]) {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities,
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

// The write API is typed for generated schemas; a builder schema's rows pass
// through it untyped.
const put = (
	client: SyncClient,
	schema: DescMessage,
	id: string,
	body: string,
	expectedVersion?: bigint,
) =>
	client.mutate.put(
		schema,
		{ id, body } as never,
		expectedVersion === undefined ? undefined : { expectedVersion },
	);
const patch = (client: SyncClient, schema: DescMessage, id: string, body: string) =>
	client.mutate.patch(schema, id, { body } as never);
const bodyOf = async (client: SyncClient, schema: DescMessage, id: string) =>
	((await client.get(schema, id)) as { body?: string } | undefined)?.body;

describe('entity access rules on local writes', () => {
	it('leaves an entity without access rules alone', async () => {
		const client = newClient([ Plain ]);
		await put(client, Plain, 'p1', 'first');
		await patch(client, Plain, 'p1', 'edited');
		await client.mutate.del(Plain, 'p1');
		await client.close();
	});

	it('leaves an entity whose rules admit every write alone', async () => {
		const client = newClient();
		await put(client, Memo, 'm1', 'first');
		await put(client, Memo, 'm2', 'second', 0n);
		await patch(client, Memo, 'm1', 'edited');
		await client.mutate.del(Memo, 'm2');
		expect(await bodyOf(client, Memo, 'm1')).toBe('edited');
		await client.close();
	});

	it('refuses every write to a server-owned entity', async () => {
		const client = newClient();

		await expect(put(client, Activity, 'a1', 'made')).rejects.toThrow(
			'entity "activity": no access rule admits create or update, so only a trusted server-side '
				+ 'write may write its rows',
		);
		await expect(put(client, Activity, 'a1', 'made', 0n)).rejects.toThrow(
			'entity "activity": no access rule admits create, so only a trusted server-side write may '
				+ 'create its rows',
		);
		await expect(put(client, Activity, 'a1', 'made', 3n)).rejects.toThrow(
			/no access rule admits update, so only a trusted server-side write may change its rows/,
		);
		await expect(patch(client, Activity, 'a1', 'edited')).rejects.toThrow(
			'entity "activity": no access rule admits update, so only a trusted server-side write may '
				+ 'change its rows',
		);
		await expect(client.mutate.del(Activity, 'a1')).rejects.toThrow(
			'entity "activity": no access rule admits delete, so only a trusted server-side write may '
				+ 'delete its rows',
		);

		expect(await client.get(Activity, 'a1')).toBeUndefined();
		await client.close();
	});

	it('lets an append-only entity be created, guarded or not', async () => {
		const client = newClient();

		await put(client, Comment, 'c1', 'posted', 0n);
		// An unguarded put is a create when the row is absent, which only the
		// server can tell, so it passes here and the server decides.
		await put(client, Comment, 'c2', 'posted');

		expect(await bodyOf(client, Comment, 'c1')).toBe('posted');
		expect(await bodyOf(client, Comment, 'c2')).toBe('posted');
		await client.close();
	});

	it('refuses to change or remove an append-only row', async () => {
		const client = newClient();
		await put(client, Comment, 'c1', 'posted', 0n);

		await expect(patch(client, Comment, 'c1', 'edited')).rejects.toThrow(
			/no access rule admits update/,
		);
		await expect(put(client, Comment, 'c1', 'edited', 1n)).rejects.toThrow(
			/no access rule admits update/,
		);
		await expect(client.mutate.del(Comment, 'c1')).rejects.toThrow(/no access rule admits delete/);

		// The refusals left the row exactly as it was posted.
		expect(await bodyOf(client, Comment, 'c1')).toBe('posted');
		await client.close();
	});

	it('refuses through update(), which is a patch underneath', async () => {
		const client = newClient();
		await put(client, Comment, 'c1', 'posted', 0n);

		await expect(client.mutate.update(Comment, 'c1', (draft) => {
			(draft as { body?: string }).body = 'edited';
		})).rejects.toThrow(/no access rule admits update/);

		expect(await bodyOf(client, Comment, 'c1')).toBe('posted');
		await client.close();
	});

	it('restricts only what the rules leave out', async () => {
		const client = newClient();

		// "ledger" admits creates and updates but not deletes, so writes that
		// are not a delete pass.
		await put(client, Ledger, 'l1', 'made');
		await put(client, Ledger, 'l2', 'made', 0n);
		await patch(client, Ledger, 'l1', 'edited');
		await expect(client.mutate.del(Ledger, 'l1')).rejects.toThrow(/no access rule admits delete/);

		expect(await bodyOf(client, Ledger, 'l1')).toBe('edited');
		await client.close();
	});

	it('refuses only a guarded create where rules admit updates alone', async () => {
		const client = newClient();

		await expect(put(client, Setting, 's1', 'made', 0n)).rejects.toThrow(
			/no access rule admits create, so only a trusted server-side write may create its rows/,
		);
		expect(await client.get(Setting, 's1')).toBeUndefined();

		// An unguarded put is an update when the row is present; the server
		// decides on the stored row.
		await put(client, Setting, 's1', 'made');
		await put(client, Setting, 's1', 'changed', 1n);
		await patch(client, Setting, 's1', 'edited');
		await expect(client.mutate.del(Setting, 's1')).rejects.toThrow(/no access rule admits delete/);

		expect(await bodyOf(client, Setting, 's1')).toBe('edited');
		await client.close();
	});

	it('reads the rules from a compiled schema and refuses through the typed db facade', async () => {
		// Built here rather than by newClient, so the entities keep the types
		// client.db derives its keys from.
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ EntrySchema, LabelSchema, NoteSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});

		await client.db.entry.mutate.put({ id: 'e1', body: 'posted' }, { expectedVersion: 0n });
		await expect(client.db.entry.mutate.patch('e1', { body: 'edited' }))
			.rejects.toThrow('entity "entry": no access rule admits update');
		await expect(client.db.label.mutate.put({ id: 'l1', name: 'red' }))
			.rejects.toThrow('entity "label": no access rule admits create or update');
		await expect(client.db.label.mutate.del('l1'))
			.rejects.toThrow('entity "label": no access rule admits delete');
		// The admitted source counts like any other.
		await client.db.note.mutate.put({ id: 'n1', body: 'anyone' });
		await client.db.note.mutate.del('n1');

		expect((await client.db.entry.get('e1'))?.body).toBe('posted');
		await client.close();
	});
});
