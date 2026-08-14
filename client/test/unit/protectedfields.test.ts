// Fields a client may not write, the half a schema states — the engine's
// FieldPermissions hook can protect more per caller, and none of that is
// knowable here. The fixture is "guarded" in
// proto/private/syncplane/testdata/v1: `owner`, `audit`, `summary` and the
// `state` oneof are marked TRUSTED_ONLY, `label` and `settings` are not.
//
// A patch masking one is refused at the call site rather than applied to the
// overlay and rolled back on the rejection ack. A put is not refused: it names
// no field, so the stored value is substituted, which is what the server does
// with the row it has. That the two substitute the SAME value is what
// test/integration covers; here it is the local half.

import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	GuardedSchema,
	ProtectedKeyEntitySchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { carryProtectedFields, entityDefOf } from '../../src/mutations.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, GuardedSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('protected fields', () => {
	describe('the entity definition', () => {
		it('names a oneof by its group, once, however many members are marked', () => {
			expect(entityDefOf(GuardedSchema).protectedFields)
				.toEqual([ 'owner', 'audit', 'summary', 'state' ]);
		});

		it('leaves an entity that marks nothing with an empty set', () => {
			expect(entityDefOf(TodoSchema).protectedFields).toEqual([]);
		});

		it('refuses a protected key field, which no write could ever honour', () => {
			expect(() => entityDefOf(ProtectedKeyEntitySchema))
				.toThrow(/key field "id" is marked write_policy TRUSTED_ONLY/);
		});
	});

	describe('patch, which names what it writes', () => {
		it('refuses a mask naming a protected field', async () => {
			const client = newClient();

			await expect(client.mutate.patch(GuardedSchema, 'g1', { owner: 'mallory' }))
				.rejects.toThrow(/field "owner" may not be written by a client/);

			await client.close();
		});

		it('refuses a mask reaching INTO a protected field', async () => {
			const client = newClient();

			await expect(
				client.mutate.patch(GuardedSchema, 'g1', { audit: { size: 3 } }, {
					paths: [ 'audit.size' ],
				}),
			).rejects.toThrow(/masking "audit.size"/);

			await client.close();
		});

		it('refuses a mask naming the group a protected oneof member belongs to', async () => {
			const client = newClient();

			await expect(
				client.mutate.patch(GuardedSchema, 'g1', { state: { case: 'stateCode', value: 7 } }),
			).rejects.toThrow(/field "state" may not be written by a client/);

			await client.close();
		});

		it('applies a mask that names only writable fields', async () => {
			const client = newClient();
			await client.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });

			await client.mutate.patch(GuardedSchema, 'g1', { label: 'edited' });

			expect((await client.get(GuardedSchema, 'g1'))?.label).toBe('edited');
			await client.close();
		});

		it('refuses an update whose recipe touched a protected field', async () => {
			const client = newClient();
			await client.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });

			await expect(client.mutate.update(GuardedSchema, 'g1', (draft) => {
				draft.owner = 'mallory';
			})).rejects.toThrow(/field "owner" may not be written by a client/);

			await client.close();
		});
	});

	describe('put, which names no field', () => {
		it('lands, with the protected field cleared on a row this client has none of', async () => {
			const client = newClient();

			await client.mutate.put(GuardedSchema, { id: 'g1', label: 'made', owner: 'mallory' });

			const row = await client.get(GuardedSchema, 'g1');
			expect(row?.label).toBe('made');
			expect(row?.owner).toBe('');
			await client.close();
		});

		it('keeps what the local row holds', async () => {
			const client = newClient();
			// The only way a protected field acquires a value is a server write, so
			// carrying over is exercised directly below; here the row is whatever
			// the previous put left, which a second put must not move.
			await client.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });

			await client.mutate.put(GuardedSchema, { id: 'g1', label: 'edited', owner: 'mallory' });

			const row = await client.get(GuardedSchema, 'g1');
			expect(row?.label).toBe('edited');
			expect(row?.owner).toBe('');
			await client.close();
		});

		it('refuses a text value for a protected collaborative-text field', async () => {
			const client = newClient();

			await expect(
				client.mutate.put(GuardedSchema, { id: 'g1', label: 'made', summary: 'client wrote this' }),
			).rejects.toThrow(/field "summary" may not be written by a client/);

			await client.close();
		});
	});

	describe('the substitution itself', () => {
		const def = entityDefOf(GuardedSchema);

		it('takes every protected field from the stored row and nothing else', () => {
			const base = create(GuardedSchema, {
				id: 'g1',
				label: 'stored',
				owner: 'alice',
				audit: { color: 'green' },
				state: { case: 'stateName', value: 'open' },
			});
			const value = create(GuardedSchema, {
				id: 'g1',
				label: 'edited',
				owner: 'mallory',
				audit: { color: 'red' },
				settings: { color: 'blue' },
				state: { case: 'stateCode', value: 9 },
			});

			carryProtectedFields(def, GuardedSchema, value, base);

			expect(value.label).toBe('edited');
			expect(value.settings?.color).toBe('blue');
			expect(value.owner).toBe('alice');
			expect(value.audit?.color).toBe('green');
			expect(value.state).toEqual({ case: 'stateName', value: 'open' });
		});

		it('resets a protected field to its zero value when there is no stored row', () => {
			const value = create(GuardedSchema, {
				id: 'g1',
				label: 'made',
				owner: 'mallory',
				audit: { color: 'red' },
				state: { case: 'stateCode', value: 9 },
			});

			carryProtectedFields(def, GuardedSchema, value, undefined);

			expect(value.label).toBe('made');
			expect(value.owner).toBe('');
			expect(value.audit).toBeUndefined();
			expect(value.state).toEqual({ case: undefined });
		});

		it('copies, so the stored row cannot be reached through the write', () => {
			const base = create(GuardedSchema, { id: 'g1', audit: { color: 'green' } });
			const value = create(GuardedSchema, { id: 'g1', audit: { color: 'red' } });

			carryProtectedFields(def, GuardedSchema, value, base);
			value.audit!.color = 'mutated';

			expect(base.audit?.color).toBe('green');
		});
	});
});
