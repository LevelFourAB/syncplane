// A PATCH names a row it expects to exist. When none does, delete-wins makes
// the local apply a no-op and the server answers `conflict` a round trip later,
// naming an entity, a key and a workspace far from the call that asked for them
// — so the client says so at the write instead.
//
// These specs pin where it is said, and where it is not: a rebase replay hits
// the same outcome whenever the server has deleted a row the queue still
// patches, and warning there would fire on the policy working.

import { create, toBinary } from '@bufbuild/protobuf';
import { afterEach, beforeEach, describe, expect, it, type MockInstance, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import { applyMutationLocally, type MirrorMap, rebase } from '../../src/optimistic/rebase.js';
import { TouchSet } from '../../src/touch.js';

const WS = 'ws1';
const registry = new EntityRegistry([ TodoSchema ]);
const noMirrors: MirrorMap = {};

function newClient(kv: MemoryKVStore) {
	return new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

function serverRow(key: string, version: bigint, title: string): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: {
			case: 'value',
			value: toBinary(TodoSchema, create(TodoSchema, { id: key, title })),
		},
	});
}

function tombstone(key: string, version: bigint): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'tombstone', value: true },
	});
}

function patchMutation(id: bigint, key: string, title: string): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'todo',
				key,
				op: CrudOp.PATCH,
				value: toBinary(TodoSchema, create(TodoSchema, { title })),
				fieldMask: { paths: [ 'title' ] },
			},
		},
	});
}

describe('patch with no row to land on', () => {
	let warn: MockInstance<typeof console.warn>;

	beforeEach(() => {
		warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
	});
	afterEach(() => {
		warn.mockRestore();
	});

	it('names the entity, key and workspace the app got wrong', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);

		await c.db.todo.mutate.patch('nope', { completed: true });

		expect(warn).toHaveBeenCalledTimes(1);
		const message = String(warn.mock.calls[0]?.[0]);
		// The three facts that make the warning actionable where a rejection
		// arriving later is not: which entity it was written through, which key,
		// and which workspace.
		expect(message).toContain('todo');
		expect(message).toContain('"nope"');
		expect(message).toContain(`"${WS}"`);
		await c.close();
	});

	it('queues the patch anyway, so the server still has the last word', async () => {
		// The warning is advice, not a veto: refusing the write here would make a
		// row the client has not pulled yet unpatchable.
		const kv = new MemoryKVStore();
		const c = newClient(kv);

		await c.db.todo.mutate.patch('nope', { completed: true });

		expect(await c.pendingCount()).toBe(1);
		await c.close();
	});

	it('stays quiet when the row is there', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.todo.mutate.put({ id: 't1', title: 'a' });

		await c.db.todo.mutate.patch('t1', { completed: true });

		expect(warn).not.toHaveBeenCalled();
		await c.close();
	});

	it('stays quiet when a rebase replays a patch the server deleted out from under', async () => {
		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, 'original') ],
			nextCursor: new Uint8Array([ 1 ]),
			lastAckedMutationId: 0n,
		});

		const mutation = patchMutation(1n, 't1', 'local rename');
		const tx = await kv.write();
		await enqueuePending(tx, WS, mutation);
		await applyMutationLocally(tx, registry, noMirrors, WS, mutation, new TouchSet());
		await tx.commit();

		// Another client deleted the row. The patch is still queued and still
		// unacked, so the rebase replays it onto a base that is now gone.
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ tombstone('t1', 2n) ],
			nextCursor: new Uint8Array([ 2 ]),
			lastAckedMutationId: 0n,
		});

		expect(warn).not.toHaveBeenCalled();
	});
});
