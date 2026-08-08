// The client refuses a write its schema forbids before enqueuing it, and the
// server refuses the same write authoritatively. Only a cross-language test can
// show the two agree on which writes those are: the unit vectors pin each side
// against the same table, this pins the table against the real server.

import { create, toBinary } from '@bufbuild/protobuf';
import { createClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import { CodePermissionDenied } from '../../src/codes.js';
import { CommentSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	CrudMutationSchema,
	CrudOp,
	MutationSchema,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	MutationStatus,
	PushRequestSchema,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

/** Pushes one CRUD write straight at the server, bypassing the client's own
 * refusal, so the server's verdict on it is observable. */
async function pushRaw(
	workspaceId: string,
	op: CrudOp,
	key: string,
	body: string,
	expectedVersion?: bigint,
) {
	const rpc = createClient(
		SyncService,
		createConnectTransport({ baseUrl: inject('serverUrl'), httpVersion: '1.1' }),
	);
	const resp = await rpc.push(create(PushRequestSchema, {
		batches: [ {
			workspaceId,
			mutations: [ create(MutationSchema, {
				clientId: 'raw',
				mutationId: 1n,
				payload: {
					case: 'crud',
					value: create(CrudMutationSchema, {
						entity: 'comment',
						key,
						op,
						value: toBinary(CommentSchema, create(CommentSchema, { id: key, body })),
						// A PATCH is otherwise well-formed, so a rejection can only be
						// the policy talking — applyCrud's own mask check never runs.
						fieldMask: op === CrudOp.PATCH ? { paths: [ 'body' ] } : undefined,
						expectedVersion,
					}),
				},
			}) ],
		} ],
	}));
	return resp.results[0]!.results[0]!;
}

describe('entity write policies', () => {
	it('refuses an unguarded put locally, and the server refuses it too', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws, { entities: [ CommentSchema ] });
		await client.start();

		await expect(client.mutate.put(CommentSchema, { id: 'c1', body: 'posted' }))
			.rejects.toThrow(/pass expectedVersion 0n to assert a create/);
		expect(await client.pendingCount()).toBe(0);

		const raw = await pushRaw(ws, CrudOp.PUT, 'c1', 'posted');
		expect(raw.status).toBe(MutationStatus.REJECTED);
		expect(raw.errorCode).toBe(CodePermissionDenied);

		await client.close();
	});

	it('takes the guarded create both sides classify as a create', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws, { entities: [ CommentSchema ] });
		await client.start();

		await client.mutate.put(CommentSchema, { id: 'c1', body: 'posted' }, { expectedVersion: 0n });
		await client.syncNow();

		// It reached the server intact: expectedVersion 0n has explicit presence
		// on the wire, so the server saw an asserted create rather than an
		// unguarded put.
		expect(await client.pendingCount()).toBe(0);
		expect((await client.getRow(CommentSchema, 'c1'))?.version).toBe(1n);

		await client.close();
	});

	it('refuses an edit locally, and the server refuses it too', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws, { entities: [ CommentSchema ] });
		await client.start();
		await client.mutate.put(CommentSchema, { id: 'c1', body: 'posted' }, { expectedVersion: 0n });
		await client.syncNow();

		await expect(client.mutate.patch(CommentSchema, 'c1', { body: 'edited' }))
			.rejects.toThrow(/updatePolicy is trustedOnly/);
		await expect(client.mutate.del(CommentSchema, 'c1'))
			.rejects.toThrow(/deletePolicy is trustedOnly/);

		const raw = await pushRaw(ws, CrudOp.PATCH, 'c1', 'edited');
		expect(raw.status).toBe(MutationStatus.REJECTED);
		expect(raw.errorCode).toBe(CodePermissionDenied);

		// Nothing moved on either side.
		await client.syncNow();
		expect((await client.get(CommentSchema, 'c1'))?.body).toBe('posted');

		await client.close();
	});
});
