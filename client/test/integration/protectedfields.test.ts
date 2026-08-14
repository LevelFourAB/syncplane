// Fields a client may not write, across the wire. Two halves that behave
// differently from this side: the schema's own marks, which the client knows
// and refuses at the call site, and the ones testserver's policy adds for the
// subject "viewer", which it cannot know and learns as a rejection ack.
//
// The substitution a PUT gets is the part only a real server can show. The unit
// suite pins what this client puts in the mutation; only here is there a stored
// row on the other side to keep.

import { create, toBinary } from '@bufbuild/protobuf';
import { createClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { CodePermissionDenied } from '../../src/codes.js';
import { GuardedSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
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
import { authenticatedTransport, freshWorkspace, newClient } from './helpers.js';

/** Pushes one PATCH straight at the server, bypassing the client's own
 * refusal, so the server's verdict on it is observable. */
async function patchRaw(workspaceId: string, key: string, paths: string[], owner: string) {
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
						entity: 'guarded',
						key,
						op: CrudOp.PATCH,
						value: toBinary(GuardedSchema, create(GuardedSchema, { id: key, owner })),
						fieldMask: { paths },
					}),
				},
			}) ],
		} ],
	}));
	return resp.results[0]!.results[0]!;
}

/** Gives a protected field a value the way an app has to: through a mutator,
 * whose rows the verdict on its invocation covers. */
async function setOwner(client: ReturnType<typeof newClient>, id: string, owner: string) {
	await client.mutate.custom('set_owner', GuardedSchema, { id, owner });
	await client.syncNow();
}

describe('protected fields', () => {
	it('keeps the stored value through a put that tries to change it', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws, { entities: [ GuardedSchema ] });
		await client.start();
		await client.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });
		await setOwner(client, 'g1', 'alice');

		await client.mutate.put(GuardedSchema, { id: 'g1', label: 'edited', owner: 'mallory' });
		await client.syncNow();

		// The write landed for the field this caller may write, and the server's
		// own value survived for the one it may not.
		const row = await client.get(GuardedSchema, 'g1');
		expect(row?.label).toBe('edited');
		expect(row?.owner).toBe('alice');
		expect(await client.pendingCount()).toBe(0);

		// A second client reading it fresh sees the same, so this is the stored
		// row and not a local artefact of the overlay.
		const other = newClient(ws, { entities: [ GuardedSchema ] });
		await other.start();
		await other.syncNow();
		expect((await other.get(GuardedSchema, 'g1'))?.owner).toBe('alice');

		await client.close();
		await other.close();
	});

	it('refuses a patch of a marked field locally, and the server refuses it too', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws, { entities: [ GuardedSchema ] });
		await client.start();
		await client.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });
		await setOwner(client, 'g1', 'alice');

		await expect(client.mutate.patch(GuardedSchema, 'g1', { owner: 'mallory' }))
			.rejects.toThrow(/field "owner" may not be written by a client/);
		expect(await client.pendingCount()).toBe(0);

		const raw = await patchRaw(ws, 'g1', [ 'owner' ], 'mallory');
		expect(raw.status).toBe(MutationStatus.REJECTED);
		expect(raw.errorCode).toBe(CodePermissionDenied);

		await client.syncNow();
		expect((await client.get(GuardedSchema, 'g1'))?.owner).toBe('alice');

		await client.close();
	});

	describe('the half only the server knows', () => {
		it('enqueues and comes back rejected, because the client cannot classify it', async () => {
			const ws = freshWorkspace();
			const rejections: RejectionInfo[] = [];
			const client = newClient(ws, {
				entities: [ GuardedSchema ],
				transport: authenticatedTransport('viewer'),
				onMutationRejected: (info) => rejections.push(info),
			});
			await client.start();
			// Seeded as somebody the policy leaves alone: a put from `viewer`
			// would have had its own `label` carried over, and there would be
			// nothing for the patch below to fail to change.
			const editor = newClient(ws, {
				entities: [ GuardedSchema ],
				transport: authenticatedTransport('editor'),
			});
			await editor.start();
			await editor.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });
			await editor.syncNow();
			await client.syncNow();

			// `label` carries no mark, so nothing here refuses it — the policy
			// protects it for this subject alone.
			await client.mutate.patch(GuardedSchema, 'g1', { label: 'edited' });
			await client.syncNow();

			expect(rejections).toHaveLength(1);
			expect(rejections[0]?.errorCode).toBe(CodePermissionDenied);
			expect(await client.pendingCount()).toBe(0);
			expect((await client.get(GuardedSchema, 'g1'))?.label).toBe('made');

			await client.close();
			await editor.close();
		});

		it('takes the same write from a caller the policy does not protect it from', async () => {
			const ws = freshWorkspace();
			const client = newClient(ws, {
				entities: [ GuardedSchema ],
				transport: authenticatedTransport('editor'),
			});
			await client.start();
			await client.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });
			await client.syncNow();

			await client.mutate.patch(GuardedSchema, 'g1', { label: 'edited' });
			await client.syncNow();

			expect(await client.pendingCount()).toBe(0);
			expect((await client.get(GuardedSchema, 'g1'))?.label).toBe('edited');

			await client.close();
		});

		it('carries a per-caller field over on a put, the same as a marked one', async () => {
			const ws = freshWorkspace();
			const client = newClient(ws, {
				entities: [ GuardedSchema ],
				transport: authenticatedTransport('viewer'),
			});
			await client.start();
			// Written as somebody the policy leaves alone, so there is a stored
			// value for the put below to keep.
			const editor = newClient(ws, {
				entities: [ GuardedSchema ],
				transport: authenticatedTransport('editor'),
			});
			await editor.start();
			await editor.mutate.put(GuardedSchema, { id: 'g1', label: 'stored' });
			await editor.syncNow();

			await client.syncNow();
			await client.mutate.put(GuardedSchema, { id: 'g1', label: 'edited' });
			await client.syncNow();

			expect(await client.pendingCount()).toBe(0);
			expect((await client.get(GuardedSchema, 'g1'))?.label).toBe('stored');

			await client.close();
			await editor.close();
		});
	});
});
