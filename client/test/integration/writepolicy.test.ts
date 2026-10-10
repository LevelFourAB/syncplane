// The client refuses a write its schema forbids before enqueuing it, and the
// server refuses the same write authoritatively. Only a cross-language test can
// show the two agree on which writes those are: the unit vectors pin each side
// against the same table, this pins the table against the real server.
//
// It runs the access-model schema: "entry" is append-only (a rule admits
// read and create, none admits update or delete) and "label" is server-owned
// (a rule admits read, none admits a write).

import { create, toBinary } from '@bufbuild/protobuf';
import { createClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CodeAccessDenied } from '../../src/codes.js';
import { EntrySchema, LabelSchema } from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
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
import { MemoryKVStore } from '../../src/kv/memory.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { freshWorkspace } from './helpers.js';

// A schema that uses the access model admits no caller without grant rows,
// which every other suite's server would then need, so this suite runs its own.
let server: TestServer;
beforeAll(async () => {
	server = await startTestServer([ '-access' ]);
});
afterAll(() => server?.stop());

/** The caller every spec writes as. Its org_member role grants view_tasks and
 * view_labels at the workspace root, which admits it to the workspace and lets
 * it create entries. */
const USER = 'member';

const transport = () =>
	createConnectTransport({
		baseUrl: server.url,
		httpVersion: '1.1',
		interceptors: [ (next) => (req) => {
			req.header.set('Authorization', `Bearer ${USER}`);
			return next(req);
		} ],
	});

const open: SyncClient[] = [];
afterEach(async () => {
	await Promise.all(open.splice(0).map((c) => c.close()));
});

async function startedClient(workspaceId: string): Promise<SyncClient> {
	const client = new SyncClient({
		transport: transport(),
		workspaceId,
		kvStore: new MemoryKVStore(),
		entities: [ EntrySchema, LabelSchema ],
		pollIntervalMs: 0,
		streamEnabled: false,
	});
	open.push(client);
	await client.start();
	await client.syncNow();
	return client;
}

/** Writes rows as a trusted caller, through the testserver's debug endpoint. */
async function trusted(workspace: string, writes: object[]): Promise<void> {
	const res = await fetch(`${server.url}/debug/trusted`, {
		method: 'POST',
		body: JSON.stringify({ workspace, writes }),
	});
	if (!res.ok) throw new Error(`trusted write failed: ${await res.text()}`);
}

async function seededWorkspace(): Promise<string> {
	const ws = freshWorkspace();
	await trusted(ws, [
		{ entity: 'org_member', value: { id: 'om-member', userId: USER, role: 'org_member' } },
		{ entity: 'label', value: { id: 'l1', name: 'red' } },
	]);
	return ws;
}

/** Raw writes share one client id, so each needs a mutation id above the last
 * one's watermark. */
let rawMutationId = 0n;

/** Pushes one CRUD write straight at the server, bypassing the client's own
 * refusal, so the server's verdict on it is observable. */
async function pushRaw(
	workspaceId: string,
	write: {
		entity: string;
		key: string;
		op: CrudOp;
		value?: Uint8Array;
		mask?: string[];
		expectedVersion?: bigint;
	},
) {
	const rpc = createClient(SyncService, transport());
	const resp = await rpc.push(create(PushRequestSchema, {
		batches: [ {
			workspaceId,
			mutations: [ create(MutationSchema, {
				clientId: 'raw',
				mutationId: ++rawMutationId,
				payload: {
					case: 'crud',
					value: create(CrudMutationSchema, {
						entity: write.entity,
						key: write.key,
						op: write.op,
						value: write.value,
						fieldMask: write.mask === undefined ? undefined : { paths: write.mask },
						expectedVersion: write.expectedVersion,
					}),
				},
			}) ],
		} ],
	}));
	return resp.results[0]!.results[0]!;
}

const entry = (id: string, body: string) =>
	toBinary(EntrySchema, create(EntrySchema, { id, body }));
const label = (id: string, name: string) =>
	toBinary(LabelSchema, create(LabelSchema, { id, name }));

describe('entity access rules on writes', () => {
	it('takes the guarded create both sides classify as a create', async () => {
		const ws = await seededWorkspace();
		const client = await startedClient(ws);

		await client.mutate.put(EntrySchema, { id: 'e1', body: 'posted' }, { expectedVersion: 0n });
		await client.syncNow();

		// It reached the server intact: expectedVersion 0n has explicit presence
		// on the wire, so the server saw an asserted create rather than an
		// unguarded put.
		expect(await client.pendingCount()).toBe(0);
		// A row version is a workspace sequence, and the seed took the first.
		expect((await client.getRow(EntrySchema, 'e1'))?.version).toBeGreaterThan(0n);
	});

	it('refuses an edit of an append-only row locally, and the server refuses it too', async () => {
		const ws = await seededWorkspace();
		const client = await startedClient(ws);
		await client.mutate.put(EntrySchema, { id: 'e1', body: 'posted' }, { expectedVersion: 0n });
		await client.syncNow();

		await expect(client.mutate.patch(EntrySchema, 'e1', { body: 'edited' }))
			.rejects.toThrow(/no access rule admits update/);
		await expect(
			client.mutate.put(EntrySchema, { id: 'e1', body: 'edited' }, { expectedVersion: 1n }),
		).rejects.toThrow(/no access rule admits update/);
		await expect(client.mutate.del(EntrySchema, 'e1'))
			.rejects.toThrow(/no access rule admits delete/);
		expect(await client.pendingCount()).toBe(0);

		for (
			const write of [
				{ op: CrudOp.PATCH, value: entry('e1', 'edited'), mask: [ 'body' ] },
				{ op: CrudOp.PUT, value: entry('e1', 'edited'), expectedVersion: 1n },
				{ op: CrudOp.DELETE },
			]
		) {
			const raw = await pushRaw(ws, { entity: 'entry', key: 'e1', ...write });
			expect(raw.status, CrudOp[write.op]).toBe(MutationStatus.REJECTED);
			expect(raw.errorCode, CrudOp[write.op]).toBe(CodeAccessDenied);
		}

		// Nothing moved on either side.
		await client.syncNow();
		expect((await client.get(EntrySchema, 'e1'))?.body).toBe('posted');
	});

	it('leaves an unguarded put to the server, which decides on the stored row', async () => {
		const ws = await seededWorkspace();
		const client = await startedClient(ws);

		// The row is absent, so the server takes the write as a create.
		await client.mutate.put(EntrySchema, { id: 'e1', body: 'posted' });
		await client.syncNow();
		expect(await client.pendingCount()).toBe(0);
		// A row version is a workspace sequence, and the seed took the first.
		expect((await client.getRow(EntrySchema, 'e1'))?.version).toBeGreaterThan(0n);

		// The row is present now, so the same write is an update, which no rule
		// admits.
		const raw = await pushRaw(ws, {
			entity: 'entry',
			key: 'e1',
			op: CrudOp.PUT,
			value: entry('e1', 'replaced'),
		});
		expect(raw.status).toBe(MutationStatus.REJECTED);
		expect(raw.errorCode).toBe(CodeAccessDenied);

		await client.syncNow();
		expect((await client.get(EntrySchema, 'e1'))?.body).toBe('posted');
	});

	it('refuses every write to a server-owned entity locally, and the server refuses it too', async () => {
		const ws = await seededWorkspace();
		const client = await startedClient(ws);
		expect((await client.get(LabelSchema, 'l1'))?.name).toBe('red');

		await expect(client.mutate.put(LabelSchema, { id: 'l2', name: 'blue' }))
			.rejects.toThrow(/no access rule admits create or update/);
		await expect(client.mutate.put(LabelSchema, { id: 'l2', name: 'blue' }, { expectedVersion: 0n }))
			.rejects.toThrow(/no access rule admits create/);
		await expect(client.mutate.patch(LabelSchema, 'l1', { name: 'green' }))
			.rejects.toThrow(/no access rule admits update/);
		await expect(client.mutate.del(LabelSchema, 'l1'))
			.rejects.toThrow(/no access rule admits delete/);
		expect(await client.pendingCount()).toBe(0);

		for (
			const write of [
				{ key: 'l2', op: CrudOp.PUT, value: label('l2', 'blue') },
				{ key: 'l2', op: CrudOp.PUT, value: label('l2', 'blue'), expectedVersion: 0n },
				{ key: 'l1', op: CrudOp.PATCH, value: label('l1', 'green'), mask: [ 'name' ] },
				{ key: 'l1', op: CrudOp.DELETE },
			]
		) {
			const raw = await pushRaw(ws, { entity: 'label', ...write });
			expect(raw.status, `${CrudOp[write.op]} ${write.key}`).toBe(MutationStatus.REJECTED);
			expect(raw.errorCode, `${CrudOp[write.op]} ${write.key}`).toBe(CodeAccessDenied);
		}

		await client.syncNow();
		expect((await client.get(LabelSchema, 'l1'))?.name).toBe('red');
		expect(await client.get(LabelSchema, 'l2')).toBeUndefined();
	});
});
