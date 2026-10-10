// Field guards across the wire: the server decides which fields of a row a
// caller reads, clears the rest and names them in RowChange.hidden, and the
// client turns that into Row.hidden. The fixtures are "staff", whose guards
// restrict who reads fields, and "expense", whose guards restrict who writes
// them, in the access test proto.
//
// Only a real server shows the projection and the PUT that keeps a field the
// writer never received; the unit suite pins the client side against the test
// kit.

import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	ExpenseSchema,
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	StaffSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { freshWorkspace } from './helpers.js';

let server: TestServer;
beforeAll(async () => {
	server = await startTestServer([ '-access' ]);
});
afterAll(() => server?.stop());

const entities = [
	OrgMemberSchema,
	ProjectSchema,
	ProjectMemberSchema,
	StaffSchema,
	ExpenseSchema,
];

const open: SyncClient[] = [];
afterEach(async () => {
	await Promise.all(open.splice(0).map((c) => c.close()));
});

function clientAs(user: string, workspaceId: string): SyncClient {
	const client = new SyncClient({
		transport: createConnectTransport({
			baseUrl: server.url,
			httpVersion: '1.1',
			interceptors: [ (next) => (req) => {
				req.header.set('Authorization', `Bearer ${user}`);
				return next(req);
			} ],
		}),
		workspaceId,
		kvStore: new MemoryKVStore(),
		entities,
		pollIntervalMs: 0,
		streamEnabled: false,
	});
	open.push(client);
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

/** p1 with guest reading it, owner owning it and admin over every project. */
async function seed(ws: string): Promise<void> {
	await trusted(ws, [
		{ entity: 'org_member', value: { id: 'om-admin', userId: 'admin', role: 'org_admin' } },
		{ entity: 'project', value: { id: 'p1', name: 'one' } },
		{
			entity: 'project_member',
			value: { id: 'pm-guest', project: { id: 'p1' }, userId: 'guest', role: 'project_reader' },
		},
		{
			entity: 'project_member',
			value: { id: 'pm-owner', project: { id: 'p1' }, userId: 'owner', role: 'project_owner' },
		},
		{
			entity: 'staff',
			value: {
				id: 's1',
				project: { id: 'p1' },
				name: 'ann',
				salary: '5000',
				review: 'good',
				personId: 'guest',
				bonus: '7',
				pin: '1234',
			},
		},
	]);
}

async function synced(client: SyncClient): Promise<SyncClient> {
	await client.start();
	await client.syncNow();
	return client;
}

describe('field read rules on the wire', () => {
	it('sends a row without the fields the caller does not read, and names them', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = await synced(clientAs('guest', ws));

		const row = await guest.getRow(StaffSchema, 's1');
		expect(row?.hidden).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
		expect(row?.value.salary).toBe(0n);
		expect(row?.value.review).toBe('good');

		const owner = await synced(clientAs('owner', ws));
		const owned = await owner.getRow(StaffSchema, 's1');
		expect(owned?.hidden).toEqual([ 'bonus', 'pin', 'review' ]);
		expect(owned?.value.salary).toBe(5000n);
	});

	it('predicts per field what the server sends', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = await synced(clientAs('guest', ws));
		const owner = await synced(clientAs('owner', ws));

		expect(await guest.can(StaffSchema, 's1', 'read', { fields: [ 'salary' ] })).toBe('no');
		expect(await guest.can(StaffSchema, 's1', 'read', { fields: [ 'review' ] })).toBe('yes');
		expect(await owner.can(StaffSchema, 's1', 'read', { fields: [ 'salary' ] })).toBe('yes');
		expect(await owner.can(StaffSchema, 's1', 'read', { fields: [ 'review' ] })).toBe('no');
	});

	it('sends a row again when a grant shows a field', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = await synced(clientAs('guest', ws));
		expect((await guest.getRow(StaffSchema, 's1'))?.hidden).toContain('salary');

		await trusted(ws, [ {
			entity: 'project_member',
			value: { id: 'pm-guest', project: { id: 'p1' }, userId: 'guest', role: 'project_owner' },
		} ]);
		await guest.syncNow();

		const row = await guest.getRow(StaffSchema, 's1');
		expect(row?.hidden).toEqual([ 'bonus', 'pin' ]);
		expect(row?.value.salary).toBe(5000n);
	});

	it('keeps on a put the stored value of each field the writer did not read', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		await trusted(ws, [ {
			entity: 'project_member',
			value: { id: 'pm-padmin', project: { id: 'p1' }, userId: 'padmin', role: 'project_admin' },
		} ]);
		const padmin = await synced(clientAs('padmin', ws));
		const row = await padmin.getRow(StaffSchema, 's1');
		await padmin.mutate.put(StaffSchema, { ...row!.value, name: 'anna' });
		await padmin.syncNow();

		const admin = await synced(clientAs('admin', ws));
		const stored = await admin.getRow(StaffSchema, 's1');
		expect(stored?.value.name).toBe('anna');
		expect(stored?.value.salary).toBe(5000n);
	});

	it('refuses a server query that names a field the caller does not read on every row', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = await synced(clientAs('guest', ws));

		await expect(guest.queryServer(StaffSchema, { where: (s) => s.salary.gt(0n) })).rejects.toThrow(
			/the caller does not read on every row/,
		);
		const rows = await guest.queryServer(StaffSchema, { where: (s) => s.name.eq('ann') });
		expect(rows.map((r) => r.hidden)).toEqual([ [ 'bio', 'bonus', 'pin', 'salary' ] ]);

		const admin = await synced(clientAs('admin', ws));
		expect(
			(await admin.queryServer(StaffSchema, { where: (s) => s.salary.gt(0n) })).map((r) => r.key),
		)
			.toEqual([ 's1' ]);
	});
});

describe('field write rules on the wire', () => {
	it('keeps on a put the stored value of a field the writer may not write', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		await trusted(ws, [
			{
				entity: 'project_member',
				value: { id: 'pm-padmin', project: { id: 'p1' }, userId: 'padmin', role: 'project_admin' },
			},
			{
				entity: 'expense',
				value: { id: 'e1', project: { id: 'p1' }, title: 'lunch', status: 'open', total: '100' },
			},
		]);
		const padmin = await synced(clientAs('padmin', ws));
		const row = await padmin.getRow(ExpenseSchema, 'e1');
		await padmin.mutate.put(ExpenseSchema, {
			...row!.value,
			title: 'dinner',
			total: 1n,
			status: 'paid',
		});
		await padmin.syncNow();

		const stored = await padmin.getRow(ExpenseSchema, 'e1');
		expect(stored?.value.title).toBe('dinner');
		expect(stored?.value.total).toBe(100n);
		expect(stored?.value.status).toBe('open');
		await expect(padmin.mutate.patch(ExpenseSchema, 'e1', { total: 2n })).rejects.toThrow(
			'field "total" may not be written by a client',
		);
	});
});
