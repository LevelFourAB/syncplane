import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	LabelSchema,
	NoteSchema,
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { freshWorkspace } from './helpers.js';

// The access model is decided by the engine and predicted by the client from
// the summary the engine sends, so only a real server shows the two agree.
// It runs its own testserver: a schema that uses the model admits no caller
// without grant rows, which every other suite's server would then need.
let server: TestServer;
beforeAll(async () => {
	server = await startTestServer([ '-access', '-heartbeat=250ms' ]);
});
afterAll(() => server?.stop());

const entities = [
	OrgMemberSchema,
	ProjectSchema,
	ProjectMemberSchema,
	TaskSchema,
	LabelSchema,
	NoteSchema,
];

const open: SyncClient[] = [];
afterEach(async () => {
	await Promise.all(open.splice(0).map((c) => c.close()));
});

function clientAs(
	user: string,
	workspaceId: string,
	kvStore: KVStore = new MemoryKVStore(),
): SyncClient {
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
		kvStore,
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

async function seed(ws: string): Promise<void> {
	await trusted(ws, [
		{ entity: 'org_member', value: { id: 'om-admin', userId: 'admin', role: 'org_admin' } },
		{ entity: 'project', value: { id: 'p1', name: 'one' } },
		{ entity: 'project', value: { id: 'p2', name: 'two' } },
		{
			entity: 'project_member',
			value: { id: 'pm-guest', project: { id: 'p1' }, userId: 'guest', role: 'project_reader' },
		},
		{
			entity: 'project_member',
			value: { id: 'pm-writer', project: { id: 'p2' }, userId: 'writer', role: 'project_writer' },
		},
		{ entity: 'task', value: { id: 't1', project: { id: 'p1' }, title: 'one' } },
		{ entity: 'task', value: { id: 't2', project: { id: 'p2' }, title: 'two' } },
	]);
}

async function synced(client: SyncClient): Promise<SyncClient> {
	await client.start();
	await client.syncNow();
	return client;
}

describe('access model on the client', () => {
	it('answers from the summary the server sends', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = clientAs('guest', ws);
		expect(await guest.can(TaskSchema, 't1', 'view_tasks')).toBe('unknown');
		await synced(guest);

		expect(await guest.can(TaskSchema, 't1', 'view_tasks')).toBe('yes');
		expect(await guest.can(TaskSchema, 't1', 'edit_tasks')).toBe('no');
		expect(await guest.can(ProjectSchema, 'p1', 'view_projects')).toBe('yes');
		expect(await guest.can(ProjectSchema, 'p2', 'view_projects')).toBe('no');
		// The guest does not hold t2, so its access parent is unknown here.
		expect(await guest.can(TaskSchema, 't2', 'view_tasks')).toBe('unknown');

		const writer = await synced(clientAs('writer', ws));
		expect(await writer.can(TaskSchema, 't2', 'edit_tasks')).toBe('yes');
		const admin = await synced(clientAs('admin', ws));
		expect(await admin.can(TaskSchema, 'not-here', 'edit_tasks')).toBe('yes');
	});

	it("follows a change of the caller's grants", async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = await synced(clientAs('guest', ws));
		const changed: string[] = [];
		guest.onAccessChange((w) => changed.push(w));
		expect(await guest.can(TaskSchema, 't1', 'edit_tasks')).toBe('no');

		await trusted(ws, [
			{
				entity: 'project_member',
				value: { id: 'pm-guest2', project: { id: 'p1' }, userId: 'guest', role: 'project_writer' },
			},
		]);
		await guest.syncNow();
		expect(await guest.can(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		expect(changed).toContain(ws);
	});

	it('sends the rows a new grant reveals, and drops the ones a lost grant hides', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = await synced(clientAs('guest', ws));
		expect(await guest.get(TaskSchema, 't2')).toBeUndefined();

		await trusted(ws, [
			{
				entity: 'project_member',
				value: { id: 'pm-guest2', project: { id: 'p2' }, userId: 'guest', role: 'project_reader' },
			},
		]);
		await guest.syncNow();
		expect((await guest.get(TaskSchema, 't2'))?.title).toBe('two');

		await trusted(ws, [ { entity: 'project_member', key: 'pm-guest2', delete: true } ]);
		await guest.syncNow();
		expect(await guest.get(TaskSchema, 't2')).toBeUndefined();
		expect((await guest.get(TaskSchema, 't1'))?.title).toBe('one');
	});

	it('keeps the summary with the store', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const kv = new MemoryKVStore();
		const first = await synced(clientAs('writer', ws, kv));
		expect(await first.can(TaskSchema, 't2', 'edit_tasks')).toBe('yes');
		await first.close();

		const second = clientAs('writer', ws, kv);
		expect(await second.can(TaskSchema, 't2', 'edit_tasks')).toBe('yes');
	});

	it('answers unknown while a local grant write waits for its ack', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const admin = await synced(clientAs('admin', ws));
		expect(await admin.can(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		await admin.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(await admin.can(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await admin.syncNow();
		expect(await admin.can(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
	});
});
