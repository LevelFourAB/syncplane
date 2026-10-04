import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RejectionInfo } from '../../src/client.js';
import {
	CommentSchema,
	DraftSchema,
	IssueSchema,
	LabelSchema,
	NoteSchema,
	OrgMemberSchema,
	PostSchema,
	ProjectMemberSchema,
	ProjectSchema,
	RepoMemberSchema,
	RepoSchema,
	RepoSquadSchema,
	RoomMemberSchema,
	RoomSchema,
	SettingSchema,
	SquadMemberSchema,
	SquadSchema,
	TaskSchema,
	TicketSchema,
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
	CommentSchema,
	LabelSchema,
	NoteSchema,
	RoomSchema,
	RoomMemberSchema,
	PostSchema,
	DraftSchema,
	TicketSchema,
	SettingSchema,
	SquadSchema,
	SquadMemberSchema,
	RepoSchema,
	RepoMemberSchema,
	RepoSquadSchema,
	IssueSchema,
];

const open: SyncClient[] = [];
afterEach(async () => {
	await Promise.all(open.splice(0).map((c) => c.close()));
});

function clientAs(
	user: string,
	workspaceId: string,
	kvStore: KVStore = new MemoryKVStore(),
	onMutationRejected?: (info: RejectionInfo) => void,
): SyncClient {
	const client = new SyncClient({
		onMutationRejected,
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
		expect(await guest.hasPermission(TaskSchema, 't1', 'view_tasks')).toBe('unknown');
		await synced(guest);

		expect(await guest.hasPermission(TaskSchema, 't1', 'view_tasks')).toBe('yes');
		expect(await guest.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('no');
		expect(await guest.hasPermission(ProjectSchema, 'p1', 'view_projects')).toBe('yes');
		expect(await guest.hasPermission(ProjectSchema, 'p2', 'view_projects')).toBe('no');
		// The guest does not hold t2, so its access parent is unknown here.
		expect(await guest.hasPermission(TaskSchema, 't2', 'view_tasks')).toBe('unknown');

		const writer = await synced(clientAs('writer', ws));
		expect(await writer.hasPermission(TaskSchema, 't2', 'edit_tasks')).toBe('yes');
		const admin = await synced(clientAs('admin', ws));
		expect(await admin.hasPermission(TaskSchema, 'not-here', 'edit_tasks')).toBe('yes');
	});

	it("follows a change of the caller's grants", async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const guest = await synced(clientAs('guest', ws));
		const changed: string[] = [];
		guest.onAccessChange((w) => changed.push(w));
		expect(await guest.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('no');

		await trusted(ws, [
			{
				entity: 'project_member',
				value: { id: 'pm-guest2', project: { id: 'p1' }, userId: 'guest', role: 'project_writer' },
			},
		]);
		await guest.syncNow();
		expect(await guest.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
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
		expect(await first.hasPermission(TaskSchema, 't2', 'edit_tasks')).toBe('yes');
		await first.close();

		const second = clientAs('writer', ws, kv);
		expect(await second.hasPermission(TaskSchema, 't2', 'edit_tasks')).toBe('yes');
	});

	it('answers unknown while a local grant write waits for its ack', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const admin = await synced(clientAs('admin', ws));
		expect(await admin.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		await admin.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(await admin.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await admin.syncNow();
		expect(await admin.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
	});

	it('moves the comments of a task that moves to another project', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		await trusted(ws, [ {
			entity: 'comment',
			value: { id: 'c1', task: { id: 't1' }, body: 'on one' },
		} ]);
		const guest = await synced(clientAs('guest', ws));
		const writer = await synced(clientAs('writer', ws));
		expect((await guest.get(CommentSchema, 'c1'))?.body).toBe('on one');
		expect(await guest.hasPermission(CommentSchema, 'c1', 'view_tasks')).toBe('yes');
		expect(await writer.get(CommentSchema, 'c1')).toBeUndefined();

		await trusted(ws, [ {
			entity: 'task',
			value: { id: 't1', project: { id: 'p2' }, title: 'one' },
		} ]);
		await guest.syncNow();
		await writer.syncNow();
		expect(await guest.get(CommentSchema, 'c1')).toBeUndefined();
		expect((await writer.get(CommentSchema, 'c1'))?.body).toBe('on one');
		expect(await writer.hasPermission(CommentSchema, 'c1', 'edit_tasks')).toBe('yes');
	});
});

describe('owner rules on the client', () => {
	async function seedRoom(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'room', value: { id: 'r1' } },
			{
				entity: 'room_member',
				value: { id: 'rm-ann', room: { id: 'r1' }, userId: 'ann', role: 'room_poster' },
			},
			{
				entity: 'room_member',
				value: { id: 'rm-bob', room: { id: 'r1' }, userId: 'bob', role: 'room_poster' },
			},
			{ entity: 'post', value: { id: 'po-bob', room: { id: 'r1' }, authorId: 'bob', body: 'yo' } },
			{ entity: 'draft', value: { id: 'd-bob', room: { id: 'r1' }, authorId: 'bob' } },
		]);
	}

	it('stores the caller as the creator of a row it creates', async () => {
		const ws = freshWorkspace();
		await seedRoom(ws);
		const ann = await synced(clientAs('ann', ws));
		await ann.mutate.put(PostSchema, {
			id: 'po-ann',
			room: { id: 'r1' },
			authorId: 'bob',
			body: 'hi',
		});
		expect((await ann.get(PostSchema, 'po-ann'))?.authorId).toBe('ann');
		await ann.syncNow();

		const bob = await synced(clientAs('bob', ws));
		expect((await bob.get(PostSchema, 'po-ann'))?.authorId).toBe('ann');
	});

	it('predicts what the server admits and refuses', async () => {
		const ws = freshWorkspace();
		await seedRoom(ws);
		const rejections: RejectionInfo[] = [];
		const ann = await synced(
			clientAs('ann', ws, new MemoryKVStore(), (info) => rejections.push(info)),
		);
		await ann.mutate.put(PostSchema, { id: 'po-ann', room: { id: 'r1' }, body: 'hi' });
		await ann.syncNow();

		expect(await ann.can(PostSchema, 'po-ann', 'update', { fields: [ 'body' ] })).toBe('yes');
		await ann.mutate.patch(PostSchema, 'po-ann', { body: 'edited' });
		expect(await ann.can(PostSchema, 'po-ann', 'update', { fields: [ 'title' ] })).toBe('no');
		await ann.mutate.patch(PostSchema, 'po-ann', { title: 'renamed' });
		expect(await ann.can(PostSchema, 'po-bob', 'delete')).toBe('no');
		await ann.mutate.del(PostSchema, 'po-bob');
		expect(await ann.canCreate(SettingSchema, { userId: 'bob' })).toBe('no');
		await ann.mutate.put(SettingSchema, { userId: 'bob', theme: 'pink' });
		expect(await ann.canCreate(SettingSchema, { userId: 'ann' })).toBe('yes');
		await ann.mutate.put(SettingSchema, { userId: 'ann', theme: 'pink' });
		await ann.syncNow();

		expect(rejections.map((r) => r.errorCode)).toEqual([
			'permission_denied',
			'permission_denied',
			'permission_denied',
		]);
		const post = await ann.get(PostSchema, 'po-ann');
		expect(post?.body).toBe('edited');
		expect(post?.title).toBe('');
		expect((await ann.get(PostSchema, 'po-bob'))?.body).toBe('yo');
		expect((await ann.get(SettingSchema, 'ann'))?.theme).toBe('pink');
		expect(await ann.get(SettingSchema, 'bob')).toBeUndefined();
	});

	it('syncs only the rows a subject check lets the caller read', async () => {
		const ws = freshWorkspace();
		await seedRoom(ws);
		const ann = await synced(clientAs('ann', ws));
		const bob = await synced(clientAs('bob', ws));
		expect(await ann.get(DraftSchema, 'd-bob')).toBeUndefined();
		expect(await bob.get(DraftSchema, 'd-bob')).toBeDefined();
		expect(await bob.can(DraftSchema, 'd-bob', 'read')).toBe('yes');
	});
});

describe('teams on the client', () => {
	// Dave is in two squads, so leaving design keeps him in the workspace.
	async function seedSquads(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'squad', value: { id: 'design' } },
			{ entity: 'squad', value: { id: 'ops' } },
			{
				entity: 'squad_member',
				value: { id: 'sm-dave', squad: { id: 'design' }, userId: 'dave', role: 'squad_member' },
			},
			{
				entity: 'squad_member',
				value: { id: 'sm-dave-ops', squad: { id: 'ops' }, userId: 'dave', role: 'squad_member' },
			},
			{ entity: 'repo', value: { id: 'r1' } },
			{
				entity: 'repo_member',
				value: { id: 'rm-design', repo: { id: 'r1' }, squad: { id: 'design' }, role: 'repo_writer' },
			},
			{
				entity: 'repo_member',
				value: { id: 'rm-rita', repo: { id: 'r1' }, userId: 'rita', role: 'repo_reader' },
			},
			{ entity: 'issue', value: { id: 'i1', repo: { id: 'r1' }, title: 'one' } },
		]);
	}

	it("predicts what a squad's grant lets a member do, until the member leaves", async () => {
		const ws = freshWorkspace();
		await seedSquads(ws);
		const dave = await synced(clientAs('dave', ws));
		expect(await dave.get(IssueSchema, 'i1')).toBeDefined();
		expect(await dave.can(IssueSchema, 'i1', 'update')).toBe('yes');
		expect(await dave.can(RepoMemberSchema, 'rm-design', 'delete')).toBe('no');

		await trusted(ws, [ { entity: 'squad_member', key: 'sm-dave', delete: true } ]);
		await dave.syncNow();
		expect(await dave.get(IssueSchema, 'i1')).toBeUndefined();
		expect(await dave.hasPermission(RepoSchema, 'r1', 'edit_issues')).toBe('no');
	});

	it('lets a user leave through a subject check on a member of a oneof', async () => {
		const ws = freshWorkspace();
		await seedSquads(ws);
		const rita = await synced(clientAs('rita', ws));
		expect(await rita.can(RepoMemberSchema, 'rm-rita', 'delete')).toBe('yes');
		expect(await rita.can(RepoMemberSchema, 'rm-design', 'delete')).toBe('no');

		await rita.mutate.del(RepoMemberSchema, 'rm-rita');
		await rita.syncNow();
		const dave = await synced(clientAs('dave', ws));
		expect(await dave.get(RepoMemberSchema, 'rm-rita')).toBeUndefined();
		expect(await dave.get(RepoMemberSchema, 'rm-design')).toBeDefined();
	});
});
