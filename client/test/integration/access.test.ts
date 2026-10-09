import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RejectionInfo } from '../../src/client.js';
import {
	AccountMemberSchema,
	AccountSchema,
	BillerSchema,
	ChargeSchema,
	CommentSchema,
	DraftSchema,
	InvoiceSchema,
	IssueSchema,
	LabelSchema,
	NoteSchema,
	OrgMemberSchema,
	PageSchema,
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
	ThreadSchema,
	TicketSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { decodeTextDoc, encodeTextDoc, plainTextOf, TextDoc } from '../../src/text/index.js';
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
	PageSchema,
	LabelSchema,
	NoteSchema,
	RoomSchema,
	RoomMemberSchema,
	PostSchema,
	DraftSchema,
	TicketSchema,
	SettingSchema,
	ThreadSchema,
	SquadSchema,
	SquadMemberSchema,
	RepoSchema,
	RepoMemberSchema,
	RepoSquadSchema,
	IssueSchema,
	AccountSchema,
	AccountMemberSchema,
	BillerSchema,
	InvoiceSchema,
	ChargeSchema,
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

	it('answers for rows that hold different permissions, and at the root besides', async () => {
		const ws = freshWorkspace();
		// Mia holds repo_reader at r1, repo_admin at r2, and at r3 repo_reader
		// herself and repo_writer through a squad. org_member holds some
		// project permissions at the root, and project_writer adds the rest at
		// p2.
		await trusted(ws, [
			{ entity: 'org_member', value: { id: 'om-mia', userId: 'mia', role: 'org_member' } },
			{ entity: 'project', value: { id: 'p1', name: 'one' } },
			{ entity: 'project', value: { id: 'p2', name: 'two' } },
			{
				entity: 'project_member',
				value: { id: 'pm-mia', project: { id: 'p2' }, userId: 'mia', role: 'project_writer' },
			},
			{ entity: 'task', value: { id: 't1', project: { id: 'p1' }, title: 'one' } },
			{ entity: 'task', value: { id: 't2', project: { id: 'p2' }, title: 'two' } },
			{ entity: 'squad', value: { id: 'design' } },
			{
				entity: 'squad_member',
				value: { id: 'sm-mia', squad: { id: 'design' }, userId: 'mia', role: 'squad_member' },
			},
			...[ 'r1', 'r2', 'r3' ].flatMap((r, i) => [
				{ entity: 'repo', value: { id: r } },
				{ entity: 'issue', value: { id: `i${i + 1}`, repo: { id: r }, title: r } },
			]),
			{
				entity: 'repo_member',
				value: { id: 'rm-mia1', repo: { id: 'r1' }, userId: 'mia', role: 'repo_reader' },
			},
			{
				entity: 'repo_member',
				value: { id: 'rm-mia2', repo: { id: 'r2' }, userId: 'mia', role: 'repo_admin' },
			},
			{
				entity: 'repo_member',
				value: { id: 'rm-mia3', repo: { id: 'r3' }, userId: 'mia', role: 'repo_reader' },
			},
			{
				entity: 'repo_member',
				value: { id: 'rm-design', repo: { id: 'r3' }, squad: { id: 'design' }, role: 'repo_writer' },
			},
		]);
		const mia = await synced(clientAs('mia', ws));

		const repoPermissions = [
			'view_repos',
			'edit_repos',
			'view_repo_members',
			'manage_repo_members',
			'view_issues',
			'edit_issues',
		];
		const heldAt: Record<string, string[]> = {
			r1: [ 'view_repos', 'view_repo_members', 'view_issues' ],
			r2: repoPermissions,
			r3: [ 'view_repos', 'view_repo_members', 'view_issues', 'edit_issues' ],
		};
		for (const [ i, repo ] of [ 'r1', 'r2', 'r3' ].entries()) {
			for (const permission of repoPermissions) {
				const want = heldAt[repo]!.includes(permission) ? 'yes' : 'no';
				expect(await mia.hasPermission(RepoSchema, repo, permission), `${permission} at ${repo}`)
					.toBe(want);
				expect(
					await mia.hasPermission(IssueSchema, `i${i + 1}`, permission),
					`${permission} below ${repo}`,
				).toBe(want);
			}
		}

		expect(await mia.hasPermission(ProjectSchema, 'p1', 'view_projects')).toBe('yes');
		expect(await mia.hasPermission(TaskSchema, 't1', 'view_tasks')).toBe('yes');
		expect(await mia.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('no');
		expect(await mia.hasPermission(ProjectSchema, 'p1', 'view_members')).toBe('no');
		expect(await mia.hasPermission(TaskSchema, 't2', 'view_tasks')).toBe('yes');
		expect(await mia.hasPermission(TaskSchema, 't2', 'edit_tasks')).toBe('yes');
		expect(await mia.hasPermission(ProjectSchema, 'p2', 'view_members')).toBe('yes');
		expect(await mia.hasPermission(ProjectSchema, 'p2', 'manage_members')).toBe('no');
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

describe('onAccessChange', () => {
	it('calls back for a queued grant write and again for its ack', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const admin = await synced(clientAs('admin', ws));
		const changed: string[] = [];
		admin.onAccessChange((w) => changed.push(w));

		await admin.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(changed).toEqual([ ws ]);

		await admin.syncNow();
		expect(changed.length).toBeGreaterThan(1);
		expect(new Set(changed)).toEqual(new Set([ ws ]));
	});

	it('calls back when a row that other rows sit below changes, by pull and by a local write', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const admin = await synced(clientAs('admin', ws));
		const changed: string[] = [];
		admin.onAccessChange((w) => changed.push(w));

		await trusted(ws, [ {
			entity: 'task',
			value: { id: 't1', project: { id: 'p2' }, title: 'one' },
		} ]);
		await admin.syncNow();
		expect(changed).toEqual([ ws ]);

		await admin.mutate.patch(TaskSchema, 't1', { project: { id: 'p1' } });
		expect(changed).toEqual([ ws, ws ]);
	});

	it('does not call back for a write to a row that no other row sits below', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		await trusted(ws, [ {
			entity: 'comment',
			value: { id: 'c1', task: { id: 't1' }, body: 'one' },
		} ]);
		const admin = await synced(clientAs('admin', ws));
		const changed: string[] = [];
		admin.onAccessChange((w) => changed.push(w));

		await admin.mutate.patch(CommentSchema, 'c1', { body: 'edited' });
		await admin.syncNow();
		expect((await admin.get(CommentSchema, 'c1'))?.body).toBe('edited');
		expect(admin.status().pendingMutations).toBe(0);
		expect(changed).toEqual([]);
	});

	it("does not call back for another caller's grant row that a pull brings", async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const admin = await synced(clientAs('admin', ws));
		const changed: string[] = [];
		admin.onAccessChange((w) => changed.push(w));

		await trusted(ws, [ {
			entity: 'project_member',
			value: { id: 'pm-other', project: { id: 'p1' }, userId: 'someone', role: 'project_reader' },
		} ]);
		await admin.syncNow();
		expect((await admin.get(ProjectMemberSchema, 'pm-other'))?.userId).toBe('someone');
		expect(changed).toEqual([]);
	});
});

// A project member reads pages through the access model's filter, and still
// catches up on text edits as fragments. The byte claim is asserted against
// the testserver's counters, as in text-delta-down.test.ts.
describe('text delta state below the workspace root', () => {
	interface RPCStats {
		total: Record<string, number>;
		pullBytesOut: Record<string, number>;
	}

	async function rpcStats(): Promise<RPCStats> {
		const res = await fetch(`${server.url}/debug/rpc`);
		if (!res.ok) throw new Error(`rpc stats failed: ${res.status}`);
		return (await res.json()) as RPCStats;
	}

	/** A large document of pseudo-random letters, which response compression
	 * cannot shrink to nothing the way it shrinks repeated text. */
	function bigDoc(peer: string): Uint8Array {
		let seed = 1;
		let s = '';
		for (let i = 0; i < 8000; i++) {
			seed = (seed * 1103515245 + 12345) % 2147483648;
			s += String.fromCharCode(97 + ((seed >>> 16) % 26));
		}
		const d = new TextDoc(peer);
		d.insert(0, s);
		return encodeTextDoc(d);
	}

	it('catches a project member up on a fraction of the page', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const admin = await synced(clientAs('admin', ws));
		const base = bigDoc('seed');
		await admin.mutate.put(PageSchema, { id: 'pg1', task: { id: 't1' }, body: base });
		await admin.syncNow();
		const guest = await synced(clientAs('guest', ws));
		expect((await guest.get(PageSchema, 'pg1'))?.body?.length).toBeGreaterThan(base.length / 2);

		const before = (await rpcStats()).pullBytesOut[ws] ?? 0;
		const d = decodeTextDoc((await admin.get(PageSchema, 'pg1'))?.body, 'alice');
		d.insert(0, 'HELLO ');
		await admin.mutate.patch(PageSchema, 'pg1', { body: encodeTextDoc(d) });
		await admin.syncNow();
		await guest.syncNow();
		const caughtUp = ((await rpcStats()).pullBytesOut[ws] ?? 0) - before;

		expect(caughtUp).toBeGreaterThan(0);
		expect(caughtUp).toBeLessThan(base.length * 0.2);
		expect(plainTextOf((await guest.get(PageSchema, 'pg1'))?.body)).toBe(
			plainTextOf((await admin.get(PageSchema, 'pg1'))?.body),
		);
		expect(plainTextOf((await guest.get(PageSchema, 'pg1'))?.body)).toMatch(/^HELLO /);
	});

	it('hands a project member a page that moves into its project without a bootstrap', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const admin = await synced(clientAs('admin', ws));
		await admin.mutate.put(PageSchema, { id: 'pg2', task: { id: 't2' }, body: bigDoc('seed') });
		await admin.syncNow();
		const guest = await synced(clientAs('guest', ws));
		expect(await guest.get(PageSchema, 'pg2')).toBeUndefined();

		await admin.mutate.patch(PageSchema, 'pg2', { task: { id: 't1' } });
		await admin.syncNow();
		const bootstraps = (await rpcStats()).total['Bootstrap'] ?? 0;
		await guest.syncNow();

		expect(plainTextOf((await guest.get(PageSchema, 'pg2'))?.body)).toBe(
			plainTextOf((await admin.get(PageSchema, 'pg2'))?.body),
		);
		expect((await rpcStats()).total['Bootstrap'] ?? 0).toBe(bootstraps);
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

describe('when checks on the client', () => {
	async function seedThreads(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'room', value: { id: 'r1' } },
			{
				entity: 'room_member',
				value: { id: 'rm-ann', room: { id: 'r1' }, userId: 'ann', role: 'room_poster' },
			},
			{ entity: 'thread', value: { id: 'th-open', room: { id: 'r1' }, title: 'open' } },
			{ entity: 'thread', value: { id: 'th-archived', room: { id: 'r1' }, archived: true } },
			{ entity: 'thread', value: { id: 'th-hidden', room: { id: 'r1' }, hidden: true } },
		]);
	}

	it('predicts what the server admits and refuses', async () => {
		const ws = freshWorkspace();
		await seedThreads(ws);
		const rejections: RejectionInfo[] = [];
		const ann = await synced(
			clientAs('ann', ws, new MemoryKVStore(), (info) => rejections.push(info)),
		);

		expect(await ann.can(ThreadSchema, 'th-open', 'update', { fields: [ 'title' ] })).toBe('yes');
		await ann.mutate.patch(ThreadSchema, 'th-open', { title: 'renamed' });
		expect(await ann.can(ThreadSchema, 'th-archived', 'update', { fields: [ 'title' ] })).toBe('no');
		await ann.mutate.patch(ThreadSchema, 'th-archived', { title: 'renamed' });
		expect(await ann.canCreate(ThreadSchema, { id: 'th-new', room: { id: 'r1' }, archived: true }))
			.toBe('no');
		await ann.mutate.put(ThreadSchema, { id: 'th-new', room: { id: 'r1' }, archived: true });
		await ann.syncNow();

		expect(rejections.map((r) => r.errorCode)).toEqual([ 'permission_denied', 'permission_denied' ]);
		expect((await ann.get(ThreadSchema, 'th-open'))?.title).toBe('renamed');
		expect((await ann.get(ThreadSchema, 'th-archived'))?.title).toBe('');
		expect(await ann.get(ThreadSchema, 'th-new')).toBeUndefined();
	});

	it('removes a row that stops matching, and sends it back when it matches again', async () => {
		const ws = freshWorkspace();
		await seedThreads(ws);
		const ann = await synced(clientAs('ann', ws));
		expect(await ann.get(ThreadSchema, 'th-hidden')).toBeUndefined();
		expect(await ann.can(ThreadSchema, 'th-open', 'read')).toBe('yes');

		await trusted(ws, [ {
			entity: 'thread',
			value: { id: 'th-open', room: { id: 'r1' }, hidden: true },
		} ]);
		await ann.syncNow();
		expect(await ann.get(ThreadSchema, 'th-open')).toBeUndefined();

		await trusted(ws, [ {
			entity: 'thread',
			value: { id: 'th-open', room: { id: 'r1' }, title: 'back' },
		} ]);
		await ann.syncNow();
		expect((await ann.get(ThreadSchema, 'th-open'))?.title).toBe('back');
	});
});

describe('rules that need several permissions on the client', () => {
	// Ann edits in a1 and a2 and uses billing at the root; Bob edits in a1
	// and reads billing at the root, but cannot use it.
	async function seedBilling(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'account', value: { id: 'a1' } },
			{ entity: 'account', value: { id: 'a2' } },
			{
				entity: 'account_member',
				value: { id: 'am-ann-1', account: { id: 'a1' }, userId: 'ann', role: 'account_editor' },
			},
			{
				entity: 'account_member',
				value: { id: 'am-ann-2', account: { id: 'a2' }, userId: 'ann', role: 'account_editor' },
			},
			{
				entity: 'account_member',
				value: { id: 'am-bob', account: { id: 'a1' }, userId: 'bob', role: 'account_editor' },
			},
			{ entity: 'biller', value: { id: 'b-ann', userId: 'ann', role: 'billing_clerk' } },
			{ entity: 'biller', value: { id: 'b-bob', userId: 'bob', role: 'billing_reader' } },
			{ entity: 'invoice', value: { id: 'i1', account: { id: 'a1' }, title: 'one' } },
			{ entity: 'invoice', value: { id: 'i2', account: { id: 'a2' }, title: 'two' } },
			{ entity: 'charge', value: { id: 'c1', invoice: { id: 'i1' }, memo: 'one' } },
		]);
	}

	it('predicts what the server admits and refuses', async () => {
		const ws = freshWorkspace();
		await seedBilling(ws);
		const rejections: RejectionInfo[] = [];
		const ann = await synced(clientAs('ann', ws));
		const bob = await synced(
			clientAs('bob', ws, new MemoryKVStore(), (info) => rejections.push(info)),
		);

		expect(await ann.can(InvoiceSchema, 'i1', 'update', { fields: [ 'title' ] })).toBe('yes');
		await ann.mutate.patch(InvoiceSchema, 'i1', { title: 'renamed' });
		await ann.syncNow();
		expect(await bob.can(InvoiceSchema, 'i1', 'update', { fields: [ 'title' ] })).toBe('no');
		await bob.mutate.patch(InvoiceSchema, 'i1', { title: 'by bob' });
		expect(await bob.canCreate(ChargeSchema, { id: 'c2', invoice: { id: 'i1' } })).toBe('no');
		await bob.mutate.put(ChargeSchema, { id: 'c2', invoice: { id: 'i1' } });
		await bob.syncNow();

		expect(rejections.map((r) => r.errorCode)).toEqual([ 'permission_denied', 'permission_denied' ]);
		expect((await bob.get(InvoiceSchema, 'i1'))?.title).toBe('renamed');
		expect(await bob.get(ChargeSchema, 'c2')).toBeUndefined();
	});

	it('syncs the rows where the caller holds both, and drops them when one is revoked', async () => {
		const ws = freshWorkspace();
		await seedBilling(ws);
		const bob = await synced(clientAs('bob', ws));
		expect(await bob.get(InvoiceSchema, 'i1')).toBeDefined();
		expect(await bob.get(ChargeSchema, 'c1')).toBeDefined();
		expect(await bob.get(InvoiceSchema, 'i2')).toBeUndefined();
		expect(await bob.can(ChargeSchema, 'c1', 'read')).toBe('yes');

		await trusted(ws, [ { entity: 'biller', key: 'b-bob', delete: true } ]);
		await bob.syncNow();
		expect(await bob.get(InvoiceSchema, 'i1')).toBeUndefined();
		expect(await bob.get(ChargeSchema, 'c1')).toBeUndefined();
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
