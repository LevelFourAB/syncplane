import { createConnectTransport } from '@connectrpc/connect-node';
import { type ChildProcess, execFileSync, spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { Row } from '../../src/read.js';
import { fetchSchema, registerSchema } from '../../src/schema/admin.js';
import {
	bool,
	defineSchema,
	entity,
	type Infer,
	int32,
	int64,
	keyedList,
	map,
	message,
	ref,
	refList,
	text,
} from '../../src/schema/builder.js';
import { freshWorkspace } from './helpers.js';

// A schema that exists ONLY as runtime-registered bytes — there is no generated
// Go or TS for it. The same definition yields the descriptors the client syncs
// with and the FileDescriptorSet the engine ingests.
const todo = entity('todo', {
	id: text().key(),
	title: text(),
	done: bool(),
	createdAtMs: int64(),
});
const bundle = defineSchema(todo);

// Descriptors built at runtime carry no static shape (`descMessage` is a
// name lookup, and `fetchSchema` rebuilds from server bytes), so reads come
// back as a bare Message. `Infer` is the escape hatch that names the row.
type Todo = Infer<typeof todo>;

// A referencing schema exercising the full parity tier end to end. Self-ref
// uses the `any` escape hatch (the runtime test reads through Record casts).
function makeRefBundle() {
	const project = entity('project', { id: text().key(), name: text() });
	const user = entity('user', { id: text().key(), name: text() });
	const issue: any = entity('issue', {
		id: text().key(),
		title: text(),
		settings: message({ color: text(), size: int32() }),
		project: ref(() => project),
		assignee: ref(() => user),
		reporter: ref(() => user).reverseRelation('reportedIssues'),
		blockedBy: refList(() => issue),
		members: keyedList(message({ uid: text(), name: text() }), 'uid'),
		items: keyedList(message({ id: text(), pos: text(), label: text() }), 'id').order('pos'),
		attrs: map('string', text()),
	});
	return defineSchema(project, user, issue);
}

const TOKEN = 'integration-secret';

// syncplaned hosts apps with no compiled entities; we register `todo` over the
// admin endpoint and then sync it. The two listeners (public sync, private
// admin) are read from stdout.
let proc: ChildProcess;
let syncUrl = '';
let adminUrl = '';

beforeAll(async () => {
	const repoRoot = fileURLToPath(new URL('../../..', import.meta.url));
	const bin = join(mkdtempSync(join(tmpdir(), 'syncplaned-')), 'syncplaned');
	execFileSync('go', [ 'build', '-o', bin, './cmd/syncplaned' ], {
		cwd: repoRoot,
		stdio: 'inherit',
	});

	proc = spawn(bin, [ `-token=${TOKEN}`, '-heartbeat=250ms', '-chunk-size=8' ], {
		stdio: [ 'ignore', 'pipe', 'inherit' ],
	});
	await new Promise<void>((resolve, reject) => {
		let buf = '';
		proc.stdout!.on('data', (chunk: Buffer) => {
			buf += chunk.toString();
			const sync = buf.match(/^SYNC (\S+)$/m);
			const admin = buf.match(/^ADMIN (\S+)$/m);
			if (sync?.[1] && admin?.[1]) {
				syncUrl = sync[1];
				adminUrl = admin[1];
				resolve();
			}
		});
		proc.on('error', reject);
		proc.on('exit', (code) => reject(new Error(`syncplaned exited with ${code} before listening`)));
	});
});

afterAll(() => {
	proc?.kill();
});

const adminTransport = () => createConnectTransport({ baseUrl: adminUrl, httpVersion: '1.1' });

function appClient(
	appId: string,
	workspaceId: string,
	entities = bundle.descMessages(),
): SyncClient {
	return new SyncClient({
		transport: createConnectTransport({ baseUrl: `${syncUrl}/${appId}`, httpVersion: '1.1' }),
		workspaceId,
		kvStore: new MemoryKVStore(),
		entities,
		pollIntervalMs: 0,
		streamEnabled: false,
	});
}

describe('runtime app schema registration', () => {
	it('rejects registration without the bearer token', async () => {
		await expect(registerSchema(adminTransport(), 'noauth', bundle)).rejects.toThrow();
	});

	it('registers a schema over the admin endpoint and syncs CRUD for that app', async () => {
		const res = await registerSchema(adminTransport(), 'app1', bundle, { token: TOKEN });
		expect(res.entityNames).toEqual([ 'todo' ]);
		expect(res.revision).not.toBe('');

		const ws = freshWorkspace();
		const a = appClient('app1', ws);
		const b = appClient('app1', ws);

		const TodoDesc = bundle.descMessage('todo');
		await a.mutate.put(TodoDesc, { id: 't1', title: 'buy milk', done: false, createdAtMs: 1n });
		await a.start();
		await a.syncNow();

		await b.start();
		await b.syncNow();
		expect(((await b.get(TodoDesc, 't1')) as Todo | undefined)?.title).toBe('buy milk');

		await a.close();
		await b.close();
	});

	it('isolates two apps that declare the same entity name', async () => {
		await registerSchema(adminTransport(), 'app-a', bundle, { token: TOKEN });
		await registerSchema(adminTransport(), 'app-b', bundle, { token: TOKEN });

		const ws = freshWorkspace();
		const TodoDesc = bundle.descMessage('todo');

		const a = appClient('app-a', ws);
		await a.start();
		await a.mutate.put(TodoDesc, { id: 'shared', title: 'only in a' });
		await a.syncNow();

		const b = appClient('app-b', ws);
		await b.start();
		await b.syncNow();
		expect(await b.get(TodoDesc, 'shared')).toBeUndefined();

		await a.close();
		await b.close();
	});

	it('syncs a referencing schema: sub-objects, refs, keyed/movable lists, maps', async () => {
		const refBundle = makeRefBundle();
		const res = await registerSchema(adminTransport(), 'refs', refBundle, { token: TOKEN });
		expect(res.entityNames).toEqual([ 'issue', 'project', 'user' ]);

		const Project = refBundle.descMessage('project');
		const User = refBundle.descMessage('user');
		const Issue = refBundle.descMessage('issue');

		const ws = freshWorkspace();
		const ents = refBundle.descMessages();
		const a = appClient('refs', ws, ents);
		await a.start();

		await a.mutate.put(Project, { id: 'p1', name: 'alpha' });
		await a.mutate.put(User, { id: 'ada', name: 'Ada' });
		await a.mutate.put(User, { id: 'bob', name: 'Bob' });
		await a.mutate.put(Issue, {
			id: 'i1',
			title: 'first bug',
			settings: { color: 'red', size: 2 },
			// A fat ref stub: the server projects it down to the key fields.
			project: { id: 'p1', name: 'ignored' },
			assignee: { id: 'ada' },
			reporter: { id: 'bob' },
			members: [ { uid: 'm1', name: 'Mia' }, { uid: 'm2', name: 'Max' } ],
			items: [ { id: 'x', pos: 'a0', label: 'one' }, { id: 'y', pos: 'a1', label: 'two' } ],
			attrs: { env: 'prod' },
		});
		await a.mutate.put(Issue, { id: 'i2', title: 'blocked', blockedBy: [ { id: 'i1' } ] });
		await a.syncNow();

		// A second client boots from the same schema and pulls everything.
		const b = appClient('refs', ws, ents);
		await b.start();
		await b.syncNow();

		const i1 = (await b.get(Issue, 'i1')) as Record<string, any>;
		expect(i1.title).toBe('first bug');
		expect(i1.settings.color).toBe('red');
		// Reference projection: only the key field survives the write.
		expect(i1.project.id).toBe('p1');
		expect(i1.project.name).toBe('');
		expect(i1.attrs.env).toBe('prod');

		// Forward ref resolves to the live target.
		const proj = (await b.resolveRef(Issue, 'project', i1 as never)) as Row<Record<string, any>>;
		expect(proj.value.name).toBe('alpha');

		// Two refs to user: assignee takes the name of its field, reporter the
		// reverse relation name it sets.
		const ada = (await b.get(User, 'ada')) as Record<string, any>;
		const bob = (await b.get(User, 'bob')) as Record<string, any>;
		const assigned = await b.resolveReverseRef(User, 'issuesByAssignee', ada as never);
		expect(assigned.map((r) => r.key)).toEqual([ 'i1' ]);
		const reported = await b.resolveReverseRef(User, 'reportedIssues', bob as never);
		expect(reported.map((r) => r.key)).toEqual([ 'i1' ]);

		// Self-ref (to-many): i2.blockedBy resolves to i1.
		const i2 = (await b.get(Issue, 'i2')) as Record<string, any>;
		const blocking = (await b.resolveRef(Issue, 'blockedBy', i2 as never)) as Row<
			Record<string, any>
		>[];
		expect(blocking.map((r) => r.key)).toEqual([ 'i1' ]);

		// PATCH a single keyed-list member: the other member must survive.
		await a.mutate.patch(Issue, 'i1', {
			members: [ { uid: 'm1', name: 'Mia Hamm' }, { uid: 'm2', name: 'Max' } ],
		});
		await a.syncNow();
		await b.syncNow();
		const afterMember = (await b.get(Issue, 'i1')) as Record<string, any>;
		const byUid = Object.fromEntries(afterMember.members.map((m: any) => [ m.uid, m.name ]));
		expect(byUid).toEqual({ m1: 'Mia Hamm', m2: 'Max' });

		// Movable list: re-point item x past y; only x changed, so the mask
		// narrows to items.x and y survives. Order by pos becomes [y, x].
		await a.mutate.patch(Issue, 'i1', {
			items: [ { id: 'x', pos: 'a2', label: 'one' }, { id: 'y', pos: 'a1', label: 'two' } ],
		});
		await a.syncNow();
		await b.syncNow();
		const afterMove = (await b.get(Issue, 'i1')) as Record<string, any>;
		const ordered = [ ...afterMove.items ].sort((p: any, q: any) =>
			p.pos < q.pos ? -1 : p.pos > q.pos ? 1 : 0
		);
		expect(ordered.map((it: any) => it.id)).toEqual([ 'y', 'x' ]);

		await a.close();
		await b.close();
	});

	it('ships its schema back via fetchSchema and boots a client from it', async () => {
		await registerSchema(adminTransport(), 'app-fetch', bundle, { token: TOKEN });

		// Rebuild descriptors purely from the server's bytes — no shared import.
		const entities = await fetchSchema(adminTransport(), 'app-fetch');
		expect(entities.map((e) => e.name)).toContain('Todo');

		const ws = freshWorkspace();
		const fetched = entities.find((e) => e.name === 'Todo')!;
		const a = appClient('app-fetch', ws, entities);
		await a.start();
		await a.mutate.put(fetched, { id: 'f1', title: 'from fetched schema' });
		await a.syncNow();
		expect(((await a.get(fetched, 'f1')) as Todo | undefined)?.title).toBe('from fetched schema');

		await a.close();
	});
});

// The access-model declarations are lowered by the builder and judged by the
// engine, so only a round trip shows that the two agree on what was declared.
describe('runtime registration of an access-model schema', () => {
	function makeAccessBundle(opts: { orgViewerRole?: boolean; unusedGrant?: boolean } = {}) {
		const project = entity('project', { id: text().key(), name: text() }, {
			access: {
				allow: [
					{ read: true, requires: 'view_projects' },
					{ write: true, requires: 'edit_projects' },
				],
			},
		});
		const orgMember = entity('org_member', { id: text().key(), userId: text(), role: text() }, {
			grant: {
				subject: 'userId',
				role: 'role',
				roles: [
					{
						name: 'org_admin',
						grants: [
							'view_members',
							'manage_members',
							'view_projects',
							'edit_projects',
							'view_tasks',
							'edit_tasks',
							...(opts.unusedGrant ? [ 'delete_org' ] : []),
						],
					},
					...(opts.orgViewerRole ? [ { name: 'org_viewer', grants: [ 'view_tasks' ] } ] : []),
				],
			},
			access: {
				allow: [
					{ read: true, requires: 'view_members' },
					{ write: true, requires: 'manage_members' },
				],
			},
		});
		const task = entity('task', { id: text().key(), project: ref(() => project) }, {
			access: {
				parents: [ 'project' ],
				allow: [
					{ read: true, requires: 'view_tasks' },
					{ create: true, update: true, requires: 'edit_tasks' },
					{ delete: true, requires: 'manage_members' },
				],
			},
		});
		const label = entity('label', { id: text().key() }, {
			access: { allow: [ { read: true, write: true, admitted: true } ] },
		});
		return defineSchema(project, orgMember, task, label);
	}

	it('registers a schema that uses the access model', async () => {
		const res = await registerSchema(adminTransport(), 'access-ok', makeAccessBundle(), {
			token: TOKEN,
		});
		expect(res.entityNames).toEqual([ 'label', 'org_member', 'project', 'task' ]);
	});

	// Only server code and hasPermission() ask about such a permission.
	it('registers a role that grants a permission no rule uses', async () => {
		const res = await registerSchema(
			adminTransport(),
			'access-unused',
			makeAccessBundle({ unusedGrant: true }),
			{
				token: TOKEN,
			},
		);
		expect(res.entityNames).toEqual([ 'label', 'org_member', 'project', 'task' ]);
	});

	it("changes an entity's access model only when the registration allows it", async () => {
		await registerSchema(adminTransport(), 'access-evolve', makeAccessBundle(), { token: TOKEN });
		const changed = makeAccessBundle({ orgViewerRole: true });
		await expect(registerSchema(adminTransport(), 'access-evolve', changed, { token: TOKEN }))
			.rejects.toThrow(/entity "org_member" changes its access model/);
		const res = await registerSchema(adminTransport(), 'access-evolve', changed, {
			token: TOKEN,
			allowAccessChange: true,
		});
		expect(res.entityNames).toContain('org_member');
	});

	// Without a grant entity the schema declares no role, so the app's
	// workspace hook alone admits a caller, and the rules still decide what
	// each caller can do. syncplaned admits every caller.
	function makeNoRoleBundle(opts: { withMembers?: boolean } = {}) {
		const entry = entity('entry', { id: text().key(), body: text() }, {
			access: { allow: [ { read: true, create: true, admitted: true } ] },
		});
		if (!opts.withMembers) return defineSchema(entry);
		const member = entity('member', { id: text().key(), userId: text(), role: text() }, {
			grant: {
				subject: 'userId',
				role: 'role',
				roles: [ { name: 'admin', grants: [ 'view_members' ] } ],
			},
			access: { allow: [ { read: true, requires: 'view_members' } ] },
		});
		return defineSchema(entry, member);
	}

	it('syncs a schema that declares no role, and holds writes to its rules', async () => {
		const noRole = makeNoRoleBundle();
		await registerSchema(adminTransport(), 'access-norole', noRole, { token: TOKEN });
		const Entry = noRole.descMessage('entry');
		const ws = freshWorkspace();

		const a = appClient('access-norole', ws, noRole.descMessages());
		await a.start();
		await a.mutate.put(Entry, { id: 'e1', body: 'posted' }, { expectedVersion: 0n });
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);

		const b = appClient('access-norole', ws, noRole.descMessages());
		await b.start();
		await b.syncNow();
		expect(((await b.get(Entry, 'e1')) as Record<string, any> | undefined)?.body).toBe('posted');
		await expect(b.mutate.patch(Entry, 'e1', { body: 'edited' }))
			.rejects.toThrow(/no access rule admits update/);
		await expect(b.mutate.del(Entry, 'e1')).rejects.toThrow(/no access rule admits delete/);

		await a.close();
		await b.close();
	});

	it('declares the first role of a schema only when the registration allows it', async () => {
		await registerSchema(adminTransport(), 'access-first-role', makeNoRoleBundle(), { token: TOKEN });
		const withMembers = makeNoRoleBundle({ withMembers: true });
		await expect(registerSchema(adminTransport(), 'access-first-role', withMembers, { token: TOKEN }))
			.rejects.toThrow(/the schema declares its first role/);
		const res = await registerSchema(adminTransport(), 'access-first-role', withMembers, {
			token: TOKEN,
			allowAccessChange: true,
		});
		expect(res.entityNames).toContain('member');
	});
});
