import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import {
	type Task,
	TaskSchema,
	type User,
	UserRefSchema,
	UserSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Row } from '../../src/read.js';
import { freshWorkspace, newClient, newMultiClient } from './helpers.js';

// Workspace-qualified references against the real engine: a Task's UserRef
// stub naming another workspace survives the round trip through the server,
// resolves locally in the workspace it names, and is answered by the server's
// own matches() inside one workspace alone.

const entities = { entities: [ TaskSchema, UserSchema ] };
const keysOf = (rows: readonly { key: string }[]) => rows.map((r) => r.key).sort();

describe('workspace-qualified references', () => {
	it('round-trips a stub naming another workspace and resolves it there', async () => {
		const proj = freshWorkspace();
		const org = freshWorkspace();
		const writer = newMultiClient([ proj, org ], entities);
		await writer.start();
		await writer.ws(org).mutate.put(UserSchema, { id: 'u1', name: 'Org Maya' });
		await writer.ws(proj).mutate.put(UserSchema, { id: 'u1', name: 'Proj Maya' });
		await writer.ws(proj).mutate.put(TaskSchema, {
			id: 't1',
			title: 'org assignee',
			// A fat inner stub: the server keeps the key and the workspace only.
			assignee: { workspaceId: org, user: { id: 'u1', name: 'smuggled' } },
		});
		await writer.ws(proj).mutate.put(TaskSchema, {
			id: 't2',
			title: 'local assignee',
			assignee: { user: { id: 'u1' } },
		});
		await writer.syncNow();
		expect(writer.status().pendingMutations).toBe(0);

		const reader = newMultiClient([ proj, org ], entities);
		await reader.start();
		await reader.syncNow();
		const t1 = (await reader.ws(proj).get(TaskSchema, 't1'))!;
		expect(t1.assignee?.workspaceId).toBe(org);
		expect(t1.assignee?.user?.id).toBe('u1');
		expect(t1.assignee?.user?.name).toBe('');

		const resolved = await reader.ws(proj).resolveRef(TaskSchema, 'assignee', t1) as
			| Row<User>
			| undefined;
		expect(resolved?.workspaceId).toBe(org);
		expect(resolved?.value.name).toBe('Org Maya');
		const t2 = (await reader.ws(proj).get(TaskSchema, 't2'))!;
		const local = await reader.ws(proj).resolveRef(TaskSchema, 'assignee', t2) as
			| Row<User>
			| undefined;
		expect(local?.value.name).toBe('Proj Maya');

		const orgUser = (await reader.ws(org).get(UserSchema, 'u1'))!;
		const tasks = await reader.ws(org).resolveReverseRef(
			UserSchema,
			'tasksByAssignee',
			orgUser,
		) as Row<
			Task
		>[];
		expect(keysOf(tasks)).toEqual([ 't1' ]);

		await reader.close();
		await writer.close();
	});

	it('is answered by the server inside one workspace, and locally where the stub points', async () => {
		const proj = freshWorkspace();
		const org = freshWorkspace();
		const client = newMultiClient([ proj, org ], entities);
		await client.start();
		await client.ws(org).mutate.put(UserSchema, { id: 'u1', name: 'Org Maya' });
		await client.ws(proj).mutate.put(UserSchema, { id: 'u1', name: 'Proj Maya' });
		await client.ws(proj).mutate.put(TaskSchema, {
			id: 't1',
			assignee: { workspaceId: org, user: { id: 'u1' } },
		});
		await client.ws(proj).mutate.put(TaskSchema, { id: 't2', assignee: { user: { id: 'u1' } } });
		await client.syncNow();

		// The client holds both workspaces, so it resolves t1's stub in the org
		// workspace and finds Org Maya there.
		const local = await client.ws(proj).query(TaskSchema, {
			where: (t) => t.assignee.matches((u) => u.name.eq('Org Maya')),
		});
		expect(keysOf(local)).toEqual([ 't1' ]);

		// The server evaluates matches() inside the request's workspace: t1's
		// stub names another one, so it matches nothing there — and never the
		// same-key project user.
		const server = await client.ws(proj).queryServer(TaskSchema, {
			where: (t) => t.assignee.matches((u) => u.name.eq('Org Maya')),
		});
		expect(keysOf(server)).toEqual([]);
		const serverLocal = await client.ws(proj).queryServer(TaskSchema, {
			where: (t) => t.assignee.matches((u) => u.name.eq('Proj Maya')),
		});
		expect(keysOf(serverLocal)).toEqual([ 't2' ]);

		await client.close();
	});
});

// A repeated workspace-qualified reference over the wire: the client expands
// an edit to the list into one mask path per element that moved, and the
// server merges each element on its own. Two clients that add and remove
// different watchers offline therefore both keep their edit, where a list
// replaced whole would keep only the later arrival's copy.
describe('repeated workspace-qualified references', () => {
	const uref = (workspaceId: string, id: string) => ({ workspaceId, user: { id } });
	const names = (t: Task) => t.watchers.map((w) => `${w.workspaceId}/${w.user?.id}`).sort();

	it('two clients add and remove different elements offline → both survive', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, entities);
		const b = newClient(ws, entities);
		await a.start();
		await b.start();

		await a.mutate.put(TaskSchema, {
			id: 't1',
			title: 'watched',
			watchers: [ uref('org', 'u1'), uref('org', 'u2') ],
		});
		await a.syncNow();
		await b.syncNow();

		// A drops u1 while B adds u3 in another workspace, neither seeing the other.
		await a.mutate.update(TaskSchema, 't1', (t) => {
			t.watchers = t.watchers.filter((w) => w.user?.id !== 'u1');
		});
		await b.mutate.update(TaskSchema, 't1', (t) => {
			t.watchers.push(create(UserRefSchema, uref('proj', 'u3')));
		});

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			expect(names((await client.get(TaskSchema, 't1'))!)).toEqual([ 'org/u2', 'proj/u3' ]);
		}

		// A fresh replica bootstraps the server's copy, which is the same set.
		const c = newClient(ws, entities);
		await c.start();
		await c.syncNow();
		expect(names((await c.get(TaskSchema, 't1'))!)).toEqual([ 'org/u2', 'proj/u3' ]);

		await a.close();
		await b.close();
		await c.close();
	});
});
