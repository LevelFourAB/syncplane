import { describe, expect, it } from 'vitest';

import {
	CommentSchema,
	LabelSchema,
	OrgMemberSchema,
	PageSchema,
	ProjectMemberSchema,
	ProjectSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { createTestClient, createTestServer } from '../../src/testing/index.js';

// The access model in the app-author test kit: setCallerAccess resolves roles
// from the grant options the server registers, and a server that serves
// readable rows sends a client only what the real server would, so the
// predictions for rows a client does not hold are reachable in a unit test.

const entities = [
	OrgMemberSchema,
	ProjectSchema,
	ProjectMemberSchema,
	TaskSchema,
	CommentSchema,
	PageSchema,
	LabelSchema,
] as const;

/** The keys of `rows`, sorted. */
function ids(rows: readonly { value: { id: string } }[]): string[] {
	return rows.map((r) => r.value.id).sort();
}

/** Two projects with one task each, and a comment on the first task. */
function seed(server: ReturnType<typeof createTestServer<typeof entities>>): void {
	server.put(ProjectSchema, { id: 'p1' });
	server.put(ProjectSchema, { id: 'p2' });
	server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
	server.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
	server.put(CommentSchema, { id: 'c1', task: { id: 't1' } });
}

describe('syncplane/testing access', () => {
	describe('setCallerAccess roles', () => {
		it('grants a role and the roles it includes at a row and the rows below it', async () => {
			const t = await createTestClient({ entities });
			try {
				seed(t.server);
				t.server.setCallerAccess({
					subjects: [ 'ann' ],
					roles: [ { role: 'project_admin', at: [ ProjectSchema, 'p1' ] } ],
				});
				await t.sync();

				// manage_members from project_admin, edit_tasks from project_writer
				// and view_tasks from project_reader, two includes down.
				expect(await t.client.hasPermission(TaskSchema, 't1', 'manage_members')).toBe('yes');
				expect(await t.client.can(TaskSchema, 't1', 'update')).toBe('yes');
				expect(await t.client.can(CommentSchema, 'c1', 'read')).toBe('yes');
				expect(await t.client.can(TaskSchema, 't2', 'read')).toBe('no');
			} finally {
				await t.close();
			}
		});

		it('grants a role without at at the workspace root', async () => {
			const t = await createTestClient({ entities });
			try {
				seed(t.server);
				t.server.setCallerAccess({ roles: [ { role: 'org_member' } ] });
				await t.sync();

				expect(await t.client.can(TaskSchema, 't1', 'read')).toBe('yes');
				expect(await t.client.can(TaskSchema, 't2', 'read')).toBe('yes');
				expect(await t.client.can(TaskSchema, 't2', 'update')).toBe('no');
			} finally {
				await t.close();
			}
		});

		it('adds permissions to those of the roles', async () => {
			const t = await createTestClient({ entities });
			try {
				seed(t.server);
				t.server.setCallerAccess({
					roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] } ],
					permissions: [ {
						permission: 'edit_tasks',
						anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
					} ],
				});
				await t.sync();

				expect(await t.client.can(TaskSchema, 't1', 'update')).toBe('yes');
				expect(await t.client.can(TaskSchema, 't1', 'read')).toBe('yes');
			} finally {
				await t.close();
			}
		});

		it('refuses a role no registered entity declares, and lists the declared ones', () => {
			const server = createTestServer({ entities });
			expect(() => server.setCallerAccess({ roles: [ { role: 'project_admn' } ] })).toThrow(
				/no registered entity declares role "project_admn"; the declared roles are org_admin, org_member, project_admin,/,
			);
		});

		it('refuses a role held at a row its grant rows cannot point at', () => {
			const server = createTestServer({ entities });
			expect(() =>
				server.setCallerAccess({ roles: [ { role: 'project_admin', at: [ TaskSchema, 't1' ] } ] })
			).toThrow(
				/role "project_admin" of project_member cannot be held at a row of task; it is held at a row of project/,
			);
			expect(() =>
				server.setCallerAccess({ roles: [ { role: 'org_admin', at: [ ProjectSchema, 'p1' ] } ] })
			).toThrow(/org_member has no access parent, so omit at/);
		});
	});

	describe("serve: 'readable'", () => {
		it('serves no row of an entity with access rules before access is set', async () => {
			const server = createTestServer({ entities, serve: 'readable' });
			seed(server);
			const t = await createTestClient({ server });
			try {
				await t.sync();
				expect(await t.db.task.get('t1')).toBeUndefined();

				server.setCallerAccess({ roles: [ { role: 'org_member' } ] });
				await t.sync();
				expect(ids(await t.db.task.scan())).toEqual([ 't1', 't2' ]);
			} finally {
				await t.close();
			}
		});

		it('serves only the rows the caller reads, so a row it does not hold answers unknown', async () => {
			const server = createTestServer({ entities, serve: 'readable' });
			seed(server);
			server.setCallerAccess({ roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] } ] });
			const t = await createTestClient({ server });
			try {
				await t.sync();
				expect(ids(await t.db.project.scan())).toEqual([ 'p1' ]);
				expect(ids(await t.db.task.scan())).toEqual([ 't1' ]);
				expect(ids(await t.db.comment.scan())).toEqual([ 'c1' ]);

				expect(await t.client.can(TaskSchema, 't1', 'read')).toBe('yes');
				// The caller reads tasks at a project, and the client cannot tell
				// which project t2 is in.
				expect(await t.client.can(TaskSchema, 't2', 'read')).toBe('unknown');
			} finally {
				await t.close();
			}
		});

		it('sends rows the caller starts reading and tombstones rows it stops reading', async () => {
			const server = createTestServer({ entities, serve: 'readable' });
			seed(server);
			server.setCallerAccess({ roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] } ] });
			const t = await createTestClient({ server });
			try {
				await t.sync();
				server.setCallerAccess({ roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p2' ] } ] });
				await t.sync();

				expect(ids(await t.db.task.scan())).toEqual([ 't2' ]);
				expect(await t.db.comment.get('c1')).toBeUndefined();
				expect(await t.client.can(TaskSchema, 't2', 'read')).toBe('yes');
			} finally {
				await t.close();
			}
		});

		it('tombstones a row whose row above moves out of reach', async () => {
			const server = createTestServer({ entities, serve: 'readable' });
			seed(server);
			server.setCallerAccess({ roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] } ] });
			const t = await createTestClient({ server });
			try {
				await t.sync();
				expect(await t.db.comment.get('c1')).toBeDefined();

				// c1 does not change; the task it sits on moves to p2.
				server.put(TaskSchema, { id: 't1', project: { id: 'p2' } });
				await t.sync();

				expect(await t.db.task.get('t1')).toBeUndefined();
				expect(await t.db.comment.get('c1')).toBeUndefined();
			} finally {
				await t.close();
			}
		});

		it("decides a rule's subject check against the caller's subjects", async () => {
			const server = createTestServer({ entities, serve: 'readable' });
			seed(server);
			server.put(PageSchema, { id: 'ann-page', task: { id: 't1' }, ownerId: 'ann' });
			server.put(PageSchema, { id: 'bob-page', task: { id: 't1' }, ownerId: 'bob' });
			// project_editor grants view_projects but not view_tasks: a page is
			// readable to its owner alone.
			server.setCallerAccess({
				subjects: [ 'ann' ],
				roles: [ { role: 'project_editor', at: [ ProjectSchema, 'p1' ] } ],
			});
			const t = await createTestClient({ server });
			try {
				await t.sync();
				expect(ids(await t.db.page.scan())).toEqual([ 'ann-page' ]);
				expect(await t.db.task.get('t1')).toBeUndefined();
			} finally {
				await t.close();
			}
		});

		it('pages a resync to a client on the subscribe stream with no new commit', async () => {
			const server = createTestServer({ entities, serve: 'readable' });
			seed(server);
			server.setCallerAccess({ roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] } ] });
			const t = await createTestClient({ server, streamEnabled: true });
			try {
				await t.sync();
				expect(ids(await t.db.task.scan())).toEqual([ 't1' ]);

				server.setCallerAccess({ roles: [ { role: 'org_member' } ] });
				await expect.poll(async () => ids(await t.db.task.scan())).toEqual([ 't1', 't2' ]);
			} finally {
				await t.close();
			}
		});
	});
	describe('the summary on the wire', () => {
		it('sends the summary on a pull only when it differs from the one the client holds, as the real server does', async () => {
			// Given a client that synced once with ann's access set
			const t = await createTestClient({ entities });
			try {
				seed(t.server);
				t.server.setCallerAccess({
					subjects: [ 'ann' ],
					roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] } ],
				});
				await t.sync();
				const changes: string[] = [];
				t.client.onAccessChange((ws) => changes.push(ws));

				// When it pulls again, and neither the rows nor the access changed
				await t.sync();
				await t.sync();

				// Then no pull stored a summary, so nothing told the access
				// listeners. The real server leaves CallerAccess out of a response
				// whose cursor already carries it, so an app test against this
				// server sees the access hooks asked again only when production
				// asks them again.
				expect(changes).toEqual([]);
			} finally {
				await t.close();
			}
		});
	});
});
