// The server checks an update against more than the entity's rules and
// guards: a move needs the right to read the row, every field of it a guard
// can hide, and every row below it, where the row is; a change of a field
// that decides who reads a guarded field needs the right to read that field
// before and after; a grant row may grant only what the writer holds where
// it applies. can() decides each check that the client holds the inputs of,
// so it does not answer yes for an update the server refuses.

import { clone, create, createFileRegistry } from '@bufbuild/protobuf';
import {
	EnumOptionsSchema,
	EnumValueDescriptorProtoSchema,
	FileDescriptorProtoSchema,
} from '@bufbuild/protobuf/wkt';
import { describe, expect, it } from 'vitest';

import {
	DocSchema,
	file_syncplane_testdata_access_v1_access,
	FolderSchema,
	ProjectSchema,
	RepoMemberSchema,
	RepoSchema,
	RepoSquadSchema,
	SquadMemberSchema,
	SquadSchema,
	StaffSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { defineSchema, entity, ref, text, within } from '../../src/schema/builder.js';
import { createTestClient } from '../../src/testing/index.js';

/** Permissions held at the workspace root. */
const atRoot = (subject: string, ...permissions: string[]) => ({
	subjects: [ subject ],
	permissions: permissions.map((permission) => ({ permission, root: true })),
});

/**
 * A project, and a personnel record below it whose review the person it is
 * about reads, and every caller that one of `alsoRead` admits.
 */
function personnel(alsoRead: { read: true; requires: string }[]) {
	const project = entity('project', { id: text().key() }, {
		access: { allow: [ { read: true, write: true, requires: 'view_projects' } ] },
	});
	const record = entity('personnel', {
		id: text().key(),
		project: ref(() => project),
		personId: text(),
		review: text(),
	}, {
		access: {
			parents: [ 'project' ],
			allow: [ { read: true, requires: 'view_tasks' }, { write: true, requires: 'edit_tasks' } ],
			guard: [ {
				fields: [ 'review' ],
				allow: [
					{ read: true, admitted: true, callerIs: 'personId' },
					...alsoRead,
					{ write: true, requires: 'edit_projects' },
				],
			} ],
		},
	});
	const bundle = defineSchema(project, record);
	return [ bundle.descMessage('project')!, bundle.descMessage('personnel')! ] as const;
}

describe('can() for a move of a row', () => {
	it('answers no when a caller with a permission reads a field of the row that the caller does not read', async () => {
		// Given staff s1 in p1, whose salary a caller with edit_projects reads
		// where the record sits, and a caller who reads and edits staff
		// everywhere without edit_projects
		const t = await createTestClient({ entities: [ ProjectSchema, StaffSchema ] });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(ProjectSchema, { id: 'p2' });
		t.server.put(StaffSchema, { id: 's1', project: { id: 'p1' }, name: 'ann' });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When
		const rename = await t.client.can(StaffSchema, 's1', 'update', { fields: [ 'name' ] });
		const move = await t.client.can(StaffSchema, 's1', 'update', { fields: [ 'project' ] });

		// Then the move would carry the salary to a project where other callers
		// read it, which the server refuses
		expect(rename).toBe('yes');
		expect(move).toBe('no');
		await t.close();
	});

	it('answers yes when the fields the caller does not read are read by no client, or only by the caller the row names', async () => {
		// Given staff s1 in p1, whose bonus and pin no client reads and whose
		// review only the person it is about reads, and a caller who also
		// reads salary and bio everywhere
		const t = await createTestClient({ entities: [ ProjectSchema, StaffSchema ] });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(ProjectSchema, { id: 'p2' });
		t.server.put(StaffSchema, { id: 's1', project: { id: 'p1' }, name: 'ann', personId: 'guest' });
		t.server.setCallerAccess(
			atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks', 'edit_projects'),
		);
		await t.sync();

		// When
		const move = await t.client.can(StaffSchema, 's1', 'update', { fields: [ 'project' ] });

		// Then the move shows those fields to nobody new, as the server decides
		expect(move).toBe('yes');
		await t.close();
	});

	it('answers no when a caller with a permission also reads a field that the caller the row names reads', async () => {
		// Given a record whose review the person it is about reads, and a
		// caller with edit_projects reads too where the record sits
		const [ P, S ] = personnel([ { read: true, requires: 'edit_projects' } ]);
		const t = await createTestClient({ entities: [ P, S ] });
		t.server.put(P, { id: 'p1' });
		t.server.put(P, { id: 'p2' });
		t.server.put(S, { id: 's1', project: { id: 'p1' }, personId: 'guest' });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When
		const move = await t.client.can(S, 's1', 'update', { fields: [ 'project' ] });

		// Then the move would show the review to other callers, which the
		// server refuses
		expect(move).toBe('no');
		await t.close();
	});

	it('answers yes when only the caller the row names reads a field the caller does not read', async () => {
		// Given the same record, whose review only the person it is about reads
		const [ P, S ] = personnel([]);
		const t = await createTestClient({ entities: [ P, S ] });
		t.server.put(P, { id: 'p1' });
		t.server.put(P, { id: 'p2' });
		t.server.put(S, { id: 's1', project: { id: 'p1' }, personId: 'guest' });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When
		const move = await t.client.can(S, 's1', 'update', { fields: [ 'project' ] });

		// Then
		expect(move).toBe('yes');
		await t.close();
	});

	it('answers no when the caller reads the rows below only through a callerIs rule', async () => {
		// Given notes below tasks, which a caller reads only when it owns them:
		// that rule admits the caller on some notes, not on every note that
		// moves with the task
		const project = entity('project', { id: text().key() }, {
			access: { allow: [ { read: true, write: true, requires: 'view_projects' } ] },
		});
		const task = entity('task', { id: text().key(), project: ref(() => project) }, {
			access: {
				parents: [ 'project' ],
				allow: [ { read: true, requires: 'view_tasks' }, { write: true, requires: 'edit_tasks' } ],
			},
		});
		const note = entity('note', { id: text().key(), task: ref(() => task), ownerId: text() }, {
			access: {
				parents: [ 'task' ],
				allow: [ { read: true, write: true, requires: 'view_tasks', callerIs: 'ownerId' } ],
			},
		});
		const bundle = defineSchema(project, task, note);
		const [ P, T, N ] = [ 'project', 'task', 'note' ].map((name) => bundle.descMessage(name));
		const t = await createTestClient({ entities: [ P!, T!, N! ] });
		t.server.put(P!, { id: 'p1' });
		t.server.put(P!, { id: 'p2' });
		t.server.put(T!, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When
		const move = await t.client.can(T!, 't1', 'update', { fields: [ 'project' ] });

		// Then
		expect(move).toBe('no');
		await t.close();
	});
});

describe('can() for a move of a row with rows below it', () => {
	it('answers yes when each row below is read only by the caller it names', async () => {
		// Given notes below tasks, which only their owner reads, wherever they
		// sit: moving the task shows them to nobody new
		const project = entity('project', { id: text().key() }, {
			access: { allow: [ { read: true, write: true, requires: 'view_projects' } ] },
		});
		const task = entity('task', { id: text().key(), project: ref(() => project) }, {
			access: {
				parents: [ 'project' ],
				allow: [ { read: true, requires: 'view_tasks' }, { write: true, requires: 'edit_tasks' } ],
			},
		});
		const note = entity('note', { id: text().key(), task: ref(() => task), ownerId: text() }, {
			access: {
				parents: [ 'task' ],
				allow: [ { read: true, admitted: true, callerIs: 'ownerId' }, {
					write: true,
					requires: 'edit_tasks',
				} ],
			},
		});
		const bundle = defineSchema(project, task, note);
		const [ P, T, N ] = [ 'project', 'task', 'note' ].map((name) => bundle.descMessage(name));
		const t = await createTestClient({ entities: [ P!, T!, N! ] });
		t.server.put(P!, { id: 'p1' });
		t.server.put(P!, { id: 'p2' });
		t.server.put(T!, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When
		const move = await t.client.can(T!, 't1', 'update', { fields: [ 'project' ] });

		// Then
		expect(move).toBe('yes');
		await t.close();
	});
});

describe('can() for a change of a field that decides who reads a guarded field', () => {
	it('answers no for a caller who does not read the guarded field now', async () => {
		// Given staff s1 about zed, whose review only zed reads, and ed, who
		// edits staff
		const t = await createTestClient({ entities: [ ProjectSchema, StaffSchema ] });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(StaffSchema, { id: 's1', project: { id: 'p1' }, name: 'zed', personId: 'zed' });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When ed asks to point the record at himself
		const repoint = await t.client.can(StaffSchema, 's1', 'update', { fields: [ 'personId' ] });

		// Then the server refuses it: ed would read the review he could not
		// read before
		expect(repoint).toBe('no');
		await t.close();
	});

	it('answers unknown for a caller who reads the guarded field now, as the new value decides', async () => {
		// Given staff s1 about ed, who edits staff
		const t = await createTestClient({ entities: [ ProjectSchema, StaffSchema ] });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(StaffSchema, { id: 's1', project: { id: 'p1' }, name: 'ed', personId: 'ed' });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When
		const repoint = await t.client.can(StaffSchema, 's1', 'update', {
			fields: [ 'personId' ],
			detail: true,
		});

		// Then
		expect(repoint).toEqual({ answer: 'unknown', reason: 'new_value' });
		await t.close();
	});
});

describe('can() for a row the client does not hold', () => {
	it('answers unknown for an update or a delete, which the server refuses for a row it does not hold live', async () => {
		// Given a caller who edits staff everywhere, and no staff row s1 held
		// locally: the server may not hold it live either
		const t = await createTestClient({ entities: [ ProjectSchema, StaffSchema ] });
		t.server.setCallerAccess(atRoot('ed', 'view_projects', 'view_tasks', 'edit_tasks'));
		await t.sync();

		// When
		const rename = await t.client.can(StaffSchema, 's1', 'update', {
			fields: [ 'name' ],
			detail: true,
		});
		const remove = await t.client.can(StaffSchema, 's1', 'delete', { detail: true });

		// Then
		expect(rename).toEqual({ answer: 'unknown', reason: 'row_not_held' });
		expect(remove).toEqual({ answer: 'unknown', reason: 'row_not_held' });
		await t.close();
	});
});

describe('can() for a delete of a row other rows sit below', () => {
	it('answers no while the client holds a live row below it, as the server refuses the delete', async () => {
		// Given a caller who edits projects everywhere, and project p1 with task
		// t1 below it
		const t = await createTestClient({ entities: [ ProjectSchema, TaskSchema ] });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(ProjectSchema, { id: 'p2' });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess(
			atRoot('ed', 'view_projects', 'edit_projects', 'view_tasks', 'edit_tasks'),
		);
		await t.sync();

		// When, Then: p1 holds a live task, p2 holds none
		expect(await t.client.can(ProjectSchema, 'p1', 'delete')).toBe('no');
		expect(await t.client.can(ProjectSchema, 'p2', 'delete')).toBe('yes');

		// And when a local write puts a task below p2 and deletes t1
		await t.client.mutate.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
		await t.client.mutate.del(TaskSchema, 't1');

		// Then the answers follow the rows the client holds
		expect(await t.client.can(ProjectSchema, 'p1', 'delete')).toBe('yes');
		expect(await t.client.can(ProjectSchema, 'p2', 'delete')).toBe('no');
		await t.close();
	});
});

// The server also refuses the delete of a team row while a live grant row
// names it as its subject: a team created later with the same key would take
// over the grant.
describe('can() for a delete of a team row that a grant row names', () => {
	const squads = [ SquadSchema, SquadMemberSchema, RepoSchema, RepoMemberSchema, RepoSquadSchema ];
	const manager = atRoot('ann', 'view_squads', 'manage_squads', 'view_repos', 'view_repo_members');

	it('answers no while the client holds a live grant row whose subject field names it', async () => {
		// Given a caller who manages squads everywhere, squads sq1 and sq2, and
		// repo_squad rs1, which grants to sq1
		const t = await createTestClient({ entities: squads });
		t.server.put(SquadSchema, { id: 'sq1' });
		t.server.put(SquadSchema, { id: 'sq2' });
		t.server.put(RepoSchema, { id: 'r1' });
		t.server.put(RepoSquadSchema, {
			id: 'rs1',
			repo: { id: 'r1' },
			squad: { id: 'sq1' },
			role: 'repo_squad_reader',
		});
		t.server.setCallerAccess(manager);
		await t.sync();

		// When, Then: rs1 names sq1, and no grant row names sq2
		expect(await t.client.can(SquadSchema, 'sq1', 'delete')).toBe('no');
		expect(await t.client.can(SquadSchema, 'sq2', 'delete')).toBe('yes');

		// And when rs1 is deleted on the server
		t.server.del(RepoSquadSchema, 'rs1');
		await t.sync();

		// Then sq1 can be deleted
		expect(await t.client.can(SquadSchema, 'sq1', 'delete')).toBe('yes');
		await t.close();
	});

	it('answers no while the client holds a live grant row whose subject oneof names it', async () => {
		// Given the same caller, and repo_member rm1, whose member oneof holds sq1
		const t = await createTestClient({ entities: squads });
		t.server.put(SquadSchema, { id: 'sq1' });
		t.server.put(RepoSchema, { id: 'r1' });
		t.server.put(RepoMemberSchema, {
			id: 'rm1',
			repo: { id: 'r1' },
			member: { case: 'squad', value: { id: 'sq1' } },
			role: 'repo_reader',
		});
		t.server.setCallerAccess(manager);
		await t.sync();

		// When
		const answer = await t.client.can(SquadSchema, 'sq1', 'delete');

		// Then
		expect(answer).toBe('no');
		await t.close();
	});
});

// In a chain of access parents that can lead back to the same entity, as
// folder in folder does, the server refuses a client write that adds an access
// parent the caller does not read: the row must exist, live, and a read rule
// must admit the caller there.
describe('can() and canCreate() for a write that adds an access parent in a recursive chain', () => {
	it('answers no when the caller does not read the parent', async () => {
		// Given a caller who edits folders everywhere but reads none, and f1
		const t = await createTestClient({ entities: [ FolderSchema, DocSchema ] });
		t.server.put(FolderSchema, { id: 'f1' });
		t.server.setCallerAccess(atRoot('ed', 'edit_folders'));
		await t.sync();

		// When
		const folder = await t.client.canCreate(FolderSchema, { id: 'f2', parent: { id: 'f1' } });
		const doc = await t.client.canCreate(DocSchema, { id: 'd1', folder: { id: 'f1' } });

		// Then
		expect(folder).toBe('no');
		expect(doc).toBe('no');
		await t.close();
	});

	it('answers unknown when the client does not hold the parent', async () => {
		// Given a caller who reads and edits folders everywhere, and f1 alone
		const t = await createTestClient({ entities: [ FolderSchema, DocSchema ] });
		t.server.put(FolderSchema, { id: 'f1' });
		t.server.setCallerAccess(atRoot('ed', 'view_folders', 'edit_folders'));
		await t.sync();

		// When, Then: f1 is read, and f9 can be missing or hidden
		expect(await t.client.canCreate(FolderSchema, { id: 'f2', parent: { id: 'f1' } })).toBe('yes');
		expect(
			await t.client.canCreate(FolderSchema, { id: 'f2', parent: { id: 'f9' } }, { detail: true }),
		)
			.toEqual({ answer: 'unknown', reason: 'row_not_held' });
		expect(await t.client.canCreate(DocSchema, { id: 'd1', folder: { id: 'f9' } }, { detail: true }))
			.toEqual({ answer: 'unknown', reason: 'row_not_held' });
		// A folder at the top adds no parent.
		expect(await t.client.canCreate(FolderSchema, { id: 'f3' })).toBe('yes');
		await t.close();
	});

	it('answers unknown for an update that moves the row, as the new parent decides', async () => {
		// Given f2 in f1, and a caller who reads and edits folders everywhere
		const t = await createTestClient({ entities: [ FolderSchema ] });
		t.server.put(FolderSchema, { id: 'f1' });
		t.server.put(FolderSchema, { id: 'f2', parent: { id: 'f1' } });
		t.server.setCallerAccess(atRoot('ed', 'view_folders', 'edit_folders'));
		await t.sync();

		// When
		const rename = await t.client.can(FolderSchema, 'f2', 'update', { fields: [ 'name' ] });
		const move = await t.client.can(FolderSchema, 'f2', 'update', {
			fields: [ 'parent' ],
			detail: true,
		});

		// Then
		expect(rename).toBe('yes');
		expect(move).toEqual({ answer: 'unknown', reason: 'new_value' });
		await t.close();
	});
});

describe('canCreate() for a rule with a within entry, on a row below more than one parent', () => {
	it('needs the entry to hold through each parent on its own', async () => {
		// Given items below two boxes, which a caller creates when it owns the
		// box: a box ann owns and a box bob owns
		const box = entity('box', { id: text().key(), ownerId: text() }, {
			access: { allow: [ { read: true, write: true, requires: 'view_boxes' } ] },
		});
		const item = entity('item', { id: text().key(), a: ref(() => box), b: ref(() => box) }, {
			access: {
				parents: [ 'a', 'b' ],
				allow: [ {
					read: true,
					write: true,
					admitted: true,
					within: within(box, { callerIs: 'ownerId' }),
				} ],
			},
		});
		const bundle = defineSchema(box, item);
		const [ B, I ] = [ 'box', 'item' ].map((name) => bundle.descMessage(name));
		const t = await createTestClient({ entities: [ B!, I! ] });
		t.server.put(B!, { id: 'b1', ownerId: 'ann' });
		t.server.put(B!, { id: 'b2', ownerId: 'bob' });
		t.server.setCallerAccess(atRoot('ann', 'view_boxes'));
		await t.sync();

		// When, Then: an item in b1 and b2 would show to the readers of b2,
		// which ann does not own
		expect(await t.client.canCreate(I!, { id: 'i1', a: { id: 'b1' }, b: { id: 'b1' } })).toBe('yes');
		expect(await t.client.canCreate(I!, { id: 'i1', a: { id: 'b1' }, b: { id: 'b2' } })).toBe('no');
		expect(
			await t.client.canCreate(I!, { id: 'i1', a: { id: 'b1' }, b: { id: 'b9' } }, { detail: true }),
		)
			.toEqual({ answer: 'unknown', reason: 'row_not_held' });
		await t.close();
	});
});

/**
 * Team and TeamMember of the access test schema, with alias `name` of
 * TeamRole inserted before the value that holds `number`, so a stored
 * `number` reads as `name`, which no role declares.
 */
function teamWithRoleAlias(name: string, number: number) {
	const file = file_syncplane_testdata_access_v1_access;
	const proto = clone(FileDescriptorProtoSchema, file.proto);
	const roles = proto.enumType.find((e) => e.name === 'TeamRole')!;
	roles.options = create(EnumOptionsSchema, { allowAlias: true });
	const at = roles.value.findIndex((v) => v.number === number);
	roles.value.splice(at, 0, create(EnumValueDescriptorProtoSchema, { name, number }));
	const registry = createFileRegistry(
		proto,
		(dep) => file.dependencies.find((d) => d.proto.name === dep),
	);
	return [
		registry.getMessage('syncplane.testdata.access.v1.Team')!,
		registry.getMessage('syncplane.testdata.access.v1.TeamMember')!,
	] as const;
}

describe('canCreate() for a grant row whose enum role has an alias', () => {
	it('decides the subset rule with the role declared on the stored number', async () => {
		// Given TEAM_ROLE_MANAGER inserted before TEAM_ROLE_EDITOR (2), and a
		// caller who edits teams everywhere but does not view them
		const [ team, member ] = teamWithRoleAlias('TEAM_ROLE_MANAGER', 2);
		const t = await createTestClient({ entities: [ team, member ] });
		t.server.put(team, { id: 't1' });
		t.server.setCallerAccess(atRoot('ed', 'edit_team'));
		await t.sync();

		// When it asks to grant role 2, which is TEAM_ROLE_EDITOR and grants
		// view_team
		const answer = await t.client.canCreate(member, {
			id: 'tm1',
			team: { id: 't1' },
			userId: 'u1',
			role: 2,
		});

		// Then the row grants more than the caller holds, which the server
		// refuses
		expect(answer).toBe('no');
		await t.close();
	});
});
