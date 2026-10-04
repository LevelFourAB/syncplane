import { create, fromBinary, getOption, hasOption, toBinary } from '@bufbuild/protobuf';
import { describe, expect, expectTypeOf, it } from 'vitest';

import { CrudOp } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	entity as entityExt,
	field_behavior as fieldBehaviorExt,
	FieldWritePolicy,
} from '../../src/gen/syncplane/v1/options_pb.js';
import {
	assertWritePolicy,
	entityDefOf,
	EntityRegistry,
	isRef,
	keyFieldsFor,
	keyOf,
	refKeyFields,
} from '../../src/mutations.js';
import {
	type AccessDecl,
	type AccessRuleDecl,
	bool,
	bytes,
	defineSchema,
	double,
	entitiesFromDescriptorSet,
	entity,
	type EntitySchema,
	type Field,
	type Infer,
	int32,
	int64,
	keyedList,
	list,
	map,
	message,
	ref,
	refList,
	type RoleDecl,
	text,
	workspaceRef,
} from '../../src/schema/builder.js';

// One definition. The inferred type, the runtime descriptor, and the
// register-endpoint bytes all come from this.
const todo = entity('todo', {
	id: text().key(),
	title: text(),
	completed: bool(),
	createdAtMs: int64(),
	tags: list(text()),
	blob: bytes().opaque(),
});

type Todo = Infer<typeof todo>;

describe('schema builder', () => {
	const bundle = defineSchema(todo);

	it('produces a descriptor the existing entityDefOf accepts', () => {
		const desc = bundle.descMessage('todo');
		const def = entityDefOf(desc);
		expect(def.name).toBe('todo');
		expect(def.keyFields.map((f) => f.name)).toEqual([ 'id' ]);
	});

	it('drops into the existing EntityRegistry unchanged', () => {
		const reg = new EntityRegistry(bundle.descMessages());
		expect(reg.names()).toEqual([ 'todo' ]);
		expect(reg.get('todo')?.name).toBe('todo');
	});

	it('snake_cases proto field names but keeps camelCase accessors', () => {
		const desc = bundle.descMessage('todo');
		const byLocal = Object.fromEntries(desc.fields.map((f) => [ f.localName, f.name ]));
		expect(byLocal.createdAtMs).toBe('created_at_ms');
		expect(byLocal.id).toBe('id');
	});

	it('marks the opaque bytes field with (syncplane.v1.field_behavior)', () => {
		const blob = bundle.descMessage('todo').fields.find((f) => f.name === 'blob')!;
		expect(hasOption(blob, fieldBehaviorExt)).toBe(true);
		expect(getOption(blob, fieldBehaviorExt).opaque).toBe(true);
	});

	it('reads which writes the access rules admit onto the EntityDef every local write consults', () => {
		const writeAccessOf = (e: EntitySchema) =>
			entityDefOf(defineSchema(e).descMessage(e.name)).writeAccess;

		// Append-only: creating is admitted, changing and deleting are not.
		const entry = entity('entry', { id: text().key() }, {
			access: { allow: [ { read: true, create: true, permission: 'post' } ] },
		});
		expect(writeAccessOf(entry)).toEqual({ create: true, update: false, delete: false });

		// `write` admits every write operation, whatever the source.
		const note = entity('note', { id: text().key() }, {
			access: { allow: [ { read: true, permission: 'view' }, { write: true, admitted: true } ] },
		});
		expect(writeAccessOf(note)).toEqual({ create: true, update: true, delete: true });

		// Server-owned: no rule admits a write.
		const label = entity('label', { id: text().key() }, {
			access: { allow: [ { read: true, permission: 'view' } ] },
		});
		expect(writeAccessOf(label)).toEqual({ create: false, update: false, delete: false });

		// Rules add up: no single rule here admits both update and delete.
		const comment = entity('comment', { id: text().key() }, {
			access: {
				allow: [
					{ read: true, create: true, permission: 'view' },
					{ update: true, permission: 'edit' },
					{ delete: true, permission: 'moderate' },
				],
			},
		});
		expect(writeAccessOf(comment)).toEqual({ create: true, update: true, delete: true });

		expect(entityDefOf(bundle.descMessage('todo')).writeAccess).toBeUndefined();
	});

	it('lowers .protected() onto every field kind that can carry it', () => {
		const person = entity('person', { id: text().key() });
		const task = entity('task', {
			id: text().key(),
			title: text(),
			status: text().protected(),
			audit: message({ at: text() }).protected(),
			labels: map('string', text()).protected(),
			owner: ref(() => person).protected(),
		});
		const desc = defineSchema(person, task).descMessage('task');
		const policyOf = (name: string) => {
			const field = desc.fields.find((f) => f.name === name)!;
			return hasOption(field, fieldBehaviorExt)
				? getOption(field, fieldBehaviorExt).writePolicy
				: FieldWritePolicy.UNSPECIFIED;
		};

		for (const name of [ 'status', 'audit', 'labels', 'owner' ]) {
			expect(policyOf(name), name).toBe(FieldWritePolicy.TRUSTED_ONLY);
		}
		expect(policyOf('title')).toBe(FieldWritePolicy.UNSPECIFIED);
		expect(entityDefOf(desc).protectedFields).toEqual([ 'status', 'audit', 'labels', 'owner' ]);
	});

	it('refuses a protected key field, whichever order it is declared in', () => {
		expect(() => text().key().protected()).toThrow(/not valid on a key field/);
		expect(() => text().protected().key()).toThrow(/key field cannot be protected/);
	});

	it('round-trips a value through the runtime descriptor', () => {
		const desc = bundle.descMessage('todo');
		const value = create(desc, { id: 't1', title: 'hi', tags: [ 'a', 'b' ] });
		const back = fromBinary(desc, toBinary(desc, value)) as Record<string, unknown>;
		expect(back.title).toBe('hi');
		expect(back.tags).toEqual([ 'a', 'b' ]);
		// The existing key derivation reads it the same as a generated entity.
		expect(keyOf(entityDefOf(desc), back as never)).toBe('t1');
	});

	// The full "server ships schema" path: serialize on one side, rebuild the
	// descriptor purely from bytes on the other — no codegen, no shared imports.
	it('survives a FileDescriptorSet wire round-trip', () => {
		const wire = bundle.toBinary();
		expect(wire.byteLength).toBeGreaterThan(0);

		const [ desc, ...rest ] = entitiesFromDescriptorSet(wire);
		expect(rest).toHaveLength(0);
		const def = entityDefOf(desc!);
		expect(def.name).toBe('todo');
		expect(def.keyFields.map((f) => f.name)).toEqual([ 'id' ]);
	});

	it('rejects an entity with no key field', () => {
		expect(() => entity('bad', { title: text() })).toThrow(/no key field/);
	});

	it('rejects opaque on non-bytes fields', () => {
		expect(() => text().opaque()).toThrow(/singular bytes/);
	});

	// Type-level: the inferred row shape is the real, fully-typed thing — this
	// fails to compile if inference drifts from the declared fields.
	it('infers the row type from the fields', () => {
		const v: Todo = {
			id: 't1',
			title: 'x',
			completed: false,
			createdAtMs: 0n,
			tags: [],
			blob: new Uint8Array(),
		};
		expect(v.id).toBe('t1');
	});
});

// Full parity tier: sub-objects, typed references, keyed/movable lists, maps.
// One schema mirrors syncplane/testdata/v1's Issue/Project/User reference fixtures.
const project = entity('project', { id: text().key(), name: text() });
const user = entity('user', { id: text().key(), name: text() });

// Self-reference escape hatch: name the recursive field map so the row type is
// not inferred from the entity's own (still-being-computed) type.
type IssueFields = {
	id: Field<string>;
	title: Field<string>;
	settings: Field<{ color: string; size: number }>;
	project: Field<Infer<typeof project>>;
	assignee: Field<Infer<typeof user>>;
	reporter: Field<Infer<typeof user>>;
	blockedBy: Field<Infer<EntitySchema<IssueFields>>[]>;
	members: Field<{ uid: string; name: string }[]>;
	items: Field<{ id: string; pos: string; label: string }[]>;
	attrs: Field<Record<string, string>>;
};
const issue: EntitySchema<IssueFields> = entity('issue', {
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

describe('schema builder — full parity', () => {
	const bundle = defineSchema(project, user, issue);
	const Issue = bundle.descMessage('issue');
	const field = (name: string) => Issue.fields.find((f) => f.name === name)!;

	it('synthesizes an embedded sub-message as a nested type', () => {
		const settings = field('settings');
		expect(settings.fieldKind).toBe('message');
		expect(isRef(settings)).toBe(false);
		expect(settings.message!.typeName).toBe('sync.dynamic.Issue.Settings');
		expect(settings.message!.fields.map((f) => f.name)).toEqual([ 'color', 'size' ]);
	});

	it('synthesizes typed references that isRef/refKeyFields agree on', () => {
		const proj = field('project');
		expect(isRef(proj)).toBe(true);
		expect(refKeyFields(proj).map((f) => f.name)).toEqual([ 'id' ]);
		expect(proj.message!.typeName).toBe('sync.dynamic.Project');
		// No field_behavior: ref detection is annotation-free.
		expect(hasOption(proj, fieldBehaviorExt)).toBe(false);
	});

	it('lowers .reverseRelation() onto the reference field', () => {
		expect(getOption(field('reporter'), fieldBehaviorExt).reverseRelation).toBe('reportedIssues');
		expect(hasOption(field('assignee'), fieldBehaviorExt)).toBe(false);
	});

	it('synthesizes a to-many self reference', () => {
		const blocked = field('blocked_by');
		expect(isRef(blocked)).toBe(true);
		expect(blocked.fieldKind).toBe('list');
		expect(blocked.message!.typeName).toBe('sync.dynamic.Issue');
	});

	it('marks keyed and movable lists with (syncplane.v1.field_behavior)', () => {
		const members = field('members');
		expect(getOption(members, fieldBehaviorExt).keyField).toEqual([ 'uid' ]);
		expect(keyFieldsFor(members).map((f) => f.name)).toEqual([ 'uid' ]);

		const items = field('items');
		const fb = getOption(items, fieldBehaviorExt);
		expect(fb.keyField).toEqual([ 'id' ]);
		expect(fb.orderField).toBe('pos');
	});

	it('synthesizes a scalar map the runtime recognizes as a map', () => {
		const attrs = field('attrs');
		expect(attrs.fieldKind).toBe('map');
		const intMap = entity('intmap', { id: text().key(), m: map('int64', text()) });
		const desc = defineSchema(intMap).descMessage('intmap');
		expect(desc.fields.find((f) => f.name === 'm')!.fieldKind).toBe('map');
	});

	it('synthesizes a message-valued map, the shape custom fields take', () => {
		// The value message is a SIBLING nested type of the entry, not a child:
		// a map_entry message is a synthetic shape both descriptor layers
		// pattern-match on.
		const withCustom = entity('withcustom', {
			id: text().key(),
			custom: map('string', message({ text: text(), number: double() })),
		});
		const desc = defineSchema(withCustom).descMessage('withcustom');
		const custom = desc.fields.find((f) => f.name === 'custom')!;
		if (custom.fieldKind !== 'map') throw new Error('custom should be a map field');
		expect(custom.mapKind).toBe('message');
		expect(custom.message!.typeName).toBe('sync.dynamic.Withcustom.CustomValue');
		expect(custom.message!.fields.map((f) => f.name)).toEqual([ 'text', 'number' ]);
	});

	it('carries a map fan-out path through the accessor→proto translation', () => {
		// `*` and the `key`/`value` after it are not field names, so they must
		// survive untouched — the engine would otherwise look for a field
		// literally called "*". Everything else still snake_cases, which is what
		// `sortKey` is here to show.
		const withCustom = entity('withfan', {
			id: text().key(),
			custom: map('string', message({ sortKey: text() })),
		}, {
			indexes: [ {
				name: 'by_value',
				key: [
					{ path: 'custom.*.value.sortKey' },
					{ path: 'custom.*.key' },
				],
			} ],
		});
		const desc = defineSchema(withCustom).descMessage('withfan');
		expect(getOption(desc, entityExt).index[0]!.key.map((k) => k.path)).toEqual([
			'custom.*.value.sort_key',
			'custom.*.key',
		]);
	});

	it('drops only the entities (not nested types) into EntityRegistry', () => {
		const reg = new EntityRegistry(bundle.descMessages());
		expect(reg.names().sort()).toEqual([ 'issue', 'project', 'user' ]);
		// Two refs to user: the one that names no reverse relation takes the
		// name of its field, the other the name it sets.
		const revNames = reg.reverseRefsOf('user').map((r) => r.name).sort();
		expect(revNames).toEqual([ 'issuesByAssignee', 'reportedIssues' ]);
	});

	it('survives a FileDescriptorSet wire round-trip', () => {
		const entities = entitiesFromDescriptorSet(bundle.toBinary());
		expect(entities.map((e) => entityDefOf(e).name).sort()).toEqual([
			'issue',
			'project',
			'user',
		]);
	});

	it('round-trips a value carrying every kind', () => {
		const v = create(Issue, {
			id: 'i1',
			title: 'a bug',
			settings: { color: 'red', size: 2 },
			project: { id: 'p1' },
			members: [ { uid: 'u1', name: 'Ada' } ],
			items: [ { id: 'x', pos: 'a0', label: 'first' } ],
			attrs: { env: 'prod' },
		}) as Record<string, any>;
		const back = fromBinary(Issue, toBinary(Issue, v as never)) as Record<string, any>;
		expect(back.settings.color).toBe('red');
		expect(back.members[0].uid).toBe('u1');
		expect(back.attrs.env).toBe('prod');
	});

	// Type-level assertions: the new factories enrich Infer correctly.
	it('infers rich row types', () => {
		type Iss = Infer<typeof issue>;
		expectTypeOf<Iss['settings']>().toEqualTypeOf<{ color: string; size: number }>();
		expectTypeOf<Iss['project']>().toEqualTypeOf<{ id: string; name: string }>();
		expectTypeOf<Iss['blockedBy']>().toEqualTypeOf<Iss[]>();
		expectTypeOf<Iss['attrs']>().toEqualTypeOf<Record<string, string>>();
	});

	describe('validation', () => {
		it('rejects a message-typed key field', () => {
			expect(() => message({ a: text() }).key()).toThrow(/singular scalar/);
		});
		it('rejects a reference as a key field', () => {
			expect(() => ref(() => project).key()).toThrow(/singular scalar/);
		});
		it('rejects opaque on a reference', () => {
			expect(() => ref(() => project).opaque()).toThrow(/bytes\(\) or message\(\)/);
		});
		it('rejects indexed on a reference', () => {
			// The engine derives an index over every reference field on its own,
			// so the mark has nothing to add — and the lowering has no place to
			// put it, since a reference is detected by its target being an
			// entity rather than by any annotation. Accepting it would drop it.
			expect(() => ref(() => project).indexed()).toThrow(/already indexed/);
		});
		it('rejects reverseRelation on a field that is not a reference', () => {
			expect(() => text().reverseRelation('posts')).toThrow(/only valid on a ref\(\)/);
			expect(() => message({ a: text() }).reverseRelation('posts')).toThrow(/only valid on a ref\(\)/);
		});
		it('rejects a reverse relation name that is not an identifier', () => {
			expect(() => ref(() => project).reverseRelation('owned.issues')).toThrow(
				'reverseRelation(): reverse relation name "owned.issues" is not an identifier',
			);
		});
		it('keeps both marks on a protected reference with a named reverse relation', () => {
			const person = entity('person', { id: text().key() });
			const task = entity('task', {
				id: text().key(),
				owner: ref(() => person).protected().reverseRelation('ownedTasks'),
			});
			const owner = defineSchema(person, task).descMessage('task').fields.find((f) =>
				f.name === 'owner'
			)!;
			const fb = getOption(owner, fieldBehaviorExt);
			expect(fb.writePolicy).toBe(FieldWritePolicy.TRUSTED_ONLY);
			expect(fb.reverseRelation).toBe('ownedTasks');
		});
		it('rejects opaque on a map', () => {
			expect(() => map('string', text()).opaque()).toThrow(/bytes\(\) or message\(\)/);
		});
		it('rejects .order() naming a non-element field', () => {
			expect(() => keyedList(message({ id: text() }), 'id').order('nope')).toThrow(
				/not a field of the list element/,
			);
		});
		it('rejects a keyedList key field that is not on the element', () => {
			expect(() => keyedList(message({ id: text() }), 'missing')).toThrow(/not a field/);
		});
		it('rejects an out-of-schema reference target', () => {
			const orphan = entity('orphan', { id: text().key(), p: ref(() => project) });
			expect(() => defineSchema(orphan)).toThrow(/not part of this schema/);
		});
		it('allows an opaque singular embedded message', () => {
			const e = entity('blobby', { id: text().key(), doc: message({ data: bytes() }).opaque() });
			const desc = defineSchema(e).descMessage('blobby');
			const doc = desc.fields.find((f) => f.name === 'doc')!;
			expect(getOption(doc, fieldBehaviorExt).opaque).toBe(true);
		});
	});
});

describe('schema builder — access model', () => {
	const project = entity('project', { id: text().key() }, {
		access: {
			allow: [
				{ read: true, permission: 'view_projects' },
				{ write: true, permission: 'edit_projects' },
			],
		},
	});
	const member = entity('project_member', {
		id: text().key(),
		project: ref(() => project).accessParent(),
		userId: text(),
		role: text(),
	}, {
		grant: {
			subject: 'userId',
			role: 'role',
			roles: [ { name: 'project_writer', grants: [ 'view_projects', 'edit_projects' ] } ],
		},
		access: {
			allow: [
				{ read: true, permission: 'view_projects' },
				{ write: true, permission: 'edit_projects' },
			],
		},
	});
	const note = entity('note', { id: text().key() }, {
		access: { allow: [ { read: true, write: true, admitted: true } ] },
	});
	const bundle = defineSchema(project, member, note);
	const options = (name: string) => getOption(bundle.descMessage(name), entityExt);

	it('lowers access rules onto the entity option', () => {
		const [ read, write, ...rest ] = options('project').access!.allow;
		expect(rest).toEqual([]);
		expect(read).toMatchObject({
			read: true,
			create: false,
			update: false,
			delete: false,
			write: false,
		});
		expect(read!.source).toEqual({ case: 'permission', value: 'view_projects' });
		expect(write).toMatchObject({
			read: false,
			create: false,
			update: false,
			delete: false,
			write: true,
		});
		expect(write!.source).toEqual({ case: 'permission', value: 'edit_projects' });
	});

	it('lowers an admitted rule onto the source oneof', () => {
		const [ rule, ...rest ] = options('note').access!.allow;
		expect(rest).toEqual([]);
		expect(rule).toMatchObject({ read: true, write: true });
		expect(rule!.source.case).toBe('admitted');
	});

	it('keeps the rules through the serialized descriptor set', () => {
		// AdmittedCaller is an empty message, so only its presence on the wire
		// says the rule has a source.
		const decoded = entitiesFromDescriptorSet(bundle.toBinary()).find((d) =>
			getOption(d, entityExt).name === 'note'
		)!;
		const [ rule ] = getOption(decoded, entityExt).access!.allow;
		expect(rule!.source.case).toBe('admitted');
		expect(rule).toMatchObject({ read: true, write: true });
	});

	it('lowers grant with proto field names', () => {
		const grant = options('project_member').grant!;
		expect(grant.subject).toBe('user_id');
		expect(grant.role).toBe('role');
		expect(grant.roles.map((r) => [ r.name, r.grants ])).toEqual([
			[ 'project_writer', [ 'view_projects', 'edit_projects' ] ],
		]);
	});

	it('lowers role includes', () => {
		const viewer = entity('viewer', {
			id: text().key(),
			project: ref(() => project).accessParent(),
			userId: text(),
			role: text(),
		}, {
			grant: {
				subject: 'userId',
				role: 'role',
				roles: [
					{ name: 'project_viewer', grants: [ 'view_projects' ] },
					{ name: 'project_editor', includes: [ 'project_viewer' ], grants: [ 'edit_projects' ] },
					{ name: 'project_lead', includes: [ 'project_editor' ] },
				],
			},
			access: { allow: [ { read: true, permission: 'view_projects' } ] },
		});
		const roles =
			getOption(defineSchema(project, viewer).descMessage('viewer'), entityExt).grant!.roles;
		expect(roles.map((r) => [ r.name, r.grants, r.includes ])).toEqual([
			[ 'project_viewer', [ 'view_projects' ], [] ],
			[ 'project_editor', [ 'edit_projects' ], [ 'project_viewer' ] ],
			[ 'project_lead', [], [ 'project_editor' ] ],
		]);
	});

	it('lowers members, and a grant whose subject is a ref() to a team', () => {
		const team = entity('team', { id: text().key() }, {
			members: { entity: 'team_member' },
			access: { allow: [ { read: true, write: true, permission: 'view_teams' } ] },
		});
		const teamMember = entity('team_member', {
			id: text().key(),
			team: ref(() => team).accessParent(),
			userId: text(),
			role: text(),
		}, {
			grant: {
				subject: 'userId',
				role: 'role',
				roles: [ { name: 'team_member', grants: [ 'view_teams' ] } ],
			},
			access: { allow: [ { read: true, write: true, permission: 'view_teams' } ] },
		});
		const projectTeam = entity('project_team', {
			id: text().key(),
			project: ref(() => project).accessParent(),
			ownerTeam: ref(() => team),
			role: text(),
		}, {
			grant: {
				subject: 'ownerTeam',
				role: 'role',
				roles: [ { name: 'project_team_writer', grants: [ 'view_projects' ] } ],
			},
			access: { allow: [ { read: true, write: true, permission: 'view_projects' } ] },
		});
		const teams = defineSchema(project, team, teamMember, projectTeam);

		expect(getOption(teams.descMessage('team'), entityExt).members?.entity).toBe('team_member');
		expect(getOption(teams.descMessage('project_team'), entityExt).grant?.subject).toBe('owner_team');
	});

	it('lowers .accessParent() onto the reference field', () => {
		const field = bundle.descMessage('project_member').fields.find((f) => f.name === 'project')!;
		expect(getOption(field, fieldBehaviorExt).accessParent).toBeDefined();
	});

	it('leaves the options unset on an entity that declares none', () => {
		const plain = defineSchema(entity('plain', { id: text().key() })).descMessage('plain');
		expect(getOption(plain, entityExt).access).toBeUndefined();
		expect(getOption(plain, entityExt).grant).toBeUndefined();
	});

	describe('validation', () => {
		const fields = { id: text().key(), userId: text(), role: text() };
		const access: AccessDecl = {
			allow: [ { read: true, permission: 'view' }, { write: true, permission: 'edit' } ],
		};
		const roles = [ { name: 'writer', grants: [ 'view', 'edit' ] } ];
		const allowing = (...allow: AccessRuleDecl[]) => () => entity('e', fields, { access: { allow } });

		it('rejects accessParent() on anything but a singular ref()', () => {
			expect(() => text().accessParent()).toThrow(/only valid on a ref\(\)/);
			expect(() => refList(() => project).accessParent()).toThrow(/only valid on a ref\(\)/);
		});
		it('rejects a permission name that is not an identifier', () => {
			expect(allowing({ read: true, permission: 'view-all' })).toThrow(
				'entity "e" access.allow[0]: permission name "view-all" is not an identifier',
			);
			expect(allowing({ read: true, permission: '' })).toThrow(
				'entity "e" access.allow[0]: permission name is empty',
			);
		});
		it('rejects an access declaration without rules', () => {
			expect(allowing()).toThrow('entity "e": access.allow declares no rules');
			// A declaration without `allow` at all, as plain JS can pass.
			const shapeless = { read: 'view', write: 'edit' } as unknown as AccessDecl;
			expect(() => entity('e', fields, { access: shapeless })).toThrow(
				/access.allow declares no rules/,
			);
		});
		it('rejects a rule that admits no operation', () => {
			expect(allowing({ read: true, permission: 'view' }, { permission: 'edit' })).toThrow(
				'entity "e" access.allow[1]: the rule admits no operation; '
					+ 'set read, create, update, delete or write',
			);
			expect(allowing({ read: false, write: false, admitted: true })).toThrow(
				/access.allow\[0\]: the rule admits no operation/,
			);
		});
		it('rejects a rule without exactly one source', () => {
			// @ts-expect-error a rule needs a source
			const sourceless: AccessRuleDecl = { read: true };
			expect(allowing(sourceless)).toThrow(
				'entity "e" access.allow[0]: the rule has no source; set permission or admitted: true',
			);
			// @ts-expect-error a rule takes one source
			const both: AccessRuleDecl = { read: true, permission: 'view', admitted: true };
			expect(allowing(both)).toThrow(
				'entity "e" access.allow[0]: the rule sets both permission and admitted; set exactly one',
			);
			const unadmitted = { read: true, admitted: false } as unknown as AccessRuleDecl;
			expect(allowing(unadmitted)).toThrow('entity "e" access.allow[0]: admitted must be true');
		});
		it('rejects a rule declared twice', () => {
			expect(allowing(
				{ read: true, permission: 'view' },
				{ write: true, permission: 'edit' },
				{ read: true, create: false, permission: 'view' },
			)).toThrow('entity "e" access.allow[2]: the rule repeats access.allow[0]');
			expect(allowing({ read: true, admitted: true }, { read: true, admitted: true })).toThrow(
				/access.allow\[1\]: the rule repeats access.allow\[0\]/,
			);
		});
		it('accepts rules that differ in operations or source', () => {
			expect(allowing(
				{ read: true, permission: 'view' },
				{ read: true, admitted: true },
				{ read: true, create: true, permission: 'view' },
				{ write: true, permission: 'view' },
			)).not.toThrow();
		});
		it('rejects two access parents', () => {
			expect(() =>
				entity('e', {
					id: text().key(),
					a: ref(() => project).accessParent(),
					b: ref(() => project).accessParent(),
				}, { access })
			).toThrow(/fields "a" and "b" are both marked accessParent\(\)/);
		});
		it('rejects an access parent without access', () => {
			expect(() => entity('e', { id: text().key(), p: ref(() => project).accessParent() })).toThrow(
				'entity "e": field "p" is marked accessParent(), but the entity declares no access',
			);
			// Any access declaration will do, including one admitting every caller.
			expect(() =>
				entity('e', { id: text().key(), p: ref(() => project).accessParent() }, {
					access: { allow: [ { read: true, admitted: true } ] },
				})
			).not.toThrow();
		});
		it('rejects a grant entity without access', () => {
			expect(() => entity('e', fields, { grant: { subject: 'userId', role: 'role', roles } })).toThrow(
				'entity "e": a grant entity must declare access',
			);
		});
		it('rejects a grant field that is not a declared text() field', () => {
			expect(() => entity('e', fields, { access, grant: { subject: 'who', role: 'role', roles } }))
				.toThrow(
					/grant subject field "who" is not declared/,
				);
			expect(() =>
				entity('e', { ...fields, level: int32() }, {
					access,
					grant: { subject: 'userId', role: 'level', roles },
				})
			).toThrow(/grant role field "level" must be a singular text\(\) field/);
		});
		it('rejects a grant subject that is neither text() nor a singular ref()', () => {
			expect(() =>
				entity('e', { ...fields, team: refList(() => project) }, {
					access,
					grant: { subject: 'team', role: 'role', roles },
				})
			).toThrow(/grant subject field "team" must be a singular text\(\) field or a singular ref\(\)/);
			expect(() =>
				entity('e', { ...fields, team: workspaceRef(() => project) }, {
					access,
					grant: { subject: 'team', role: 'role', roles },
				})
			).toThrow(/grant subject field "team" must be a singular text\(\) field or a singular ref\(\)/);
		});
		it('rejects members without access or without an entity', () => {
			expect(() => entity('e', fields, { members: { entity: 'm' } })).toThrow(
				'entity "e": an entity with members must declare access',
			);
			expect(() => entity('e', fields, { access, members: { entity: '' } })).toThrow(
				'entity "e": members names no entity',
			);
		});
		it('rejects a role declared twice or granting a permission twice', () => {
			expect(() =>
				entity('e', fields, {
					access,
					grant: { subject: 'userId', role: 'role', roles: [ ...roles, ...roles ] },
				})
			).toThrow(/declares role "writer" twice/);
			expect(() =>
				entity('e', fields, {
					access,
					grant: {
						subject: 'userId',
						role: 'role',
						roles: [ { name: 'w', grants: [ 'edit', 'edit' ] } ],
					},
				})
			).toThrow(/grant role "w" grants "edit" twice/);
		});
		it('accepts a role that includes another role, with or without grants of its own', () => {
			const ladder: RoleDecl[] = [
				{ name: 'reader', grants: [ 'view' ] },
				{ name: 'writer', includes: [ 'reader' ], grants: [ 'view', 'edit' ] },
				{ name: 'member', includes: [ 'writer' ] },
			];
			expect(() =>
				entity('e', fields, { access, grant: { subject: 'userId', role: 'role', roles: ladder } })
			)
				.not.toThrow();
		});
		it('rejects an include of an undeclared role, a repeated include and an include cycle', () => {
			const granting = (...roles: RoleDecl[]) => () =>
				entity('e', fields, { access, grant: { subject: 'userId', role: 'role', roles } });
			expect(granting({ name: 'w', includes: [ 'r' ] })).toThrow(
				'entity "e": grant role "w" includes "r", which is not a role of this grant',
			);
			expect(granting({ name: 'r', grants: [ 'view' ] }, { name: 'w', includes: [ 'r', 'r' ] }))
				.toThrow(
					'entity "e": grant role "w" includes "r" twice',
				);
			expect(granting({ name: 'w', includes: [ 'w' ] })).toThrow(
				'entity "e": grant role "w" includes itself through w → w',
			);
			expect(granting(
				{ name: 'a', grants: [ 'view' ], includes: [ 'b' ] },
				{ name: 'b', includes: [ 'c' ] },
				{ name: 'c', includes: [ 'b' ] },
			)).toThrow('entity "e": grant role "b" includes itself through b → c → b');
		});
		it('rejects a role that neither grants nor includes', () => {
			// @ts-expect-error a role declares grants or includes
			const bare: RoleDecl = { name: 'w' };
			expect(bare.name).toBe('w');
			const empty = { name: 'w', grants: [] };
			expect(() =>
				entity('e', fields, { access, grant: { subject: 'userId', role: 'role', roles: [ empty ] } })
			).toThrow('entity "e": grant role "w" grants no permissions');
		});
	});
});

describe('schema builder — owner rules', () => {
	const room = entity('room', { id: text().key() }, {
		access: { allow: [ { read: true, write: true, permission: 'view' } ] },
	});
	const post = entity('post', {
		id: text().key(),
		room: ref(() => room).accessParent(),
		authorId: text().creator(),
		body: text(),
		title: text(),
	}, {
		access: {
			allow: [
				{ read: true, permission: 'view' },
				{ create: true, delete: true, permission: 'post', subject: 'authorId' },
				{ update: true, permission: 'post', subject: 'authorId', fields: [ 'title', 'body' ] },
			],
		},
	});
	const bundle = defineSchema(room, post);

	it('lowers subject and fields with proto field names, the fields sorted on the server', () => {
		const [ , create, update ] = getOption(bundle.descMessage('post'), entityExt).access!.allow;
		expect(create).toMatchObject({ create: true, delete: true, subject: 'author_id', fields: [] });
		expect(update).toMatchObject({ update: true, subject: 'author_id', fields: [ 'title', 'body' ] });
	});

	it('lowers .creator() onto the field', () => {
		const field = bundle.descMessage('post').fields.find((f) => f.name === 'author_id')!;
		expect(getOption(field, fieldBehaviorExt).creator).toBeDefined();
	});

	it('reads the rules and the creator field onto the EntityDef', () => {
		const def = entityDefOf(bundle.descMessage('post'));
		expect(def.creatorFields.map((f) => f.name)).toEqual([ 'author_id' ]);
		expect(def.protectedFields).toEqual([ 'author_id' ]);
		expect(def.accessRules![2]).toMatchObject({
			update: true,
			permission: 'post',
			fields: [ 'body', 'title' ],
		});
		expect(def.accessRules![2]!.subject?.name).toBe('author_id');
		expect(def.writeAccess).toEqual({
			create: true,
			update: true,
			delete: true,
			updateFields: [ [ 'body', 'title' ] ],
		});
	});

	it('refuses locally a patch whose mask no fields list covers', () => {
		const def = entityDefOf(bundle.descMessage('post'));
		expect(() => assertWritePolicy(def, CrudOp.PATCH, undefined, [ 'body' ])).not.toThrow();
		expect(() => assertWritePolicy(def, CrudOp.PATCH, undefined, [ 'id', 'title' ])).not.toThrow();
		expect(() => assertWritePolicy(def, CrudOp.PATCH, undefined, [ 'room' ])).toThrow(
			'entity "post": no access rule admits an update of "room" together, so a patch masking them is refused',
		);
	});

	describe('validation', () => {
		const fields = { id: text().key(), ownerId: text(), count: int32(), note: text() };
		const allowing = (...allow: AccessRuleDecl[]) => () => entity('e', fields, { access: { allow } });

		it('rejects creator() on anything but a singular text() that is not a key or protected', () => {
			expect(() => int32().creator()).toThrow(/only valid on a singular text\(\) field/);
			expect(() => text().key().creator()).toThrow(/not valid on a key field/);
			expect(() => text().creator().key()).toThrow(/cannot be creator\(\)/);
			expect(() => text().protected().creator()).toThrow(/not valid on a protected\(\) field/);
			expect(() => text().creator().protected()).toThrow(/not valid on a creator\(\) field/);
		});
		it('rejects a subject that is not a declared text() field', () => {
			expect(allowing({ read: true, permission: 'p', subject: 'who' })).toThrow(
				'entity "e" access.allow[0]: subject field "who" is not declared',
			);
			expect(allowing({ read: true, permission: 'p', subject: 'count' })).toThrow(
				'entity "e" access.allow[0]: subject field "count" must be a singular text() field',
			);
		});
		it('rejects fields on a rule that names another operation than update', () => {
			expect(allowing({ update: true, delete: true, permission: 'p', fields: [ 'note' ] })).toThrow(
				'entity "e" access.allow[0]: fields is valid only on a rule whose one operation is update',
			);
			expect(allowing({ write: true, permission: 'p', fields: [ 'note' ] })).toThrow(
				/fields is valid only/,
			);
		});
		it('rejects a fields list that is empty, names an unknown or key field, or repeats one', () => {
			expect(allowing({ update: true, permission: 'p', fields: [] })).toThrow(/fields lists no field/);
			expect(allowing({ update: true, permission: 'p', fields: [ 'text' ] })).toThrow(
				/fields names "text", which is not declared/,
			);
			expect(allowing({ update: true, permission: 'p', fields: [ 'id' ] })).toThrow(
				/fields names key field "id"/,
			);
			expect(allowing({ update: true, permission: 'p', fields: [ 'note', 'note' ] })).toThrow(
				/fields names "note" twice/,
			);
		});
		it('treats rules that differ only in the order of their fields as the same rule', () => {
			expect(allowing(
				{ update: true, permission: 'p', fields: [ 'note', 'count' ] },
				{ update: true, permission: 'p', fields: [ 'count', 'note' ] },
			)).toThrow('entity "e" access.allow[1]: the rule repeats access.allow[0]');
			expect(allowing(
				{ read: true, permission: 'p', subject: 'ownerId' },
				{ read: true, permission: 'p' },
			)).not.toThrow();
		});
	});
});
