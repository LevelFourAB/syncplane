import { create, fromBinary, getOption, hasOption, toBinary } from '@bufbuild/protobuf';
import { describe, expect, expectTypeOf, it } from 'vitest';

import { CrudOp } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	entity as entityExt,
	field_behavior as fieldBehaviorExt,
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
import { condFromProto } from '../../src/query/proto.js';
import { and, or } from '../../src/query/proxy.js';
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
	type FieldGuardDecl,
	type Fields,
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
	within,
	type WithinDecl,
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
			access: { allow: [ { read: true, create: true, requires: 'post' } ] },
		});
		expect(writeAccessOf(entry)).toEqual({ create: true, update: false, delete: false });

		// `write` admits every write operation, whatever the source.
		const note = entity('note', { id: text().key() }, {
			access: { allow: [ { read: true, requires: 'view' }, { write: true, admitted: true } ] },
		});
		expect(writeAccessOf(note)).toEqual({ create: true, update: true, delete: true });

		// Server-owned: no rule admits a write.
		const label = entity('label', { id: text().key() }, {
			access: { allow: [ { read: true, requires: 'view' } ] },
		});
		expect(writeAccessOf(label)).toEqual({ create: false, update: false, delete: false });

		// Rules add up: no single rule here admits both update and delete.
		const comment = entity('comment', { id: text().key() }, {
			access: {
				allow: [
					{ read: true, create: true, requires: 'view' },
					{ update: true, requires: 'edit' },
					{ delete: true, requires: 'moderate' },
				],
			},
		});
		expect(writeAccessOf(comment)).toEqual({ create: true, update: true, delete: true });

		expect(entityDefOf(bundle.descMessage('todo')).writeAccess).toBeUndefined();
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
				{ read: true, requires: 'view_projects' },
				{ write: true, requires: 'edit_projects' },
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
				{ read: true, requires: 'view_projects' },
				{ write: true, requires: 'edit_projects' },
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
		expect(read!.requires).toEqual([ 'view_projects' ]);
		expect(read!.admitted).toBeUndefined();
		expect(write).toMatchObject({
			read: false,
			create: false,
			update: false,
			delete: false,
			write: true,
		});
		expect(write!.requires).toEqual([ 'edit_projects' ]);
	});

	it('lowers an admitted rule without permissions', () => {
		const [ rule, ...rest ] = options('note').access!.allow;
		expect(rest).toEqual([]);
		expect(rule).toMatchObject({ read: true, write: true, requires: [] });
		expect(rule!.admitted).toBeDefined();
	});

	it('keeps the rules through the serialized descriptor set', () => {
		// AdmittedCaller is an empty message, so only its presence on the wire
		// says the rule has a source.
		const decoded = entitiesFromDescriptorSet(bundle.toBinary()).find((d) =>
			getOption(d, entityExt).name === 'note'
		)!;
		const [ rule ] = getOption(decoded, entityExt).access!.allow;
		expect(rule!.admitted).toBeDefined();
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
			access: { allow: [ { read: true, requires: 'view_projects' } ] },
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
			access: { allow: [ { read: true, write: true, requires: 'view_teams' } ] },
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
			access: { allow: [ { read: true, write: true, requires: 'view_teams' } ] },
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
			access: { allow: [ { read: true, write: true, requires: 'view_projects' } ] },
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
		const access = {
			allow: [ { read: true, requires: 'view' }, { write: true, requires: 'edit' } ],
		} satisfies AccessDecl;
		const roles = [ { name: 'writer', grants: [ 'view', 'edit' ] } ];
		// Untyped on purpose: these cases check what entity() throws at run time.
		const allowing = (...allow: AccessRuleDecl[]) => () =>
			entity('e', fields as Fields, { access: { allow } });

		it('rejects accessParent() on anything but a ref() or a refList()', () => {
			expect(() => text().accessParent()).toThrow(/only valid on a ref\(\) or refList\(\)/);
			expect(() => refList(() => project).accessParent()).not.toThrow();
		});
		it('rejects a permission name that is not an identifier', () => {
			expect(allowing({ read: true, requires: 'view-all' })).toThrow(
				'entity "e" access.allow[0] requires: permission name "view-all" is not an identifier',
			);
			expect(allowing({ read: true, requires: '' })).toThrow(
				'entity "e" access.allow[0] requires: permission name is empty',
			);
		});
		it('rejects an access declaration without rules', () => {
			expect(allowing()).toThrow('entity "e": access.allow declares no rules');
			// A declaration without `allow` at all, as plain JS can pass.
			const shapeless = { read: 'view', write: 'edit' } as unknown as AccessDecl;
			expect(() => entity('e', fields as Fields, { access: shapeless })).toThrow(
				/access.allow declares no rules/,
			);
		});
		it('rejects a rule that admits no operation', () => {
			expect(allowing({ read: true, requires: 'view' }, { requires: 'edit' })).toThrow(
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
				'entity "e" access.allow[0]: the rule has no source; set requires or admitted: true',
			);
			// @ts-expect-error a rule takes one source
			const both: AccessRuleDecl = { read: true, requires: 'view', admitted: true };
			expect(allowing(both)).toThrow(
				'entity "e" access.allow[0]: the rule sets both requires and admitted; set exactly one',
			);
			const unadmitted = { read: true, admitted: false } as unknown as AccessRuleDecl;
			expect(allowing(unadmitted)).toThrow('entity "e" access.allow[0]: admitted must be true');
		});
		it('rejects a rule declared twice', () => {
			expect(allowing(
				{ read: true, requires: 'view' },
				{ write: true, requires: 'edit' },
				{ read: true, create: false, requires: 'view' },
			)).toThrow('entity "e" access.allow[2]: the rule repeats access.allow[0]');
			expect(allowing({ read: true, admitted: true }, { read: true, admitted: true })).toThrow(
				/access.allow\[1\]: the rule repeats access.allow\[0\]/,
			);
		});
		it('accepts rules that differ in operations or source', () => {
			expect(allowing(
				{ read: true, requires: 'view' },
				{ read: true, admitted: true },
				{ read: true, create: true, requires: 'view' },
				{ write: true, requires: 'view' },
			)).not.toThrow();
		});
		it('accepts several access parents', () => {
			expect(() =>
				entity('e', {
					id: text().key(),
					a: ref(() => project).accessParent(),
					b: refList(() => project).accessParent(),
				}, { access })
			).not.toThrow();
		});
		it('rejects a grant entity with more than one access parent', () => {
			expect(() =>
				entity('e', {
					...fields,
					a: ref(() => project).accessParent(),
					b: ref(() => project).accessParent(),
				}, { access, grant: { subject: 'userId', role: 'role', roles } })
			).toThrow('entity "e": a grant entity has one access parent');
			expect(() =>
				entity('e', { ...fields, a: refList(() => project).accessParent() }, {
					access,
					grant: { subject: 'userId', role: 'role', roles },
				})
			).toThrow('entity "e": a grant entity has one access parent');
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
			// @ts-expect-error not a field of the entity
			expect(() => entity('e', fields, { access, grant: { subject: 'who', role: 'role', roles } }))
				.toThrow(
					/grant subject field "who" is not declared/,
				);
			expect(() =>
				entity('e', { ...fields, level: int32() }, {
					access,
					// @ts-expect-error a role is a text() field
					grant: { subject: 'userId', role: 'level', roles },
				})
			).toThrow(/grant role field "level" must be a singular text\(\) field/);
		});
		it('rejects a grant subject that is neither text() nor a singular ref()', () => {
			expect(() =>
				entity('e', { ...fields, team: refList(() => project) }, {
					access,
					// @ts-expect-error a subject is a text() field or a singular ref()
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
		access: { allow: [ { read: true, write: true, requires: 'view' } ] },
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
				{ read: true, requires: 'view' },
				{ create: true, delete: true, requires: 'post', callerIs: 'authorId' },
				{ update: true, requires: 'post', callerIs: 'authorId', fields: [ 'title', 'body' ] },
			],
		},
	});
	const bundle = defineSchema(room, post);

	it('lowers callerIs and fields with proto field names, the fields sorted on the server', () => {
		const [ , create, update ] = getOption(bundle.descMessage('post'), entityExt).access!.allow;
		expect(create).toMatchObject({ create: true, delete: true, callerIs: 'author_id', fields: [] });
		expect(update).toMatchObject({
			update: true,
			callerIs: 'author_id',
			fields: [ 'title', 'body' ],
		});
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
			requires: [ 'post' ],
			fields: [ 'body', 'title' ],
		});
		expect(def.accessRules![2]!.callerIs?.name).toBe('author_id');
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
		// Untyped on purpose: these cases check what entity() throws at run time.
		const allowing = (...allow: AccessRuleDecl[]) => () =>
			entity('e', fields as Fields, { access: { allow } });

		it('rejects creator() on anything but a singular text() that is not a key', () => {
			expect(() => int32().creator()).toThrow(/only valid on a singular text\(\) field/);
			expect(() => text().key().creator()).toThrow(/not valid on a key field/);
			expect(() => text().creator().key()).toThrow(/cannot be creator\(\)/);
		});
		it('rejects a callerIs that is not a declared text() field', () => {
			expect(allowing({ read: true, requires: 'p', callerIs: 'who' })).toThrow(
				'entity "e" access.allow[0]: callerIs field "who" is not declared',
			);
			expect(allowing({ read: true, requires: 'p', callerIs: 'count' })).toThrow(
				'entity "e" access.allow[0]: callerIs field "count" must be a singular text() field',
			);
		});
		it('rejects fields on a rule that names another operation than update', () => {
			expect(allowing({ update: true, delete: true, requires: 'p', fields: [ 'note' ] })).toThrow(
				'entity "e" access.allow[0]: fields is valid only on a rule whose one operation is update',
			);
			expect(allowing({ write: true, requires: 'p', fields: [ 'note' ] })).toThrow(
				/fields is valid only/,
			);
		});
		it('rejects a fields list that is empty, names an unknown or key field, or repeats one', () => {
			expect(allowing({ update: true, requires: 'p', fields: [] })).toThrow(/fields lists no field/);
			expect(allowing({ update: true, requires: 'p', fields: [ 'text' ] })).toThrow(
				/fields names "text", which is not declared/,
			);
			expect(allowing({ update: true, requires: 'p', fields: [ 'id' ] })).toThrow(
				/fields names key field "id"/,
			);
			expect(allowing({ update: true, requires: 'p', fields: [ 'note', 'note' ] })).toThrow(
				/fields names "note" twice/,
			);
		});
		it('treats rules that differ only in the order of their fields as the same rule', () => {
			expect(allowing(
				{ update: true, requires: 'p', fields: [ 'note', 'count' ] },
				{ update: true, requires: 'p', fields: [ 'count', 'note' ] },
			)).toThrow('entity "e" access.allow[1]: the rule repeats access.allow[0]');
			expect(allowing(
				{ read: true, requires: 'p', callerIs: 'ownerId' },
				{ read: true, requires: 'p' },
			)).not.toThrow();
		});
	});
});

describe('schema builder — when checks', () => {
	const room = entity('room', { id: text().key() }, {
		access: { allow: [ { read: true, write: true, requires: 'view' } ] },
	});
	const thread = entity('thread', {
		id: text().key(),
		room: ref(() => room).accessParent(),
		isArchived: bool(),
		meta: message({ lockState: text() }),
		tags: map('string', text()),
	}, {
		access: {
			allow: [
				{ read: true, requires: 'view' },
				{
					update: true,
					requires: 'view',
					when: (t) => and(t.isArchived.eq(false), t.meta.lockState.neq('locked'), t.room.id.eq('r1')),
				},
				{ delete: true, requires: 'view', when: (t) => t.tags.at('pinKey').isNotSet() },
			],
		},
	});
	const bundle = defineSchema(room, thread);

	it('lowers the condition with proto field names, and an entry key as written', () => {
		const [ , update, del ] = getOption(bundle.descMessage('thread'), entityExt).access!.allow;
		expect(condFromProto(update!.when!)).toEqual({
			kind: 'and',
			conds: [
				{ kind: 'cmp', path: [ 'is_archived' ], op: 'eq', value: false },
				{ kind: 'cmp', path: [ 'meta', 'lock_state' ], op: 'neq', value: 'locked' },
				{ kind: 'cmp', path: [ 'room', 'id' ], op: 'eq', value: 'r1' },
			],
		});
		expect(condFromProto(del!.when!)).toEqual({
			kind: 'present',
			path: [ 'tags', 'pinKey' ],
			op: 'isNotSet',
		});
	});

	it('reads the condition onto the EntityDef', () => {
		const def = entityDefOf(bundle.descMessage('thread'));
		expect(def.accessRules![0]!.when).toBeUndefined();
		expect(def.accessRules![1]!.when).toBeDefined();
	});

	describe('validation', () => {
		const fields = { id: text().key(), ref: ref(() => room), note: text() };
		const allowing = (...allow: AccessRuleDecl<typeof fields>[]) => () =>
			entity('e', fields, { access: { allow } });

		it('rejects a condition that reads another row', () => {
			expect(
				allowing({ read: true, requires: 'p', when: (e) => e.ref.matches((r) => r.id.eq('x')) }),
			)
				.toThrow('entity "e" access.allow[0]: when uses matches(), which reads another row');
		});
		it('rejects a match condition', () => {
			expect(allowing({ read: true, requires: 'p', when: (e) => or(e.note.eq('x'), e.match('y')) }))
				.toThrow('entity "e" access.allow[0]: when uses match(), which cannot decide access');
		});
		it('tells apart rules that differ only in their condition', () => {
			expect(allowing(
				{ read: true, requires: 'p', when: (e) => e.note.eq('a') },
				{ read: true, requires: 'p', when: (e) => e.note.eq('b') },
				{ read: true, requires: 'p' },
			)).not.toThrow();
			expect(allowing(
				{ read: true, requires: 'p', when: (e) => e.note.eq('a') },
				{ read: true, requires: 'p', when: (e) => e.note.eq('a') },
			)).toThrow('entity "e" access.allow[1]: the rule repeats access.allow[0]');
		});
	});
});

describe('schema builder — field guards', () => {
	const project = entity('project', { id: text().key() }, {
		access: { allow: [ { read: true, write: true, requires: 'edit_tasks' } ] },
	});
	const expense = entity('expense', {
		id: text().key(),
		project: ref(() => project).accessParent(),
		title: text(),
		status: text(),
		total: int64(),
		reporterId: text().creator(),
		notes: text(),
		estimate: int32(),
		closed: bool(),
	}, {
		access: {
			allow: [ { read: true, requires: 'view_tasks' }, { write: true, requires: 'edit_tasks' } ],
			guard: [
				{ fields: [ 'total' ], allow: [ { read: true, admitted: true } ] },
				{
					fields: [ 'status' ],
					allow: [ { read: true, admitted: true }, { write: true, requires: 'edit_projects' } ],
				},
				{
					fields: [ 'notes' ],
					allow: [ { read: true, admitted: true }, {
						write: true,
						requires: 'edit_tasks',
						callerIs: 'reporterId',
					} ],
				},
				{
					fields: [ 'estimate' ],
					allow: [
						{ read: true, admitted: true },
						{ create: true, requires: 'edit_tasks' },
						{ update: true, requires: 'edit_tasks', when: (e) => e.closed.eq(false) },
					],
				},
			],
		},
	});
	const bundle = defineSchema(project, expense);

	it('lowers each guard with proto field names', () => {
		const guards = getOption(bundle.descMessage('expense'), entityExt).access!.guard;
		expect(guards.map((g) => g.fields)).toEqual([ [ 'total' ], [ 'status' ], [ 'notes' ], [
			'estimate',
		] ]);
		expect(guards[0]!.allow).toHaveLength(1);
		expect(guards[0]!.allow[0]).toMatchObject({ read: true });
		expect(guards[0]!.allow[0]!.admitted).toBeDefined();
		expect(guards[2]!.allow[1]).toMatchObject({
			write: true,
			requires: [ 'edit_tasks' ],
			callerIs: 'reporter_id',
		});
		expect(condFromProto(guards[3]!.allow[2]!.when!)).toEqual({
			kind: 'cmp',
			path: [ 'closed' ],
			op: 'eq',
			value: false,
		});
	});

	it('reads the guards onto the EntityDef', () => {
		const def = entityDefOf(bundle.descMessage('expense'));
		expect(def.guards.map((g) => g.units)).toEqual([ [ 'total' ], [ 'status' ], [ 'notes' ], [
			'estimate',
		] ]);
		expect(def.guards[1]!.rules[1]).toMatchObject({ create: true, update: true, delete: false });
		// A guard with no update rule keeps its fields after the create, as a
		// creator field does; one with no create rule clears them on a create.
		expect(def.protectedFields).toEqual([ 'reporter_id', 'total' ]);
		expect(def.clearedOnCreate).toEqual([ 'total' ]);
	});

	it('refuses locally a patch of a field that no guard rule admits updating', () => {
		const def = entityDefOf(bundle.descMessage('expense'));
		expect(() => assertWritePolicy(def, CrudOp.PATCH, undefined, [ 'total' ])).toThrow(
			'entity "expense": field "total" may not be written by a client',
		);
		// The server decides a guard that has an update rule.
		expect(() => assertWritePolicy(def, CrudOp.PATCH, undefined, [ 'status' ])).not.toThrow();
		expect(() => assertWritePolicy(def, CrudOp.PATCH, undefined, [ 'title' ])).not.toThrow();
	});

	describe('validation', () => {
		const fields = {
			id: text().key(),
			parent: ref(() => project).accessParent(),
			authorId: text().creator(),
			ownerId: text(),
			total: int64(),
			note: text(),
		};
		const guarding = (...guard: FieldGuardDecl<typeof fields>[]) => () =>
			entity('e', fields, {
				access: { allow: [ { read: true, write: true, admitted: true } ], guard },
			});
		const read = { read: true, admitted: true } as const;

		it('accepts a guard that admits reading and no write', () => {
			expect(guarding({ fields: [ 'total' ], allow: [ read ] })).not.toThrow();
		});
		it('rejects a guard without fields', () => {
			expect(guarding({ fields: [], allow: [ read ] })).toThrow(
				'entity "e" access.guard[0]: names no field; list the fields it restricts',
			);
		});
		it('rejects a field that cannot be guarded', () => {
			expect(guarding({ fields: [ 'id' ], allow: [ read ] })).toThrow(
				'entity "e" access.guard[0]: fields names key field "id"',
			);
			expect(guarding({ fields: [ 'parent' ], allow: [ read ] })).toThrow(
				'entity "e" access.guard[0]: names "parent", which is an access parent',
			);
			expect(guarding({ fields: [ 'authorId' ], allow: [ read ] })).toThrow(
				'entity "e" access.guard[0]: names "authorId", which is a creator field',
			);
			expect(guarding({ fields: [ 'nope' ] as never, allow: [ read ] })).toThrow(
				'entity "e" access.guard[0]: fields names "nope", which is not declared',
			);
		});
		it('rejects a field that two guards name', () => {
			expect(
				guarding({ fields: [ 'total' ], allow: [ read ] }, {
					fields: [ 'note', 'total' ],
					allow: [ read ],
				}),
			)
				.toThrow('entity "e" access.guard[1] and access.guard[0] both name "total"');
		});
		it('accepts a guard without a read rule, and read rules with a source or a check', () => {
			expect(guarding({ fields: [ 'total' ], allow: [ { update: true, requires: 'p' } ] })).not
				.toThrow();
			expect(guarding({ fields: [ 'total' ], allow: [] })).not.toThrow();
			expect(guarding({ fields: [ 'total' ], allow: [ { read: true, write: true, requires: 'p' } ] }))
				.not.toThrow();
			expect(guarding({ fields: [ 'total' ], allow: [ { ...read, callerIs: 'ownerId' } ] })).not
				.toThrow();
		});
		it('rejects hiding a field that a rule of the entity checks', () => {
			const hiding = (rule: AccessRuleDecl<typeof fields>) => () =>
				entity('e', fields, {
					access: {
						allow: [ { read: true, write: true, admitted: true }, rule ],
						guard: [ { fields: [ 'ownerId', 'note' ], allow: [ { read: true, requires: 'p' } ] } ],
					},
				});
			expect(hiding({ read: true, admitted: true, callerIs: 'ownerId' })).toThrow(
				'entity "e" access.allow[1]: reads "owner_id", which a guard can hide',
			);
			expect(hiding({ update: true, requires: 'p', when: (e) => e.note.eq('x') })).toThrow(
				'entity "e" access.allow[1]: reads "note", which a guard can hide',
			);
			// A guard that every reader of the row reads through hides nothing.
			expect(() =>
				entity('e', fields, {
					access: {
						allow: [ { read: true, write: true, admitted: true, callerIs: 'ownerId' } ],
						guard: [ { fields: [ 'ownerId' ], allow: [ read ] } ],
					},
				})
			).not.toThrow();
		});
		it('rejects hiding the subject or the role of a grant entity', () => {
			const granting = (hidden: 'userId' | 'role') => () =>
				entity('member', { id: text().key(), userId: text(), role: text() }, {
					grant: { subject: 'userId', role: 'role', roles: [ { name: 'r', grants: [ 'p' ] } ] },
					access: {
						allow: [ { read: true, write: true, requires: 'p' } ],
						guard: [ { fields: [ hidden ], allow: [ { read: true, requires: 'p' } ] } ],
					},
				});
			expect(granting('userId')).toThrow(
				'entity "member": grant field "userId" is restricted by a guard that can hide it',
			);
			expect(granting('role')).toThrow('entity "member": grant field "role" is restricted by a guard');
		});
		it('rejects a guard rule with delete or fields', () => {
			expect(
				guarding({ fields: [ 'total' ], allow: [ read, { delete: true, requires: 'p' } as never ] }),
			)
				.toThrow('entity "e" access.guard[0].allow[1]: names delete');
			expect(
				guarding({
					fields: [ 'total' ],
					allow: [ read, { update: true, requires: 'p', fields: [ 'note' ] } as never ],
				}),
			).toThrow('entity "e" access.guard[0].allow[1]: names fields');
		});
		it('rejects a repeated guard rule', () => {
			expect(guarding({
				fields: [ 'total' ],
				allow: [ read, { update: true, requires: 'p' }, { update: true, requires: 'p' } ],
			})).toThrow(
				'entity "e" access.guard[0].allow[2]: the rule repeats entity "e" access.guard[0].allow[1]',
			);
		});
		it('rejects a guard rule whose checks read a guarded field', () => {
			expect(guarding(
				{ fields: [ 'ownerId' ], allow: [ read ] },
				{ fields: [ 'total' ], allow: [ read, { update: true, requires: 'p', callerIs: 'ownerId' } ] },
			)).toThrow('entity "e" access.guard[1].allow[1]: reads "owner_id", which a guard restricts');
			expect(guarding(
				{ fields: [ 'note' ], allow: [ read ] },
				{
					fields: [ 'total' ],
					allow: [ read, { update: true, requires: 'p', when: (e) => e.note.eq('x') } ],
				},
			)).toThrow('entity "e" access.guard[1].allow[1]: reads "note", which a guard restricts');
		});
	});
});

describe('schema builder — within entries', () => {
	const project = entity('project', {
		id: text().key(),
		isPrivate: bool(),
		ownerId: text(),
	}, {
		access: { allow: [ { read: true, requires: 'view_tasks' } ] },
	});
	const task = entity('task', {
		id: text().key(),
		project: ref(() => project).accessParent(),
	}, {
		access: {
			allow: [
				{ read: true, requires: 'view_tasks' },
				{
					read: true,
					admitted: true,
					within: [
						within(project, { when: (p) => p.isPrivate.eq(false) }),
						{ entity: 'project', callerIs: 'ownerId' },
					],
				},
			],
		},
	});

	it('lowers each entry with proto field names, and reads it onto the EntityDef', () => {
		const bundle = defineSchema(project, task);
		const within = getOption(bundle.descMessage('task'), entityExt).access!.allow[1]!.within;
		expect(within.map((w) => [ w.entity, w.callerIs ])).toEqual([ [ 'project', '' ], [
			'project',
			'owner_id',
		] ]);
		expect(condFromProto(within[0]!.when!)).toEqual({
			kind: 'cmp',
			path: [ 'is_private' ],
			op: 'eq',
			value: false,
		});
		const def = entityDefOf(bundle.descMessage('task'));
		expect(def.accessRules![0]!.within).toEqual([]);
		expect(def.accessRules![1]!.within.map((w) => w.callerIs)).toEqual([ undefined, 'owner_id' ]);
	});

	describe('validation', () => {
		const fields = { id: text().key(), project: ref(() => project).accessParent() };
		const allowing = (...within: WithinDecl[]) => () =>
			defineSchema(
				project,
				entity('e', fields, { access: { allow: [ { read: true, requires: 'p', within } ] } }),
			);

		it('rejects an entry without a check', () => {
			expect(allowing({ entity: 'project' }))
				.toThrow('entity "e" access.allow[0] within[0]: sets no check; set callerIs or when');
		});
		it('rejects an entry that repeats another', () => {
			expect(
				allowing({ entity: 'project', callerIs: 'ownerId' }, {
					entity: 'project',
					callerIs: 'ownerId',
				}),
			)
				.toThrow('entity "e" access.allow[0] within[1]: repeats an earlier entry');
		});
		it('rejects an entity outside the schema', () => {
			expect(allowing({ entity: 'nothing', callerIs: 'ownerId' }))
				.toThrow(
					'entity "e" access.allow[0] within[0]: names entity "nothing", which is not part of this schema',
				);
		});
		it('rejects a callerIs field that is not a text() field of the entity', () => {
			expect(allowing({ entity: 'project', callerIs: 'missing' }))
				.toThrow('callerIs field "missing" is not declared on entity "project"');
			expect(allowing({ entity: 'project', callerIs: 'isPrivate' }))
				.toThrow('callerIs field "isPrivate" must be a singular text() field');
		});
		it('rejects a condition that cannot decide access', () => {
			expect(allowing(within(project, { when: (p) => or(p.ownerId.eq('x'), p.match('y')) })))
				.toThrow('entity "e" access.allow[0] within[0]: when uses match(), which cannot decide access');
		});
		it('rejects a thunk that returns an entity outside the schema, or nothing', () => {
			const other = entity('other', { id: text().key(), ownerId: text() }, {
				access: { allow: [ { read: true, requires: 'p' } ] },
			});
			expect(allowing(within(() => other, { callerIs: 'ownerId' })))
				.toThrow(
					'entity "e" access.allow[0] within[0]: names entity "other", which is not part of this schema',
				);
			const missing = undefined as unknown as EntitySchema;
			expect(allowing({ entity: () => missing, callerIs: 'ownerId' }))
				.toThrow(
					'entity "e" access.allow[0] within[0]: names entity <unresolved>, which is not part of this schema',
				);
		});
		it('rejects an entry that sets both self and entity', () => {
			const both = { self: true, entity: 'project', callerIs: 'ownerId' } as unknown as WithinDecl;
			expect(allowing(both)).toThrow(
				'entity "e" access.allow[0] within[0]: sets both self and entity; set one',
			);
		});
		it('rejects rules that repeat each other through within thunks, in defineSchema()', () => {
			const repeated = entity('e', fields, {
				access: {
					allow: [
						{ read: true, requires: 'p', within: within(() => project, { callerIs: 'ownerId' }) },
						{ read: true, requires: 'p', within: within(() => project, { callerIs: 'ownerId' }) },
					],
				},
			});
			expect(() => defineSchema(project, repeated))
				.toThrow('entity "e" access.allow[1]: the rule repeats access.allow[0]');
		});
	});

	it('names an entity declared later through a thunk, with when typed against its fields', () => {
		const comment = entity('comment', {
			id: text().key(),
			doc: ref(() => doc).accessParent(),
		}, {
			access: {
				allow: [
					{ read: true, requires: 'view_docs' },
					{
						read: true,
						admitted: true,
						within: within(() => doc, { when: (d) => d.published.eq(true) }),
					},
				],
			},
		});
		const doc = entity('doc', { id: text().key(), published: bool() }, {
			access: { allow: [ { read: true, requires: 'view_docs' } ] },
		});
		within(() => doc, {
			// @ts-expect-error not a field of a doc
			when: (d) => d.publishd.eq(true),
		});
		const bundle = defineSchema(comment, doc);
		const [ entry ] = getOption(bundle.descMessage('comment'), entityExt).access!.allow[1]!.within;
		expect(entry!.entity).toBe('doc');
		expect(condFromProto(entry!.when!)).toEqual({
			kind: 'cmp',
			path: [ 'published' ],
			op: 'eq',
			value: true,
		});
	});

	it("names the rule's own entity through self, with callerIs and when typed against its fields", () => {
		// A self reference needs a named field map; see the issue entity above.
		type FolderFields = {
			id: Field<string>;
			parent: Field<Infer<EntitySchema<FolderFields>>>;
			isPrivate: Field<boolean>;
			ownerId: Field<string>;
		};
		const folder: EntitySchema<FolderFields> = entity('folder', {
			id: text().key(),
			parent: ref(() => folder).accessParent(),
			isPrivate: bool(),
			ownerId: text(),
		}, {
			access: {
				allow: [
					{ read: true, requires: 'view_private' },
					{ read: true, admitted: true, within: { self: true, when: (f) => f.isPrivate.eq(false) } },
					{ write: true, admitted: true, within: { self: true, callerIs: 'ownerId' } },
				],
			},
		});
		const allow = (
			within: AccessRuleDecl<FolderFields>['within'],
		) => [ { read: true, admitted: true, within } ];
		// @ts-expect-error callerIs names a text() field
		allow({ self: true, callerIs: 'isPrivate' });
		// @ts-expect-error not a field of a folder
		allow({ self: true, when: (f) => f.private.eq(false) });
		const rules = getOption(defineSchema(folder).descMessage('folder'), entityExt).access!.allow;
		expect(rules[1]!.within.map((w) => w.entity)).toEqual([ 'folder' ]);
		expect(condFromProto(rules[1]!.within[0]!.when!))
			.toEqual({ kind: 'cmp', path: [ 'is_private' ], op: 'eq', value: false });
		expect(rules[2]!.within.map((w) => [ w.entity, w.callerIs ])).toEqual([ [
			'folder',
			'owner_id',
		] ]);
	});
});

describe('schema builder — checks across entities', () => {
	const access = { allow: [ { read: true, requires: 'view' } ] } satisfies AccessDecl;
	const roles = [ { name: 'member', grants: [ 'view' ] } ];

	it('rejects an access parent that points at an entity without access', () => {
		const open = entity('open', { id: text().key() });
		const child = entity('child', { id: text().key(), open: ref(() => open).accessParent() }, {
			access,
		});
		expect(() => defineSchema(open, child)).toThrow(
			'entity "child" field "open" is marked accessParent(), but points at entity "open", which declares no access; '
				+ 'declare access on "open"',
		);
	});

	it('rejects a grant subject that points at an entity without members', () => {
		const group = entity('group', { id: text().key() }, { access });
		const grantee = entity('grantee', { id: text().key(), group: ref(() => group), role: text() }, {
			access,
			grant: { subject: 'group', role: 'role', roles },
		});
		expect(() => defineSchema(group, grantee)).toThrow(
			'entity "grantee" grant subject field "group" points at entity "group", which declares no members; '
				+ 'declare members on "group" to grant to its members',
		);
	});

	describe('members', () => {
		const team = entity('team', { id: text().key() }, { access, members: { entity: 'team_member' } });
		const memberFields = {
			id: text().key(),
			team: ref(() => team).accessParent(),
			userId: text(),
			role: text(),
		};

		it('rejects a members entity outside the schema', () => {
			expect(() => defineSchema(team)).toThrow(
				'entity "team" members names entity "team_member", which is not part of this schema',
			);
		});
		it('rejects a members entity that is not a grant entity', () => {
			const plain = entity('team_member', memberFields, { access });
			expect(() => defineSchema(team, plain)).toThrow(
				'entity "team" members names entity "team_member", which is not a grant entity',
			);
		});
		it('rejects a members entity whose access parent is not the team', () => {
			const other = entity('other', { id: text().key() }, { access });
			const member = entity(
				'team_member',
				{ ...memberFields, team: ref(() => other).accessParent() },
				{
					access,
					grant: { subject: 'userId', role: 'role', roles },
				},
			);
			expect(() => defineSchema(team, other, member)).toThrow(
				'entity "team" members names entity "team_member", whose accessParent() does not point at "team" alone',
			);
			const rootMember = entity('team_member', { ...memberFields, team: ref(() => team) }, {
				access,
				grant: { subject: 'userId', role: 'role', roles },
			});
			expect(() => defineSchema(team, rootMember)).toThrow(
				'whose accessParent() does not point at "team" alone',
			);
		});
		it('rejects a members entity whose grant subject is a team', () => {
			const member = entity('team_member', { ...memberFields, userId: ref(() => team) }, {
				access,
				grant: { subject: 'userId', role: 'role', roles },
			});
			expect(() => defineSchema(team, member)).toThrow(
				'entity "team" members names entity "team_member", whose grant subject is not a text() field',
			);
		});
	});
});

describe('schema builder — rules that need several permissions', () => {
	const project = entity('project', { id: text().key() }, {
		access: { allow: [ { read: true, requires: 'view_tasks' } ] },
	});
	const task = entity('task', {
		id: text().key(),
		project: ref(() => project).accessParent(),
		title: text(),
	}, {
		access: {
			allow: [
				{ read: true, requires: 'view_tasks' },
				{ update: true, requires: [ 'edit_tasks', 'use_billing', 'audit' ] },
			],
		},
	});
	const bundle = defineSchema(project, task);

	it('lowers the list onto the rule as written', () => {
		const [ read, update ] = getOption(bundle.descMessage('task'), entityExt).access!.allow;
		expect(read!.requires).toEqual([ 'view_tasks' ]);
		expect(update!.requires).toEqual([ 'edit_tasks', 'use_billing', 'audit' ]);
	});

	it('reads the list onto the EntityDef, sorted', () => {
		const def = entityDefOf(bundle.descMessage('task'));
		expect(def.accessRules![0]!.requires).toEqual([ 'view_tasks' ]);
		expect(def.accessRules![1]!.requires).toEqual([ 'audit', 'edit_tasks', 'use_billing' ]);
	});

	describe('validation', () => {
		const fields = { id: text().key() };
		const allowing = (...allow: AccessRuleDecl<typeof fields>[]) => () =>
			entity('e', fields, { access: { allow } });

		it('rejects an empty list', () => {
			// @ts-expect-error requires names at least one permission.
			const empty: AccessRuleDecl<typeof fields> = { read: true, requires: [] };
			expect(allowing(empty)).toThrow('entity "e" access.allow[0]: requires names no permission');
		});
		it('rejects a list that names a permission twice', () => {
			expect(allowing({ read: true, requires: [ 'q', 'p', 'q' ] }))
				.toThrow('entity "e" access.allow[0]: requires "q" twice');
		});
		it('rejects a permission name that breaks the naming rule', () => {
			expect(allowing({ read: true, requires: [ 'p', 'q-r' ] })).toThrow('access.allow[0] requires');
		});
		it('treats rules that require the same permissions in another order as the same rule', () => {
			expect(allowing(
				{ read: true, requires: [ 'p', 'q' ] },
				{ read: true, requires: [ 'q', 'p' ] },
			)).toThrow('entity "e" access.allow[1]: the rule repeats access.allow[0]');
			expect(allowing(
				{ read: true, requires: [ 'p', 'q' ] },
				{ read: true, requires: 'p' },
			)).not.toThrow();
		});
	});
});

describe('schema builder — typed access names', () => {
	const team = entity('team', { id: text().key() }, {
		members: { entity: 'team_member' },
		access: { allow: [ { read: true, requires: 'view_teams' } ] },
	});
	const postFields = {
		id: text().key(),
		authorId: text(),
		body: text(),
		pinned: bool(),
	};

	it('accepts the names of fields the entity declares', () => {
		const post = entity('post', postFields, {
			access: {
				allow: [
					{ read: true, requires: 'view_posts' },
					{ update: true, requires: 'post', callerIs: 'authorId', fields: [ 'body' ] },
				],
			},
		});
		expect(post.access?.allow).toHaveLength(2);
	});

	it('refuses a rule callerIs or fields entry that is not a matching field, at compile time', () => {
		const opts = (allow: AccessRuleDecl<typeof postFields>[]) => ({ access: { allow } });
		opts([
			// @ts-expect-error not a field of a post
			{ update: true, requires: 'post', callerIs: 'authorIdd' },
		]);
		opts([
			// @ts-expect-error callerIs names a text() field
			{ update: true, requires: 'post', callerIs: 'pinned' },
		]);
		opts([
			// @ts-expect-error not a field of a post
			{ update: true, requires: 'post', fields: [ 'titel' ] },
		]);
	});

	it('refuses a grant subject or role that is not a matching field, at compile time', () => {
		const memberFields = {
			id: text().key(),
			team: ref(() => team).accessParent(),
			userId: text(),
			role: text(),
			active: bool(),
		};
		const access = { allow: [ { read: true, requires: 'view_teams' } ] } satisfies AccessDecl;
		const roles = [ { name: 'team_member', grants: [ 'view_teams' ] } ];
		entity('team_member', memberFields, {
			access,
			grant: { subject: 'userId', role: 'role', roles },
		});
		expect(() =>
			entity('team_member', memberFields, {
				access,
				// @ts-expect-error not a field of a team member
				grant: { subject: 'user', role: 'role', roles },
			})
		).toThrow('grant subject field "user" is not declared');
		expect(() =>
			entity('team_member', memberFields, {
				access,
				// @ts-expect-error a role is a text() field
				grant: { subject: 'userId', role: 'active', roles },
			})
		).toThrow('grant role field "active" must be a singular text() field');
	});

	it('accepts readonly lists, so a schema can share them', () => {
		const reader = [ 'view_posts' ] as const;
		const editable = [ 'body' ] as const;
		const post = entity('post', postFields, {
			access: {
				allow: [
					{ read: true, requires: 'view_posts' },
					{ update: true, requires: 'post', fields: editable },
				],
			},
			grant: undefined,
		});
		const member = entity('post_reader', {
			id: text().key(),
			userId: text(),
			role: text(),
		}, {
			access: { allow: [ { read: true, requires: 'view_posts' } ] },
			grant: {
				subject: 'userId',
				role: 'role',
				roles: [
					{ name: 'reader', grants: reader },
					{ name: 'poster', includes: [ 'reader' ] as const, grants: [ 'post' ] },
				] as const,
			},
		});
		expect(post.access?.allow).toHaveLength(2);
		expect(member.grant?.roles).toHaveLength(2);
	});

	it('takes any name where the entity is not known', () => {
		const allow: AccessRuleDecl[] = [ {
			update: true,
			requires: 'post',
			callerIs: 'anyField',
			fields: [ 'x' ],
		} ];
		expect(allow).toHaveLength(1);
	});
});
