// Polymorphic references: a oneof of references, so one stored value points
// at one of several entities. Three fixtures declare one — OneofRefEntity
// holds a oneof of entity-typed fields, Memo holds OwnerRef (a reference
// message whose target is a oneof beside a workspace) to-one and to-many, and
// Attachment mixes an entity-typed member, a workspace-qualified reference
// message and a plain string in one oneof. These specs cover the client side:
// the reference graph, projection, the merge unit, resolution, hydration,
// `matches()` and live watchers. The server's own rules are pinned in
// engine/server; the wire round trip in the integration suite.

import { create, createFileRegistry, setExtension } from '@bufbuild/protobuf';
import {
	type DescriptorProto,
	DescriptorProtoSchema,
	type FieldDescriptorProto,
	FieldDescriptorProto_Label,
	FieldDescriptorProto_Type,
	FieldDescriptorProtoSchema,
	FieldOptionsSchema,
	FileDescriptorProtoSchema,
	FileDescriptorSetSchema,
	MessageOptionsSchema,
	OneofDescriptorProtoSchema,
} from '@bufbuild/protobuf/wkt';
import { describe, expect, expectTypeOf, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { ReverseRelAccessor } from '../../src/db.js';
import {
	type Attachment,
	AttachmentSchema,
	type Memo,
	MemoSchema,
	type OneofRefEntity,
	OneofRefEntitySchema,
	type OwnerRef,
	OwnerRefSchema,
	type Project,
	ProjectSchema,
	type User,
	UserSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	entity as entityExt,
	EntityOptionsSchema,
	field_behavior as fieldBehaviorExt,
	FieldBehaviorSchema,
	reference as referenceExt,
	ReferenceOptionsSchema,
} from '../../src/gen/syncplane/v1/options_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import {
	EntityRegistry,
	expandCollectionMask,
	mergeMask,
	projectRefs,
	refCasePath,
	refItemKeyOf,
	refShape,
	refShapes,
} from '../../src/mutations.js';
import { compileQuery } from '../../src/query/proxy.js';
import type { RefTargetOf } from '../../src/query/types.js';
import { prepareQuery } from '../../src/query/wire.js';
import type { Row } from '../../src/read.js';
import type { HydratedRow } from '../../src/refs.js';

const ORG = 'org';
const PROJ = 'proj';

const entities = [ ProjectSchema, UserSchema, OneofRefEntitySchema, MemoSchema, AttachmentSchema ];

function newClient() {
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ ORG, PROJ ],
		defaultWorkspace: PROJ,
		kvStore: new MemoryKVStore(),
		entities,
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

const ownerProject = (id: string, workspaceId = ''): OwnerRef =>
	({ workspaceId, owner: { case: 'project', value: { id } } }) as OwnerRef;
const ownerUser = (id: string, workspaceId = ''): OwnerRef =>
	({ workspaceId, owner: { case: 'user', value: { id } } }) as OwnerRef;

/**
 * One project and one user in the project workspace, a user of the same id
 * in the org workspace, and rows pointing at them through every shape.
 */
async function seed(client: ReturnType<typeof newClient>) {
	await client.ws(PROJ).db.project.mutate.put({ id: 'p1', name: 'Alpha' });
	await client.ws(PROJ).db.user.mutate.put({ id: 'u1', name: 'Proj Maya' });
	await client.ws(ORG).db.user.mutate.put({ id: 'u1', name: 'Org Maya' });

	await client.ws(PROJ).db.oneofRefEntity.mutate.put({
		id: 'r-proj',
		target: { case: 'project', value: { id: 'p1', name: 'stale copy' } },
	});
	await client.ws(PROJ).db.oneofRefEntity.mutate.put({
		id: 'r-user',
		target: { case: 'user', value: { id: 'u1' } },
	});
	await client.ws(PROJ).db.oneofRefEntity.mutate.put({ id: 'r-none' });

	await client.ws(PROJ).db.memo.mutate.put({ id: 'm-proj', owner: ownerProject('p1', PROJ) });
	await client.ws(PROJ).db.memo.mutate.put({ id: 'm-user-bare', owner: ownerUser('u1') });
	await client.ws(PROJ).db.memo.mutate.put({ id: 'm-user-org', owner: ownerUser('u1', ORG) });
	await client.ws(PROJ).db.memo.mutate.put({
		id: 'm-mentions',
		mentions: [ ownerProject('p1'), ownerUser('u1', ORG), ownerUser('u1') ],
	});

	await client.ws(PROJ).db.attachment.mutate.put({
		id: 'a-proj',
		parent: { case: 'project', value: { id: 'p1' } },
	});
	await client.ws(PROJ).db.attachment.mutate.put({
		id: 'a-user-org',
		parent: { case: 'user', value: { workspaceId: ORG, user: { id: 'u1' } } },
	});
	await client.ws(PROJ).db.attachment.mutate.put({
		id: 'a-user-here',
		parent: { case: 'user', value: { user: { id: 'u1' } } },
	});
	await client.ws(PROJ).db.attachment.mutate.put({
		id: 'a-label',
		parent: { case: 'label', value: 'loose' },
	});
}

const flush = () => new Promise((r) => setTimeout(r, 5));
const keysOf = (rows: readonly { key: string }[]) => rows.map((r) => r.key).sort();
const nameOf = (row: Row<unknown> | undefined) =>
	(row?.value as { name?: string } | undefined)?.name;
const typeOf = (row: Row<unknown> | undefined) =>
	(row?.value as { $typeName: string } | undefined)?.$typeName;

describe('polymorphic reference graph', () => {
	const registry = new EntityRegistry(entities);

	it('surfaces a oneof of references on the holder as one relation with a case per member', () => {
		const cases = registry.forwardCasesOf('oneof_ref', 'target');
		expect(cases.map((c) => [ c.caseName, c.targetEntity, c.field.localName ])).toEqual([
			[ 'project', 'project', 'project' ],
			[ 'user', 'user', 'user' ],
		]);
		expect(cases.every((c) => !c.repeated && c.shape.group?.localName === 'target')).toBe(true);
		expect(registry.forwardCasesOf('oneof_ref', 'project')).toEqual([]);
		// The plain member of a mixed oneof is not a case.
		expect(registry.forwardCasesOf('attachment', 'parent').map((c) => c.caseName)).toEqual([
			'project',
			'user',
		]);
	});

	it('surfaces a reference message whose target is a oneof as one case per member', () => {
		const owner = registry.forwardCasesOf('memo', 'owner');
		expect(owner.map((c) => [ c.caseName, c.targetEntity ])).toEqual([ [ 'project', 'project' ], [
			'user',
			'user',
		] ]);
		for (const c of owner) {
			expect(c.shape.workspaceField?.localName).toBe('workspaceId');
			expect(c.shape.valueField?.oneof?.localName).toBe('owner');
			expect(c.shape.group).toBeUndefined();
		}
		expect(registry.forwardCasesOf('memo', 'mentions').every((c) => c.repeated)).toBe(true);
		// No single target, so the one-entity view declines while the field is
		// still a reference for every consumer that asks.
		const field = MemoSchema.fields.find((f) => f.name === 'owner')!;
		expect(refShape(field)).toBeUndefined();
		expect(refShapes(field)).toHaveLength(2);
	});

	it('spells the query-builder path to each case', () => {
		const target = OneofRefEntitySchema.fields.find((f) => f.name === 'user')!;
		expect(refCasePath(target, refShape(target)!)).toEqual([ 'target', 'user' ]);
		const owner = MemoSchema.fields.find((f) => f.name === 'owner')!;
		expect(refCasePath(owner, refShapes(owner)[1]!)).toEqual([ 'owner', 'owner', 'user' ]);
		const parentUser = AttachmentSchema.fields.find((f) => f.name === 'user')!;
		expect(refCasePath(parentUser, refShape(parentUser)!)).toEqual([ 'parent', 'user' ]);
	});

	it('derives one reverse relation per case, disambiguated by relation and then by case', () => {
		expect(registry.reverseRefsOf('project').map((r) => r.name).sort()).toEqual([
			'attachments',
			'memosByMentions',
			'memosByOwner',
			'oneof_refs',
		]);
		expect(registry.reverseRefsOf('user').map((r) => r.name).sort()).toEqual([
			'attachments',
			'memosByMentions',
			'memosByOwner',
			'oneof_refs',
		]);
		const byOwner = registry.reverseRef('user', 'memosByOwner')!;
		expect(byOwner.viaPath).toEqual([ 'owner', 'owner', 'user' ]);
		expect(byOwner.holderField.localName).toBe('owner');
		expect(registry.reverseRef('project', 'oneof_refs')!.viaPath).toEqual([ 'target', 'project' ]);
	});
});

// Hand-built descriptors, for the shapes registration must refuse.
const strField = (
	name: string,
	number: number,
	opts: { workspace?: boolean; oneofIndex?: number } = {},
) => {
	const field: FieldDescriptorProto = create(FieldDescriptorProtoSchema, {
		name,
		number,
		label: FieldDescriptorProto_Label.OPTIONAL,
		type: FieldDescriptorProto_Type.STRING,
		...(opts.oneofIndex !== undefined ? { oneofIndex: opts.oneofIndex } : {}),
	});
	if (opts.workspace) {
		const options = create(FieldOptionsSchema);
		setExtension(options, fieldBehaviorExt, create(FieldBehaviorSchema, { workspace: true }));
		field.options = options;
	}
	return field;
};
const msgField = (
	name: string,
	number: number,
	typeName: string,
	oneofIndex?: number,
): FieldDescriptorProto =>
	create(FieldDescriptorProtoSchema, {
		name,
		number,
		label: FieldDescriptorProto_Label.OPTIONAL,
		type: FieldDescriptorProto_Type.MESSAGE,
		typeName: `.hand.${typeName}`,
		...(oneofIndex !== undefined ? { oneofIndex } : {}),
	});
const entityMsg = (
	name: string,
	wire: string,
	field: FieldDescriptorProto[],
	oneofs: string[] = [],
): DescriptorProto => {
	const options = create(MessageOptionsSchema);
	setExtension(options, entityExt, create(EntityOptionsSchema, { name: wire, keyFields: [ 'id' ] }));
	return create(DescriptorProtoSchema, {
		name,
		field,
		options,
		oneofDecl: oneofs.map((n) => create(OneofDescriptorProtoSchema, { name: n })),
	});
};
const refMsg = (
	name: string,
	field: FieldDescriptorProto[],
	oneofs: string[] = [],
): DescriptorProto => {
	const options = create(MessageOptionsSchema);
	setExtension(options, referenceExt, create(ReferenceOptionsSchema));
	return create(DescriptorProtoSchema, {
		name,
		field,
		options,
		oneofDecl: oneofs.map((n) => create(OneofDescriptorProtoSchema, { name: n })),
	});
};
const handWritten = (...messageType: DescriptorProto[]) =>
	createFileRegistry(
		create(FileDescriptorSetSchema, {
			file: [
				create(FileDescriptorProtoSchema, {
					name: 'hand/written.proto',
					package: 'hand',
					syntax: 'proto3',
					messageType,
				}),
			],
		}),
	);

describe('polymorphic reference registration', () => {
	const a = entityMsg('A', 'a', [ strField('id', 1) ]);
	const b = entityMsg('B', 'b', [ strField('id', 1) ]);

	it('refuses a polymorphic reference message as a member of a oneof', () => {
		const files = handWritten(
			a,
			b,
			refMsg('EitherRef', [ msgField('a', 1, 'A', 0), msgField('b', 2, 'B', 0) ], [ 'target' ]),
			entityMsg('Holder', 'holder', [
				strField('id', 1),
				msgField('a', 2, 'A', 0),
				msgField('either', 3, 'EitherRef', 0),
			], [ 'parent' ]),
		);
		expect(() =>
			new EntityRegistry([
				files.getMessage('hand.A')!,
				files.getMessage('hand.B')!,
				files.getMessage('hand.Holder')!,
			])
		).toThrow(
			/reference field is a member of oneof "parent" and names one of several targets itself/,
		);
	});

	it("refuses a workspace field inside the reference message's oneof", () => {
		const files = handWritten(
			a,
			refMsg('Ref', [
				strField('workspace_id', 1, { workspace: true, oneofIndex: 0 }),
				msgField('a', 2, 'A', 0),
			], [ 'target' ]),
			entityMsg('Holder', 'holder', [ strField('id', 1), msgField('ref', 2, 'Ref') ]),
		);
		expect(() =>
			new EntityRegistry([ files.getMessage('hand.A')!, files.getMessage('hand.Holder')! ])
		)
			.toThrow(/workspace field "workspace_id" is a member of oneof "target"/);
	});

	it('refuses entity-typed fields that are not the members of one oneof', () => {
		const files = handWritten(
			a,
			b,
			refMsg('Ref', [ msgField('a', 1, 'A'), msgField('b', 2, 'B', 0) ], [ 'target' ]),
			entityMsg('Holder', 'holder', [ strField('id', 1), msgField('ref', 2, 'Ref') ]),
		);
		expect(() =>
			new EntityRegistry([
				files.getMessage('hand.A')!,
				files.getMessage('hand.B')!,
				files.getMessage('hand.Holder')!,
			])
		)
			.toThrow(
				/fields "a" and "b" are both entity-typed; a reference names one target, or one of several as the members of a single oneof/,
			);
	});
});

describe('polymorphic reference storage', () => {
	it("projects the active case down to its target's key", () => {
		const r = projectRefs(
			OneofRefEntitySchema,
			create(OneofRefEntitySchema, {
				id: 'r1',
				target: { case: 'project', value: { id: 'p1', name: 'fat' } },
			}),
		);
		expect(r.target).toEqual({ case: 'project', value: create(ProjectSchema, { id: 'p1' }) });

		const m = projectRefs(
			MemoSchema,
			create(MemoSchema, {
				id: 'm1',
				owner: { workspaceId: ORG, owner: { case: 'user', value: { id: 'u1', name: 'fat' } } },
				mentions: [ { owner: { case: 'project', value: { id: 'p1', name: 'fat' } } } ],
			}),
		);
		expect(m.owner?.workspaceId).toBe(ORG);
		expect(m.owner?.owner).toEqual({ case: 'user', value: create(UserSchema, { id: 'u1' }) });
		expect(m.mentions[0]?.owner).toEqual({
			case: 'project',
			value: create(ProjectSchema, { id: 'p1' }),
		});

		const a = projectRefs(
			AttachmentSchema,
			create(AttachmentSchema, {
				id: 'a1',
				parent: { case: 'user', value: { workspaceId: ORG, user: { id: 'u1', name: 'fat' } } },
			}),
		);
		expect(a.parent).toEqual({
			case: 'user',
			value: {
				$typeName: 'syncplane.testdata.v1.UserRef',
				workspaceId: ORG,
				user: create(UserSchema, { id: 'u1' }),
			},
		});
		const label = projectRefs(
			AttachmentSchema,
			create(AttachmentSchema, { id: 'a2', parent: { case: 'label', value: 'x' } }),
		);
		expect(label.parent).toEqual({ case: 'label', value: 'x' });
	});

	it('identifies an element of a repeated polymorphic reference by workspace, case and key', () => {
		const mentions = MemoSchema.fields.find((f) => f.name === 'mentions')!;
		expect(refItemKeyOf(mentions, create(OwnerRefSchema, ownerProject('p/1', ORG)))).toBe(
			'org/project/p\\/1',
		);
		expect(refItemKeyOf(mentions, create(OwnerRefSchema, ownerUser('p/1')))).toBe('/user/p\\/1');
		expect(refItemKeyOf(mentions, create(OwnerRefSchema, { workspaceId: ORG }))).toBe('org//');
		expect(refItemKeyOf(mentions, create(OwnerRefSchema, {}))).toBe('//');
	});

	it('expands a change to a repeated polymorphic reference into per-element paths', () => {
		const prior = create(MemoSchema, {
			id: 'm1',
			mentions: [ ownerProject('x', ORG), ownerUser('x', ORG) ],
		});
		const next = create(MemoSchema, { id: 'm1', mentions: [ ownerUser('x', ORG), ownerUser('u2') ] });
		expect(expandCollectionMask(MemoSchema, prior, next, [ 'mentions' ]).sort()).toEqual([
			'mentions./user/u2',
			'mentions.org/project/x',
		]);
		mergeMask(MemoSchema, prior, next, [ 'mentions.org/project/x', 'mentions./user/u2' ]);
		expect(
			prior.mentions.map((e) => [ e.workspaceId, e.owner.case, (e.owner.value as { id: string }).id ]),
		).toEqual([
			[ ORG, 'user', 'x' ],
			[ '', 'user', 'u2' ],
		]);
	});

	it('switches the case of a oneof on the holder with a mask naming the group', async () => {
		const client = newClient();
		await seed(client);
		await client.ws(PROJ).db.oneofRefEntity.mutate.patch('r-proj', {
			target: { case: 'user', value: { id: 'u1', name: 'fat' } },
		});
		const row = (await client.ws(PROJ).db.oneofRefEntity.get('r-proj'))!;
		expect(row.target).toEqual({ case: 'user', value: create(UserSchema, { id: 'u1' }) });
		await client.close();
	});
});

describe('polymorphic forward resolution', () => {
	it("resolves the case a row holds, in that case's entity", async () => {
		const client = newClient();
		await seed(client);
		const db = client.ws(PROJ).db;
		const load = (id: string) =>
			db.oneofRefEntity.get(id).then((r) => db.oneofRefEntity.refs.target.load(r!));
		expect(nameOf(await load('r-proj'))).toBe('Alpha');
		expect(typeOf(await load('r-proj'))).toBe('syncplane.testdata.v1.Project');
		expect(nameOf(await load('r-user'))).toBe('Proj Maya');
		expect(await load('r-none')).toBeUndefined();
		expectTypeOf<Awaited<ReturnType<typeof db.oneofRefEntity.refs.target.load>>>().toEqualTypeOf<
			Row<Project | User> | undefined
		>();
		await client.close();
	});

	it('resolves a polymorphic reference message in the workspace it names', async () => {
		const client = newClient();
		await seed(client);
		const db = client.ws(PROJ).db;
		const load = (id: string) => db.memo.get(id).then((m) => db.memo.refs.owner.load(m!));
		expect(nameOf(await load('m-proj'))).toBe('Alpha');
		expect(nameOf(await load('m-user-bare'))).toBe('Proj Maya');
		expect(nameOf(await load('m-user-org'))).toBe('Org Maya');
		const mentions = await db.memo.get('m-mentions').then((m) => db.memo.refs.mentions.load(m!));
		expect(mentions.map(nameOf)).toEqual([ 'Alpha', 'Org Maya', 'Proj Maya' ]);
		expectTypeOf<Awaited<ReturnType<typeof db.memo.refs.owner.load>>>().toEqualTypeOf<
			Row<Project | User> | undefined
		>();
		expectTypeOf<Awaited<ReturnType<typeof db.memo.refs.mentions.load>>>().toEqualTypeOf<
			Row<Project | User>[]
		>();
		await client.close();
	});

	it('resolves the cases of a mixed oneof and nothing for its plain member', async () => {
		const client = newClient();
		await seed(client);
		const db = client.ws(PROJ).db;
		const load = (id: string) =>
			db.attachment.get(id).then((a) => db.attachment.refs.parent.load(a!));
		expect(nameOf(await load('a-proj'))).toBe('Alpha');
		expect(nameOf(await load('a-user-org'))).toBe('Org Maya');
		expect(nameOf(await load('a-user-here'))).toBe('Proj Maya');
		expect(await load('a-label')).toBeUndefined();
		expectTypeOf<Awaited<ReturnType<typeof db.attachment.refs.parent.load>>>().toEqualTypeOf<
			Row<Project | User> | undefined
		>();
		await client.close();
	});

	it('hydrates an include tree through whichever case each row holds', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.ws(PROJ).db.memo.query(
			{ orderBy: (m) => [ m.id.asc() ] },
			{ include: { owner: { memosByOwner: true }, mentions: true } },
		);
		const hydrated = rows as unknown as HydratedRow<Memo>[];
		expect(hydrated.map((r) => nameOf(r.refs.owner as Row<unknown> | undefined))).toEqual([
			undefined,
			'Alpha',
			'Proj Maya',
			'Org Maya',
		]);
		expect(hydrated.map((r) => (r.refs.mentions as Row<unknown>[]).map(nameOf))).toEqual([
			[ 'Alpha', 'Org Maya', 'Proj Maya' ],
			[],
			[],
			[],
		]);
		// The nested tree continues through each target's own relations — for
		// the org user, across the workspaces its qualified reverse relation
		// walks.
		const owners = hydrated.map((r) => r.refs.owner as HydratedRow | undefined);
		expect(keysOf(owners[1]!.refs.memosByOwner as Row<unknown>[])).toEqual([ 'm-proj' ]);
		expect(keysOf(owners[2]!.refs.memosByOwner as Row<unknown>[])).toEqual([ 'm-user-bare' ]);
		expect(keysOf(owners[3]!.refs.memosByOwner as Row<unknown>[])).toEqual([ 'm-user-org' ]);
		await client.close();
	});

	it('refuses include options on a polymorphic list relation', async () => {
		const client = newClient();
		await seed(client);
		await expect(client.ws(PROJ).db.memo.query({}, { include: { mentions: { $limit: 1 } } }))
			.rejects.toThrow(/resolves to rows of "project" or "user"/);
		await client.close();
	});
});

describe('polymorphic reverse resolution', () => {
	it("answers each case's reverse relation with the rows whose case points here", async () => {
		const client = newClient();
		await seed(client);
		const project = (await client.ws(PROJ).db.project.get('p1'))!;
		const projUser = (await client.ws(PROJ).db.user.get('u1'))!;
		const orgUser = (await client.ws(ORG).db.user.get('u1'))!;
		const db = client.ws(PROJ).db;
		expect(keysOf(await (db.project.refs.oneof_refs as ReverseRelAccessor)(project))).toEqual([
			'r-proj',
		]);
		expect(keysOf(await (db.user.refs.oneof_refs as ReverseRelAccessor)(projUser))).toEqual([
			'r-user',
		]);
		expect(keysOf(await (db.project.refs.memosByOwner as ReverseRelAccessor)(project))).toEqual([
			'm-proj',
		]);
		expect(keysOf(await (db.user.refs.memosByOwner as ReverseRelAccessor)(projUser))).toEqual([
			'm-user-bare',
		]);
		expect(keysOf(await (client.ws(ORG).db.user.refs.memosByOwner as ReverseRelAccessor)(orgUser)))
			.toEqual([
				'm-user-org',
			]);
		expect(keysOf(await (db.project.refs.memosByMentions as ReverseRelAccessor)(project))).toEqual([
			'm-mentions',
		]);
		expect(keysOf(await (db.project.refs.attachments as ReverseRelAccessor)(project))).toEqual([
			'a-proj',
		]);
		expect(keysOf(await (db.user.refs.attachments as ReverseRelAccessor)(projUser))).toEqual([
			'a-user-here',
		]);
		expect(keysOf(await (client.ws(ORG).db.user.refs.attachments as ReverseRelAccessor)(orgUser)))
			.toEqual([
				'a-user-org',
			]);
		await client.close();
	});

	it('hydrates a reverse relation of a case', async () => {
		const client = newClient();
		await seed(client);
		const users = await client.ws(PROJ).db.user.query({}, {
			include: { oneof_refs: true, attachments: true },
		});
		const hydrated = users as unknown as HydratedRow<User>[];
		expect(keysOf(hydrated[0]!.refs.oneof_refs as Row<unknown>[])).toEqual([ 'r-user' ]);
		expect(keysOf(hydrated[0]!.refs.attachments as Row<unknown>[])).toEqual([ 'a-user-here' ]);
		await client.close();
	});
});

describe('polymorphic references in queries', () => {
	it('crosses one case of a oneof on the holder', async () => {
		const client = newClient();
		await seed(client);
		const db = client.ws(PROJ).db;
		expect(
			keysOf(
				await db.oneofRefEntity.query({
					where: (r) => r.target.project.matches((p) => p.name.eq('Alpha')),
				}),
			),
		)
			.toEqual([ 'r-proj' ]);
		expect(
			keysOf(
				await db.oneofRefEntity.query({
					where: (r) => r.target.user.matches((u) => u.name.eq('Proj Maya')),
				}),
			),
		)
			.toEqual([ 'r-user' ]);
		expect(keysOf(await db.oneofRefEntity.query({ where: (r) => r.target.project.id.eq('p1') })))
			.toEqual([ 'r-proj' ]);
		expect(keysOf(await db.oneofRefEntity.query({ where: (r) => r.target.user.isSet() }))).toEqual([
			'r-user',
		]);
		await client.close();
	});

	it('crosses one case of a polymorphic reference message in the workspace it names', async () => {
		const client = newClient();
		await seed(client);
		const db = client.ws(PROJ).db;
		expect(
			keysOf(
				await db.memo.query({ where: (m) => m.owner.owner.project.matches((p) => p.name.eq('Alpha')) }),
			),
		)
			.toEqual([ 'm-proj' ]);
		expect(
			keysOf(
				await db.memo.query({ where: (m) => m.owner.owner.user.matches((u) => u.name.eq('Org Maya')) }),
			),
		)
			.toEqual([ 'm-user-org' ]);
		expect(
			keysOf(
				await db.memo.query({
					where: (m) => m.owner.owner.user.matches((u) => u.name.eq('Proj Maya')),
				}),
			),
		)
			.toEqual([ 'm-user-bare' ]);
		expect(
			keysOf(
				await db.memo.query({
					where: (m) => m.mentions.owner.user.matches((u) => u.name.eq('Org Maya')),
				}),
			),
		)
			.toEqual([ 'm-mentions' ]);
		expect(keysOf(await db.memo.query({ where: (m) => m.owner.owner.user.id.eq('u1') }))).toEqual([
			'm-user-bare',
			'm-user-org',
		]);
		expect(keysOf(await db.memo.query({ where: (m) => m.owner.workspaceId.eq(ORG) }))).toEqual([
			'm-user-org',
		]);
		await client.close();
	});

	it('crosses the reference-message case of a mixed oneof by the workspace it names', async () => {
		const client = newClient();
		await seed(client);
		const db = client.ws(PROJ).db;
		expect(
			keysOf(
				await db.attachment.query({
					where: (a) => a.parent.user.matches((u) => u.name.eq('Org Maya')),
				}),
			),
		)
			.toEqual([ 'a-user-org' ]);
		expect(
			keysOf(
				await db.attachment.query({
					where: (a) => a.parent.project.matches((p) => p.name.eq('Alpha')),
				}),
			),
		)
			.toEqual([ 'a-proj' ]);
		expect(keysOf(await db.attachment.query({ where: (a) => a.parent.label.eq('loose') }))).toEqual([
			'a-label',
		]);
		await client.close();
	});

	it('translates a case to the wire path the server resolves', () => {
		const inner = compileQuery<Memo>({
			where: (m) => m.owner.owner.user.matches((u) => u.name.eq('x')),
		});
		expect(prepareQuery(MemoSchema, inner).where).toMatchObject({
			kind: 'rel',
			path: [ 'owner', 'user' ],
		});
		const list = compileQuery<Memo>({
			where: (m) => m.mentions.owner.project.matches((p) => p.name.eq('x')),
		});
		expect(prepareQuery(MemoSchema, list).where).toMatchObject({
			kind: 'rel',
			path: [ 'mentions', 'project' ],
		});
		const holder = compileQuery<OneofRefEntity>({
			where: (r) => r.target.user.matches((u) => u.name.eq('x')),
		});
		expect(prepareQuery(OneofRefEntitySchema, holder).where).toMatchObject({
			kind: 'rel',
			path: [ 'user' ],
		});
	});

	it('refuses matches() on the polymorphic message itself', () => {
		const q = compileQuery<Memo>({
			where: (m) => (m.owner as unknown as { matches(c: unknown): never }).matches(() => 0),
		});
		expect(() => prepareQuery(MemoSchema, q))
			.toThrow(
				/matches\(\) must name one case of polymorphic reference "owner".*owner\.owner\.project, owner\.owner\.user/,
			);
	});

	it("refuses a path reaching past a case's stored stub", () => {
		const q = compileQuery<Memo>({
			where: (m) => (m.owner.owner.user as unknown as { name: { eq(v: string): never } }).name.eq('x'),
		});
		expect(() => prepareQuery(MemoSchema, q)).toThrow(
			/cannot descend through reference "user" into "name"/,
		);
	});

	it('types the target of a polymorphic reference as the union of its cases', () => {
		expectTypeOf<RefTargetOf<OwnerRef>>().toEqualTypeOf<Project | User>();
		expectTypeOf<RefTargetOf<Attachment['parent']>>().not.toEqualTypeOf<Project | User>();
	});
});

describe('polymorphic references in watchers', () => {
	it('re-fires an include watcher when the target of the held case changes', async () => {
		const client = newClient();
		await seed(client);
		const names: (string | undefined)[] = [];
		const handle = client.ws(PROJ).db.oneofRefEntity.watchQuery(
			{ where: (r) => r.id.eq('r-user') },
			(rows) =>
				names.push(
					nameOf(
						(rows[0] as unknown as HydratedRow | undefined)?.refs.target as Row<unknown> | undefined,
					),
				),
			{ include: { target: true } },
		);
		await flush();
		expect(names).toEqual([ 'Proj Maya' ]);
		await client.ws(PROJ).db.user.mutate.put({ id: 'u1', name: 'Renamed' });
		await flush();
		expect(names.at(-1)).toBe('Renamed');
		// Switching the case moves the row to the other entity's target.
		await client.ws(PROJ).db.oneofRefEntity.mutate.patch('r-user', {
			target: { case: 'project', value: { id: 'p1' } },
		});
		await flush();
		expect(names.at(-1)).toBe('Alpha');
		handle.close();
		await client.close();
	});

	it('re-fires a matches() watcher on a case when the row switches case', async () => {
		const client = newClient();
		await seed(client);
		const seen: string[][] = [];
		const handle = client.ws(PROJ).db.oneofRefEntity.watchQuery(
			{ where: (r) => r.target.user.matches((u) => u.name.eq('Proj Maya')) },
			(rows) => seen.push(keysOf(rows)),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'r-user' ]);
		await client.ws(PROJ).db.oneofRefEntity.mutate.patch('r-proj', {
			target: { case: 'user', value: { id: 'u1' } },
		});
		await flush();
		expect(seen.at(-1)).toEqual([ 'r-proj', 'r-user' ]);
		await client.ws(PROJ).db.user.mutate.put({ id: 'u1', name: 'Someone Else' });
		await flush();
		expect(seen.at(-1)).toEqual([]);
		handle.close();
		await client.close();
	});
});
