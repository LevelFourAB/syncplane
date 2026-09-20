// Workspace-qualified references: a reference message (UserRef) wraps a User
// stub together with the workspace it lives in, so a Task may name a user held
// in another workspace. These specs cover the client side — the reference
// graph, projection, the merge unit, resolution across the workspaces a client
// holds, hydration, `matches()`, live watchers and the schema builder. The
// server's own rules are pinned in engine/server; the wire round trip in the
// integration suite.

import { create, createFileRegistry, setExtension } from '@bufbuild/protobuf';
import type { GenMessage } from '@bufbuild/protobuf/codegenv2';
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
} from '@bufbuild/protobuf/wkt';
import { describe, expect, expectTypeOf, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { ReverseRelAccessor } from '../../src/db.js';
import {
	type Task,
	TaskSchema,
	type User,
	type UserRef,
	UserRefSchema,
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
	refItemKey,
	refShape,
} from '../../src/mutations.js';
import { compileQuery } from '../../src/query/proxy.js';
import type { QueryRef, RefTargetOf } from '../../src/query/types.js';
import { prepareQuery } from '../../src/query/wire.js';
import type { Row } from '../../src/read.js';
import type { HydratedRow } from '../../src/refs.js';
import {
	defineSchema,
	entity,
	type Infer,
	text,
	type WorkspaceRef,
	workspaceRef,
	workspaceRefList,
} from '../../src/schema/builder.js';

const ORG = 'org';
const PROJ = 'proj';

function newClient() {
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ ORG, PROJ ],
		defaultWorkspace: PROJ,
		kvStore: new MemoryKVStore(),
		entities: [ TaskSchema, UserSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

/**
 * A user with the same id in both workspaces, so a resolver that ignores the
 * workspace a stub names reads the wrong row and the assertions catch it.
 */
async function seed(client: ReturnType<typeof newClient>) {
	await client.ws(ORG).db.user.mutate.put({ id: 'u1', name: 'Org Maya' });
	await client.ws(PROJ).db.user.mutate.put({ id: 'u1', name: 'Proj Maya' });
	await client.ws(PROJ).db.user.mutate.put({ id: 'u2', name: 'Proj Noor' });
	await client.ws(PROJ).db.task.mutate.put({
		id: 't1',
		title: 'org assignee',
		assignee: { workspaceId: ORG, user: { id: 'u1' } },
	});
	await client.ws(PROJ).db.task.mutate.put({
		id: 't2',
		title: 'local assignee',
		assignee: { user: { id: 'u1' } },
	});
	await client.ws(PROJ).db.task.mutate.put({
		id: 't3',
		title: 'watchers',
		watchers: [ { workspaceId: ORG, user: { id: 'u1' } }, { user: { id: 'u2' } } ],
	});
	await client.ws(ORG).db.task.mutate.put({
		id: 't4',
		title: 'in org',
		assignee: { user: { id: 'u1' } },
	});
}

const flush = () => new Promise((r) => setTimeout(r, 5));
const keysOf = (rows: readonly { key: string }[]) => rows.map((r) => r.key).sort();

// Hand-built descriptors, for shapes the generated fixtures and the builder
// cannot express: a stray workspace flag, and a self-referential
// workspace-qualified reference.
const strField = (name: string, number: number, workspace = false): FieldDescriptorProto => {
	const field = create(FieldDescriptorProtoSchema, {
		name,
		number,
		label: FieldDescriptorProto_Label.OPTIONAL,
		type: FieldDescriptorProto_Type.STRING,
	});
	if (workspace) {
		const options = create(FieldOptionsSchema);
		setExtension(options, fieldBehaviorExt, create(FieldBehaviorSchema, { workspace: true }));
		field.options = options;
	}
	return field;
};
const msgField = (name: string, number: number, typeName: string): FieldDescriptorProto =>
	create(FieldDescriptorProtoSchema, {
		name,
		number,
		label: FieldDescriptorProto_Label.OPTIONAL,
		type: FieldDescriptorProto_Type.MESSAGE,
		typeName: `.hand.${typeName}`,
	});
const entityMsg = (name: string, wire: string, field: FieldDescriptorProto[]): DescriptorProto => {
	const options = create(MessageOptionsSchema);
	setExtension(options, entityExt, create(EntityOptionsSchema, { name: wire, keyFields: [ 'id' ] }));
	return create(DescriptorProtoSchema, { name, field, options });
};
const refMsg = (name: string, field: FieldDescriptorProto[]): DescriptorProto => {
	const options = create(MessageOptionsSchema);
	setExtension(options, referenceExt, create(ReferenceOptionsSchema));
	return create(DescriptorProtoSchema, { name, field, options });
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

describe('workspace-qualified reference graph', () => {
	const registry = new EntityRegistry([ TaskSchema, UserSchema ]);

	it('surfaces a reference message field as a forward ref with its stored shape', () => {
		const fwd = registry.forwardRef('task', 'assignee')!;
		expect(fwd.targetEntity).toBe('user');
		expect(fwd.repeated).toBe(false);
		expect(fwd.shape.valueField?.localName).toBe('user');
		expect(fwd.shape.workspaceField?.localName).toBe('workspaceId');
		expect(registry.forwardRef('task', 'watchers')?.repeated).toBe(true);
	});

	it('derives the reverse relations on the target as for any reference', () => {
		expect(registry.reverseRefsOf('user').map((r) => r.name).sort()).toEqual([
			'tasksByAssignee',
			'tasksByWatchers',
		]);
	});
});

describe('workspace-qualified reference storage', () => {
	it('projects the inner stub down to its key and keeps the workspace', () => {
		const projected = projectRefs(
			TaskSchema,
			create(TaskSchema, {
				id: 't',
				assignee: { workspaceId: ORG, user: { id: 'u1', name: 'smuggled' } },
				watchers: [ { user: { id: 'u2', name: 'smuggled' } } ],
			}),
		);
		expect(projected.assignee?.workspaceId).toBe(ORG);
		expect(projected.assignee?.user?.id).toBe('u1');
		expect(projected.assignee?.user?.name).toBe('');
		expect(projected.watchers[0]?.workspaceId).toBe('');
		expect(projected.watchers[0]?.user?.name).toBe('');
	});

	it('refuses a stub naming a workspace that can never be joined', async () => {
		// The id fails the workspace-id rule, so addWorkspace would refuse it
		// and the reference could never resolve.
		expect(() =>
			projectRefs(
				TaskSchema,
				create(TaskSchema, { id: 't', assignee: { workspaceId: 'acme/prod', user: { id: 'u1' } } }),
			)
		).toThrow(/workspace id "acme\/prod" is not valid/);
		expect(() =>
			projectRefs(
				TaskSchema,
				create(TaskSchema, {
					id: 't',
					watchers: [ { user: { id: 'u1' } }, { workspaceId: 'acme/prod', user: { id: 'u2' } } ],
				}),
			)
		).toThrow(/workspace id "acme\/prod" is not valid/);
		const client = newClient();
		await expect(
			client.ws(PROJ).db.task.mutate.put({
				id: 't',
				assignee: { workspaceId: 'acme/prod', user: { id: 'u1' } },
			}),
		).rejects.toThrow(/workspace id "acme\/prod" is not valid/);
		await client.close();
	});

	it('rejects a workspace flag outside a reference message at registration', () => {
		const registry = handWritten(
			entityMsg('Stray', 'stray', [ strField('id', 1), strField('home', 2, true) ]),
		);
		expect(() => new EntityRegistry([ registry.getMessage('hand.Stray')! ])).toThrow(
			/field "home" is marked workspace, but hand\.Stray is not a reference message/,
		);
	});

	it('replaces a reference message whole when a mask reaches into it', () => {
		const dst = create(TaskSchema, {
			id: 't',
			assignee: { workspaceId: ORG, user: { id: 'u1' } },
		});
		const src = create(TaskSchema, { assignee: { user: { id: 'u2' } } });
		mergeMask(TaskSchema, dst, src, [ 'assignee.workspace_id' ]);
		// The workspace and the key move together: the source's empty workspace
		// lands alongside its key, never the old workspace beside a new key.
		expect(dst.assignee?.workspaceId).toBe('');
		expect(dst.assignee?.user?.id).toBe('u2');
	});

	// A repeated reference message merges as a set keyed by the stored value —
	// the workspace beside the target key — so an add and a remove from two
	// writers both land, as on a repeated entity-typed reference.
	describe('a repeated reference message', () => {
		const uref = (workspaceId: string, id: string) => ({ workspaceId, user: { id } });
		const names = (t: Task) => t.watchers.map((w) => `${w.workspaceId}/${w.user?.id}`);
		const watchers = TaskSchema.field.watchers;

		it("renders an element's identity as its workspace beside its key", () => {
			const shape = refShape(watchers)!;
			const key = (ws: string, id: string) => refItemKey(shape, create(UserRefSchema, uref(ws, id)));
			expect(key(ORG, 'u1')).toBe('org/u1');
			// An empty workspace is a part of its own, so it never collides with
			// a key that happens to contain the separator.
			expect(key('', 'u1')).toBe('/u1');
			expect(key('a/b', 'c.d')).toBe('a\\/b/c\\.d');
		});

		it('merges per element, so an add and a remove from different writers both land', () => {
			const dst = create(TaskSchema, { id: 't', watchers: [ uref(ORG, 'u1'), uref(ORG, 'u2') ] });
			mergeMask(TaskSchema, dst, create(TaskSchema, { watchers: [ uref(ORG, 'u2') ] }), [
				'watchers.org/u1',
			]);
			mergeMask(
				TaskSchema,
				dst,
				create(TaskSchema, { watchers: [ uref(ORG, 'u1'), uref(ORG, 'u2'), uref(ORG, 'u3') ] }),
				[ 'watchers.org/u3' ],
			);
			expect(names(dst)).toEqual([ 'org/u2', 'org/u3' ]);
		});

		it('keeps the same key in two workspaces as two elements', () => {
			const dst = create(TaskSchema, { id: 't', watchers: [ uref(ORG, 'u1') ] });
			mergeMask(TaskSchema, dst, create(TaskSchema, { watchers: [ uref('', 'u1') ] }), [
				'watchers./u1',
			]);
			expect(names(dst)).toEqual([ 'org/u1', '/u1' ]);
			mergeMask(TaskSchema, dst, create(TaskSchema), [ 'watchers.org/u1' ]);
			expect(names(dst)).toEqual([ '/u1' ]);
		});

		it('holds each element once, whatever either side repeats', () => {
			const dst = create(TaskSchema, { id: 't', watchers: [ uref(ORG, 'u1'), uref(ORG, 'u1') ] });
			mergeMask(
				TaskSchema,
				dst,
				create(TaskSchema, { watchers: [ uref(ORG, 'u1'), uref(ORG, 'u1') ] }),
				[
					'watchers.org/u1',
				],
			);
			expect(names(dst)).toEqual([ 'org/u1' ]);
			mergeMask(TaskSchema, dst, create(TaskSchema), [ 'watchers.org/u1' ]);
			expect(names(dst)).toEqual([]);
		});

		it('addresses the whole element when a mask reaches below it', () => {
			const dst = create(TaskSchema, { id: 't' });
			mergeMask(TaskSchema, dst, create(TaskSchema, { watchers: [ uref(ORG, 'u1') ] }), [
				'watchers.org/u1.user.id',
			]);
			expect(names(dst)).toEqual([ 'org/u1' ]);
		});

		it('expands a bare mask into the elements that moved', () => {
			const prior = create(TaskSchema, { watchers: [ uref(ORG, 'u1'), uref(ORG, 'u2') ] });
			const next = create(TaskSchema, {
				watchers: [ uref(ORG, 'u2'), uref('', 'u2'), uref(PROJ, 'u3') ],
			});
			expect(expandCollectionMask(TaskSchema, prior, next, [ 'watchers' ]).sort()).toEqual([
				'watchers./u2',
				'watchers.org/u1',
				'watchers.proj/u3',
			]);
			// Nothing moved: the mask names nothing, so the write touches nothing.
			expect(expandCollectionMask(TaskSchema, prior, prior, [ 'watchers' ])).toEqual([]);
		});
	});
});

describe('workspace-qualified forward resolution', () => {
	it("resolves a stub in the workspace it names, else in the holder's own", async () => {
		const client = newClient();
		await seed(client);
		const t1 = (await client.ws(PROJ).db.task.get('t1'))!;
		const org = await client.ws(PROJ).db.task.refs.assignee.load(t1);
		expectTypeOf(org).toEqualTypeOf<Row<User> | undefined>();
		expect(org?.workspaceId).toBe(ORG);
		expect(org?.value.name).toBe('Org Maya');

		const t2 = (await client.ws(PROJ).db.task.get('t2'))!;
		const local = await client.ws(PROJ).db.task.refs.assignee.load(t2);
		expect(local?.workspaceId).toBe(PROJ);
		expect(local?.value.name).toBe('Proj Maya');
		await client.close();
	});

	it('resolves each element of a to-many reference where it points', async () => {
		const client = newClient();
		await seed(client);
		const t3 = (await client.ws(PROJ).db.task.get('t3'))!;
		const watchers = await client.ws(PROJ).db.task.refs.watchers.load(t3);
		expectTypeOf(watchers).toEqualTypeOf<Row<User>[]>();
		expect(watchers.map((r) => r.value.name)).toEqual([ 'Org Maya', 'Proj Noor' ]);
		await client.close();
	});

	it('hydrates an include tree with each stub resolved where it points', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.ws(PROJ).db.task.query(
			{ orderBy: (t) => [ t.id.asc() ] },
			{ include: { assignee: true, watchers: true } },
		);
		const hydrated = rows as unknown as HydratedRow<Task>[];
		const names = hydrated.map((r) => (r.refs.assignee as Row<User> | undefined)?.value.name);
		expect(names).toEqual([ 'Org Maya', 'Proj Maya', undefined ]);
		// t1 and t2 name the same key in different workspaces: a batch grouped by
		// key alone hands one of them the other's row.
		expect(hydrated.map((r) => (r.refs.assignee as Row<User> | undefined)?.workspaceId))
			.toEqual([ ORG, PROJ, undefined ]);
		// t3's list spans both workspaces, so element order and per-element
		// workspace both have to survive the batch.
		expect(hydrated.map((r) => (r.refs.watchers as Row<User>[]).map((u) => u.value.name)))
			.toEqual([ [], [], [ 'Org Maya', 'Proj Noor' ] ]);
		await client.close();
	});
});

describe('workspace-qualified reverse resolution', () => {
	it('walks every held workspace and matches the workspace as well as the key', async () => {
		const client = newClient();
		await seed(client);
		const orgUser = (await client.ws(ORG).db.user.get('u1'))!;
		const projUser = (await client.ws(PROJ).db.user.get('u1'))!;
		// t1 names the org workspace outright; t4 sits in it and names none.
		const byAssignee = client.ws(ORG).db.user.refs.tasksByAssignee as ReverseRelAccessor;
		expect(keysOf(await byAssignee(orgUser))).toEqual([
			't1',
			't4',
		]);
		// The same key in the project workspace is a different row.
		const byAssigneeInProj = client.ws(PROJ).db.user.refs.tasksByAssignee as ReverseRelAccessor;
		expect(keysOf(await byAssigneeInProj(projUser))).toEqual([
			't2',
		]);
		const byWatchers = client.ws(ORG).db.user.refs.tasksByWatchers as ReverseRelAccessor;
		expect(keysOf(await byWatchers(orgUser))).toEqual([ 't3' ]);
		await client.close();
	});

	it('hydrates a reverse relation across workspaces', async () => {
		const client = newClient();
		await seed(client);
		const users = await client.ws(ORG).db.user.query({}, { include: { tasksByAssignee: true } });
		expect(users).toHaveLength(1);
		const hydrated = users as unknown as HydratedRow<User>[];
		expect(keysOf(hydrated[0]!.refs.tasksByAssignee as Row<unknown>[])).toEqual([ 't1', 't4' ]);
		await client.close();
	});
});

describe('workspace-qualified references in queries', () => {
	it('compares a reference by the key path inside the reference message', () => {
		// A bare reference leaf as a comparison operand, the form a hand-built AST
		// or a Go client sends; the typed DSL spells the key path itself.
		const prepared = prepareQuery(TaskSchema, {
			where: { kind: 'cmp', path: [ 'assignee' ], op: 'eq', value: 'u1' },
		}).where;
		expect(prepared).toEqual({
			kind: 'cmp',
			path: [ 'assignee', 'user', 'id' ],
			op: 'eq',
			value: 'u1',
		});
		const byWorkspace = prepareQuery(
			TaskSchema,
			compileQuery({ where: (t: QueryRef<Task>) => t.assignee.workspaceId.eq(ORG) }),
		).where;
		expect(byWorkspace).toEqual({
			kind: 'cmp',
			path: [ 'assignee', 'workspaceId' ],
			op: 'eq',
			value: ORG,
		});
	});

	it('refuses a path reaching past the stored stub', () => {
		expect(() =>
			prepareQuery(
				TaskSchema,
				compileQuery({ where: (t: QueryRef<Task>) => t.assignee.user.name.eq('x') }),
			)
		).toThrow(/cannot descend through reference "user" into "name"/);
		expect(() =>
			prepareQuery(TaskSchema, {
				where: {
					kind: 'exists',
					path: [ 'watchers' ],
					cond: { kind: 'cmp', path: [ 'label' ], op: 'eq', value: 'x' },
				},
			})
		).toThrow(/cannot ask about "label"/);
	});

	it('refuses matches() on the entity field inside the reference message', () => {
		// Resolved as a plain reference, assignee.user would match t1 against
		// the project's u1 — the workspace the stub names ignored.
		expect(() =>
			prepareQuery(
				TaskSchema,
				compileQuery({
					where: (t: QueryRef<Task>) => t.assignee.user.matches((u) => u.name.eq('Proj Maya')),
				}),
			)
		).toThrow(/matches\(\) names the reference field itself/);
	});

	it('types the target of a reference by the stored shape', () => {
		type Address = { $typeName: 'hand.Address'; street: string };
		// An entity holding one message field beside its strings is not a
		// wrapper: the callback of matches() sees the entity itself.
		type Person = { $typeName: 'hand.Person'; id: string; address?: Address; name: string };
		type Membership = { $typeName: 'hand.Membership'; id: string; person?: Person };
		type Bare = { $typeName: 'hand.Bare'; user?: User };
		expectTypeOf<RefTargetOf<Person>>().toEqualTypeOf<Person>();
		expectTypeOf<RefTargetOf<Membership>>().toEqualTypeOf<Membership>();
		expectTypeOf<RefTargetOf<UserRef>>().toEqualTypeOf<User>();
		expectTypeOf<RefTargetOf<Bare>>().toEqualTypeOf<User>();
	});

	it('evaluates matches() against the target in the workspace the stub names', async () => {
		const client = newClient();
		await seed(client);
		const org = await client.ws(PROJ).db.task.query({
			where: (t) => t.assignee.matches((u) => u.name.eq('Org Maya')),
		});
		expect(keysOf(org)).toEqual([ 't1' ]);
		const local = await client.ws(PROJ).db.task.query({
			where: (t) => t.assignee.matches((u) => u.name.eq('Proj Maya')),
		});
		expect(keysOf(local)).toEqual([ 't2' ]);
		const watched = await client.ws(PROJ).db.task.query({
			where: (t) => t.watchers.matches((u) => u.name.eq('Org Maya')),
		});
		expect(keysOf(watched)).toEqual([ 't3' ]);
		// Both stubs name a u1 somewhere; the key comparison does not care where.
		const anyU1 = await client.ws(PROJ).db.task.query({ where: (t) => t.assignee.user.id.eq('u1') });
		expect(keysOf(anyU1)).toEqual([ 't1', 't2' ]);
		await client.close();
	});

	it('answers matches() the same way across workspaces', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.queryAll(TaskSchema, {
			where: (t) => t.assignee.matches((u) => u.name.eq('Org Maya')),
		});
		// t1 from the project workspace and t4 from the org workspace both
		// point at the org user; t2 points at the project user of the same id.
		expect(rows.map((r) => `${r.workspaceId}/${r.key}`).sort()).toEqual([ 'org/t4', 'proj/t1' ]);
		await client.close();
	});
});

describe('workspace-qualified references in watchers', () => {
	it('re-fires an include watcher when a target in another workspace changes', async () => {
		const client = newClient();
		await seed(client);
		const names: (string | undefined)[] = [];
		const handle = client.ws(PROJ).db.task.watchQuery(
			{ where: (t) => t.id.eq('t1') },
			(rows) => {
				const first = rows[0] as unknown as
					| { refs?: Record<string, Row<User> | undefined> }
					| undefined;
				names.push(first?.refs?.assignee?.value.name);
			},
			{ include: { assignee: true } },
		);
		await flush();
		expect(names).toEqual([ 'Org Maya' ]);
		await client.ws(ORG).db.user.mutate.put({ id: 'u1', name: 'Org Maya Renamed' });
		await flush();
		expect(names.at(-1)).toBe('Org Maya Renamed');
		handle.close();
		await client.close();
	});

	it('re-fires a matches() watcher when the target moves in another workspace', async () => {
		const client = newClient();
		await seed(client);
		const seen: string[][] = [];
		const handle = client.ws(PROJ).db.task.watchQuery(
			{ where: (t) => t.assignee.matches((u) => u.name.eq('Org Maya')) },
			(rows) => seen.push(keysOf(rows)),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 't1' ]);
		await client.ws(ORG).db.user.mutate.put({ id: 'u1', name: 'Someone Else' });
		await flush();
		expect(seen.at(-1)).toEqual([]);
		handle.close();
		await client.close();
	});

	it('re-fires an include watcher when a self-referenced target in another workspace changes', async () => {
		// The included entity is the watcher's own, so the commit in the other
		// workspace names the watched entity outside the watcher's scope.
		const registry = handWritten(
			entityMsg('Node', 'node', [
				strField('id', 1),
				strField('name', 2),
				msgField('parent', 3, 'NodeRef'),
			]),
			refMsg('NodeRef', [ strField('workspace_id', 1, true), msgField('value', 2, 'Node') ]),
		);
		type Node = {
			$typeName: 'hand.Node';
			id: string;
			name: string;
			parent?: { $typeName: 'hand.NodeRef'; workspaceId: string; value?: Node };
		};
		const nodeDesc = registry.getMessage('hand.Node')! as GenMessage<Node>;
		const client = new SyncClient({
			accountId: 'acct',
			workspaces: [ ORG, PROJ ],
			defaultWorkspace: PROJ,
			kvStore: new MemoryKVStore(),
			entities: [ nodeDesc ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		await client.ws(ORG).mutate.put(nodeDesc, { id: 'p1', name: 'Org Parent' });
		await client.ws(PROJ).mutate.put(nodeDesc, {
			id: 'c1',
			name: 'Child',
			parent: { workspaceId: ORG, value: { id: 'p1' } },
		});
		type Hydrated = { refs?: Record<string, Row<{ name: string }> | undefined> } | undefined;
		const parentOf = (row: Hydrated) => row?.refs?.parent?.value.name;

		const queried: (string | undefined)[] = [];
		const query = client.ws(PROJ).watchQuery(
			nodeDesc,
			{ where: (n) => n.id.eq('c1') },
			(rows) => queried.push(parentOf(rows[0] as Hydrated)),
			{ include: { parent: true } },
		);
		const keyed: (string | undefined)[] = [];
		const rows = client.ws(PROJ).watchRows(
			nodeDesc,
			[ 'c1' ],
			(map) => keyed.push(parentOf(map.get('c1') as Hydrated)),
			{ include: { parent: true } },
		);
		await flush();
		expect(queried).toEqual([ 'Org Parent' ]);
		expect(keyed).toEqual([ 'Org Parent' ]);
		await client.ws(ORG).mutate.put(nodeDesc, { id: 'p1', name: 'Org Parent Renamed' });
		await flush();
		expect(queried.at(-1)).toBe('Org Parent Renamed');
		expect(keyed.at(-1)).toBe('Org Parent Renamed');
		query.close();
		rows.close();
		await client.close();
	});
});

describe('workspace-qualified references in the schema builder', () => {
	const person = entity('person', { id: text().key(), name: text() });
	const task = entity('bTask', {
		id: text().key(),
		assignee: workspaceRef(() => person),
		watchers: workspaceRefList(() => person),
	});

	it('synthesizes a reference message the registry reads as a qualified ref', () => {
		const registry = new EntityRegistry(defineSchema(person, task).descMessages());
		const fwd = registry.forwardRef('bTask', 'assignee')!;
		expect(fwd.targetEntity).toBe('person');
		expect(fwd.shape.valueField?.localName).toBe('value');
		expect(fwd.shape.workspaceField?.localName).toBe('workspaceId');
		expect(registry.forwardRef('bTask', 'watchers')?.repeated).toBe(true);
		expect(registry.reverseRefsOf('person').map((r) => r.name).sort()).toEqual([
			'bTasksByAssignee',
			'bTasksByWatchers',
		]);
	});

	it('types the stored shape as the workspace beside the target', () => {
		type Task = Infer<typeof task>;
		type Person = Infer<typeof person>;
		expectTypeOf<Task['assignee']>().toEqualTypeOf<WorkspaceRef<Person>>();
		expectTypeOf<Task['watchers']>().toEqualTypeOf<WorkspaceRef<Person>[]>();
	});

	it('resolves through the synthesized message like a hand-written one', async () => {
		const descs = defineSchema(person, task).descMessages();
		const personDesc = descs.find((d) => d.name === 'Person')!;
		const taskDesc = descs.find((d) => d.name === 'BTask')!;
		const client = new SyncClient({
			accountId: 'acct',
			workspaces: [ ORG, PROJ ],
			defaultWorkspace: PROJ,
			kvStore: new MemoryKVStore(),
			entities: descs,
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		await client.ws(ORG).mutate.put(personDesc, { id: 'p1', name: 'Org Pat' });
		await client.ws(PROJ).mutate.put(personDesc, { id: 'p1', name: 'Proj Pat' });
		await client.ws(PROJ).mutate.put(taskDesc, {
			id: 't1',
			assignee: { workspaceId: ORG, value: { id: 'p1' } },
		});
		const t1 = (await client.ws(PROJ).get(taskDesc, 't1'))!;
		const resolved = await client.ws(PROJ).resolveRef(taskDesc, 'assignee', t1);
		expect((resolved as Row<{ name: string }> | undefined)?.value.name).toBe('Org Pat');
		await client.close();
	});
});
