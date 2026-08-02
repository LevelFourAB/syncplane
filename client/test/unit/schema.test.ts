import { create, fromBinary, getOption, hasOption, toBinary } from '@bufbuild/protobuf';
import { describe, expect, expectTypeOf, it } from 'vitest';

import {
	DeletePolicy,
	entity as entityExt,
	field_behavior as fieldBehaviorExt,
} from '../../src/gen/syncplane/v1/options_pb.js';
import {
	entityDefOf,
	EntityRegistry,
	isRef,
	keyFieldsFor,
	keyOf,
	refKeyFields,
} from '../../src/mutations.js';
import {
	bool,
	bytes,
	defineSchema,
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
	text,
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

	it('defaults delete_policy to ALLOW and carries trustedOnly through to the option', () => {
		expect(getOption(bundle.descMessage('todo'), entityExt).deletePolicy)
			.toBe(DeletePolicy.ALLOW);

		const ledger = entity('ledger', { id: text().key() }, { deletePolicy: 'trustedOnly' });
		const desc = defineSchema(ledger).descMessage('ledger');
		expect(getOption(desc, entityExt).deletePolicy).toBe(DeletePolicy.TRUSTED_ONLY);
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
	reporter: ref(() => user),
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

	it('drops only the entities (not nested types) into EntityRegistry', () => {
		const reg = new EntityRegistry(bundle.descMessages());
		expect(reg.names().sort()).toEqual([ 'issue', 'project', 'user' ]);
		// Two refs to user disambiguate into auto-named reverse relations.
		const revNames = reg.reverseRefsOf('user').map((r) => r.name).sort();
		expect(revNames).toEqual([ 'issuesByAssignee', 'issuesByReporter' ]);
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
