import { describe, expect, expectTypeOf, it, vi } from 'vitest';

import type { Widget } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { EntityRegistry } from '../../src/mutations.js';
import type { Cond } from '../../src/query/ast.js';
import {
	and,
	compileQuery,
	not,
	or,
	RESERVED_QUERY_NAMES,
	rootRef,
} from '../../src/query/proxy.js';
import type { PresenceOps, QueryRef } from '../../src/query/types.js';
import { defineSchema, entity, int32, text } from '../../src/schema/builder.js';

describe('compileQuery', () => {
	it('compiles the approved example into the expected AST', () => {
		const compiled = compileQuery<Widget>({
			where: (w) =>
				and(
					w.settings.color.eq('red'),
					or(w.count.gt(10), w.tags.has('urgent')),
					not(w.label.startsWith('tmp-')),
				),
			orderBy: (w) => [ w.count.desc() ],
			limit: 20,
		});

		expect(compiled).toEqual({
			where: {
				kind: 'and',
				conds: [
					{ kind: 'cmp', path: [ 'settings', 'color' ], op: 'eq', value: 'red' },
					{
						kind: 'or',
						conds: [
							{ kind: 'cmp', path: [ 'count' ], op: 'gt', value: 10 },
							{ kind: 'rep', path: [ 'tags' ], op: 'has', value: 'urgent' },
						],
					},
					{ kind: 'not', cond: { kind: 'str', path: [ 'label' ], op: 'startsWith', value: 'tmp-' } },
				],
			},
			orderBy: [ { path: [ 'count' ], dir: 'desc' } ],
			limit: 20,
		});
	});

	it('compiles presence, repeated and in operators', () => {
		const compiled = compileQuery<Widget>({
			where: (w) =>
				and(
					w.settings.isNotSet(),
					w.doc.isSet(),
					w.tags.isEmpty(),
					w.tags.hasAll([ 'a', 'b' ]),
					w.count.in([ 1, 2 ]),
				),
			orderBy: (w) => [ w.label.asc(), w.settings.size.desc() ],
			offset: 5,
		});
		expect(compiled).toEqual({
			where: {
				kind: 'and',
				conds: [
					{ kind: 'present', path: [ 'settings' ], op: 'isNotSet' },
					{ kind: 'present', path: [ 'doc' ], op: 'isSet' },
					{ kind: 'rep', path: [ 'tags' ], op: 'isEmpty' },
					{ kind: 'rep', path: [ 'tags' ], op: 'hasAll', values: [ 'a', 'b' ] },
					{ kind: 'in', path: [ 'count' ], values: [ 1, 2 ] },
				],
			},
			orderBy: [
				{ path: [ 'label' ], dir: 'asc' },
				{ path: [ 'settings', 'size' ], dir: 'desc' },
			],
			offset: 5,
		});
	});

	it('scopes an any() sub-condition to the element, with paths relative to it', () => {
		const compiled = compileQuery<Widget>({
			where: (w) => w.members.any((m) => and(m.uid.eq('u1'), m.role.gt(2))),
		});
		expect(compiled).toEqual({
			where: {
				kind: 'exists',
				path: [ 'members' ],
				cond: {
					kind: 'and',
					conds: [
						// Relative to the element: `uid`, not `members.uid`. That is
						// what makes the two conditions describe ONE member.
						{ kind: 'cmp', path: [ 'uid' ], op: 'eq', value: 'u1' },
						{ kind: 'cmp', path: [ 'role' ], op: 'gt', value: 2 },
					],
				},
			},
		});
	});

	it('nests any(), each level rooted at its own element', () => {
		const compiled = compileQuery<Widget>({
			where: (w) => w.members.any((m) => m.tags.has('x')),
		});
		expect(compiled).toEqual({
			where: {
				kind: 'exists',
				path: [ 'members' ],
				cond: { kind: 'rep', path: [ 'tags' ], op: 'has', value: 'x' },
			},
		});
	});

	it('omits absent clauses', () => {
		expect(compileQuery<Widget>({})).toEqual({});
		expect(compileQuery<Widget>({ limit: 1 })).toEqual({ limit: 1 });
	});
});

describe('combinators', () => {
	const a: Cond = { kind: 'cmp', path: [ 'count' ], op: 'eq', value: 1 };
	const b: Cond = { kind: 'cmp', path: [ 'count' ], op: 'eq', value: 2 };
	const c: Cond = { kind: 'cmp', path: [ 'count' ], op: 'eq', value: 3 };

	it('flattens nested same-kind combinators', () => {
		expect(and(a, and(b, c))).toEqual({ kind: 'and', conds: [ a, b, c ] });
		expect(or(or(a, b), c)).toEqual({ kind: 'or', conds: [ a, b, c ] });
	});

	it('does not flatten across kinds', () => {
		expect(and(a, or(b, c))).toEqual({ kind: 'and', conds: [ a, { kind: 'or', conds: [ b, c ] } ] });
		expect(not(and(a, b))).toEqual({ kind: 'not', cond: { kind: 'and', conds: [ a, b ] } });
	});
});

describe('query types', () => {
	const w = rootRef<Widget>();

	it('exposes the right operators per field kind', () => {
		expectTypeOf(w.count.gt).toBeFunction();
		expectTypeOf(w.count).not.toHaveProperty('startsWith');
		expectTypeOf(w.count).not.toHaveProperty('has');

		expectTypeOf(w.label.startsWith).toBeFunction();
		expectTypeOf(w.label).not.toHaveProperty('gt');

		expectTypeOf(w.tags.has).toBeFunction();
		expectTypeOf(w.tags).not.toHaveProperty('eq');

		expectTypeOf(w.settings.color.eq).toBeFunction();
		expectTypeOf(w.settings.isSet).toBeFunction();
		expectTypeOf(w.doc.isNotSet).toBeFunction();
		// doc is a message field, so it can also be drilled into.
		expectTypeOf(w.doc.note.contains).toBeFunction();
		expectTypeOf(w.doc.data).toEqualTypeOf<PresenceOps>();

		expectTypeOf<QueryRef<Widget>>().not.toHaveProperty('$typeName');
		expectTypeOf<QueryRef<Widget>>().not.toHaveProperty('$unknown');
	});

	it('rejects wrong value types', () => {
		// @ts-expect-error count is numeric; string values are rejected
		void (() => w.count.eq('x'));
		// @ts-expect-error tags elements are strings
		void (() => w.tags.has(1));
		// @ts-expect-error label is a string field
		void (() => w.label.eq(5));
	});

	it('addresses a map entry through at(), typed by the map value', () => {
		// A map's operator surface is at()/isEmpty and nothing else — the whole
		// map compares against nothing.
		expectTypeOf(w.attrs.at).toBeFunction();
		expectTypeOf(w.attrs.isEmpty).toBeFunction();
		expectTypeOf(w.attrs).not.toHaveProperty('eq');
		expectTypeOf(w.attrs).not.toHaveProperty('has');

		// A scalar-valued map yields that scalar's operators.
		expectTypeOf(w.attrs.at('color').startsWith).toBeFunction();
		// A message-valued map yields the value's own surface, oneof group and
		// all — the same spelling a oneof takes anywhere else.
		expectTypeOf(w.custom.at('priority').kind.number.gt).toBeFunction();
		expectTypeOf(w.custom.at('priority').kind.text.contains).toBeFunction();
		expectTypeOf(w.custom.at('tags').kind.labels.values.hasAny).toBeFunction();
		// An entry has explicit presence, unlike the map holding it.
		expectTypeOf(w.custom.at('priority').isNotSet).toBeFunction();
	});

	it('scopes one map ENTRY through any(), with both sides addressable', () => {
		// The mirror of at(): at() names the field and asks about its value,
		// any() asks about the value and leaves the field open.
		expectTypeOf(w.custom.any).toBeFunction();
		expectTypeOf(w.attrs.any).toBeFunction();
	});

	it('records an entry sub-condition relative to the entry', () => {
		// The sub-proxy is rooted at the EMPTY path, so `key` and `value` are
		// ordinary first segments — which is the form both evaluators expect.
		const cond = compileQuery<Widget>({
			where: (r) => r.custom.any((e) => and(e.key.eq('priority'), e.value.kind.number.eq(5))),
		}).where;
		// ONE exists holding a conjunction, not two exists — which is exactly
		// what makes the two halves describe the same entry.
		expect(cond).toEqual({
			kind: 'exists',
			path: [ 'custom' ],
			cond: {
				kind: 'and',
				conds: [
					{ kind: 'cmp', path: [ 'key' ], op: 'eq', value: 'priority' },
					{ kind: 'cmp', path: [ 'value', 'kind', 'number' ], op: 'eq', value: 5 },
				],
			},
		});
	});

	it('rejects a wrong-typed literal through at()', () => {
		// @ts-expect-error attrs values are strings
		void (() => w.attrs.at('color').eq(5));
		// @ts-expect-error the `number` variant is numeric
		void (() => w.custom.at('priority').kind.number.gt('x'));
		// @ts-expect-error `nope` is not a member of the value's oneof
		void (() => w.custom.at('priority').kind.nope.eq('x'));
	});
});

describe('reserved operator names', () => {
	it('warns at registration when a field is shadowed by an operator', () => {
		// A field named after an operator is unreachable from `where`/`orderBy`:
		// the proxy returns the operator builder, and the type layer offers the
		// operator surface. Registration is the last chance to say so.
		const shadowed = entity('shadowed', {
			id: text().key(),
			in: text(),
			contains: int32(),
		});
		const bundle = defineSchema(shadowed);
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			new EntityRegistry(bundle.descMessages());
			const messages = warn.mock.calls.map((c) => String(c[0]));
			expect(messages.some((m) => m.includes('"in"'))).toBe(true);
			expect(messages.some((m) => m.includes('"contains"'))).toBe(true);
		} finally {
			warn.mockRestore();
		}
	});

	it('stays quiet for an ordinary schema', () => {
		const plain = entity('plain', { id: text().key(), title: text() });
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			new EntityRegistry(defineSchema(plain).descMessages());
			expect(warn).not.toHaveBeenCalled();
		} finally {
			warn.mockRestore();
		}
	});

	it('reserves exactly the names the proxy intercepts', () => {
		// Pinned so adding an operator without a thought for shadowing shows up
		// here rather than as a field silently disappearing from someone's query.
		expect([ ...RESERVED_QUERY_NAMES ].sort()).toEqual([
			'any',
			'asc',
			'at',
			'contains',
			'desc',
			'endsWith',
			'eq',
			'gt',
			'gte',
			'has',
			'hasAll',
			'hasAny',
			'in',
			'isEmpty',
			'isNotSet',
			'isSet',
			'lt',
			'lte',
			'match',
			'matches',
			'neq',
			'relevance',
			'startsWith',
		]);
	});
});
