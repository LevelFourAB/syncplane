import { describe, expect, it } from 'vitest';

import { type Todo, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	IssueSchema,
	type Widget,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { CompiledQuery } from '../../src/query/ast.js';
import { and, compileQuery, not, or } from '../../src/query/proxy.js';
import { prepareQuery, toWirePaths } from '../../src/query/wire.js';

describe('toWirePaths', () => {
	it('translates top-level localNames to proto field names', () => {
		const compiled = compileQuery<Todo>({
			where: (t) => t.createdAtMs.gt(100n),
			orderBy: (t) => [ t.createdAtMs.desc(), t.title.asc() ],
			limit: 10,
			offset: 5,
		});
		expect(toWirePaths(TodoSchema, compiled)).toEqual({
			where: { kind: 'cmp', path: [ 'created_at_ms' ], op: 'gt', value: 100n },
			orderBy: [
				{ path: [ 'created_at_ms' ], dir: 'desc' },
				{ path: [ 'title' ], dir: 'asc' },
			],
			limit: 10,
			offset: 5,
		});
	});

	it('descends through nested message fields', () => {
		const compiled = compileQuery<Widget>({
			where: (w) => w.settings.color.eq('red'),
		});
		expect(toWirePaths(WidgetSchema, compiled).where).toEqual({
			kind: 'cmp',
			path: [ 'settings', 'color' ],
			op: 'eq',
			value: 'red',
		});
	});

	it('translates every Cond kind, including nested boolean structure', () => {
		const compiled = compileQuery<Widget>({
			where: (w) =>
				or(
					and(w.label.startsWith('a'), w.count.in([ 1, 2 ])),
					not(w.tags.has('x')),
					w.tags.isEmpty(),
					w.settings.isSet(),
				),
		});
		const wire = toWirePaths(WidgetSchema, compiled);
		expect(wire.where).toEqual({
			kind: 'or',
			conds: [
				{
					kind: 'and',
					conds: [
						{ kind: 'str', path: [ 'label' ], op: 'startsWith', value: 'a' },
						{ kind: 'in', path: [ 'count' ], values: [ 1, 2 ] },
					],
				},
				{ kind: 'not', cond: { kind: 'rep', path: [ 'tags' ], op: 'has', value: 'x' } },
				{ kind: 'rep', path: [ 'tags' ], op: 'isEmpty' },
				{ kind: 'present', path: [ 'settings' ], op: 'isSet' },
			],
		});
	});

	it('does not mutate the input query', () => {
		const compiled = compileQuery<Todo>({ where: (t) => t.title.eq('x') });
		const before = JSON.stringify(compiled);
		toWirePaths(TodoSchema, compiled);
		expect(JSON.stringify(compiled)).toBe(before);
	});

	it('throws on unknown fields', () => {
		const bogus: CompiledQuery = {
			where: { kind: 'cmp', path: [ 'nope' ], op: 'eq', value: 1 },
		};
		expect(() => toWirePaths(TodoSchema, bogus)).toThrow(/has no field "nope"/);
	});

	it('throws when descending through a non-message field', () => {
		const bogus: CompiledQuery = {
			where: { kind: 'cmp', path: [ 'title', 'length' ], op: 'eq', value: 1 },
		};
		expect(() => toWirePaths(TodoSchema, bogus)).toThrow(/not a message field/);
	});
});

describe('prepareQuery', () => {
	it('extends a singular ref leaf with its target key field', () => {
		const compiled: CompiledQuery = {
			where: { kind: 'cmp', path: [ 'project' ], op: 'eq', value: 'p1' },
		};
		expect(prepareQuery(IssueSchema, compiled).where).toEqual({
			kind: 'cmp',
			path: [ 'project', 'id' ],
			op: 'eq',
			value: 'p1',
		});
	});

	it('resolves ref leaves inside orderBy too', () => {
		const compiled: CompiledQuery = { orderBy: [ { path: [ 'assignee' ], dir: 'asc' } ] };
		expect(prepareQuery(IssueSchema, compiled).orderBy).toEqual([
			{ path: [ 'assignee', 'id' ], dir: 'asc' },
		]);
	});

	it('leaves a present check on the bare ref', () => {
		const compiled: CompiledQuery = {
			where: { kind: 'present', path: [ 'project' ], op: 'isSet' },
		};
		expect(prepareQuery(IssueSchema, compiled).where).toEqual({
			kind: 'present',
			path: [ 'project' ],
			op: 'isSet',
		});
	});

	it('translates a resolved ref leaf onto the wire as project.id', () => {
		const compiled = prepareQuery(IssueSchema, {
			where: { kind: 'cmp', path: [ 'project' ], op: 'eq', value: 'p1' },
		});
		expect(toWirePaths(IssueSchema, compiled).where).toEqual({
			kind: 'cmp',
			path: [ 'project', 'id' ],
			op: 'eq',
			value: 'p1',
		});
	});

	it('rejects descending through a ref into a non-key field', () => {
		const bogus: CompiledQuery = {
			where: { kind: 'cmp', path: [ 'project', 'name' ], op: 'eq', value: 'x' },
		};
		expect(() => prepareQuery(IssueSchema, bogus)).toThrow(/only its key fields are addressable/);
	});

	it('rejects scalar comparison of a composite-key ref', () => {
		const bogus: CompiledQuery = { where: { kind: 'cmp', path: [ 'pair' ], op: 'eq', value: 'x' } };
		expect(() => prepareQuery(IssueSchema, bogus)).toThrow(/composite key/);
	});

	it('rejects scalar comparison of a repeated ref', () => {
		const bogus: CompiledQuery = {
			where: { kind: 'cmp', path: [ 'blockedBy' ], op: 'eq', value: 'x' },
		};
		expect(() => prepareQuery(IssueSchema, bogus)).toThrow(/not comparable/);
	});

	// Leaf/operator compatibility. The Go compiler rejects the same set with
	// the same wording (engine/internal/queryeval), and the shared vectors pin
	// the rejections themselves; these pin the hint the developer reads.
	it('names the operator that works when a leaf is not comparable', () => {
		const bogus: CompiledQuery = { where: { kind: 'cmp', path: [ 'raw' ], op: 'eq', value: 'x' } };
		expect(() => prepareQuery(WidgetSchema, bogus)).toThrow(/bytes field raw is not comparable/);
		expect(() => prepareQuery(WidgetSchema, bogus)).toThrow(/use isSet\/isNotSet/);
	});

	it('points a comparison on a repeated field at the rep operators', () => {
		const bogus: CompiledQuery = { where: { kind: 'cmp', path: [ 'tags' ], op: 'eq', value: 'x' } };
		expect(() => prepareQuery(WidgetSchema, bogus)).toThrow(/use has\/hasAny\/hasAll\/isEmpty/);
	});

	it('does not offer isSet for a map, which is always set', () => {
		const bogus: CompiledQuery = { where: { kind: 'cmp', path: [ 'attrs' ], op: 'eq', value: 'x' } };
		expect(() => prepareQuery(WidgetSchema, bogus)).toThrow(/map fields cannot be queried/);
		expect(() => prepareQuery(WidgetSchema, bogus)).not.toThrow(/isSet/);
	});

	it('points a comparison on a message leaf at its scalar fields', () => {
		const bogus: CompiledQuery = {
			where: { kind: 'cmp', path: [ 'settings' ], op: 'eq', value: 'x' },
		};
		expect(() => prepareQuery(WidgetSchema, bogus)).toThrow(/compare a scalar field inside it/);
	});

	it('rejects a string operator on a non-string leaf', () => {
		const bogus: CompiledQuery = {
			where: { kind: 'str', path: [ 'count' ], op: 'contains', value: '3' },
		};
		expect(() => prepareQuery(WidgetSchema, bogus)).toThrow(
			/startsWith\/endsWith\/contains need a string or text field/,
		);
	});

	it('allows string operators and ordering on a collaborative text field', () => {
		// Text compares as its visible plain string, so it is a string leaf.
		expect(() =>
			prepareQuery(WidgetSchema, {
				where: { kind: 'str', path: [ 'body' ], op: 'contains', value: 'x' },
				orderBy: [ { path: [ 'body' ], dir: 'asc' } ],
			})
		).not.toThrow();
	});

	it('allows isEmpty on a repeated message field, and points membership at any()', () => {
		expect(() =>
			prepareQuery(WidgetSchema, { where: { kind: 'rep', path: [ 'members' ], op: 'isEmpty' } })
		).not.toThrow();
		expect(() =>
			prepareQuery(WidgetSchema, {
				where: { kind: 'rep', path: [ 'members' ], op: 'has', value: 'u1' },
			})
		).toThrow(/use any\(\)/);
	});

	it('resolves an any() sub-condition against the ELEMENT type', () => {
		expect(() =>
			prepareQuery(WidgetSchema, {
				where: {
					kind: 'exists',
					path: [ 'members' ],
					cond: { kind: 'cmp', path: [ 'uid' ], op: 'eq', value: 'u1' },
				},
			})
		).not.toThrow();
		// `label` is a field of Widget, not of Member — proof the sub-condition
		// is not resolving against the row.
		expect(() =>
			prepareQuery(WidgetSchema, {
				where: {
					kind: 'exists',
					path: [ 'members' ],
					cond: { kind: 'cmp', path: [ 'label' ], op: 'eq', value: 'x' },
				},
			})
		).toThrow(/has no field "label"/);
	});

	it('rejects any() over anything but a repeated message field', () => {
		expect(() =>
			prepareQuery(WidgetSchema, {
				where: {
					kind: 'exists',
					path: [ 'tags' ],
					cond: { kind: 'cmp', path: [ 'uid' ], op: 'eq', value: 'u1' },
				},
			})
		).toThrow(/has\/hasAny\/hasAll for a repeated scalar/);
	});

	it('rewrites membership over a to-many reference into an any() on the target key', () => {
		const out = prepareQuery(IssueSchema, {
			where: { kind: 'rep', path: [ 'blockedBy' ], op: 'has', value: 'i2' },
		});
		expect(out.where).toEqual({
			kind: 'exists',
			path: [ 'blockedBy' ],
			cond: { kind: 'cmp', path: [ 'id' ], op: 'eq', value: 'i2' },
		});
	});

	it('spells hasAll over a to-many reference as one existential per value', () => {
		const out = prepareQuery(IssueSchema, {
			where: { kind: 'rep', path: [ 'blockedBy' ], op: 'hasAll', values: [ 'i2', 'i3' ] },
		});
		// One any() per value, not one any() over all of them: no single element
		// can be two keys at once.
		expect(out.where).toEqual({
			kind: 'and',
			conds: [
				{
					kind: 'exists',
					path: [ 'blockedBy' ],
					cond: { kind: 'cmp', path: [ 'id' ], op: 'eq', value: 'i2' },
				},
				{
					kind: 'exists',
					path: [ 'blockedBy' ],
					cond: { kind: 'cmp', path: [ 'id' ], op: 'eq', value: 'i3' },
				},
			],
		});
	});

	it('keeps an any() over a reference list to the target key fields', () => {
		expect(() =>
			prepareQuery(IssueSchema, {
				where: {
					kind: 'exists',
					path: [ 'blockedBy' ],
					cond: { kind: 'str', path: [ 'title' ], op: 'contains', value: 'x' },
				},
			})
		).toThrow(/holds only its key fields/);
	});

	it('rejects a rep operator on a singular field', () => {
		const bogus: CompiledQuery = { where: { kind: 'rep', path: [ 'label' ], op: 'isEmpty' } };
		expect(() => prepareQuery(WidgetSchema, bogus)).toThrow(
			/has\/hasAny\/hasAll\/isEmpty need a repeated field/,
		);
	});

	it('keeps a literal/field type mismatch a runtime no-match, not an error', () => {
		// count is comparable; only the literal is the wrong kind. That is a
		// legitimate query matching nothing, unlike an incomparable leaf.
		expect(() =>
			prepareQuery(WidgetSchema, { where: { kind: 'cmp', path: [ 'count' ], op: 'eq', value: 'x' } })
		).not.toThrow();
	});

	it('still allows isSet on any leaf', () => {
		for (const path of [ [ 'raw' ], [ 'attrs' ], [ 'doc' ], [ 'tags' ] ]) {
			expect(() => prepareQuery(WidgetSchema, { where: { kind: 'present', path, op: 'isSet' } }))
				.not.toThrow();
		}
	});
});
