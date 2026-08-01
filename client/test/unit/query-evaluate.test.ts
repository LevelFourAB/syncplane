import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Cond, OrderKey } from '../../src/query/ast.js';
import { compareRows, evalCond, runQuery } from '../../src/query/evaluate.js';
import type { Row } from '../../src/read.js';

function widget(init: Parameters<typeof create<typeof WidgetSchema>>[1]): Widget {
	return create(WidgetSchema, init);
}

const cmp = (
	path: string[],
	op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte',
	value: string | number | bigint | boolean,
): Cond => ({
	kind: 'cmp',
	path,
	op,
	value,
});

describe('evalCond', () => {
	const w = widget({
		id: 'w1',
		label: 'alpha',
		count: 10,
		tags: [ 'a', 'b' ],
		settings: { color: 'red', size: 3 },
	});

	it('compares numbers with eq/neq/gt/gte/lt/lte', () => {
		expect(evalCond(w, cmp([ 'count' ], 'eq', 10))).toBe(true);
		expect(evalCond(w, cmp([ 'count' ], 'eq', 11))).toBe(false);
		expect(evalCond(w, cmp([ 'count' ], 'neq', 11))).toBe(true);
		expect(evalCond(w, cmp([ 'count' ], 'neq', 10))).toBe(false);
		expect(evalCond(w, cmp([ 'count' ], 'gt', 9))).toBe(true);
		expect(evalCond(w, cmp([ 'count' ], 'gt', 10))).toBe(false);
		expect(evalCond(w, cmp([ 'count' ], 'gte', 10))).toBe(true);
		expect(evalCond(w, cmp([ 'count' ], 'lt', 11))).toBe(true);
		expect(evalCond(w, cmp([ 'count' ], 'lt', 10))).toBe(false);
		expect(evalCond(w, cmp([ 'count' ], 'lte', 10))).toBe(true);
	});

	it('sees proto3 defaults (count=0, label="") as present values', () => {
		const blank = widget({ id: 'w0' });
		expect(evalCond(blank, cmp([ 'count' ], 'eq', 0))).toBe(true);
		expect(evalCond(blank, cmp([ 'count' ], 'neq', 0))).toBe(false);
		expect(evalCond(blank, cmp([ 'label' ], 'eq', ''))).toBe(true);
		expect(evalCond(blank, cmp([ 'label' ], 'neq', 'x'))).toBe(true);
	});

	it('handles string operators case-sensitively', () => {
		expect(evalCond(w, { kind: 'str', path: [ 'label' ], op: 'startsWith', value: 'al' })).toBe(true);
		expect(evalCond(w, { kind: 'str', path: [ 'label' ], op: 'startsWith', value: 'Al' })).toBe(
			false,
		);
		expect(evalCond(w, { kind: 'str', path: [ 'label' ], op: 'endsWith', value: 'pha' })).toBe(true);
		expect(evalCond(w, { kind: 'str', path: [ 'label' ], op: 'contains', value: 'lph' })).toBe(true);
		expect(evalCond(w, { kind: 'str', path: [ 'label' ], op: 'contains', value: 'zzz' })).toBe(false);
	});

	it('handles in', () => {
		expect(evalCond(w, { kind: 'in', path: [ 'count' ], values: [ 1, 10, 100 ] })).toBe(true);
		expect(evalCond(w, { kind: 'in', path: [ 'count' ], values: [ 1, 2 ] })).toBe(false);
		expect(evalCond(w, { kind: 'in', path: [ 'label' ], values: [ 'alpha', 'beta' ] })).toBe(true);
	});

	it('handles repeated operators', () => {
		expect(evalCond(w, { kind: 'rep', path: [ 'tags' ], op: 'has', value: 'a' })).toBe(true);
		expect(evalCond(w, { kind: 'rep', path: [ 'tags' ], op: 'has', value: 'z' })).toBe(false);
		expect(evalCond(w, { kind: 'rep', path: [ 'tags' ], op: 'hasAny', values: [ 'z', 'b' ] })).toBe(
			true,
		);
		expect(evalCond(w, { kind: 'rep', path: [ 'tags' ], op: 'hasAny', values: [ 'z' ] })).toBe(false);
		expect(evalCond(w, { kind: 'rep', path: [ 'tags' ], op: 'hasAll', values: [ 'a', 'b' ] })).toBe(
			true,
		);
		expect(evalCond(w, { kind: 'rep', path: [ 'tags' ], op: 'hasAll', values: [ 'a', 'z' ] })).toBe(
			false,
		);
		expect(evalCond(w, { kind: 'rep', path: [ 'tags' ], op: 'isEmpty' })).toBe(false);
		expect(evalCond(widget({ id: 'e' }), { kind: 'rep', path: [ 'tags' ], op: 'isEmpty' })).toBe(
			true,
		);
	});

	it('resolves nested paths when the message is set', () => {
		expect(evalCond(w, cmp([ 'settings', 'color' ], 'eq', 'red'))).toBe(true);
		expect(evalCond(w, cmp([ 'settings', 'size' ], 'gt', 2))).toBe(true);
		expect(evalCond(w, { kind: 'present', path: [ 'settings' ], op: 'isSet' })).toBe(true);
		expect(evalCond(w, { kind: 'present', path: [ 'settings' ], op: 'isNotSet' })).toBe(false);
	});

	it('makes every condition false on an unset intermediate message, except isNotSet', () => {
		const unset = widget({ id: 'w2', count: 5 });
		expect(evalCond(unset, cmp([ 'settings', 'color' ], 'eq', 'red'))).toBe(false);
		expect(evalCond(unset, cmp([ 'settings', 'color' ], 'neq', 'red'))).toBe(false);
		expect(evalCond(unset, cmp([ 'settings', 'size' ], 'lt', 100))).toBe(false);
		expect(evalCond(unset, { kind: 'str', path: [ 'settings', 'color' ], op: 'contains', value: '' }))
			.toBe(false);
		expect(evalCond(unset, { kind: 'present', path: [ 'settings' ], op: 'isSet' })).toBe(false);
		expect(evalCond(unset, { kind: 'present', path: [ 'settings' ], op: 'isNotSet' })).toBe(true);
		expect(evalCond(unset, { kind: 'present', path: [ 'settings', 'color' ], op: 'isNotSet' })).toBe(
			true,
		);
	});

	it('combines with and/or/not, including not(and(...))', () => {
		const countGt5 = cmp([ 'count' ], 'gt', 5);
		const labelBeta = cmp([ 'label' ], 'eq', 'beta');
		expect(evalCond(w, { kind: 'and', conds: [ countGt5, cmp([ 'label' ], 'eq', 'alpha') ] })).toBe(
			true,
		);
		expect(evalCond(w, { kind: 'and', conds: [ countGt5, labelBeta ] })).toBe(false);
		expect(evalCond(w, { kind: 'or', conds: [ labelBeta, countGt5 ] })).toBe(true);
		expect(evalCond(w, { kind: 'or', conds: [ labelBeta, cmp([ 'count' ], 'gt', 100) ] })).toBe(
			false,
		);
		expect(evalCond(w, { kind: 'not', cond: labelBeta })).toBe(true);
		expect(evalCond(w, { kind: 'not', cond: { kind: 'and', conds: [ countGt5, labelBeta ] } })).toBe(
			true,
		);
		expect(
			evalCond(w, {
				kind: 'not',
				cond: { kind: 'and', conds: [ countGt5, cmp([ 'label' ], 'eq', 'alpha') ] },
			}),
		).toBe(false);
		// Empty combinators: and() is vacuously true, or() vacuously false.
		expect(evalCond(w, { kind: 'and', conds: [] })).toBe(true);
		expect(evalCond(w, { kind: 'or', conds: [] })).toBe(false);
	});

	it('mixes number and bigint values', () => {
		const row = { big: 10n, small: 10 };
		expect(evalCond(row, cmp([ 'big' ], 'eq', 10))).toBe(true);
		expect(evalCond(row, cmp([ 'big' ], 'gt', 9))).toBe(true);
		expect(evalCond(row, cmp([ 'big' ], 'lt', 10.5))).toBe(true);
		expect(evalCond(row, cmp([ 'small' ], 'eq', 10n))).toBe(true);
		expect(evalCond(row, cmp([ 'small' ], 'gte', 10n))).toBe(true);
		expect(evalCond(row, { kind: 'in', path: [ 'big' ], values: [ 9, 10 ] })).toBe(true);
	});
});

describe('compareRows', () => {
	const key = (path: string[], dir: 'asc' | 'desc'): OrderKey => ({ path, dir });
	const rows: Row<Widget>[] = [
		{
			key: 'a',
			value: widget({ id: 'a', count: 2, label: 'x', settings: { size: 1 } }),
			version: 0n,
			workspaceId: 'ws',
		},
		{
			key: 'b',
			value: widget({ id: 'b', count: 1, label: 'x', settings: { size: 2 } }),
			version: 0n,
			workspaceId: 'ws',
		},
		{ key: 'c', value: widget({ id: 'c', count: 2, label: 'y' }), version: 0n, workspaceId: 'ws' }, // settings unset
	];

	it('orders by a single key in both directions', () => {
		const asc = [ ...rows ].sort((a, b) => compareRows(a, b, [ key([ 'count' ], 'asc') ]));
		expect(asc.map((r) => r.key)).toEqual([ 'b', 'a', 'c' ]);
		const desc = [ ...rows ].sort((a, b) => compareRows(a, b, [ key([ 'count' ], 'desc') ]));
		expect(desc.map((r) => r.key)).toEqual([ 'a', 'c', 'b' ]);
	});

	it('applies multiple keys in order', () => {
		const sorted = [ ...rows ].sort((a, b) =>
			compareRows(a, b, [ key([ 'label' ], 'asc'), key([ 'count' ], 'desc') ])
		);
		expect(sorted.map((r) => r.key)).toEqual([ 'a', 'b', 'c' ]);
	});

	it('sorts missing values last regardless of direction', () => {
		const asc = [ ...rows ].sort((a, b) => compareRows(a, b, [ key([ 'settings', 'size' ], 'asc') ]));
		expect(asc.map((r) => r.key)).toEqual([ 'a', 'b', 'c' ]);
		const desc = [ ...rows ].sort((a, b) =>
			compareRows(a, b, [ key([ 'settings', 'size' ], 'desc') ])
		);
		expect(desc.map((r) => r.key)).toEqual([ 'b', 'a', 'c' ]);
	});

	it('breaks full ties on the row key ascending', () => {
		const tied: Row<Widget>[] = [
			{ key: 'z', value: widget({ id: 'z', count: 1 }), version: 0n, workspaceId: 'ws' },
			{ key: 'a', value: widget({ id: 'a', count: 1 }), version: 0n, workspaceId: 'ws' },
		];
		const sorted = [ ...tied ].sort((a, b) => compareRows(a, b, [ key([ 'count' ], 'desc') ]));
		expect(sorted.map((r) => r.key)).toEqual([ 'a', 'z' ]);
	});
});

describe('runQuery', () => {
	const rows: Row<Widget>[] = [ 'a', 'b', 'c', 'd' ].map((id, i) => ({
		key: id,
		value: widget({ id, count: i + 1 }),
		version: 0n,
		workspaceId: 'ws',
	}));

	it('filters, sorts and slices', () => {
		const out = runQuery(rows, {
			where: cmp([ 'count' ], 'gt', 1) as Cond,
			orderBy: [ { path: [ 'count' ], dir: 'desc' } ],
			limit: 2,
			offset: 1,
		});
		expect(out.map((r) => r.key)).toEqual([ 'c', 'b' ]);
	});

	it('keeps key order when no orderBy is given', () => {
		const out = runQuery(rows, { where: { kind: 'cmp', path: [ 'count' ], op: 'gte', value: 2 } });
		expect(out.map((r) => r.key)).toEqual([ 'b', 'c', 'd' ]);
	});

	it('applies limit without offset and offset without limit', () => {
		expect(runQuery(rows, { limit: 2 }).map((r) => r.key)).toEqual([ 'a', 'b' ]);
		expect(runQuery(rows, { offset: 3 }).map((r) => r.key)).toEqual([ 'd' ]);
		expect(runQuery(rows, {})).toHaveLength(4);
	});

	it('does not mutate the input array', () => {
		const input = [ ...rows ].reverse();
		runQuery(input, { orderBy: [ { path: [ 'count' ], dir: 'asc' } ] });
		expect(input.map((r) => r.key)).toEqual([ 'd', 'c', 'b', 'a' ]);
	});
});
