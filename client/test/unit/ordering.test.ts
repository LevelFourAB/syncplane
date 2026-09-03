import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { IssueSchema, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { generateKeyBetween, newElementId, sortOrderedFields } from '../../src/ordering.js';

describe('generateKeyBetween (ordering)', () => {
	it('creates an ordered first/after/before/between sequence', () => {
		const actor = 'actor-1';
		const first = generateKeyBetween(null, null, actor);
		const after = generateKeyBetween(first, null, actor);
		expect(after > first).toBe(true);
		const before = generateKeyBetween(null, first, actor);
		expect(before < first).toBe(true);
		const mid = generateKeyBetween(before, first, actor);
		expect(mid > before && mid < first).toBe(true);
	});

	it('stays strictly between two existing keys', () => {
		const a = generateKeyBetween(null, null, 'x');
		const b = generateKeyBetween(a, null, 'x');
		const mid = generateKeyBetween(a, b, 'x');
		expect(mid > a && mid < b).toBe(true);
	});

	it('gives distinct, still-insertable keys to different actors in one gap', () => {
		const k1 = generateKeyBetween('a0', 'a1', 'actor-AAAA');
		const k2 = generateKeyBetween('a0', 'a1', 'actor-BBBB');
		expect(k1).not.toBe(k2);
		expect(k1 > 'a0' && k1 < 'a1').toBe(true);
		expect(k2 > 'a0' && k2 < 'a1').toBe(true);
		const [ lo, hi ] = k1 < k2 ? [ k1, k2 ] : [ k2, k1 ];
		const between = generateKeyBetween(lo, hi, 'actor-CCCC');
		expect(between > lo && between < hi).toBe(true);
	});

	it('keeps many sequential inserts monotonically ordered', () => {
		let prev = generateKeyBetween(null, null, 'x');
		const keys = [ prev ];
		for (let i = 0; i < 50; i++) {
			prev = generateKeyBetween(prev, null, 'x');
			keys.push(prev);
		}
		expect([ ...keys ].sort()).toEqual(keys);
	});
});

describe('newElementId', () => {
	it('mints distinct ids', () => {
		const ids = new Set(Array.from({ length: 200 }, () => newElementId('client-1')));
		expect(ids.size).toBe(200);
	});
});

describe('sortOrderedFields', () => {
	it('orders a movable list by pos', () => {
		const w = create(WidgetSchema, {
			id: 'w',
			items: [
				{ id: 'c', pos: 'a2', text: 'C' },
				{ id: 'a', pos: 'a0', text: 'A' },
				{ id: 'b', pos: 'a1', text: 'B' },
			],
		});
		sortOrderedFields(WidgetSchema, w);
		expect(w.items.map((i) => i.text)).toEqual([ 'A', 'B', 'C' ]);
	});

	it('breaks equal positions by element id', () => {
		const w = create(WidgetSchema, {
			id: 'w',
			items: [
				{ id: 'z', pos: 'a0', text: 'Z' },
				{ id: 'a', pos: 'a0', text: 'A' },
			],
		});
		sortOrderedFields(WidgetSchema, w);
		expect(w.items.map((i) => i.id)).toEqual([ 'a', 'z' ]);
	});

	it('leaves non-movable lists untouched', () => {
		const w = create(WidgetSchema, { id: 'w', tags: [ 'c', 'a', 'b' ] });
		sortOrderedFields(WidgetSchema, w);
		expect(w.tags).toEqual([ 'c', 'a', 'b' ]);
	});

	it('orders every message of a schema it has already seen', () => {
		for (let i = 0; i < 3; i++) {
			const w = create(WidgetSchema, {
				id: `w${i}`,
				items: [
					{ id: 'c', pos: 'a2', text: 'C' },
					{ id: 'a', pos: 'a0', text: 'A' },
					{ id: 'b', pos: 'a1', text: 'B' },
				],
			});
			sortOrderedFields(WidgetSchema, w);
			expect(w.items.map((i) => i.text)).toEqual([ 'A', 'B', 'C' ]);
		}
	});

	it('leaves a schema with no movable list alone', () => {
		const issue = create(IssueSchema, {
			id: 'i1',
			labels: [ 'c', 'a', 'b' ],
			blockedBy: [ { id: 'z' }, { id: 'a' } ],
		});
		sortOrderedFields(IssueSchema, issue);
		sortOrderedFields(IssueSchema, issue);
		expect(issue.labels).toEqual([ 'c', 'a', 'b' ]);
		expect(issue.blockedBy.map((i) => i.id)).toEqual([ 'z', 'a' ]);
	});
});
