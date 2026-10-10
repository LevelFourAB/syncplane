// Conformance vectors shared with the Go engine
// (engine/server/access_within_vectors_internal_test.go): the engine decides
// whether a row sits within a row that passes an access rule's within entry,
// and the client predicts the same answer from the rows it holds. Where both
// can decide they must agree; only the client can lack a row, and answers
// 'unknown' for it.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import {
	type AccessChain,
	type AccessRow,
	answerOf,
	type CanAnswer,
	unknownBecause,
	type Verdict,
	withinReach,
} from '../../src/access.js';

interface WithinCase {
	name: string;
	entity: string;
	key: string;
	within: string;
	graph: { entity: string; key: string; parents: AccessRow[] | 'unknown' }[];
	passes: { entity: string; key: string; passes: boolean | null }[];
	entitiesAbove?: Record<string, string[]>;
	want: CanAnswer;
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/access/within.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { cases: WithinCase[] };

const id = (row: AccessRow) => JSON.stringify([ row.entity, row.key ]);

/** The entitiesAbove of a case's chain: an entity the case does not list is not known. */
function entitiesAboveOf(
	listed: Record<string, string[]> | undefined,
): (entity: string) => ReadonlySet<string> | undefined {
	return (entity) => {
		const names = listed?.[entity];
		return names === undefined ? undefined : new Set(names);
	};
}

describe('access within conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	for (const c of vectors.cases) {
		it(c.name, () => {
			// A row the graph does not list has no parents.
			const parents = new Map<string, readonly AccessRow[] | undefined>();
			for (const g of c.graph) parents.set(id(g), g.parents === 'unknown' ? undefined : g.parents);
			const above = (row: AccessRow) => (parents.has(id(row)) ? parents.get(id(row)) : []);
			const chain: AccessChain = {
				parents: above({ entity: c.entity, key: c.key }),
				above,
				entitiesAbove: entitiesAboveOf(c.entitiesAbove),
			};
			// A row of the entry's entity the case does not list does not exist.
			const passes = (at: AccessRow): Verdict => {
				const p = c.passes.find((x) => x.entity === at.entity && x.key === at.key);
				if (p === undefined) return 'no';
				return p.passes === null ? unknownBecause('row_not_held') : p.passes ? 'yes' : 'no';
			};
			expect(answerOf(withinReach({ entity: c.entity, key: c.key }, c.within, chain, passes)))
				.toBe(c.want);
		});
	}
});
