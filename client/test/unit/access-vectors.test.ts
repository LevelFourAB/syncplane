// Conformance vectors shared with the Go engine
// (engine/server/access_internal_test.go): the engine decides whether a caller
// holds a permission at a row, and the client predicts the same answer from the
// summary the engine sent. Where both can decide they must agree; only the
// client can lack an input, and answers 'unknown' for it.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import {
	type AccessChain,
	type AccessRow,
	AccessSummary,
	type CanAnswer,
	canAt,
	canAtEach,
} from '../../src/access.js';
import { callerAccessOf } from '../../src/testing/access.js';

interface AccessCase {
	name: string;
	access: Record<string, { root?: boolean; anchors?: Record<string, string[]> }> | null;
	permission: string;
	entity: string;
	key: string;
	chain?: (AccessRow | 'unknown')[];
	graph?: { entity: string; key: string; parents: AccessRow[] | 'unknown' }[];
	each?: boolean;
	entitiesAbove?: Record<string, string[]>;
	want: CanAnswer;
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/access/can.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { cases: AccessCase[] };

function summaryOf(access: AccessCase['access']): AccessSummary | undefined {
	if (access === null) return undefined;
	return new AccessSummary(callerAccessOf({
		permissions: Object.entries(access).map(([ permission, p ]) => ({
			permission,
			root: p.root ?? false,
			anchors: Object.entries(p.anchors ?? {}).map(([ entity, keys ]) => ({ entity, keys })),
		})),
	}));
}

const id = (row: AccessRow) => JSON.stringify([ row.entity, row.key ]);

/** The chain a case describes: a row it does not list has no parents. */
function chainOf(c: AccessCase): AccessChain {
	const parents = new Map<string, readonly AccessRow[] | undefined>();
	let at: AccessRow = { entity: c.entity, key: c.key };
	for (const row of c.chain ?? []) {
		if (row === 'unknown') {
			parents.set(id(at), undefined);
			break;
		}
		parents.set(id(at), [ row ]);
		at = row;
	}
	for (const node of c.graph ?? []) {
		parents.set(id(node), node.parents === 'unknown' ? undefined : node.parents);
	}
	const above = (row: AccessRow) => (parents.has(id(row)) ? parents.get(id(row)) : []);
	return {
		parents: above({ entity: c.entity, key: c.key }),
		above,
		entitiesAbove: entitiesAboveOf(c.entitiesAbove),
	};
}

/** The entitiesAbove of a case's chain: an entity the case does not list is not known. */
function entitiesAboveOf(
	listed: Record<string, string[]> | undefined,
): (entity: string) => ReadonlySet<string> | undefined {
	return (entity) => {
		const names = listed?.[entity];
		return names === undefined ? undefined : new Set(names);
	};
}

describe('access conformance vectors', () => {
	for (const c of vectors.cases) {
		it(c.name, () => {
			const decide = c.each ? canAtEach : canAt;
			expect(decide(summaryOf(c.access), c.permission, c.entity, c.key, chainOf(c))).toBe(c.want);
		});
	}
});
