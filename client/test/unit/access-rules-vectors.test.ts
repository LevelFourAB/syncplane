// Conformance vectors shared with the Go engine
// (engine/server/access_rules_vectors_internal_test.go): can() predicts with
// the decision the server's write checks make, so the two must agree on every
// case the server can decide, and the client answers 'unknown' only where an
// input is out of its reach.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { DescField } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import {
	type AccessOperation,
	type CanAnswer,
	decideRules,
	type RuleRow,
} from '../../src/access.js';
import type { AccessRuleDef } from '../../src/mutations.js';

interface VectorRule {
	read?: boolean;
	create?: boolean;
	update?: boolean;
	delete?: boolean;
	write?: boolean;
	permission?: string;
	admitted?: boolean;
	subject?: string;
	fields?: string[];
}

interface VectorRow {
	held: Record<string, boolean | null>;
	fields: Record<string, string | null>;
}

interface RulesCase {
	name: string;
	op: AccessOperation;
	rules: VectorRule[];
	subjects: string[];
	row: VectorRow;
	final?: VectorRow;
	changed?: string[] | null;
	want: CanAnswer;
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/access/rules.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { cases: RulesCase[] };

/** Only the name of a subject field is read, so the rest stays unbuilt. */
const ruleOf = (r: VectorRule): AccessRuleDef => ({
	read: r.read === true,
	create: r.write === true || r.create === true,
	update: r.write === true || r.update === true,
	delete: r.write === true || r.delete === true,
	permission: r.admitted === true ? undefined : r.permission,
	subject: r.subject === undefined ? undefined : ({ name: r.subject } as DescField),
	fields: r.fields === undefined ? undefined : [ ...r.fields ].sort(),
});

const rowOf = (v: VectorRow): RuleRow => ({
	held: (permission) => {
		const held = v.held[permission];
		return held === null ? 'unknown' : held === true ? 'yes' : 'no';
	},
	subject: (field) => v.fields[field] ?? undefined,
});

describe('access rule conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	for (const tc of vectors.cases) {
		it(tc.name, () => {
			const answer = decideRules(
				tc.rules.map(ruleOf),
				tc.op,
				tc.subjects,
				rowOf(tc.row),
				tc.final === undefined ? undefined : rowOf(tc.final),
				tc.changed,
			);
			expect(answer).toBe(tc.want);
		});
	}
});
