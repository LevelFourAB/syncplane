// Conformance vectors shared with the Go engine
// (engine/server/access_rules_vectors_internal_test.go): can() predicts with
// the decision the server's write checks make, so the two must agree on every
// case the server can decide, and the client answers 'unknown' only where an
// input is out of its reach.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { create, type DescField } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import {
	type AccessOperation,
	answerOf,
	type CanAnswer,
	decideRules,
	type RuleRow,
	unknownBecause,
	type Verdict,
} from '../../src/access.js';
import { ConditionSchema } from '../../src/gen/syncplane/v1/query_pb.js';
import type { AccessRuleDef } from '../../src/mutations.js';

interface VectorRule {
	read?: boolean;
	create?: boolean;
	update?: boolean;
	delete?: boolean;
	write?: boolean;
	requires?: string[];
	admitted?: boolean;
	callerIs?: string;
	fields?: string[];
	/** Names the rule's condition, which each row's `matches` answers. */
	when?: string;
	/** Names the rule's within entries, which each row's `within` answers. */
	within?: string[];
}

interface VectorRow {
	held: Record<string, boolean | null>;
	fields: Record<string, string | null>;
	matches?: Record<string, boolean | null>;
	within?: Record<string, boolean | null>;
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

/**
 * The name each rule's condition stands for. The vectors name a condition
 * rather than spell one out, so a placeholder carries the name to `matches`.
 */
const whenNames = new WeakMap<AccessRuleDef, string>();

/** Only the name of a `callerIs` field is read, so the rest stays unbuilt. */
const ruleOf = (r: VectorRule): AccessRuleDef => {
	const rule: AccessRuleDef = {
		read: r.read === true,
		create: r.write === true || r.create === true,
		update: r.write === true || r.update === true,
		delete: r.write === true || r.delete === true,
		requires: r.admitted === true ? [] : [ ...(r.requires ?? []) ].sort(),
		callerIs: r.callerIs === undefined ? undefined : ({ name: r.callerIs } as DescField),
		fields: r.fields === undefined ? undefined : [ ...r.fields ].sort(),
		when: r.when === undefined ? undefined : create(ConditionSchema),
		// The entity of an entry stands for its name, which each row's within
		// answers.
		within: (r.within ?? []).map((name) => ({ entity: name, callerIs: undefined, when: undefined })),
	};
	if (r.when !== undefined) whenNames.set(rule, r.when);
	return rule;
};

const missing = unknownBecause('row_not_held');

const verdictOf = (v: boolean | null | undefined): Verdict =>
	v === null ? missing : v === true ? 'yes' : 'no';

const rowOf = (v: VectorRow): RuleRow => ({
	held: (permission) => verdictOf(v.held[permission]),
	fieldValue: (field) => v.fields[field] ?? missing,
	matches: (rule) => verdictOf(v.matches?.[whenNames.get(rule)!]),
	within: (rule) => {
		let answer: Verdict = 'yes';
		for (const entry of rule.within) {
			const one = verdictOf(v.within?.[entry.entity]);
			if (one === 'no') return 'no';
			if (one !== 'yes') answer = one;
		}
		return answer;
	},
});

describe('access rule conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	for (const tc of vectors.cases) {
		it(tc.name, () => {
			const answer = answerOf(decideRules(
				tc.rules.map(ruleOf),
				tc.op,
				tc.subjects,
				rowOf(tc.row),
				tc.final === undefined ? undefined : rowOf(tc.final),
				tc.changed,
			));
			expect(answer).toBe(tc.want);
		});
	}
});
