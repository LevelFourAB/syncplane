// Conformance vectors shared with the Go evaluator
// (engine/internal/queryeval/queryeval_test.go) — both must agree on every
// case. Rows are protojson Widgets; conds/orderBy are protojson syncplane.v1
// messages, decoded here and bridged through condFromProto/orderKeyFromProto
// (whose output also round-trips back through condToProto).

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { fromJson, type JsonValue, toJson } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { IssueSchema, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { ConditionSchema, OrderKeySchema } from '../../src/gen/syncplane/v1/query_pb.js';
import { compareRows, evalCond, runQuery } from '../../src/query/evaluate.js';
import { condFromProto, condToProto, orderKeyFromProto } from '../../src/query/proto.js';
import { localPathOf, prepareQuery } from '../../src/query/wire.js';
import { toLocalNames } from '../support/vector-paths.js';

interface EvalCase {
	name: string;
	type?: string;
	row: JsonValue;
	cond: JsonValue;
	want: boolean;
}

const schemaFor = (type?: string) => (type === 'issue' ? IssueSchema : WidgetSchema);

interface CompareCase {
	name: string;
	a: JsonValue;
	b: JsonValue;
	keyA: string;
	keyB: string;
	orderBy: JsonValue[];
	want: number;
}

interface OrderCase {
	name: string;
	rows: { key: string; row: JsonValue }[];
	orderBy: JsonValue[];
	want: string[];
}

/** A query both compilers must reject; each language words the error itself. */
interface CompileErrorCase {
	name: string;
	type?: string;
	cond?: JsonValue;
	orderBy?: JsonValue[];
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/queryeval/vectors.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as {
	evalCases: EvalCase[];
	compareCases: CompareCase[];
	orderCases: OrderCase[];
	compileErrorCases: CompileErrorCase[];
};

const sign = (v: number): number => (v < 0 ? -1 : v > 0 ? 1 : 0);

describe('queryeval conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.evalCases.length).toBeGreaterThan(0);
		expect(vectors.compareCases.length).toBeGreaterThan(0);
		expect(vectors.orderCases.length).toBeGreaterThan(0);
		expect(vectors.compileErrorCases.length).toBeGreaterThan(0);
	});

	describe('eval', () => {
		for (const tc of vectors.evalCases) {
			it(tc.name, () => {
				const proto = fromJson(ConditionSchema, tc.cond);
				const cond = condFromProto(proto);
				const row = fromJson(schemaFor(tc.type), tc.row);
				// prepareQuery is this side's compiler — it is what resolves a
				// reference leaf to its key and rewrites membership over a
				// reference list into an exists — so the pair prepare+eval is what
				// answers to the Go compiler's Compile+Eval. Running eval alone
				// would let a rewrite land on one side only.
				const prepared = prepareQuery(schemaFor(tc.type), {
					where: toLocalNames(schemaFor(tc.type), cond),
				}).where!;
				expect(evalCond(row, prepared, schemaFor(tc.type))).toBe(tc.want);

				// The AST → proto direction must reproduce the original message.
				// Compared as protojson rather than with equals(), which reports
				// any message holding a NaN literal as unequal to itself.
				expect(toJson(ConditionSchema, condToProto(cond)))
					.toEqual(toJson(ConditionSchema, proto));
			});
		}
	});

	describe('order', () => {
		for (const tc of vectors.orderCases) {
			it(tc.name, () => {
				const orderBy = tc.orderBy.map((k) => orderKeyFromProto(fromJson(OrderKeySchema, k)));
				const rows = tc.rows.map((r) => ({
					key: r.key,
					value: fromJson(WidgetSchema, r.row),
					version: 0n,
					workspaceId: 'ws',
				}));
				const sorted = runQuery(rows, { orderBy }, WidgetSchema);
				expect(sorted.map((r) => r.key)).toEqual(tc.want);
			});
		}
	});

	describe('compare', () => {
		for (const tc of vectors.compareCases) {
			it(tc.name, () => {
				const orderBy = tc.orderBy.map((k) => orderKeyFromProto(fromJson(OrderKeySchema, k)));
				const a = { key: tc.keyA, value: fromJson(WidgetSchema, tc.a), version: 0n, workspaceId: 'ws' };
				const b = { key: tc.keyB, value: fromJson(WidgetSchema, tc.b), version: 0n, workspaceId: 'ws' };
				expect(sign(compareRows(a, b, orderBy, WidgetSchema))).toBe(tc.want);
				expect(sign(compareRows(b, a, orderBy, WidgetSchema))).toBe(-tc.want || 0);
			});
		}
	});

	describe('compile errors', () => {
		for (const tc of vectors.compileErrorCases) {
			it(tc.name, () => {
				const schema = schemaFor(tc.type);
				if (tc.cond) {
					const cond = condFromProto(fromJson(ConditionSchema, tc.cond));
					// The translation is part of what must reject: a path naming a
					// field the schema does not have fails here rather than in
					// prepareQuery, and either is the rejection the vector asks for.
					expect(() => prepareQuery(schema, { where: toLocalNames(schema, cond) })).toThrow();
					return;
				}
				const orderBy = (tc.orderBy ?? []).map((k) => {
					const key = orderKeyFromProto(fromJson(OrderKeySchema, k));
					return { ...key, path: localPathOf(schema, key.path) };
				});
				expect(orderBy.length).toBeGreaterThan(0);
				expect(() => prepareQuery(schema, { orderBy })).toThrow();
			});
		}
	});
});
