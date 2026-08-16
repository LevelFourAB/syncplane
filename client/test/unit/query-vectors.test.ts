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

import type { DescMessage, Message } from '@bufbuild/protobuf';
import {
	IssueSchema,
	KeyedTextEntitySchema,
	PairSchema,
	ProjectSchema,
	UserSchema,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { ConditionSchema, OrderKeySchema } from '../../src/gen/syncplane/v1/query_pb.js';
import type { Cond } from '../../src/query/ast.js';
import { compareRows, evalCond, runQuery } from '../../src/query/evaluate.js';
import { condFromProto, condToProto, orderKeyFromProto } from '../../src/query/proto.js';
import {
	collectRels,
	type RelCond,
	type RelNode,
	relReplacement,
	rewriteRels,
	targetKeyTuple,
} from '../../src/query/rel.js';
import { localPathOf, prepareQuery } from '../../src/query/wire.js';
import { toLocalNames } from '../support/vector-paths.js';

interface EvalCase {
	name: string;
	type?: string;
	row: JsonValue;
	cond: JsonValue;
	want: boolean;
}

const schemaFor = (type?: string) =>
	type === 'issue'
		? IssueSchema
		: type === 'keyed_text'
		? KeyedTextEntitySchema
		: WidgetSchema;

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

/**
 * A rel-condition (`matches`) case: rows evaluated against a lookup context of
 * target rows. `want` must come out twice — from the scan-level DEFINITION and
 * from the two-phase rewrite the engines actually run.
 */
interface RelatedCase {
	name: string;
	type?: string;
	context: Record<string, JsonValue[]>;
	rows: { key: string; row: JsonValue }[];
	cond: JsonValue;
	want: string[];
}

/** Context keys are entity wire names. */
const CONTEXT_SCHEMAS: Record<string, DescMessage> = {
	user: UserSchema,
	project: ProjectSchema,
	issue: IssueSchema,
	pair: PairSchema,
	widget: WidgetSchema,
};

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/queryeval/vectors.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as {
	evalCases: EvalCase[];
	compareCases: CompareCase[];
	orderCases: OrderCase[];
	compileErrorCases: CompileErrorCase[];
	relatedCases: RelatedCase[];
};

const sign = (v: number): number => (v < 0 ? -1 : v > 0 ? 1 : 0);

describe('queryeval conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.evalCases.length).toBeGreaterThan(0);
		expect(vectors.compareCases.length).toBeGreaterThan(0);
		expect(vectors.orderCases.length).toBeGreaterThan(0);
		expect(vectors.compileErrorCases.length).toBeGreaterThan(0);
		expect(vectors.relatedCases.length).toBeGreaterThan(0);
	});

	describe('related', () => {
		for (const tc of vectors.relatedCases) {
			it(tc.name, () => {
				const schema = schemaFor(tc.type);
				const proto = fromJson(ConditionSchema, tc.cond);
				const where = prepareQuery(schema, {
					where: toLocalNames(schema, condFromProto(proto)),
				}).where!;
				const rows = tc.rows.map((r) => ({
					key: r.key,
					value: fromJson(schema, r.row) as Message,
				}));
				const context = new Map<string, Message[]>();
				for (const [ entity, raws ] of Object.entries(tc.context)) {
					const target = CONTEXT_SCHEMAS[entity]!;
					context.set(target.typeName, raws.map((raw) => fromJson(target, raw) as Message));
				}
				const nodes = collectRels(schema, where);
				expect(nodes.length).toBeGreaterThan(0);

				// The two-phase rewrite, exactly as the execute pipeline runs it:
				// pre-pass over the context, membership test on the stubs.
				const replacements = new Map<RelCond, Cond>();
				for (const node of nodes) {
					const targets = (context.get(node.targetSchema.typeName) ?? [])
						.filter((t) => evalCond(t, node.cond.cond, node.targetSchema))
						.map((t) => targetKeyTuple(node, t));
					replacements.set(node.cond, relReplacement(node, targets));
				}
				const rewritten = rewriteRels(where, replacements);
				expect(rows.filter((r) => evalCond(r.value, rewritten, schema)).map((r) => r.key))
					.toEqual(tc.want);

				// The scan-level definition: per stub, derive the target key, look
				// the target up in the context, evaluate the inner condition.
				const byCond = new Map<RelCond, RelNode>(nodes.map((n) => [ n.cond, n ]));
				const definition = (row: Message, cond: Cond): boolean => {
					switch (cond.kind) {
						case 'and':
							return cond.conds.every((c) => definition(row, c));
						case 'or':
							return cond.conds.some((c) => definition(row, c));
						case 'not':
							return !definition(row, cond.cond);
						case 'rel': {
							const node = byCond.get(cond)!;
							const rec = row as unknown as Record<string, unknown>;
							let at: unknown = rec;
							for (const seg of cond.path) at = (at as Record<string, unknown>)?.[seg];
							const stubs = at === undefined ? [] : Array.isArray(at) ? at : [ at ];
							const targets = context.get(node.targetSchema.typeName) ?? [];
							return stubs.some((stub) => {
								const target = targets.find((t) =>
									node.keyFields.every((kf) =>
										(t as unknown as Record<string, unknown>)[kf.localName]
											=== (stub as Record<string, unknown>)[kf.localName]
									)
								);
								return target !== undefined
									&& evalCond(target, node.cond.cond, node.targetSchema);
							});
						}
						default:
							return evalCond(row, cond, schema);
					}
				};
				expect(rows.filter((r) => definition(r.value, where)).map((r) => r.key))
					.toEqual(tc.want);
			});
		}
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
