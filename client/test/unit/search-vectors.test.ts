// Runs the shared cross-language search conformance vectors. The Go
// harnesses (engine/search/vectors_test.go for the analyzer sections,
// engine/query/match_vectors_test.go for the match, rank and cursor sections)
// execute the identical cases; the pinned terms, ranks and cursor bytes prove
// both sides analyze, match, rank and page identically.

import { fromJson, type JsonValue } from '@bufbuild/protobuf';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

import type { DescMessage } from '@bufbuild/protobuf';
import {
	ArticleSchema,
	GlossarySchema,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { ConditionSchema, OrderKeySchema } from '../../src/gen/syncplane/v1/query_pb.js';
import type { Cond } from '../../src/query/ast.js';
import { cursorToBytes, OrderCursors } from '../../src/query/cursor.js';
import { evalCond, matcherOf } from '../../src/query/evaluate.js';
import { condFromProto, orderKeyFromProto } from '../../src/query/proto.js';
import { prepareQuery } from '../../src/query/wire.js';
import {
	analyze,
	type AnalyzeOptions,
	analyzeQuery,
} from '../../src/search/index.js';
import { toLocalNames } from '../support/vector-paths.js';

const vectorsPath = fileURLToPath(
	new URL('../../../testdata/search/vectors.json', import.meta.url),
);

interface VectorOptions {
	keepDiacritics?: boolean;
}

interface AnalyzeCase {
	name: string;
	text: string;
	options?: VectorOptions;
	want: string[];
}

interface QueryCase {
	name: string;
	query: string;
	options?: VectorOptions;
	want: { term: string; prefix?: boolean }[];
}

interface MatchCase {
	name: string;
	type: string;
	row: JsonValue;
	query: string;
	want: boolean;
}

interface RankCase {
	name: string;
	type: string;
	query: string;
	rows: { key: string; row: JsonValue }[];
	want: { key: string; rank: number }[];
}

interface MatchErrorCase {
	name: string;
	type: string;
	cond: JsonValue;
}

interface OrderErrorCase {
	name: string;
	type: string;
	cond?: JsonValue;
	orderBy: JsonValue[];
}

interface CursorCase {
	name: string;
	type: string;
	entity: string;
	query: string;
	orderBy: JsonValue[];
	row: JsonValue;
	key: string;
	shape: string;
	cursorHex: string;
}

interface VectorFile {
	analyzeCases: AnalyzeCase[];
	queryCases: QueryCase[];
	matchCases: MatchCase[];
	rankCases: RankCase[];
	matchErrorCases: MatchErrorCase[];
	orderErrorCases: OrderErrorCase[];
	cursorCases: CursorCase[];
}

function schemaFor(type: string): DescMessage {
	switch (type) {
		case 'article':
			return ArticleSchema;
		case 'glossary':
			return GlossarySchema;
		default:
			return WidgetSchema;
	}
}

function matchCond(query: string): Cond {
	return { kind: 'match', path: [], query };
}

const file = JSON.parse(readFileSync(vectorsPath, 'utf8')) as VectorFile;

function toOptions(options?: VectorOptions): AnalyzeOptions | undefined {
	return options;
}

describe('shared analyzer vectors', () => {
	for (const tc of file.analyzeCases) {
		it(`analyzes: ${tc.name}`, () => {
			expect(analyze(tc.text, toOptions(tc.options))).toEqual(tc.want);
		});
	}

	for (const tc of file.queryCases) {
		it(`analyzes query: ${tc.name}`, () => {
			const want = tc.want.map(w => ({
				term: w.term,
				prefix: w.prefix ?? false,
			}));
			expect(analyzeQuery(tc.query, toOptions(tc.options))).toEqual(want);
		});
	}

	for (const tc of file.matchCases) {
		it(`matches: ${tc.name}`, () => {
			const schema = schemaFor(tc.type);
			const prepared = prepareQuery(schema, { where: matchCond(tc.query) });
			const row = fromJson(schema, tc.row);
			expect(evalCond(row, prepared.where!, schema)).toBe(tc.want);
		});
	}

	for (const tc of file.rankCases) {
		it(`ranks: ${tc.name}`, () => {
			const schema = schemaFor(tc.type);
			const prepared = prepareQuery(schema, { where: matchCond(tc.query) });
			const m = matcherOf(prepared.where as Cond & { kind: 'match' }, schema);
			const got = tc.rows
				.map(r => ({ key: r.key, rank: m.rank(fromJson(schema, r.row), schema) }))
				.sort((a, b) => a.rank - b.rank || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
			expect(got).toEqual(tc.want);
		});
	}

	for (const tc of file.matchErrorCases) {
		it(`rejects: ${tc.name}`, () => {
			const schema = schemaFor(tc.type);
			const cond = condFromProto(fromJson(ConditionSchema, tc.cond));
			expect(() => prepareQuery(schema, { where: toLocalNames(schema, cond) })).toThrow();
		});
	}

	for (const tc of file.orderErrorCases) {
		it(`rejects order: ${tc.name}`, () => {
			const schema = schemaFor(tc.type);
			expect(() => {
				const where = tc.cond === undefined
					? undefined
					: toLocalNames(schema, condFromProto(fromJson(ConditionSchema, tc.cond)));
				const orderBy = tc.orderBy.map(k => orderKeyFromProto(fromJson(OrderKeySchema, k)));
				prepareQuery(schema, { where, orderBy });
			}).toThrow();
		});
	}

	for (const tc of file.cursorCases) {
		it(`encodes cursor: ${tc.name}`, () => {
			const schema = schemaFor(tc.type);
			const prepared = prepareQuery(schema, {
				where: matchCond(tc.query),
				orderBy: tc.orderBy.map(k => orderKeyFromProto(fromJson(OrderKeySchema, k))),
			});
			const cursors = new OrderCursors(schema, prepared.orderBy!);
			expect(cursors.shape(tc.entity)).toBe(tc.shape);
			const cursor = cursors.encode(tc.entity, fromJson(schema, tc.row), tc.key);
			const hex = Array.from(cursorToBytes(cursor))
				.map(b => b.toString(16).padStart(2, '0'))
				.join('');
			expect(hex).toBe(tc.cursorHex);
			expect(cursors.decode(tc.entity, cursor)).not.toHaveLength(0);
		});
	}
});
