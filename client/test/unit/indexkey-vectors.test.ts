// Conformance vectors shared with the Go encoder
// (engine/query/indexkey_test.go) — both must produce byte-identical entries,
// and both must agree with their own row comparator about the order those
// entries imply. Rows are protojson Widgets (or Issues); spec.key paths use
// proto field names, so conditions are translated to localNames before they
// reach the planner — which is what prepareQuery does in the real read path,
// and what `blocked_by`/`blockedBy` makes visible.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { type DescMessage, fromJson, type JsonValue, type Message } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { IssueSchema, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { FieldHistorySchema } from '../../src/gen/syncplane/v1/history_pb.js';
import { ConditionSchema, OrderKeySchema } from '../../src/gen/syncplane/v1/query_pb.js';
import type { Cond, OrderKey, Path } from '../../src/query/ast.js';
import { cursorToBytes, OrderCursors } from '../../src/query/cursor.js';
import { compareRows, evalCond } from '../../src/query/evaluate.js';
import { compileIndexComponent, IndexEncoder } from '../../src/query/indexkey.js';
import {
	type GroupPlan,
	type IndexRange,
	planGroup,
	planGroupRows,
	planQuery,
	type QueryPlan,
} from '../../src/query/plan.js';
import { condFromProto, orderKeyFromProto } from '../../src/query/proto.js';
import { localPathOf, prepareGroupQuery } from '../../src/query/wire.js';
import { toLocalNames } from '../support/vector-paths.js';

/** One row of a read-checking vector case. */
interface VectorRow {
	key: string;
	row: JsonValue;
}

interface SpecVector {
	name: string;
	key: { path: Path; desc: boolean }[];
}

interface EncodeCase {
	name: string;
	type?: string;
	row: JsonValue;
	rowKey: string;
	spec: SpecVector;
	want: string[];
}

interface OrderCase {
	name: string;
	type?: string;
	rows: { key: string; row: JsonValue }[];
	spec: SpecVector;
	want: string[];
}

/**
 * A query cursor: its bytes, its shape, and the two properties after/before
 * rest on. Unlike an index entry a cursor crosses the wire, so the Go loader
 * (engine/query/indexkey_test.go) asserts the same literals against the same
 * rows.
 */
interface CursorCase {
	name: string;
	type?: string;
	entity: string;
	orderBy: { path: Path; desc: boolean }[];
	rows: { key: string; row: JsonValue }[];
	shape: string;
	cursors: Record<string, string>;
	want: string[];
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/indexkey/vectors.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as {
	encodeCases: EncodeCase[];
	orderCases: OrderCase[];
	cursorCases: CursorCase[];
};

// Planner vectors also cover FieldHistory, the engine's own history entity:
// its by_source index is the one shape the feature cannot afford the planner to
// decline, because a history row key renders `seq` as unpadded decimal and so
// does not sort by sequence.
const schemaFor = (type?: string): DescMessage =>
	type === 'issue' ? IssueSchema : type === 'field_history' ? FieldHistorySchema : WidgetSchema;

const encoderFor = (spec: SpecVector, schema: DescMessage): IndexEncoder =>
	new IndexEncoder(spec.key.map((k) => compileIndexComponent(schema, k.path, k.desc)));

/** The same spec expressed as an ordering, which is what the encoding has to
 * agree with. */
const orderFor = (spec: SpecVector): OrderKey[] =>
	spec.key.map((k) => ({ path: k.path, dir: k.desc ? 'desc' : 'asc' }));

/** Entries are one character per byte, so hex is a direct rendering. */
const toHex = (entry: string): string => {
	let out = '';
	for (let i = 0; i < entry.length; i++) {
		out += entry.charCodeAt(i).toString(16).padStart(2, '0');
	}
	return out;
};

describe('indexkey conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.encodeCases.length).toBeGreaterThan(0);
		expect(vectors.orderCases.length).toBeGreaterThan(0);
		expect(vectors.cursorCases.length).toBeGreaterThan(0);
	});

	describe('encode', () => {
		for (const tc of vectors.encodeCases) {
			it(tc.name, () => {
				const schema = schemaFor(tc.type);
				const row = fromJson(schema, tc.row);
				const entries = encoderFor(tc.spec, schema).encode(row, tc.rowKey, schema);
				expect(entries.map(toHex)).toEqual(tc.want);
			});
		}
	});

	describe('order', () => {
		for (const tc of vectors.orderCases) {
			it(tc.name, () => {
				const schema = schemaFor(tc.type);
				const encoder = encoderFor(tc.spec, schema);
				const rows = tc.rows.map((r) => {
					const value = fromJson(schema, r.row);
					const entries = encoder.encode(value, r.key, schema);
					// An ordering has no meaning over a fan-out component, so every
					// order vector's spec is single-entry.
					expect(entries).toHaveLength(1);
					return {
						key: r.key,
						value,
						version: 0n,
						workspaceId: 'ws',
						entry: entries[0]!,
					};
				});

				const byEntry = [ ...rows ].sort((
					a,
					b,
				) => (a.entry < b.entry ? -1 : a.entry > b.entry ? 1 : 0));
				expect(byEntry.map((r) => r.key)).toEqual(tc.want);

				// The encoding is only useful if it agrees with the comparator it
				// replaces, so the two are asserted against each other and not
				// merely against the same literal.
				const orderBy = orderFor(tc.spec);
				const byCompare = [ ...rows ].sort((a, b) => compareRows(a, b, orderBy, schema));
				expect(byCompare.map((r) => r.key)).toEqual(byEntry.map((r) => r.key));
			});
		}
	});

	describe('cursor', () => {
		for (const tc of vectors.cursorCases) {
			it(tc.name, () => {
				const schema = schemaFor(tc.type);
				// Vector paths are proto field names; an order key addresses
				// localNames, which is what prepareQuery hands the evaluator.
				const orderBy: OrderKey[] = tc.orderBy.map((k) => ({
					path: localPathOf(schema, k.path),
					dir: k.desc ? 'desc' : 'asc',
				}));
				const cursors = new OrderCursors(schema, orderBy);
				expect(cursors.shape(tc.entity)).toBe(tc.shape);

				const rows = tc.rows.map((r) => {
					const value = fromJson(schema, r.row);
					expect(toHex(fromBase64UrlForTest(cursors.encode(tc.entity, value, r.key))))
						.toBe(tc.cursors[r.key]);
					return {
						key: r.key,
						value,
						version: 0n,
						workspaceId: 'ws',
						entry: cursors.entryOf(value, r.key),
					};
				});

				const byCursor = [ ...rows ].sort((a, b) => a.entry < b.entry ? -1 : a.entry > b.entry ? 1 : 0);
				expect(byCursor.map((r) => r.key)).toEqual(tc.want);

				// after/before are a key comparison against one row's cursor, so
				// each has to select exactly what the comparator ranks that way.
				for (const pivot of byCursor) {
					const after: string[] = [];
					const before: string[] = [];
					for (const r of byCursor) {
						const c = compareRows(r, pivot, orderBy, schema);
						if (c > 0) after.push(r.key);
						else if (c < 0) before.push(r.key);
					}
					expect(byCursor.filter((r) => r.entry > pivot.entry).map((r) => r.key))
						.toEqual(after);
					expect(byCursor.filter((r) => r.entry < pivot.entry).map((r) => r.key))
						.toEqual(before);
				}
			});
		}
	});
});

/** The framed cursor bytes behind a base64url token, for comparing against the
 * hex the vectors pin. */
function fromBase64UrlForTest(token: string): string {
	const bytes = cursorToBytes(token as never);
	let out = '';
	for (const b of bytes) out += String.fromCharCode(b);
	return out;
}

interface ProbeCase {
	name: string;
	type?: string;
	indexes: SpecVector[];
	cond?: JsonValue;
	orderBy?: JsonValue[];
	/** With `matches`, pins what the plan reads; see expectReads. */
	rows?: VectorRow[];
	matches?: string[];
	want: 'scan' | {
		index: string;
		ranges: number;
		ordered: boolean;
		residual: boolean;
		/** A union plan: one branch per entry, unordered by construction, with
		 * index/ranges/ordered unset. */
		union?: { index: string; ranges: number }[];
	};
}

const probeVectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { probeCases: ProbeCase[] };

/**
 * Asserts that a scan with `where` admits exactly `matches` of `rows`, and,
 * unless `reads` is undefined, that `reads` admits the same rows. `reads` is a
 * plan's read: whether one of the row's entries lies in the plan's ranges and
 * the row passes its residual. A shape assertion cannot see a range that
 * selects the wrong rows, and a bug both planners share passes every one of
 * them.
 */
function expectReads(
	schema: DescMessage,
	rows: VectorRow[],
	matches: string[] | undefined,
	where: Cond | undefined,
	reads: ((row: Message, key: string) => boolean) | undefined,
): void {
	const scanned: string[] = [];
	const read: string[] = [];
	for (const r of rows) {
		const row = fromJson(schema, r.row);
		if (!where || evalCond(row, where, schema)) scanned.push(r.key);
		if (reads?.(row, r.key)) read.push(r.key);
	}
	expect(scanned, 'the scan').toEqual(matches);
	if (reads) expect(read, 'the index read').toEqual(matches);
}

const inRange = (entries: string[], ranges: IndexRange[]): boolean =>
	entries.some((entry) =>
		ranges.some((r) => (r.lo === undefined || entry >= r.lo) && (r.hi === undefined || entry < r.hi))
	);

/** A row plan's read for expectReads; undefined for a scan. */
function planReads(
	schema: DescMessage,
	plan: QueryPlan | undefined,
	encoders: Map<string, IndexEncoder>,
): ((row: Message, key: string) => boolean) | undefined {
	if (!plan) return undefined;
	const branches = plan.union ?? [ { index: plan.index, ranges: plan.ranges } ];
	return (row, key) =>
		(!plan.residual || evalCond(row, plan.residual, schema))
		&& branches.some((b) => inRange(encoders.get(b.index)!.encode(row, key, schema), b.ranges));
}

/** planReads for a group plan, which reads one range of one index. */
function groupReads(
	schema: DescMessage,
	plan: GroupPlan | undefined,
	encoders: Map<string, IndexEncoder>,
): ((row: Message, key: string) => boolean) | undefined {
	if (!plan) return undefined;
	return (row, key) =>
		(!plan.residual || evalCond(row, plan.residual, schema))
		&& inRange(encoders.get(plan.index)!.encode(row, key, schema), [ plan.range ]);
}

// The planner's decisions are shared because a planner that chose differently
// would still return the right rows, just at a different cost — so nothing
// else in either suite would notice the divergence.
describe('planner conformance vectors', () => {
	it('has cases', () => {
		expect(probeVectors.probeCases.length).toBeGreaterThan(0);
	});

	for (const tc of probeVectors.probeCases) {
		it(tc.name, () => {
			const schema = schemaFor(tc.type);
			const candidates = tc.indexes.map((spec) => ({
				name: spec.name,
				encoder: encoderFor(spec, schema),
			}));
			const where = tc.cond
				? toLocalNames(schema, condFromProto(fromJson(ConditionSchema, tc.cond)))
				: undefined;
			const orderBy = tc.orderBy?.map((k) => {
				const key = orderKeyFromProto(fromJson(OrderKeySchema, k));
				return { ...key, path: localPathOf(schema, key.path) };
			});

			const plan = planQuery(candidates, where, orderBy);
			if (tc.rows) {
				const encoders = new Map(candidates.map((c) => [ c.name, c.encoder ]));
				expectReads(schema, tc.rows, tc.matches, where, planReads(schema, plan, encoders));
			}
			if (tc.want === 'scan') {
				expect(plan).toBeUndefined();
				return;
			}
			expect(plan).toBeDefined();
			if (tc.want.union !== undefined) {
				expect(plan!.union).toHaveLength(tc.want.union.length);
				for (let i = 0; i < tc.want.union.length; i++) {
					expect(plan!.union![i]!.index).toBe(tc.want.union[i]!.index);
					expect(plan!.union![i]!.ranges).toHaveLength(tc.want.union[i]!.ranges);
				}
				expect(plan!.ordered).toBe(false);
				expect(plan!.residual !== undefined).toBe(tc.want.residual);
				return;
			}
			expect(plan!.union).toBeUndefined();
			expect(plan!.index).toBe(tc.want.index);
			expect(plan!.ranges).toHaveLength(tc.want.ranges);
			expect(plan!.ordered).toBe(tc.want.ordered);
			expect(plan!.residual !== undefined).toBe(tc.want.residual);
		});
	}
});

// A case carrying `perGroup` is a grouped ROW read and runs the rows planner,
// whose extra requirement is that the components past the partition supply
// `orderBy`; one without it is a counted read, where an ordering has no meaning.
interface GroupCase {
	name: string;
	type?: string;
	indexes: SpecVector[];
	cond?: JsonValue;
	groupBy: string[];
	orderBy?: JsonValue[];
	perGroup?: number;
	/** With `matches`, pins what the plan reads; see expectReads. */
	rows?: VectorRow[];
	matches?: string[];
	want: 'scan' | { index: string; residual: boolean; desc: boolean; entriesAreRows: boolean };
}

const groupVectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { groupCases: GroupCase[] };

// The group planner's decisions are shared for the same reason the row
// planner's are: one that chose differently would still produce the right
// groups, just at a different cost.
describe('group planner conformance vectors', () => {
	it('has cases', () => {
		expect(groupVectors.groupCases.length).toBeGreaterThan(0);
	});

	for (const tc of groupVectors.groupCases) {
		it(tc.name, () => {
			const schema = schemaFor(tc.type);
			const candidates = tc.indexes.map((spec) => ({
				name: spec.name,
				encoder: encoderFor(spec, schema),
			}));
			const where = tc.cond
				? toLocalNames(schema, condFromProto(fromJson(ConditionSchema, tc.cond)))
				: undefined;
			// groupBy is a ROW path, so it resolves the way an order key does —
			// including the reference rule, which is what prepareGroupQuery applies.
			const compiled = prepareGroupQuery(schema, {
				by: localPathOf(schema, tc.groupBy),
				distinct: false,
				...(where === undefined ? {} : { where }),
			});

			const orderBy = tc.orderBy?.map((k) => {
				const key = orderKeyFromProto(fromJson(OrderKeySchema, k));
				return { ...key, path: localPathOf(schema, key.path) };
			});
			const plan = tc.perGroup === undefined
				? planGroup(candidates, compiled.where, compiled.by, compiled.multi)
				: planGroupRows(candidates, compiled.where, compiled.by, compiled.multi, orderBy ?? []);
			if (tc.rows) {
				const encoders = new Map(candidates.map((c) => [ c.name, c.encoder ]));
				expectReads(schema, tc.rows, tc.matches, compiled.where, groupReads(schema, plan, encoders));
			}
			if (tc.want === 'scan') {
				expect(plan).toBeUndefined();
				return;
			}
			expect(plan).toBeDefined();
			expect(plan!.index).toBe(tc.want.index);
			expect(plan!.residual !== undefined).toBe(tc.want.residual);
			expect(plan!.desc).toBe(tc.want.desc);
			expect(plan!.positionsAreRows).toBe(tc.want.entriesAreRows);
		});
	}
});
