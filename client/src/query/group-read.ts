// One workspace's grouped read, over the merged view.
//
// It is the seam the two paths meet at: `groupByIndex` (query/execute.ts) skips
// through an index when one partitions on the grouping path, and the scan below
// is the DEFINITION it has to reproduce — every matching row of the merged view
// bucketed by query.Group's rule. A decline from the index walk costs time and
// never changes the answer.
//
// It lives apart from execute.ts so both the one-shot reads (SyncClient) and the
// live ones (WatcherHub) reach the same function rather than each re-deriving
// the fallback.

import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import { fromBinary } from '@bufbuild/protobuf';

import type { KVRead } from '../kv/store.js';
import type { EntityDef, EntityRegistry } from '../mutations.js';
import type { CompiledIndex } from '../optimistic/index-maintain.js';
import { sortOrderedFields } from '../ordering.js';
import { decodeWholeRow, scanDecoded, scanMergedRows, scanProjected } from '../read.js';
import { compareRows, evalCond } from './evaluate.js';
import {
	groupByIndex,
	groupRowsByIndex,
	type IndexedRow,
	type IndexReport,
	type RowGroupPage,
} from './execute.js';
import {
	compareGroupValues,
	type Group,
	GroupBuckets,
	groupKeyOf,
	type GroupValue,
	groupValuesOf,
} from './group.js';
import { projectionForGroup } from './projection.js';
import type { PreparedGroupQuery, PreparedGroupRowsQuery } from './wire.js';

/**
 * The groups of one workspace's matching rows, in ascending value order.
 *
 * `report` records which index served the read, or why none did, for a caller
 * that surfaces it.
 */
export async function readGroups<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	indexes: CompiledIndex[],
	compiled: PreparedGroupQuery,
	workspaceId: string,
	report?: IndexReport,
): Promise<Group[]> {
	const indexed = await groupByIndex(
		tx,
		registry,
		def,
		schema,
		indexes,
		compiled,
		compiled.multi,
		workspaceId,
		report,
	);
	if (indexed !== undefined) return indexed.groups;
	const buckets = new GroupBuckets(compiled.distinct);
	// The read answers with group values and counts, so no candidate is ever
	// handed back and none has to be decoded a second time.
	const view = projectionForGroup(schema, compiled);
	const rows = view === undefined
		? await scanDecoded(tx, registry, def, schema, workspaceId)
		: (await scanProjected(tx, registry, def, schema, view, workspaceId)).rows;
	for (const row of rows as { value: MessageShape<D> }[]) {
		if (compiled.where && !evalCond(row.value, compiled.where, schema)) continue;
		for (const value of groupValuesOf(row.value, compiled.by, schema)) buckets.add(value, 1);
	}
	return buckets.result(compiled.limit).groups;
}

/**
 * The head of each of one workspace's groups: `perGroup` rows in the query's
 * order, the groups themselves in ascending value order.
 *
 * `report` records which index served the read, or why none did, for a caller
 * that surfaces it.
 */
export async function readRowGroups<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	indexes: CompiledIndex[],
	compiled: PreparedGroupRowsQuery,
	workspaceId: string,
	report?: IndexReport,
): Promise<RowGroupPage<MessageShape<D>>[]> {
	const indexed = await groupRowsByIndex(
		tx,
		registry,
		def,
		schema,
		indexes,
		compiled,
		compiled.multi,
		workspaceId,
		report,
	);
	if (indexed !== undefined) return indexed.groups;

	// The definition: bucket every matching row of the merged view, sort each
	// bucket, and cut it. It holds the whole match set rather than groups ×
	// perGroup rows, which is the cost an index walk exists to avoid and the
	// reason it is worth declaring one.
	const buckets = new Map<string, { value: GroupValue; rows: IndexedRow<MessageShape<D>>[] }>();
	// Candidates carry only what the grouping, the predicate and the ordering
	// read; each group's page is decoded again, whole, from the same bytes.
	const view = projectionForGroup(schema, compiled);
	const rows = await scanMergedRows(tx, registry, def.name, workspaceId);
	for (const merged of rows) {
		// Retaining unknown fields would keep exactly the bytes a projection
		// exists to skip.
		const value = view === undefined
			? fromBinary(schema, merged.v)
			: fromBinary(view, merged.v, { readUnknownFields: false }) as unknown as MessageShape<D>;
		sortOrderedFields(schema, value);
		if (compiled.where && !evalCond(value, compiled.where, schema)) continue;
		const row: IndexedRow<MessageShape<D>> = {
			row: { key: merged.key, value, workspaceId: merged.workspaceId, version: merged.version },
			bytes: merged.v,
		};
		for (const group of groupValuesOf(value, compiled.by, schema)) {
			const key = groupKeyOf(group);
			const held = buckets.get(key);
			if (held === undefined) buckets.set(key, { value: group, rows: [ row ] });
			else held.rows.push(row);
		}
	}
	const ordered = [ ...buckets.values() ]
		.sort((a, b) => compareGroupValues(a.value, b.value));
	const kept = compiled.limit === undefined ? ordered : ordered.slice(0, compiled.limit);
	return kept.map(({ value, rows: matched }) => {
		if (compiled.orderBy && compiled.orderBy.length > 0) {
			const orderBy = compiled.orderBy;
			matched.sort((a, b) => compareRows(a.row, b.row, orderBy, schema));
		} else {
			// No ordering: the rows come back the way a scan returns them, in key
			// order.
			matched.sort((a, b) => (a.row.key < b.row.key ? -1 : a.row.key > b.row.key ? 1 : 0));
		}
		const page = matched.slice(0, compiled.perGroup);
		return {
			value: value.found ? value.value : undefined,
			rows: view === undefined
				? page
				: page.map((e) => ({ row: decodeWholeRow(schema, e.row, e.bytes), bytes: e.bytes })),
			total: compiled.total === false ? undefined : matched.length,
			hasMore: matched.length > compiled.perGroup,
		};
	});
}

/** Whether two grouped results are the same answer, so a watch can suppress a
 * callback that would deliver nothing new. */
export function sameGroups(a: readonly Group[], b: readonly Group[]): boolean {
	return a.length === b.length
		&& a.every((g, i) => {
			const other = b[i]!;
			// Object.is rather than ===, so the NaN group compares equal to itself.
			return Object.is(g.value, other.value) && g.count === other.count;
		});
}

/**
 * Whether two grouped ROW results are the same answer.
 *
 * Rows are compared by the bytes they were decoded from rather than by version:
 * an optimistic edit moves the bytes and leaves the confirmed version alone, so
 * a version comparison would suppress the callback that carries the user's own
 * keystroke.
 */
export function sameRowGroups(
	a: readonly RowGroupPage<unknown>[],
	b: readonly RowGroupPage<unknown>[],
): boolean {
	return a.length === b.length
		&& a.every((g, i) => {
			const other = b[i]!;
			return Object.is(g.value, other.value)
				&& g.total === other.total
				&& g.hasMore === other.hasMore
				&& g.rows.length === other.rows.length
				&& g.rows.every((entry, j) => {
					const was = other.rows[j]!;
					return entry.row.key === was.row.key && bytesEqual(entry.bytes, was.bytes);
				});
		});
}

const bytesEqual = (a: Uint8Array, b: Uint8Array): boolean =>
	a.length === b.length && a.every((byte, i) => byte === b[i]);
