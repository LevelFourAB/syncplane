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

import type { KVRead } from '../kv/store.js';
import type { EntityDef, EntityRegistry } from '../mutations.js';
import type { CompiledIndex } from '../optimistic/index-maintain.js';
import { scanDecoded } from '../read.js';
import { evalCond } from './evaluate.js';
import { groupByIndex, type IndexReport } from './execute.js';
import { type Group, GroupBuckets, groupValuesOf } from './group.js';
import type { PreparedGroupQuery } from './wire.js';

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
	const rows = await scanDecoded(tx, registry, def, schema, workspaceId);
	for (const row of rows as { value: MessageShape<D> }[]) {
		if (compiled.where && !evalCond(row.value, compiled.where, schema)) continue;
		for (const value of groupValuesOf(row.value, compiled.by, schema)) buckets.add(value, 1);
	}
	return buckets.result(compiled.limit).groups;
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
