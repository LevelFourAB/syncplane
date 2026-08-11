import type { DescMessage, MessageShape } from '@bufbuild/protobuf';

import type { RowRef } from '../../watch.js';
import { useSyncClient } from '../context.js';
import { rowWatchAllIdentity, rowWatchAllOf, useRowsStore } from '../row-store.js';
import type { UseRowsAllOptions, UseRowsResultFor } from '../types.js';

/**
 * Live view of rows named by workspace and key, one result slot per ref in the
 * order asked for (see KeyedRowsResult).
 *
 * Reach for it over `useQueryAll` when the set comes from outside the entity —
 * a pinned list, a feed, ids held elsewhere — and no predicate describes it.
 * Refs may name any workspace the client holds, and the same key in two of them
 * is two rows.
 *
 * Every useRowsAll on the same entity and include shares ONE cross-workspace
 * watch over the union of their refs, costing a point read per ref whatever the
 * entity's size. That watch is separate from the per-workspace one `useRow` and
 * `useRows` share, so a row held through both is read twice.
 *
 * The enclosing WorkspaceProvider is ignored and the hook does not write; each
 * row names the workspace it came from, which is what routes a write back
 * through `client.ws(row.workspaceId)`.
 *
 * `refs` may be fresh objects every render — only what they name decides what
 * is watched, and they are not retained. Growing the list reads the added rows
 * alone; duplicates in it produce duplicate slots and one read.
 */
export function useRowsAllHook<D extends DescMessage>(
	schema: D,
	refs: readonly RowRef[],
	opts?: UseRowsAllOptions,
): UseRowsResultFor<D> {
	const client = useSyncClient();
	const include = opts?.include;
	return useRowsStore<MessageShape<D>>({
		identity: rowWatchAllIdentity(schema.typeName, include),
		subscribe: rowWatchAllOf(client, schema, include),
		client,
		refs,
	});
}
