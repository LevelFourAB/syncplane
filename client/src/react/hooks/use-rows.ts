import type { DescMessage, MessageShape } from '@bufbuild/protobuf';

import { useSyncClient, useWorkspaceScope } from '../context.js';
import { rowWatchIdentity, rowWatchOf, useRowsStore } from '../row-store.js';
import type { UseRowsOptions, UseRowsResultFor } from '../types.js';

/**
 * Live view of several rows by key, one result slot per key in the order asked
 * for (see KeyedRowsResult).
 *
 * The subscription is the one every useRow on the same workspace/entity/include
 * already shares, so a component reading ten keys and ten components reading
 * one key each cost the same: one watch, one point read per distinct key, and
 * per commit one read per watched key that commit named.
 *
 * `keys` may be a fresh array every render — only its contents decide what is
 * watched, and it is not retained. Growing it reads the added keys alone;
 * duplicates in it produce duplicate slots and one read.
 */
export function useRowsHook<D extends DescMessage>(
	schema: D,
	keys: readonly string[],
	opts?: UseRowsOptions,
	suspense?: boolean,
): UseRowsResultFor<D> {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId, opts?.undo);
	const include = opts?.include;
	return useRowsStore<MessageShape<D>>({
		identity: rowWatchIdentity(scope.workspaceId, schema.typeName, include),
		subscribe: rowWatchOf(scope, schema, include),
		client,
		refs: keys.map((key) => ({ workspaceId: scope.workspaceId, key })),
		suspense,
	});
}
