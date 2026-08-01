import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import type { ReadonlyRowSet } from '../../coverage.js';
import { compileQuery, type Query } from '../../query/index.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import type { WatchResult } from '../result.js';
import { stableHash } from '../stable-hash.js';
import { useWatchStore } from '../store.js';
import type { UseQueryOptions } from '../types.js';

/**
 * Live query results for an entity, in the resolved workspace. The query is
 * compiled every render and structurally hashed (no deps array): a
 * structurally-equal query reuses the subscription, a changed one resubscribes.
 *
 * The result is `[]` with `loading` true until the first callback (or, on the
 * suspense hook, the render waits for it instead).
 */
export function useQueryHook<D extends DescMessage>(
	schema: D,
	query: Query<MessageShape<D>>,
	opts?: UseQueryOptions,
	suspense?: boolean,
): WatchResult<MessageShape<D>> {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId);
	const compiled = compileQuery(query);
	const include = opts?.include;
	// The workspace belongs in the identity: the suspense cache is client-scoped
	// and one client holds every workspace, so two workspaces running the same
	// query would otherwise share — and show — one another's rows.
	const identity = stableHash(
		`q\u0000${scope.workspaceId}\u0000${schema.typeName}`,
		compiled,
		include,
	);
	// Not memoized: the store keeps the newest factory in a ref and resubscribes
	// on `identity` alone, so a per-render closure is both correct and the only
	// way a structurally-equal query keeps its subscription.
	const subscribe = (
		onRows: (rows: ReadonlyRowSet<MessageShape<D>>) => void,
		onError: (error: unknown) => void,
	): () => void => scope.watchQuery(schema, query, onRows, { include, onError });
	return useWatchStore<MessageShape<D>>({
		identity,
		subscribe,
		client,
		suspense,
	});
}
