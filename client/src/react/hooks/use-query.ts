import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import type { ReadonlyRowSet } from '../../coverage.js';
import { compileQuery, type FacetsOf, type Query, type TotalOf } from '../../query/index.js';
import type { WatchHandle } from '../../watch.js';
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
 * `opts.window` is the exception — it pages the result the subscription already
 * holds, so moving it costs a slice rather than a re-read. On the suspense hook
 * it is part of the identity instead, because that flavor shares one entry
 * between every call site with the same identity and they would otherwise fight
 * over one page; a suspense hook therefore re-reads and re-suspends on a move.
 *
 * The result is `[]` with `loading` true until the first callback (or, on the
 * suspense hook, the render waits for it instead).
 */
export function useQueryHook<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
	schema: D,
	query: Q,
	opts?: UseQueryOptions,
	suspense?: boolean,
): WatchResult<MessageShape<D>, TotalOf<Q>, FacetsOf<Q>> {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId);
	const compiled = compileQuery(query);
	const include = opts?.include;
	const window = opts?.window;
	// The workspace belongs in the identity: the suspense cache is client-scoped
	// and one client holds every workspace, so two workspaces running the same
	// query would otherwise share — and show — one another's rows.
	const identity = stableHash(
		`q\u0000${scope.workspaceId}\u0000${schema.typeName}`,
		compiled,
		include,
		suspense ? window : undefined,
	);
	// Not memoized: the store keeps the newest factory in a ref and resubscribes
	// on `identity` alone, so a per-render closure is both correct and the only
	// way a structurally-equal query keeps its subscription.
	const subscribe = (
		onRows: (rows: ReadonlyRowSet<MessageShape<D>>) => void,
		onError: (error: unknown) => void,
	): WatchHandle =>
		scope.watchQuery(schema, query as Query<MessageShape<D>>, onRows, {
			include,
			onError,
			window,
		});
	return useWatchStore<MessageShape<D>>({
		identity,
		subscribe,
		client,
		suspense,
		window,
		facets: compiled.facets?.map((f) => f.name),
	}) as WatchResult<MessageShape<D>, TotalOf<Q>, FacetsOf<Q>>;
}
