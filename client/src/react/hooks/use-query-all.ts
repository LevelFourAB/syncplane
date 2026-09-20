import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import { type QueryAcross, readScopeOf } from '../../client.js';
import type { ReadonlyRowSet } from '../../coverage.js';
import { compileQuery, type FacetsOf, type Query, type TotalOf } from '../../query/index.js';
import type { WatchHandle } from '../../watch.js';
import { useSyncClient } from '../context.js';
import type { WatchResult } from '../result.js';
import { stableHash } from '../stable-hash.js';
import { useWatchStore } from '../store.js';
import type { UseQueryAllOptions } from '../types.js';

/**
 * Tags a cross-workspace subscription's identity, so it can never collide with
 * the single-workspace one `useQuery` builds over the same entity and the same
 * predicate.
 */
const QUERY_ALL_PREFIX = 'qa:';

/**
 * Live query results for an entity across workspaces, as one subscription:
 * every workspace the client holds, or the set `opts.workspaces` names. The
 * query is compiled every render and structurally hashed (no deps array): a
 * structurally-equal query reuses the subscription, a changed one resubscribes.
 * `opts.window` pages the result the subscription already holds, so moving it
 * costs a slice rather than a re-read.
 *
 * Each row names the workspace it lives in (`Row.workspaceId`), which is what
 * groups the result and routes a write back. Ordering and limits apply to the
 * merged set rather than per workspace (see `client.watchQueryAll`).
 *
 * The workspace set joins the identity alongside `include`, as a set: naming the
 * same workspaces in another order keeps the subscription, naming different ones
 * resubscribes. A fresh array literal per render is therefore fine.
 *
 * A WorkspaceQuery in place of the query gives each workspace its own predicate
 * (see WorkspaceQuery); it throws without `opts.workspaces`.
 *
 * The result is `[]` with `loading` true until the first callback.
 */
export function useQueryAllHook<
	D extends DescMessage,
	const Q extends QueryAcross<MessageShape<D>>,
>(
	schema: D,
	query: Q,
	opts?: UseQueryAllOptions,
): WatchResult<MessageShape<D>, TotalOf<Q>, FacetsOf<Q>> {
	const client = useSyncClient();
	const compiled = compileQuery(query as Query<MessageShape<D>>);
	const include = opts?.include;
	const window = opts?.window;
	const workspaces = opts?.workspaces;
	// Resolved for the identity, and again on subscribe. A per-workspace
	// predicate is plain data once compiled, so the identity can carry what the
	// callback returned rather than the callback itself — which would be a new
	// function every render.
	const scope = readScopeOf(schema, query as Query<MessageShape<D>>, workspaces);
	const identity = stableHash(
		QUERY_ALL_PREFIX + schema.typeName,
		compiled,
		include,
		undefined,
		scope,
	);
	// Not memoized, for the reason `useQuery` gives: the store keeps the newest
	// factory in a ref and resubscribes on `identity` alone, so a per-render
	// closure is what keeps a structurally-equal query on its subscription.
	const subscribe = (
		onRows: (rows: ReadonlyRowSet<MessageShape<D>>) => void,
		onError: (error: unknown) => void,
	): WatchHandle =>
		client.watchQueryAll(schema, query as Query<MessageShape<D>>, onRows, {
			include,
			onError,
			window,
			workspaces,
		});
	return useWatchStore<MessageShape<D>>({
		identity,
		subscribe,
		client,
		window,
		facets: compiled.facets?.map((f) => f.name),
	}) as WatchResult<MessageShape<D>, TotalOf<Q>, FacetsOf<Q>>;
}
