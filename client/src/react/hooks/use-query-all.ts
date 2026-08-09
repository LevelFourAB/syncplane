import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import type { ReadonlyRowSet } from '../../coverage.js';
import { compileQuery, type Query, type TotalOf } from '../../query/index.js';
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
 * Live query results for an entity across every workspace the client holds, as
 * one subscription. The query is compiled every render and structurally hashed
 * (no deps array): a structurally-equal query reuses the subscription, a
 * changed one resubscribes. `opts.window` pages the result the subscription
 * already holds, so moving it costs a slice rather than a re-read.
 *
 * Each row names the workspace it lives in (`Row.workspaceId`), which is what
 * groups the result and routes a write back. Ordering and limits apply to the
 * merged set rather than per workspace (see `client.watchQueryAll`). The
 * workspace is not part of the identity, so every call site asking the same
 * question shares one subscription however many workspaces the client holds.
 *
 * The result is `[]` with `loading` true until the first callback.
 */
export function useQueryAllHook<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
	schema: D,
	query: Q,
	opts?: UseQueryAllOptions,
): WatchResult<MessageShape<D>, TotalOf<Q>> {
	const client = useSyncClient();
	const compiled = compileQuery(query);
	const include = opts?.include;
	const window = opts?.window;
	const identity = stableHash(QUERY_ALL_PREFIX + schema.typeName, compiled, include);
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
		});
	return useWatchStore<MessageShape<D>>({
		identity,
		subscribe,
		client,
		window,
	}) as WatchResult<MessageShape<D>, TotalOf<Q>>;
}
