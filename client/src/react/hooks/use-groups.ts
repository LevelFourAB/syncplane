import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import { useCallback, useMemo, useRef, useSyncExternalStore } from 'react';

import type {
	Group,
	GroupQuery,
	GroupRowsQuery,
	RowGroup,
	Scalar,
} from '../../query/index.js';
import type { WatchHandle } from '../../watch.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import { asError, type WatchState } from '../result.js';
import { groupHash, groupRowsHash } from '../stable-hash.js';
import type { UseWorkspaceOptions } from '../types.js';

/** Groups plus `loading`/`error`; `[]` until the first callback. */
export type GroupResult = readonly Group[] & Omit<WatchState, 'hydrating'>;

/** Distinct values plus `loading`/`error`; `[]` until the first callback. */
export type DistinctResult = readonly (Scalar | undefined)[] & Omit<WatchState, 'hydrating'>;

/**
 * Live counted groups of an entity, in the resolved workspace — a board
 * column's badge, a facet sidebar's numbers.
 *
 * The query is compiled every render and structurally hashed (no deps array):
 * a structurally-equal query reuses the subscription, a changed one
 * resubscribes. Every commit touching the entity re-runs the read, and a
 * callback whose answer is unchanged is suppressed, so a component reading a
 * facet re-renders only when its numbers move.
 *
 * There is no `hydrating`: a grouped read reports no coverage, so a client
 * still downloading its first snapshot is indistinguishable from one whose
 * entity really holds these groups. Read a `useQuery` beside it when a UI needs
 * to tell those apart.
 */
export function useGroupCountHook<D extends DescMessage>(
	schema: D,
	query: GroupQuery<MessageShape<D>>,
	opts?: UseWorkspaceOptions,
): GroupResult {
	return useGroups(schema, query, opts, false) as GroupResult;
}

/** The distinct values behind {@link useGroupCountHook}'s groups, live — what a
 * filter dropdown offers. `undefined` is among them when some matching row holds
 * no value at the path. */
export function useDistinctHook<D extends DescMessage>(
	schema: D,
	query: GroupQuery<MessageShape<D>>,
	opts?: UseWorkspaceOptions,
): DistinctResult {
	const groups = useGroups(schema, query, opts, true);
	// Derived per render rather than cached: the array is as long as the group
	// count, and mapping it is cheaper than the cell a cache would need.
	return withState(groups.map((g) => g.value), groups.loading, groups.error) as DistinctResult;
}

/** Row groups plus `loading`/`error`; `[]` until the first callback. */
export type RowGroupResult<T> = readonly RowGroup<T>[] & Omit<WatchState, 'hydrating'>;

/**
 * Live head of each group in the resolved workspace — a board's columns, each
 * with its first rows.
 *
 * Subscribes the way {@link useGroupCountHook} does: the query is compiled every
 * render and structurally hashed, every commit touching the entity re-runs the
 * read, and a callback whose groups are unchanged is suppressed. There is no
 * `hydrating` here either — read a `useQuery` beside it when a UI has to tell an
 * empty result from one that has not arrived.
 */
export function useGroupRowsHook<D extends DescMessage>(
	schema: D,
	query: GroupRowsQuery<MessageShape<D>>,
	opts?: UseWorkspaceOptions,
): RowGroupResult<MessageShape<D>> {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId);
	// The workspace belongs in the identity for useGroups' reason: one client
	// holds every workspace, and two of them running the same read must not
	// share a cell.
	const identity = groupRowsHash(
		`gr\u0000${scope.workspaceId}\u0000${schema.typeName}`,
		query as GroupRowsQuery<unknown>,
	);
	const cell = useRef<{ identity?: string; value: RowGroupResult<MessageShape<D>> }>({
		value: EMPTY as RowGroupResult<MessageShape<D>>,
	});
	if (cell.current.identity !== identity) {
		cell.current = { identity, value: EMPTY as RowGroupResult<MessageShape<D>> };
	}
	const subscribe = useCallback((onChange: () => void): () => void => {
		const handle: WatchHandle = client.watchGroupRows(
			schema,
			query as GroupRowsQuery<MessageShape<DescMessage>>,
			(groups) => {
				cell.current.value = withState(groups, false, undefined) as RowGroupResult<
					MessageShape<D>
				>;
				onChange();
			},
			{
				onError: (err) => {
					// Groups already delivered are kept: what is on screen stops
					// tracking the store rather than vanishing.
					cell.current.value = withState(
						[ ...cell.current.value ],
						false,
						asError(err),
					) as RowGroupResult<MessageShape<D>>;
					onChange();
				},
			},
		);
		return () => handle.close();
		// The identity is the resubscribe key; the query and workspace it was
		// derived from move together with it.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [ client, schema, identity ]);
	const getSnapshot = useCallback(() => cell.current.value, []);
	const server = useMemo(() => EMPTY as RowGroupResult<MessageShape<D>>, []);
	return useSyncExternalStore(subscribe, getSnapshot, () => server);
}

const EMPTY = withState([], true, undefined);

/**
 * The store behind both hooks: one cell per call site, filled on the first
 * callback.
 *
 * getSnapshot is synchronous and referentially stable — the watch hands over a
 * new array only when the answer really changed — which is what keeps
 * useSyncExternalStore from looping.
 */
function useGroups<D extends DescMessage>(
	schema: D,
	query: GroupQuery<MessageShape<D>>,
	opts: UseWorkspaceOptions | undefined,
	distinct: boolean,
): GroupResult {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId);
	// The workspace belongs in the identity: one client holds every workspace, so
	// two of them running the same read would otherwise share one another's
	// groups.
	const identity = groupHash(
		`g\u0000${scope.workspaceId}\u0000${schema.typeName}\u0000${distinct}`,
		query as GroupQuery<unknown>,
	);
	const cell = useRef<{ identity?: string; value: GroupResult }>({ value: EMPTY as GroupResult });
	if (cell.current.identity !== identity) {
		cell.current = { identity, value: EMPTY as GroupResult };
	}
	const subscribe = useCallback((onChange: () => void): () => void => {
		const handle: WatchHandle = client.watchGroupCount(
			schema,
			query as GroupQuery<MessageShape<DescMessage>>,
			(groups) => {
				cell.current.value = withState(groups, false, undefined) as GroupResult;
				onChange();
			},
			{
				onError: (err) => {
					// Groups already delivered are kept: what is on screen stops
					// tracking the store rather than vanishing.
					cell.current.value = withState(
						[ ...cell.current.value ],
						false,
						asError(err),
					) as GroupResult;
					onChange();
				},
			},
		);
		return () => handle.close();
		// The identity is the resubscribe key; the query and workspace it was
		// derived from move together with it.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [ client, schema, identity ]);
	const getSnapshot = useCallback(() => cell.current.value, []);
	const server = useMemo(() => EMPTY as GroupResult, []);
	return useSyncExternalStore(subscribe, getSnapshot, () => server);
}

/** Stamps loading/error onto a group array in place, mirroring result.ts's
 * withState for row sets. */
function withState<T>(
	values: readonly T[],
	loading: boolean,
	error: Error | undefined,
): readonly T[] & Omit<WatchState, 'hydrating'> {
	return Object.defineProperties(values, {
		// Configurable so re-stamping a re-delivered array replaces the state
		// instead of throwing.
		loading: { value: loading, enumerable: false, configurable: true },
		error: { value: error, enumerable: false, configurable: true },
	}) as readonly T[] & Omit<WatchState, 'hydrating'>;
}
