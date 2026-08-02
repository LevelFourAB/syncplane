import type { DescMessage, MessageInitShape, MessageShape } from '@bufbuild/protobuf';
import { useCallback, useMemo } from 'react';

import type { UpdateRecipe } from '../../client.js';
import type { ReadonlyRowSet } from '../../coverage.js';
import type { PatchInit } from '../../mutations.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import { useRowStore } from '../row-store.js';
import { stableHash } from '../stable-hash.js';
import type {
	RowCasOptions,
	UseRowCasOptions,
	UseRowOptions,
	UseRowPatchOptions,
	UseRowResultFor,
	UseRowUpdateOptions,
} from '../types.js';

/**
 * Resolves a write's CAS guard against the row the hook is holding. `cas: true`
 * binds `version`; an explicit `expectedVersion` always wins. `cas` itself is
 * stripped — it is this layer's spelling, not a mutate option.
 *
 * Deliberately not the default: see RowCasOptions for why a guard cut from a
 * confirmed version turns a burst of edits into a conflict with itself.
 */
function withGuard<O extends RowCasOptions & { expectedVersion?: bigint }>(
	opts: O | undefined,
	version: bigint,
): Omit<O, 'cas'> | undefined {
	if (opts === undefined) return undefined;
	const { cas, ...rest } = opts;
	if (!cas || rest.expectedVersion !== undefined) return rest;
	return { ...rest, expectedVersion: version };
}

/**
 * Live view of a single row by key, plus `patch`/`update`/`del` bound to it.
 * Writes go through per-field LWW; `{ cas: true }` guards one on the row's
 * current `version` (see RowCasOptions).
 *
 * Every useRow on the same workspace/entity/include shares ONE entity watch and
 * a key index in front of it (see row-store.ts), so a list of row hooks costs
 * one subscription, and a commit re-renders only the hooks whose own row moved.
 */
export function useRowHook<D extends DescMessage>(
	schema: D,
	key: string,
	opts?: UseRowOptions,
	suspense?: boolean,
): UseRowResultFor<D> {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId, opts?.undo);
	const include = opts?.include;
	// Identity of the SHARED watch: the row key is deliberately absent, since
	// every key on this entity is served from one subscription. Distinct
	// namespace from useQuery so the two never alias.
	const identity = stableHash(
		`row\u0000${scope.workspaceId}\u0000${schema.typeName}`,
		undefined,
		include,
	);
	// Not memoized, for the reason in use-query: the store re-subscribes on
	// `identity`, which already folds in the include spec.
	const subscribe = (
		onRows: (rows: ReadonlyRowSet<MessageShape<D>>) => void,
		onError: (error: unknown) => void,
	): () => void => scope.watch(schema, onRows, { include, onError });
	const cell = useRowStore<MessageShape<D>>({
		identity,
		subscribe,
		client,
		key,
		suspense,
	});

	const row = cell.row;
	const version = row?.version;

	const patch = useCallback(
		((init: MessageInitShape<D>, o?: UseRowPatchOptions): Promise<void> => {
			if (version === undefined) {
				return Promise.reject(
					new Error(`useRow: cannot patch "${key}" — row not present (load it before mutating)`),
				);
			}
			// One wide implementation behind both `patch` overloads (see
			// UseRowResult). The public signature already refused a whole message;
			// inside the body D is unresolved, so the cast only restates that.
			return scope.mutate.patch(schema, key, init as PatchInit<D>, withGuard(o, version));
		}) as UseRowResultFor<D>['patch'],
		[ scope, schema, key, version ],
	);

	// The row this hook hands back is exactly what tempts a whole-row patch, so
	// the read-modify-write verb is right here beside it: the recipe edits a
	// draft and only what it changed is written.
	const update = useCallback(
		(recipe: UpdateRecipe<D>, o?: UseRowUpdateOptions): Promise<void> => {
			if (version === undefined) {
				return Promise.reject(
					new Error(`useRow: cannot update "${key}" — row not present (load it before mutating)`),
				);
			}
			return scope.mutate.update(schema, key, recipe, withGuard(o, version));
		},
		[ scope, schema, key, version ],
	);

	const del = useCallback(
		(o?: UseRowCasOptions): Promise<void> => {
			if (version === undefined) {
				return Promise.reject(
					new Error(`useRow: cannot delete "${key}" — row not present`),
				);
			}
			return scope.mutate.del(schema, key, withGuard(o, version));
		},
		[ scope, schema, key, version ],
	);

	return useMemo<UseRowResultFor<D>>(
		() => ({
			row,
			loading: cell.loading,
			hydrating: cell.hydrating,
			error: cell.error,
			patch,
			update,
			del,
		}),
		[ row, cell.loading, cell.hydrating, cell.error, patch, update, del ],
	);
}
