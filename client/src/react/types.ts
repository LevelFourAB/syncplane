import type { DescMessage, MessageInitShape, MessageShape } from '@bufbuild/protobuf';

import type {
	CasOptions,
	MaskedPatchOptions,
	PatchOptions,
	UpdateOptions,
	UpdateRecipe,
} from '../client.js';
import type { EntityKey } from '../db.js';
import type { PatchInit } from '../mutations.js';
import type { Query, TotalOf } from '../query/index.js';
import type { Row } from '../read.js';
import type { IncludeSpec } from '../refs.js';
import type { WatchWindow } from '../watch.js';
import type { UseTextResult } from './hooks/use-text.js';
import type { WatchResult, WatchState } from './result.js';

/**
 * The workspace selector every hook accepts. Resolution order is this option,
 * then the nearest `<WorkspaceProvider>`, then the client's default workspace —
 * so a single-workspace app never passes it, and a per-workspace route sets it
 * once on a provider instead of on every call.
 */
export interface UseWorkspaceOptions {
	workspaceId?: string;
	/**
	 * Which undo stack this hook's writes record on. 'nearest' (the default)
	 * uses the enclosing `<UndoScopeProvider>` if there is one; 'workspace'
	 * skips every enclosing scope and records on the workspace's own stack — for
	 * the write inside a dialog that belongs to the view behind it (deleting the
	 * row being edited, say), not to the dialog's local history.
	 *
	 * Reads are unaffected: a scope and its workspace share one subscription.
	 */
	undo?: UndoTarget;
}

/** See UseWorkspaceOptions.undo. */
export type UndoTarget = 'nearest' | 'workspace';

/** Options shared by the watch-backed hooks. */
export interface UseWatchOptions extends UseWorkspaceOptions {
	/** Hydrate result rows against this include tree (folded into the identity). */
	include?: IncludeSpec;
}

/** Options for `useQuery` and `useSuspenseQuery`. */
export interface UseQueryOptions extends UseWatchOptions {
	/**
	 * Page of the result to render, in place of any `offset`/`limit` the query
	 * carries. `rows.total` reports how many rows it was cut from, which is what
	 * sizes a scroller over it.
	 *
	 * Moving it re-pages the result the subscription already holds — no store
	 * read, no resubscription — so a virtualized list can follow the scroll
	 * position. `useSuspenseQuery` is the exception: there the window is part of
	 * the subscription identity, so a move re-reads and re-suspends.
	 */
	window?: WatchWindow;
}

/**
 * Options for `useQueryAll`. `UseQueryOptions` without the workspace selector or
 * the undo target: the read spans every workspace and the hook does not write.
 */
export interface UseQueryAllOptions {
	/** Hydrate result rows against this include tree (folded into the identity). */
	include?: IncludeSpec;
	/** Page of the merged result to render; see UseQueryOptions.window. */
	window?: WatchWindow;
}

/**
 * Options for `useRow`. `include` is shared with every other useRow on the same
 * entity and workspace: the include tree is part of the shared subscription's
 * identity, so two different trees are two subscriptions.
 */
export type UseRowOptions = UseWatchOptions;

/** The stable per-entity mutate facade (identical to `client.db[key].mutate`). */
export interface EntityMutate<D extends DescMessage> {
	put(value: MessageShape<D> | MessageInitShape<D>, opts?: CasOptions): Promise<void>;
	/** Merges the given fields into a row; `init` is those fields, not a row. */
	patch(key: string, init: PatchInit<D>, opts?: PatchOptions): Promise<void>;
	/** Merges fields off any message under an explicit mask. */
	patch(key: string, init: MessageInitShape<D>, opts: MaskedPatchOptions): Promise<void>;
	/** Read-modify-write; only the fields the recipe changes are written. */
	update(key: string, recipe: UpdateRecipe<D>, opts?: UpdateOptions): Promise<void>;
	del(key: string, opts?: CasOptions): Promise<void>;
}

/**
 * Opts a `useRow` write into a compare-and-swap on the row the hook is holding.
 *
 * The guard is NOT the default. `Row.version` is the SERVER-confirmed version:
 * the optimistic overlay does not advance it, so it stands still from the
 * moment an edit is enqueued until a pull brings the row back down. A guard cut
 * from it therefore describes the row as it was before this client's own
 * unacked writes, and a second edit issued inside that window conflicts with
 * nothing but its own predecessor — which the server reports as a conflict and
 * the client resolves by DROPPING the edit. A field being typed into produces
 * exactly that pattern.
 *
 * The engine's own answer to concurrent edits is per-field last-writer-wins, and
 * an unguarded PATCH is what asks for it. Reach for `cas` where a concurrent
 * edit must be noticed rather than merged — a considered, one-shot write — and
 * bear in mind that the guard covers the whole ROW: there are no per-field
 * versions, so it also fires when someone else changed a field yours does not
 * touch.
 */
export interface RowCasOptions {
	/**
	 * Guard the write on the row's current `version` (see `Row.version`). An
	 * explicit `expectedVersion` takes precedence.
	 */
	cas?: boolean;
}

/** `useRow`'s PATCH options: the entity mutate options plus the CAS opt-in. */
export type UseRowPatchOptions = PatchOptions & RowCasOptions;
/** `useRow`'s masked-PATCH options (see MaskedPatchOptions). */
export type UseRowMaskedPatchOptions = MaskedPatchOptions & RowCasOptions;
/** `useRow`'s update options: the mask comes from the diff. */
export type UseRowUpdateOptions = UpdateOptions & RowCasOptions;
/** `useRow`'s delete options. */
export type UseRowCasOptions = CasOptions & RowCasOptions;

/**
 * Result of `useRow`. `patch`/`update`/`del` write through per-field LWW like
 * every other mutate call; pass `{ cas: true }` to guard one on the row's
 * current `version` (see RowCasOptions), or `opts.expectedVersion` for a guard
 * of your own. They are no-ops (reject with a clear error) while `row` is
 * undefined.
 *
 * `loading` distinguishes the two ways `row` can be undefined: the entity has
 * not been read yet, or it has and this key is not in it. `error` reports a
 * watch that could not be established at all.
 *
 * `update` is the one to reach for when editing the row this hook just handed
 * back: it writes only what the recipe changed, where patching `row` itself
 * would write every field, stale ones included.
 */
export interface UseRowResult<T> extends WatchState {
	row: Row<T> | undefined;
	patch(init: PatchInit<DescMessage>, opts?: UseRowPatchOptions): Promise<void>;
	patch(init: MessageInitShape<DescMessage>, opts: UseRowMaskedPatchOptions): Promise<void>;
	update(recipe: (draft: T) => void, opts?: UseRowUpdateOptions): Promise<void>;
	del(opts?: UseRowCasOptions): Promise<void>;
}

/** Schema-bound React hooks for one entity, mirroring `client.db[key]`. */
export interface ReactEntityApi<D extends DescMessage> {
	/** Rows plus `loading`/`error`; `[]` until the first callback. */
	useQuery<const Q extends Query<MessageShape<D>>>(
		query: Q,
		opts?: UseQueryOptions,
	): WatchResult<MessageShape<D>, TotalOf<Q>>;
	/**
	 * `useQuery` that suspends until the first callback instead of reporting
	 * `loading`. A separate hook rather than an option because the choice selects
	 * a fixed hook path and must not change between renders of a call site; a
	 * failed subscription throws here for an error boundary, since a suspended
	 * component has no render to carry an error state.
	 */
	useSuspenseQuery<const Q extends Query<MessageShape<D>>>(
		query: Q,
		opts?: UseQueryOptions,
	): WatchResult<MessageShape<D>, TotalOf<Q>>;
	/**
	 * `useQuery` over every workspace the client holds, as one subscription. Each
	 * row carries the `workspaceId` it came from, which is what groups the result
	 * and routes a write back; ordering and limits apply to the merged set rather
	 * than per workspace (see `client.watchQueryAll`).
	 */
	useQueryAll<const Q extends Query<MessageShape<D>>>(
		query: Q,
		opts?: UseQueryAllOptions,
	): WatchResult<MessageShape<D>, TotalOf<Q>>;
	useRow(key: string, opts?: UseRowOptions): UseRowResultFor<D>;
	/** `useRow` that suspends until the entity's first callback (see useSuspenseQuery). */
	useSuspenseRow(key: string, opts?: UseRowOptions): UseRowResultFor<D>;
	useMutate(opts?: UseWorkspaceOptions): EntityMutate<D>;
	/** Live view of a collaborative text field (see useTextHook). */
	useText(key: string, field: string, opts?: UseWorkspaceOptions): UseTextResult;
}

/** `UseRowResult` specialized to an entity's message shape. */
export interface UseRowResultFor<D extends DescMessage> extends WatchState {
	row: Row<MessageShape<D>> | undefined;
	patch(init: PatchInit<D>, opts?: UseRowPatchOptions): Promise<void>;
	patch(init: MessageInitShape<D>, opts: UseRowMaskedPatchOptions): Promise<void>;
	update(recipe: UpdateRecipe<D>, opts?: UseRowUpdateOptions): Promise<void>;
	del(opts?: UseRowCasOptions): Promise<void>;
}

/** Per-entity hook map keyed exactly like `client.db` (no pluralization). */
export type ReactEntityApis<E extends readonly DescMessage[]> = {
	[D in E[number] as EntityKey<D>]: ReactEntityApi<D>;
};
