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
import type { Query } from '../query/index.js';
import type { Row } from '../read.js';
import type { IncludeSpec } from '../refs.js';
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
}

/** Options shared by the watch-backed hooks. */
export interface UseWatchOptions extends UseWorkspaceOptions {
	/** Hydrate result rows against this include tree (folded into the identity). */
	include?: IncludeSpec;
}

export type UseQueryOptions = UseWatchOptions;

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
 * Result of `useRow`. `patch`/`update`/`del` bind the row's current `version` as
 * the default `expectedVersion` — a local-first compare-and-swap for free —
 * which a caller-supplied `opts.expectedVersion` overrides. They are no-ops
 * (reject with a clear error) while `row` is undefined.
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
	patch(init: PatchInit<DescMessage>, opts?: PatchOptions): Promise<void>;
	patch(init: MessageInitShape<DescMessage>, opts: MaskedPatchOptions): Promise<void>;
	update(recipe: (draft: T) => void, opts?: UpdateOptions): Promise<void>;
	del(opts?: CasOptions): Promise<void>;
}

/** Schema-bound React hooks for one entity, mirroring `client.db[key]`. */
export interface ReactEntityApi<D extends DescMessage> {
	/** Rows plus `loading`/`error`; `[]` until the first callback. */
	useQuery(
		query: Query<MessageShape<D>>,
		opts?: UseQueryOptions,
	): WatchResult<MessageShape<D>>;
	/**
	 * `useQuery` that suspends until the first callback instead of reporting
	 * `loading`. A separate hook rather than an option because the choice selects
	 * a fixed hook path and must not change between renders of a call site; a
	 * failed subscription throws here for an error boundary, since a suspended
	 * component has no render to carry an error state.
	 */
	useSuspenseQuery(
		query: Query<MessageShape<D>>,
		opts?: UseQueryOptions,
	): WatchResult<MessageShape<D>>;
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
	patch(init: PatchInit<D>, opts?: PatchOptions): Promise<void>;
	patch(init: MessageInitShape<D>, opts: MaskedPatchOptions): Promise<void>;
	update(recipe: UpdateRecipe<D>, opts?: UpdateOptions): Promise<void>;
	del(opts?: CasOptions): Promise<void>;
}

/** Per-entity hook map keyed exactly like `client.db` (no pluralization). */
export type ReactEntityApis<E extends readonly DescMessage[]> = {
	[D in E[number] as EntityKey<D>]: ReactEntityApi<D>;
};
