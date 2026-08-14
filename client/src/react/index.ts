// syncplane/react — a first-party React hooks layer over the
// local-first client. Mirrors the `client.db.<entity>` facade exactly and maps
// the engine's watch/status/undo surfaces onto useSyncExternalStore. React is a
// peer dependency; the core build stays JSX-free (the provider uses
// React.createElement).

// Everything here is stateful, browser-side and hook-based, so this entry is a
// client boundary: without the directive a React Server Components build (the
// Next.js App Router, say) refuses the import, and an app would have to wrap
// every hook of ours in a component of its own. Marking the entry marks the
// module graph it pulls in, so this one line covers the whole layer. It must
// stay the first statement in the file for bundlers to see it.
'use client';

import type { DescMessage } from '@bufbuild/protobuf';

import { buildReactDb } from './build-db.js';
import { useOnMutationRejected } from './hooks/use-on-mutation-rejected.js';
import { usePresenceHook, useRoomHook } from './hooks/use-room.js';
import { useSyncStatus } from './hooks/use-sync-status.js';
import { useUndo } from './hooks/use-undo.js';
import { useWorkspaceStatus } from './hooks/use-workspace-status.js';
import { SyncProvider } from './provider.js';
import type { ReactEntityApis } from './types.js';
import { UndoScopeProvider, useUndoScope } from './undo-scope.js';
import { WorkspaceProvider } from './workspace.js';

export {
	useNearestUndoScope,
	useSyncClient,
	useWorkspaceId,
	useWorkspaceScope,
} from './context.js';
export { useDistinctHook, useGroupCountHook } from './hooks/use-groups.js';
export type { DistinctResult, GroupResult } from './hooks/use-groups.js';
export { useOnMutationRejected } from './hooks/use-on-mutation-rejected.js';
export { usePresenceHook, useRoomHook } from './hooks/use-room.js';
export type { UseRoomOptions, UseRoomResult } from './hooks/use-room.js';
export { useSyncStatus } from './hooks/use-sync-status.js';
export { useTextHook } from './hooks/use-text.js';
export type { UseTextResult } from './hooks/use-text.js';
export { useUndo } from './hooks/use-undo.js';
export type { UseUndoResult } from './hooks/use-undo.js';
export { useWorkspaceStatus } from './hooks/use-workspace-status.js';
export { SyncProvider } from './provider.js';
export type { SyncProviderProps } from './provider.js';
export type { KeyedRowsResult, WatchResult, WatchState } from './result.js';
export type { RowCell } from './row-store.js';
export type {
	EntityMutate,
	ReactEntityApi,
	ReactEntityApis,
	RowCasOptions,
	UndoTarget,
	UseQueryAllOptions,
	UseQueryOptions,
	UseRowCasOptions,
	UseRowMaskedPatchOptions,
	UseRowOptions,
	UseRowPatchOptions,
	UseRowResult,
	UseRowResultFor,
	UseRowsAllOptions,
	UseRowsOptions,
	UseRowsResultFor,
	UseRowUpdateOptions,
	UseWatchOptions,
	UseWorkspaceOptions,
} from './types.js';
export { UndoScopeProvider, useUndoScope } from './undo-scope.js';
export type { UndoScopeProviderProps } from './undo-scope.js';
export { WorkspaceProvider } from './workspace.js';
export type { WorkspaceProviderProps } from './workspace.js';

/** What `createReactSync` returns: the provider plus the schema-bound hooks. */
export interface ReactSync<E extends readonly DescMessage[]> {
	/** Provides a client — caller-built or provider-owned — to the hooks below it. */
	SyncProvider: typeof SyncProvider;
	/** Re-scopes the hooks below it to one workspace of that client. */
	WorkspaceProvider: typeof WorkspaceProvider;
	/** Gives the subtree below it its own undo stack (a dialog, an editor). */
	UndoScopeProvider: typeof UndoScopeProvider;
	/** Per-entity hooks keyed exactly like `client.db` (`db.todo.useQuery(...)`). */
	db: ReactEntityApis<E>;
	useSyncStatus: typeof useSyncStatus;
	useWorkspaceStatus: typeof useWorkspaceStatus;
	useUndo: typeof useUndo;
	useUndoScope: typeof useUndoScope;
	useOnMutationRejected: typeof useOnMutationRejected;
	/** The ephemeral plane: one room, with the handle. */
	useRoom: typeof useRoomHook;
	/** The ephemeral plane: one room's roster, without the handle. */
	usePresence: typeof usePresenceHook;
}

/**
 * Builds the React adapter for a schema tuple. Inference flows from the literal
 * `entities` array (same `const E` signature as `SyncClient`), so
 * `db.<entityKey>` is typed per entity with no pluralization and a duplicate
 * simple-name throws at build time, exactly as `client.db` does.
 *
 * The returned surface is client-agnostic: render any `<SyncProvider>` above the
 * hooks. `entities` should match the client's `entities` option.
 */
export function createReactSync<const E extends readonly DescMessage[]>(
	entities: E,
): ReactSync<E> {
	return {
		SyncProvider,
		WorkspaceProvider,
		UndoScopeProvider,
		db: buildReactDb(entities),
		useSyncStatus,
		useWorkspaceStatus,
		useUndo,
		useUndoScope,
		useOnMutationRejected,
		useRoom: useRoomHook,
		usePresence: usePresenceHook,
	};
}
