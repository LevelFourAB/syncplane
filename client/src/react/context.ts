import { createContext, useContext } from 'react';

import type { SyncClient, UndoScope, WorkspaceClient } from '../client.js';
import type { UndoTarget } from './types.js';

/**
 * React context carrying the live SyncClient. `null` is the "no provider"
 * sentinel so useSyncClient() can throw a clear error rather than handing back
 * undefined and failing later inside a hook.
 */
export const SyncContext = createContext<SyncClient | null>(null);

/**
 * The workspace the hooks below this point target. `null` means "no override" —
 * the client's default workspace — which is what a single-workspace app leaves
 * it at. Separate from SyncContext so a subtree can re-scope the workspace
 * without re-providing (or knowing about) the client.
 */
export const WorkspaceContext = createContext<string | null>(null);

/**
 * The nested undo scope the hooks below this point write through, or `null` for
 * none — in which case writes record on the workspace's own undo stack. Set by
 * `<UndoScopeProvider>`; see useWorkspaceScope for how a hook picks between the
 * two.
 */
export const UndoScopeContext = createContext<UndoScope | null>(null);

/** Reads the SyncClient from the nearest SyncProvider. Throws if absent. */
export function useSyncClient(): SyncClient {
	const client = useContext(SyncContext);
	if (client === null) {
		throw new Error(
			'useSyncClient: no SyncProvider found in the tree. Wrap your app in the '
				+ '<SyncProvider client={...}> returned by createReactSync().',
		);
	}
	return client;
}

/** The workspace id from the nearest WorkspaceProvider, or undefined. */
export function useWorkspaceId(): string | undefined {
	return useContext(WorkspaceContext) ?? undefined;
}

/** The nested undo scope from the nearest UndoScopeProvider, or undefined. */
export function useNearestUndoScope(): UndoScope | undefined {
	return useContext(UndoScopeContext) ?? undefined;
}

/**
 * Resolves the workspace every hook reads and writes through, in order:
 * an explicit `workspaceId` option, then the nearest `<WorkspaceProvider>`,
 * then the client's default workspace.
 *
 * An enclosing `<UndoScopeProvider>` wins over the workspace's own scope, so
 * writes below it record on the dialog's (or editor's) undo stack rather than
 * the surrounding view's. `undo: 'workspace'` opts one call back out, and a
 * scope for another workspace than the one resolved here is ignored — an
 * explicit `workspaceId` names a workspace, and its undo stack comes with it.
 * Reads are identical either way: a scope and its forks share one subscription
 * set and one store.
 *
 * The returned scope is a long-lived object — the client's for the workspace,
 * the provider's for an undo scope — so it is referentially stable and safe as
 * a hook dependency. A new one appears only if the workspace is removed and
 * added back, the provider remounts, or the provider is handed another `scope`,
 * which is exactly when a watch should be re-established.
 */
export function useWorkspaceScope(workspaceId?: string, undo?: UndoTarget): WorkspaceClient {
	const client = useSyncClient();
	const fromContext = useWorkspaceId();
	const undoScope = useNearestUndoScope();
	const id = workspaceId ?? fromContext ?? client.defaultWorkspace;
	if (undo !== 'workspace' && undoScope !== undefined && undoScope.workspaceId === id) {
		return undoScope;
	}
	if (id === undefined) {
		throw new Error(
			'syncplane/react: this client holds several workspaces and none of them is '
				+ 'the default, so the hooks cannot tell which one you mean. Wrap the tree '
				+ 'in <WorkspaceProvider workspaceId="...">, pass { workspaceId } to the '
				+ 'hook, or give the client a defaultWorkspace.',
		);
	}
	return client.ws(id);
}
