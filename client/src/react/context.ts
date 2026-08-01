import { createContext, useContext } from 'react';

import type { SyncClient, WorkspaceClient } from '../client.js';

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

/**
 * Resolves the workspace every hook reads and writes through, in order:
 * an explicit `workspaceId` option, then the nearest `<WorkspaceProvider>`,
 * then the client's default workspace.
 *
 * The returned scope is the client's own long-lived object for that workspace,
 * so it is referentially stable and safe as a hook dependency — a new one
 * appears only if the workspace is removed and added back, which is exactly
 * when a watch should be re-established.
 */
export function useWorkspaceScope(workspaceId?: string): WorkspaceClient {
	const client = useSyncClient();
	const fromContext = useWorkspaceId();
	const id = workspaceId ?? fromContext ?? client.defaultWorkspace;
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
