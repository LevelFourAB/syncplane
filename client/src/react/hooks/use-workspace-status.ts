import { useCallback, useRef, useSyncExternalStore } from 'react';

import type { WorkspaceStatus } from '../../client.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import type { UseWorkspaceOptions } from '../types.js';

/**
 * One workspace's slice of the sync status — its pending count, scope and
 * access verdict — for the resolved workspace. `useSyncStatus()` stays the
 * account-wide view; this is what a per-workspace header renders.
 *
 * The status tracker replaces a workspace's entry only when that workspace
 * actually changed, so the snapshot is referentially stable and a busy
 * neighbour workspace does not re-render this one.
 */
export function useWorkspaceStatus(opts?: UseWorkspaceOptions): WorkspaceStatus {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId);
	const workspaceId = scope.workspaceId;
	// A workspace the tracker has no entry for (it was removed under us) still
	// needs ONE stable object per id, or getSnapshot would hand
	// useSyncExternalStore a fresh value every call and loop forever.
	const absent = useRef<WorkspaceStatus>({ workspaceId, pendingMutations: 0 });
	if (absent.current.workspaceId !== workspaceId) {
		absent.current = { workspaceId, pendingMutations: 0 };
	}

	const subscribe = useCallback((cb: () => void) => client.onStatusChange(cb), [ client ]);
	const getSnapshot = useCallback(
		() => client.status().workspaces[workspaceId] ?? absent.current,
		[ client, workspaceId ],
	);
	return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
