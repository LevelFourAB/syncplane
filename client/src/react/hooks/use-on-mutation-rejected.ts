import { useEffect, useRef } from 'react';

import type { RejectionInfo } from '../../client.js';
import { useSyncClient, useWorkspaceId } from '../context.js';
import type { UseWorkspaceOptions } from '../types.js';

/**
 * Runs `handler` for every mutation that will never be applied — a server
 * rejection, a poison drop, a purge, a format upgrade.
 *
 * This is the piece a React app cannot build from the hook API otherwise:
 * mutations resolve on LOCAL commit, so a rejection arrives long after the
 * promise that queued it settled, on a client-level channel with no component
 * attached. Subscribe here and turn it into a toast, a retry, or whatever the
 * app owes the user.
 *
 * The handler is held in a ref, so an inline arrow function does not resubscribe
 * on every render and the callback that fires is always the newest one. By
 * default only rejections for the hook's resolved workspace are delivered — a
 * per-workspace view should not toast another workspace's failure; pass
 * `{ allWorkspaces: true }` for an app-wide surface.
 *
 * A rejection whose push response was lost still arrives, one sync cycle later,
 * once the resend collects the verdict the server recorded.
 */
export function useOnMutationRejected(
	handler: (info: RejectionInfo) => void,
	opts?: UseWorkspaceOptions & { allWorkspaces?: boolean },
): void {
	const client = useSyncClient();
	const fromContext = useWorkspaceId();
	// Not useWorkspaceScope: a client with several workspaces and no default is
	// fine here — that is exactly the app that wants every workspace's failures.
	const workspaceId = opts?.allWorkspaces
		? undefined
		: opts?.workspaceId ?? fromContext ?? client.defaultWorkspace;

	const handlerRef = useRef(handler);
	handlerRef.current = handler;

	useEffect(
		() =>
			client.onMutationRejected((info) => {
				if (workspaceId !== undefined && info.workspaceId !== workspaceId) return;
				handlerRef.current(info);
			}),
		[ client, workspaceId ],
	);
}
