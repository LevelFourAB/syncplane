import { createElement, type ReactElement, type ReactNode } from 'react';

import { WorkspaceContext } from './context.js';

export interface WorkspaceProviderProps {
	/** Must be a workspace the client holds; the hooks throw otherwise. */
	workspaceId: string;
	children?: ReactNode;
}

/**
 * Points every hook below it at one workspace, overriding the client's default.
 * A per-workspace route or tab wraps its subtree once instead of threading a
 * `workspaceId` option through every call; nesting re-scopes again, and the
 * innermost provider wins.
 *
 * The client is unchanged — one client covers an account and all its
 * workspaces — so switching workspaces is a re-render, not a reconnect.
 */
export function WorkspaceProvider(props: WorkspaceProviderProps): ReactElement {
	return createElement(WorkspaceContext.Provider, { value: props.workspaceId }, props.children);
}
