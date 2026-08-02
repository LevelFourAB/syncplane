import { useCallback, useMemo, useSyncExternalStore } from 'react';

import { useWorkspaceScope } from '../context.js';
import type { UseWorkspaceOptions } from '../types.js';

export interface UseUndoResult {
	canUndo: boolean;
	canRedo: boolean;
	/** Replays the top undo group. Resolves false when there is nothing to undo. */
	undo(): Promise<boolean>;
	/** Inverse of `undo`. Resolves false when there is nothing to redo. */
	redo(): Promise<boolean>;
}

/**
 * Reactive undo/redo availability for the resolved scope: the nearest
 * `<UndoScopeProvider>`'s stack if there is one, else the workspace's own —
 * stacks never span workspaces, since the mutation ids they reference are
 * counted per workspace. Pass `{ undo: 'workspace' }` to drive the workspace's
 * stack from inside a scope.
 *
 * The snapshot is a primitive bitfield (`canUndo | canRedo<<1`) so referential
 * stability is automatic — equal state across emits produces no re-render — and
 * the subscription is the scope's `onUndoChange`. With undo disabled, both flags
 * stay false and the emitter is inert; so does a closed scope, which is what a
 * keybinding firing mid-unmount sees.
 */
export function useUndo(opts?: UseWorkspaceOptions): UseUndoResult {
	const scope = useWorkspaceScope(opts?.workspaceId, opts?.undo);
	const subscribe = useCallback((cb: () => void) => scope.onUndoChange(cb), [ scope ]);
	const getSnapshot = useCallback(
		() => (scope.canUndo() ? 1 : 0) | (scope.canRedo() ? 2 : 0),
		[ scope ],
	);
	const bits = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
	const undo = useCallback(() => scope.undo(), [ scope ]);
	const redo = useCallback(() => scope.redo(), [ scope ]);
	return useMemo<UseUndoResult>(
		() => ({ canUndo: (bits & 1) !== 0, canRedo: (bits & 2) !== 0, undo, redo }),
		[ bits, undo, redo ],
	);
}
