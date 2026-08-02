import {
	createElement,
	type ReactElement,
	type ReactNode,
	useEffect,
	useReducer,
	useRef,
} from 'react';

import type { UndoScope } from '../client.js';
import { UndoScopeContext, useWorkspaceScope } from './context.js';
import type { UseWorkspaceOptions } from './types.js';

export interface UndoScopeProviderProps {
	/** Undo groups the scope keeps; defaults to the client's `undo.maxDepth`. */
	maxDepth?: number;
	/**
	 * The workspace to fork. Defaults to the resolved one (an enclosing
	 * `<WorkspaceProvider>`, else the client's default workspace).
	 */
	workspaceId?: string;
	children?: ReactNode;
}

/**
 * Gives its subtree a private undo stack. Every write below it — the mutate
 * hooks, `useText`, transactions — records its inverse on that stack instead of
 * the workspace's, and `useUndo()` below it drives it. Reads, subscriptions and
 * syncing are untouched: a fork shares the workspace's store and queue.
 *
 * Wrap the UI whose Cmd+Z must stay local — a task dialog, an inline editor —
 * and leave the surrounding view's `useUndo()` outside it, driving the
 * workspace's own stack. The scope's lifetime is the subtree's: mounting opens
 * it, unmounting closes it and drops the stack (it is NOT merged into the
 * workspace's).
 *
 * A single write can opt back out with `{ undo: 'workspace' }` — see
 * UseWorkspaceOptions.
 */
export function UndoScopeProvider(props: UndoScopeProviderProps): ReactElement {
	const scope = useOwnedUndoScope({ workspaceId: props.workspaceId }, props.maxDepth);
	return createElement(UndoScopeContext.Provider, { value: scope }, props.children);
}

/**
 * Opens an undo scope owned by the calling component: created on mount, closed
 * on unmount. The result is a full workspace client whose writes record on its
 * own stack, so it is what to hand to an editor or a modal controller that
 * cannot take a React context — inside a tree, prefer `<UndoScopeProvider>`,
 * which puts every hook below it on the scope with no threading.
 */
export function useUndoScope(opts?: UseWorkspaceOptions & { maxDepth?: number }): UndoScope {
	return useOwnedUndoScope(opts, opts?.maxDepth);
}

/**
 * The shared lifetime: one scope per mount, closed on unmount.
 *
 * The scope is held in a ref rather than state so that a double-invoked render
 * (React StrictMode) cannot leave an orphan scope registered on the workspace —
 * a `useState` initializer would run twice and one of the two results would
 * never be closed. The effect handles the other half of StrictMode: it closes
 * the scope on cleanup, and a re-mounted effect that finds its scope already
 * closed drops it and re-renders with a fresh one. Changing workspace runs the
 * same path, since the effect's cleanup fires there too.
 */
function useOwnedUndoScope(
	opts: UseWorkspaceOptions | undefined,
	maxDepth: number | undefined,
): UndoScope {
	// 'workspace': the parent of a nested scope is the workspace itself, so
	// nesting two providers forks the workspace twice rather than chaining.
	const parent = useWorkspaceScope(opts?.workspaceId, 'workspace');
	const ref = useRef<UndoScope | null>(null);
	const [ , rerender ] = useReducer((n: number) => n + 1, 0);
	// Re-pointed at another workspace: fork the new one immediately rather than
	// waiting for the effect, so the subtree never renders with a scope whose
	// stack belongs to a workspace it is no longer reading. `maxDepth` is read
	// only here — a scope cannot be resized under the entries it already holds.
	if (ref.current !== null && ref.current.workspaceId !== parent.workspaceId) {
		ref.current.close();
		ref.current = null;
	}
	ref.current ??= parent.undoScope({ maxDepth });
	const scope = ref.current;
	useEffect(() => {
		if (scope.closed) {
			ref.current = null;
			rerender();
			return;
		}
		return () => {
			scope.close();
		};
	}, [ scope, parent ]);
	return scope;
}
