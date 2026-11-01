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

/** Props of an `<UndoScopeProvider>` that opens and closes its own scope. */
export interface OwnedUndoScopeProviderProps {
	/** Undo groups the scope keeps; defaults to the client's `undo.maxDepth`. */
	maxDepth?: number;
	/**
	 * The workspace to fork. Defaults to the resolved one (an enclosing
	 * `<WorkspaceProvider>`, else the client's default workspace).
	 */
	workspaceId?: string;
	scope?: never;
	children?: ReactNode;
}

/** Props of an `<UndoScopeProvider>` that provides a scope the caller owns. */
export interface BorrowedUndoScopeProviderProps {
	/**
	 * The scope to give the subtree, from `undoScope()` on a workspace of the
	 * client the enclosing `<SyncProvider>` supplies. The provider never closes
	 * it: unmounting leaves the stack in place, and the caller closes the scope
	 * when it is done with it. A scope closed while provided stays usable but
	 * records nothing, and `useUndo()` below it reports both flags false.
	 *
	 * Passing a different scope switches every hook below to it on the next
	 * render.
	 */
	scope: UndoScope;
	maxDepth?: never;
	workspaceId?: never;
	children?: ReactNode;
}

export type UndoScopeProviderProps = OwnedUndoScopeProviderProps | BorrowedUndoScopeProviderProps;

/**
 * Gives its subtree a private undo stack. Every write below it — the mutate
 * hooks, `useText`, transactions — records its inverse on that stack instead of
 * the workspace's, and `useUndo()` below it drives it. Reads, subscriptions and
 * syncing are untouched: a fork shares the workspace's store and queue.
 *
 * Wrap the UI whose Cmd+Z must stay local — a task dialog, an inline editor —
 * and leave the surrounding view's `useUndo()` outside it, driving the
 * workspace's own stack.
 *
 * Without `scope`, the scope's lifetime is the subtree's: mounting opens it,
 * unmounting closes it and drops the stack (it is NOT merged into the
 * workspace's). With `scope`, the stack outlives the subtree, so a UI that
 * unmounts and remounts — a tab, a page revisited — keeps its history.
 *
 * A single write can opt back out with `{ undo: 'workspace' }` — see
 * UseWorkspaceOptions.
 *
 * @example
 * ```tsx
 * // One stack per page, kept while the page's tab is not mounted.
 * const scopes = new Map<string, UndoScope>();
 *
 * function PageTab({ pageId }: { pageId: string }) {
 * 	const ws = useWorkspaceScope();
 * 	let scope = scopes.get(pageId);
 * 	if (scope === undefined) {
 * 		scope = ws.undoScope();
 * 		scopes.set(pageId, scope);
 * 	}
 * 	return (
 * 		<UndoScopeProvider scope={ scope }>
 * 			<PageEditor pageId={ pageId } />
 * 		</UndoScopeProvider>
 * 	);
 * }
 * ```
 */
export function UndoScopeProvider(props: UndoScopeProviderProps): ReactElement {
	const borrowed = props.scope;
	// Called on every render so that switching between the two modes keeps the
	// hook order. With a borrowed scope it opens nothing, and the workspace it
	// resolves is the borrowed scope's own: a client that holds several
	// workspaces and no default must still render a provider handed a scope.
	const owned = useOwnedUndoScope(
		{ workspaceId: borrowed?.workspaceId ?? props.workspaceId },
		props.maxDepth,
		borrowed === undefined,
	);
	return createElement(UndoScopeContext.Provider, { value: borrowed ?? owned! }, props.children);
}

/**
 * Opens an undo scope owned by the calling component: created on mount, closed
 * on unmount. The result is a full workspace client whose writes record on its
 * own stack, so it is what to hand to an editor or a modal controller that
 * cannot take a React context — inside a tree, prefer `<UndoScopeProvider>`,
 * which puts every hook below it on the scope with no threading.
 */
export function useUndoScope(opts?: UseWorkspaceOptions & { maxDepth?: number }): UndoScope {
	return useOwnedUndoScope(opts, opts?.maxDepth, true)!;
}

/**
 * The shared lifetime: one scope per mount, closed on unmount. With `open`
 * false it holds no scope and returns undefined.
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
	open: boolean,
): UndoScope | undefined {
	// 'workspace': the parent of a nested scope is the workspace itself, so
	// nesting two providers forks the workspace twice rather than chaining.
	const parent = useWorkspaceScope(opts?.workspaceId, 'workspace');
	const ref = useRef<UndoScope | null>(null);
	const [ , rerender ] = useReducer((n: number) => n + 1, 0);
	// Re-pointed at another workspace, or told to stop owning one: close now
	// rather than waiting for the effect, so the subtree never renders with a
	// scope whose stack belongs to a workspace it is no longer reading. `maxDepth`
	// is read only here — a scope cannot be resized under the entries it already
	// holds.
	if (ref.current !== null && (!open || ref.current.workspaceId !== parent.workspaceId)) {
		ref.current.close();
		ref.current = null;
	}
	if (open) ref.current ??= parent.undoScope({ maxDepth });
	const scope = ref.current;
	useEffect(() => {
		if (scope === null) return;
		if (scope.closed) {
			ref.current = null;
			rerender();
			return;
		}
		return () => {
			scope.close();
		};
	}, [ scope, parent ]);
	return scope ?? undefined;
}
