import type { DescMessage } from '@bufbuild/protobuf';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { TextHandle, TextSpan } from '../../text/handle.js';
import { useWorkspaceScope } from '../context.js';
import type { UseWorkspaceOptions } from '../types.js';

/** What useText returns: the live value plus the handle for edits. */
export interface UseTextResult {
	/** Current text, or undefined while the handle is opening. */
	value: string | undefined;
	/**
	 * Resolved mark spans (UTF-16 indices); empty while loading. The array
	 * keeps its reference until a delta reports the spans may have moved, so a
	 * memoized child re-renders on formatting changes and not on every
	 * keystroke.
	 */
	marks: readonly TextSpan[];
	/**
	 * The open handle — insert/delete/mark/anchors — or undefined while
	 * loading. Its edits re-render this hook via the subscription.
	 */
	handle: TextHandle | undefined;
	/**
	 * Set when opening failed (e.g. the row does not exist), and when an open
	 * handle stopped merging (see TextHandle.status). In the second case `value`
	 * and `marks` still hold what the handle last had, but they no longer track
	 * the row and writes throw — render the error and remount to recover.
	 */
	error: Error | undefined;
	/**
	 * Rewrites the whole value with a minimal multi-hunk diff — the write path
	 * for a controlled `<textarea>`, which only ever knows the new string.
	 * Items the edit did not touch survive, so concurrent remote edits and the
	 * marks anchored to them are preserved. Stable across renders; rejects
	 * while the handle is still opening.
	 */
	setText(text: string): Promise<void>;
}

/** Shared empty spans, so "no marks" is one stable reference. */
const NO_MARKS: readonly TextSpan[] = Object.freeze([]);

/**
 * What the hook reports, tagged with the row/field it describes so a changed
 * key never shows the previous row's text while the new handle opens.
 */
interface TextState {
	identity: string;
	value: string | undefined;
	marks: readonly TextSpan[];
	handle: TextHandle | undefined;
	error: Error | undefined;
}

function loading(identity: string): TextState {
	return { identity, value: undefined, marks: NO_MARKS, handle: undefined, error: undefined };
}

/**
 * Live view of one collaborative text field in the resolved workspace: opens a
 * TextHandle, subscribes to local and remote deltas, and re-renders with the
 * current value and mark spans. The handle closes when the component unmounts
 * or the workspace/row/field identity changes.
 */
export function useTextHook<D extends DescMessage>(
	schema: D,
	key: string,
	field: string,
	opts?: UseWorkspaceOptions,
): UseTextResult {
	const scope = useWorkspaceScope(opts?.workspaceId, opts?.undo);
	const identity = `${scope.workspaceId}\u0000${schema.typeName}\u0000${key}\u0000${field}`;
	const [ state, setState ] = useState<TextState>(() => loading(identity));
	const handleRef = useRef<TextHandle>(undefined);

	useEffect(() => {
		let cancelled = false;
		const unsubs: (() => void)[] = [];
		// Spans are resolved against the item order rather than stored, so they
		// are re-read exactly when a delta says they may have moved (see
		// TextDelta.marksChanged) and keep their reference otherwise.
		let marks: readonly TextSpan[] = NO_MARKS;
		void scope.text(schema, key, field).then((handle) => {
			if (cancelled) {
				handle.close();
				return;
			}
			handleRef.current = handle;
			// Terminal once set, so it is held here rather than passed in: no
			// later publish may report the handle as healthy again.
			let failure: Error | undefined;
			const publish = (marksChanged: boolean) => {
				// The handle can resolve, or a queued delta land, after the effect
				// was cleaned up; setState then would be an update on a component
				// that no longer holds this subscription.
				if (cancelled) return;
				if (marksChanged) marks = handle.marks();
				setState({ identity, value: handle.value, marks, handle, error: failure });
			};
			unsubs.push(handle.subscribe((delta) => publish(delta.marksChanged)));
			// A handle that stopped merging keeps its last value, so it is still
			// rendered — with the error beside it, because what is on screen no
			// longer tracks the row and the next keystroke will throw.
			unsubs.push(handle.onError((err) => {
				failure = new Error(err.message, { cause: err.cause });
				publish(false);
			}));
			publish(true);
		}).catch((err: unknown) => {
			if (cancelled) return;
			setState({
				identity,
				value: undefined,
				marks: NO_MARKS,
				handle: undefined,
				error: err instanceof Error ? err : new Error(String(err)),
			});
		});
		return () => {
			cancelled = true;
			for (const unsub of unsubs) unsub();
			// The PATCHes the closed handle still owes are enqueued in the
			// background; unmounting an editor must not wait on them.
			void handleRef.current?.close();
			handleRef.current = undefined;
		};
	}, [ scope, schema, key, field, identity ]);

	const setText = useCallback((text: string): Promise<void> => {
		const handle = handleRef.current;
		if (handle === undefined) {
			return Promise.reject(
				new Error(`useText: cannot write "${field}" of "${key}" — the handle is not open yet`),
			);
		}
		return handle.setString(text);
	}, [ key, field ]);

	// A state carrying a different identity belongs to the previous row/field:
	// report loading until the new handle publishes.
	const pending = useMemo(() => loading(identity), [ identity ]);
	const current = state.identity === identity ? state : pending;
	return useMemo<UseTextResult>(() => ({
		value: current.value,
		marks: current.marks,
		handle: current.handle,
		error: current.error,
		setText,
	}), [ current, setText ]);
}
