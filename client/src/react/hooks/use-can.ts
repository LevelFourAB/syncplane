import {
	create,
	type DescMessage,
	type Message,
	type MessageInitShape,
	type MessageShape,
	toBinary,
} from '@bufbuild/protobuf';
import { useEffect, useMemo, useState } from 'react';

import { accessInputsOf, type AccessOperation, type CanAnswer } from '../../access.js';
import type { CanOptions, WorkspaceClient } from '../../client.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import { rowWatchIdentity, rowWatchOf, watchRowCell } from '../row-store.js';
import type { UseWorkspaceOptions } from '../types.js';

/** Options for {@link useCan}. */
export interface UseCanOptions extends UseWorkspaceOptions, CanOptions {}

/**
 * Predicts whether the entity's access rules admit `operation` on the row of
 * `schema` with `key`, for the resolved workspace. See `SyncClient.can` for
 * what the answer means; it is advisory, so use it to shape the UI, not to
 * authorize.
 *
 * Re-evaluates when an access input in the row changes: whether the row is
 * held locally, the row its access parent names, or the value of a subject
 * field the entity's access rules name. A change of any other field does not
 * re-evaluate. Also re-evaluates when `SyncClient.onAccessChange` reports the
 * workspace: a new summary of the caller's access arrives, a local write that
 * can change the caller's grants is queued, acked or rolled back, or a row
 * that other rows sit below changes, in this tab or another sharing the
 * store. Returns `'unknown'` until the first answer resolves.
 *
 * The access hooks and `useRow` calls without `include` on rows of one entity
 * share one watch of those rows.
 *
 * @example
 * ```tsx
 * function DeleteButton({ id }: { id: string }) {
 *   const can = useCan(CommentSchema, id, 'delete');
 *   return <button disabled={can !== 'yes'}>Delete</button>;
 * }
 * ```
 */
export function useCan<D extends DescMessage>(
	schema: D,
	key: string,
	operation: Exclude<AccessOperation, 'create'>,
	opts?: UseCanOptions,
): CanAnswer {
	const fields = opts?.fields;
	// A fields array written inline is a new array on every render.
	const fieldsKey = fields === undefined ? undefined : JSON.stringify(fields);
	return useAnswer(
		opts?.workspaceId,
		schema,
		{ key, subjects: true },
		(scope) => scope.can(schema, key, operation, fields === undefined ? undefined : { fields }),
		[ schema, key, operation, fieldsKey ],
	);
}

/**
 * Predicts whether the entity's access rules admit creating `row`, for the
 * resolved workspace. See `SyncClient.canCreate` for what the answer means.
 *
 * Re-evaluates when `row` changes by value, and when
 * `SyncClient.onAccessChange` reports the workspace. Returns `'unknown'`
 * until the first answer resolves.
 *
 * @example
 * ```tsx
 * function CommentBox({ taskId }: { taskId: string }) {
 *   const can = useCanCreate(CommentSchema, { id: 'draft', task: { id: taskId } });
 *   return can === 'no' ? null : <textarea />;
 * }
 * ```
 */
export function useCanCreate<D extends DescMessage>(
	schema: D,
	row: MessageInitShape<D>,
	opts?: UseWorkspaceOptions,
): CanAnswer {
	// An object literal is a new object on every render; its encoding is not.
	const encoded = useMemo(() => toBinary(schema, create(schema, row)).join(','), [ schema, row ]);
	return useAnswer(opts?.workspaceId, schema, undefined, (scope) => scope.canCreate(schema, row), [
		schema,
		encoded,
	]);
}

/**
 * Predicts whether the caller holds `permission` at the row of `schema` with
 * `key`, for the resolved workspace. See `SyncClient.hasPermission` for what
 * the answer means. Re-evaluates as {@link useCan} does, except that a
 * subject field is not an input.
 *
 * @example
 * ```tsx
 * function InviteButton({ projectId }: { projectId: string }) {
 *   const can = useHasPermission(ProjectSchema, projectId, 'invite_members');
 *   return <button disabled={can !== 'yes'}>Invite</button>;
 * }
 * ```
 */
export function useHasPermission<D extends DescMessage>(
	schema: D,
	key: string,
	permission: string,
	opts?: UseWorkspaceOptions,
): CanAnswer {
	return useAnswer(
		opts?.workspaceId,
		schema,
		{ key, subjects: false },
		(scope) => scope.hasPermission(schema, key, permission),
		[ schema, key, permission ],
	);
}

/** The row an answer is for, and which of its fields are inputs of the answer. */
interface AnswerRow {
	key: string;
	/** Whether the subject fields of the entity's access rules are inputs. */
	subjects: boolean;
}

/**
 * Holds the newest answer `ask` gives, asked again on every access change in
 * the workspace and, when there is a row, on every change of what
 * accessInputsOf reads from it. `deps` decide when `ask` itself changes.
 */
function useAnswer<D extends DescMessage>(
	workspaceId: string | undefined,
	schema: D,
	row: AnswerRow | undefined,
	ask: (scope: WorkspaceClient) => Promise<CanAnswer>,
	deps: readonly unknown[],
): CanAnswer {
	const client = useSyncClient();
	const scope = useWorkspaceScope(workspaceId);
	const [ answer, setAnswer ] = useState<CanAnswer>('unknown');
	const key = row?.key;
	const subjects = row?.subjects ?? false;
	useEffect(() => {
		// Resolved first: an unregistered schema throws here, before anything
		// below subscribes.
		const def = key === undefined ? undefined : client.entityRegistry.bySchema(schema);
		let live = true;
		// Answers resolve out of order; only the newest one may land.
		let generation = 0;
		const evaluate = () => {
			const mine = ++generation;
			ask(scope).then(
				(a) => {
					if (live && mine === generation) setAnswer(a);
				},
				() => {
					if (live && mine === generation) setAnswer('unknown');
				},
			);
		};
		// The access change and the row's own inputs cover every input of the
		// answer. The sync status is not an input, and it changes on every local
		// write: asking again on it makes each write cost one prediction per
		// mounted hook.
		const offAccess = client.onAccessChange((ws) => {
			if (ws === scope.workspaceId) evaluate();
		});
		let offRow: (() => void) | undefined;
		if (def === undefined || key === undefined) evaluate();
		else {
			// The first ask waits for the row's first delivery and starts from
			// it, rather than running here as well. Its read of the store starts
			// after the watch read the row, and the watch delivers every later
			// change of the row, so no change falls between the two. A row that
			// another call site already holds is delivered at once.
			let seen: string | undefined;
			offRow = watchRowCell<MessageShape<D>>(
				client,
				rowWatchIdentity(scope.workspaceId, schema.typeName),
				rowWatchOf(scope, schema),
				{ workspaceId: scope.workspaceId, key },
				(cell) => {
					// A watch that fails delivers its error once, with the row it
					// last had or none, so the first ask still runs.
					const inputs = accessInputsOf(def, cell.row?.value as Message | undefined, subjects);
					if (inputs === seen) return;
					seen = inputs;
					evaluate();
				},
			);
		}
		return () => {
			live = false;
			offAccess();
			offRow?.();
		};
		// `ask` is a new closure on every render; deps lists what it reads.
	}, [ client, scope, key, subjects, ...deps ]);
	return answer;
}
